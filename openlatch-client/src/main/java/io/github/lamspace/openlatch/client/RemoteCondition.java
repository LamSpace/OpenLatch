/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.client.internal.HeldLockRegistry;
import io.github.lamspace.openlatch.protocol.ConditionOp;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link OCondition} 的远程实现（v9）：await 折叠进 {@link RemoteLock} 同
 * 一条 ACQUIRE 车道与既有 {@code AwaitTracker} 等待闭环，signal 家族走
 * {@code CONDITION_OP} 直发请求-应答车道（判例 topic SUBSCRIBE 直发）。
 *
 * <p><b>await 线路形态</b>：提交携带 {@code condition} 字段的折叠 ACQUIRE
 * （{@code wait_ms=-1} 恒挂起形态；本地限时/不限时经 {@code waitMs>0}/
 * {@code -1} 折算，线路同判）→ 服务端受理即"一步清零重入 + 清租约 +
 * 入等待集"原子完成并回 {@code QUEUED} → 唤醒通知 {@code AWAIT_NOTIFY}
 * 到达时跟踪器以同 {@code requestId} 换发无条件清 {@code condition} 的
 * 重发信封 → 授予经既有"授予→持锁登记"钩子以重入 1 级入锁。
 *
 * <p><b>超时/中断自救（返回时持锁保真）</b>：{@code await()} 以不限时
 * 预算挂起（JDK 保真：无 signal 则永睡）；限时预算到期或中断时先
 * {@code LEAVE}（fire-and-forget，尽力而为——失败由服务端三路回收兜底）
 * 摘除等待集登记，再转常规阻塞获取重新入锁，后方返回/抛出；重获取自身
 * 失败（连接不可用/重获取总超时）以对应运行时异常上抛。其余异常收束
 * （断连、等待项超限 {@code OVERLOADED}、形状违例等）按降级面原样上抛
 * 且<b>不</b>重新入锁——登记已随三路回收或从未受理
 * （{@link OCondition} 接口级声明）。中断发生在重获取阶段不提前终止
 * 等待（JDK"被 signal 后的重获取不可中断"保真），标志保持并在收束时
 * 恢复。
 *
 * <p><b>本地持有簿记联动</b>：await 提交前摘除本地持锁登记并停看门狗
 * （折叠释放的客户端侧对偶；等待期间 {@code isHeldByCurrentThread()} 为
 * {@code false}，JDK 对齐）；唤醒/自救的重获取经跟踪器授予钩子重新登记。
 *
 * <p><b>signal 权限双层</b>：本地先行（未持有即抛
 * {@link IllegalMonitorStateException}，不发请求）；服务端权威
 * （{@code NOT_HELD} 同型映射）。应答缺失码形（type 回显而载荷异型/
 * 空缺）按"未受理零生效"同信封退避重发（W10 码形纪律）；{@code
 * NOT_LEADER}/{@code OVERLOADED} 执行既有退避改道；响应读界超时属
 * 不确定窗——signal 是事件、无去重槽，MUST NOT 盲目重发（可能二次
 * 搬运多唤醒一人），显式 {@link OpenLatchTimeoutException} 抛出。
 *
 * <p><b>线程模型</b>：await 在调用线程经 future 限时 {@code get} 桥接，
 * signal/signalAll/LEAVE 经直发车道异步出站；句柄无状态（等待集归属按
 * (会话, 线程) 在服务端记录），可被任意持锁线程并发使用。
 */
final class RemoteCondition implements OCondition {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(RemoteCondition.class);
    /** 阻塞桥接上界的额外余量（毫秒），与 {@link RemoteLock} 同值纪律。 */
    private static final long SLACK_MS = 1000L;
    /** 会话未就绪时的轮询间隔（毫秒，判例 topic/queue 直发车道）。 */
    private static final long SESSION_POLL_MS = 50L;

    /** 所属客户端。 */
    private final OpenLatchClient client;
    /** 宿主锁键。 */
    private final String key;
    /** 宿主锁类型（REENTRANT/FAIR/SIMPLE 之一，装配点已守门）。 */
    private final LockType lockType;
    /** 条件名（命名寻址，(key, name) 绑定服务端等待集）。 */
    private final String name;

    /**
     * 创建远程条件句柄。仅由 {@link RemoteLock#newCondition(String)} 调用。
     *
     * @param client   所属客户端
     * @param key      宿主锁键
     * @param lockType 宿主锁类型
     * @param name     条件名（非空；装配点已校验）
     */
    RemoteCondition(OpenLatchClient client, String key, LockType lockType, String name) {
        this.client = client;
        this.key = key;
        this.lockType = lockType;
        this.name = name;
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现：以不限时预算提交折叠等待（跟踪器不挂总超时任务，JDK 保真
     * "无 signal 则永睡"），阻塞桥接为无界 {@code get}；唤醒被授予后经
     * 既有钩子入锁返回。仅异常收束（断连/超限等）与中断离开——前者以
     * 运行时异常抛出且不持锁（{@link OCondition} 降级面：登记已随三路
     * 回收或从未受理），后者走 {@code LEAVE}+重获取自救后抛出。
     */
    @Override
    public void await() throws InterruptedException {
        doAwait(-1L);
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现：本地计时经折叠 ACQUIRE 的 {@code waitMs} 传入跟踪器总预算
     * （线路恒 {@code wait_ms=-1}）；{@code timeout <= 0} 折算为 1 毫秒
     * 预算后走与超时完全一致的收束链（折叠受理=释放生效后立即
     * {@code LEAVE}+重获取，重入 1 级算术保持）。
     */
    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(unit, "unit must not be null");
        long timeoutMs = unit.toMillis(timeout);
        return doAwait(timeoutMs <= 0 ? 1 : timeoutMs);
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现：本地持有检查先行，随后一条 {@code SIGNAL} 直发交换
     * （{@code thread_id}=当前线程 id、{@code await_request_id}=0）。
     */
    @Override
    public void signal() {
        exchange(ConditionOp.CONDITION_OP_SIGNAL);
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现：本地持有检查先行，随后一条 {@code SIGNAL_ALL} 直发交换
     * （形状与 {@link #signal()} 逐项相同，仅 op 取值不同）。
     */
    @Override
    public void signalAll() {
        exchange(ConditionOp.CONDITION_OP_SIGNAL_ALL);
    }

    /**
     * await 公共路径：中断/持有检查 → 摘除本地持有簿记 → 折叠提交 →
     * 闭环等待 → 按收束方式自救、返回或抛降级面异常。
     *
     * <p>收束分派（与 {@link OCondition} 接口契约逐项对应）：future 正常
     * 完成=唤醒重发被授予（已持锁）→ {@code true}；
     * {@link LockAcquisitionTimeoutException}=本地总时限到期（含换主重挂
     * 预算耗竭的促醒形态）→ {@code LEAVE}+常规重获取自救 →
     * {@code false}（await() 忽略返回值，按虚假唤醒收束）；
     * 中断 → {@code LEAVE}+自救后抛 {@link InterruptedException}；
     * 其余异常收束（断连/超限/形状违例等）原样上抛且不重新入锁——
     * 登记已随三路回收或从未受理，降级面显式声明。
     *
     * @param timeoutMs 本地等待预算（毫秒）；{@code < 0}=不限时
     *                  （{@link #await()} 形态，跟踪器不挂总超时任务）
     * @return {@code true} 以唤醒重发被授予收束；{@code false} 以超时
     *         收束并经自救重新入锁
     * @throws InterruptedException 等待期间被中断（抛出前已重新持锁）
     */
    private boolean doAwait(long timeoutMs) throws InterruptedException {
        long threadId = Thread.currentThread().threadId();
        // JDK 同款判定顺序：中断检查先于持有检查。中断发生于提交前，
        // 本地持有原样保留（尚未释放折叠），抛出即持锁。
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException(
                    "await on condition '" + name + "' interrupted before submit");
        }
        requireHeldByCurrentThread(threadId);
        // 折叠释放的客户端侧对偶：全量摘除本地持有登记、停看门狗；
        // 服务端受理点原子完成"清零重入+清租约+入等待集"。
        HeldLockRegistry.HeldEntry held = client.heldLockRegistry().remove(key, threadId);
        if (held != null) {
            client.watchdog().stop(held);
        }
        boolean unbounded = timeoutMs < 0;
        AcquireSpec spec = new AcquireSpec(key, lockType, threadId, 0, unbounded ? -1 : timeoutMs)
                .withCondition(name);
        OpenLatchClient.AcquireSubmission submission = unbounded
                ? client.submitAcquire(spec, -1L)
                : client.submitAcquire(spec);
        boolean signalled;
        try {
            if (unbounded) {
                submission.future().get();
            } else {
                submission.future().get(timeoutMs
                        + client.config().requestTimeout().toMillis() + SLACK_MS,
                        TimeUnit.MILLISECONDS);
            }
            // 折叠受理恒 QUEUED，首个 OK 必来自唤醒换形重发——授予已经
            // "授予→持锁登记"钩子入锁（重入 1 级）。
            signalled = true;
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof LockAcquisitionTimeoutException) {
                signalled = false; // 总时限到期：LEAVE → 常规阻塞获取入锁自救
            } else if (cause instanceof RuntimeException re) {
                throw re; // 降级面：不持锁上抛（登记随三路回收兜底）
            } else {
                throw new OpenLatchException(
                        "await on condition '" + name + "' failed", cause);
            }
        } catch (TimeoutException e) {
            // 防御分支：桥接上界已含全部余量，跟踪器总超时必先触发。
            signalled = false;
        } catch (InterruptedException e) {
            leaveBestEffort(submission.awaitRequestId());
            reacquireToHold(threadId);
            throw e;
        }
        if (!signalled) {
            leaveBestEffort(submission.awaitRequestId());
            reacquireToHold(threadId);
        }
        return signalled;
    }

    /**
     * signal 家族直发交换：预算内退避重发瞬态/非权威拒绝，终态码分派
     * （OK 返回、{@code NOT_HELD} 映射 {@link IllegalMonitorStateException}、
     * 其余错误码显式抛出）。不确定窗（响应读界超时）不盲目重发。
     *
     * @param op SIGNAL 或 SIGNAL_ALL
     */
    private void exchange(ConditionOp op) {
        long threadId = Thread.currentThread().threadId();
        requireHeldByCurrentThread(threadId);
        long budgetMs = client.config().requestTimeout().toMillis();
        long deadline = System.currentTimeMillis() + budgetMs;
        Envelope env = null;
        long envSid = -1L;
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new OpenLatchTimeoutException("condition " + op + " on '" + key
                        + "' not confirmed within request timeout; the event may or may not"
                        + " have been moved (signal is an event — no blind retry)");
            }
            OpenLatchClient.LatchRoute route = client.latchRoute();
            if (route == null) {
                try {
                    Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new OpenLatchException("condition " + op + " on '" + key
                            + "' interrupted while awaiting session", ie);
                }
                continue;
            }
            if (env == null || envSid != route.session().sessionId()) {
                long rid = route.session().nextRequestId();
                env = OpenLatchClient.conditionEnvelope(rid, op, key, name, threadId, 0L);
                envSid = route.session().sessionId();
            }
            StatusCode status;
            try {
                Envelope answer = route.mux().sendWithId(env,
                        Math.min(remaining, budgetMs + SLACK_MS))
                        .get(Math.min(remaining, budgetMs + SLACK_MS), TimeUnit.MILLISECONDS);
                if (!answer.hasConditionOpResponse()) {
                    // 缺码形应答（W10 纪律）：MUST NOT 读 protobuf 默认实例
                    // （status 零值 OK）伪成功；按未受理零生效同信封退避重发。
                    continue;
                }
                status = answer.getConditionOpResponse().getStatus();
            } catch (ExecutionException e) {
                Throwable cause = unwrap(e);
                if (cause instanceof ServerUnavailableException) {
                    continue; // 会话未就绪 = 发送前失败零生效，预算内退避续行
                }
                if (cause instanceof OpenLatchTimeoutException ot) {
                    throw ot; // 不确定窗：不盲目重发（signal 是事件、无去重槽）
                }
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                throw new OpenLatchException("condition " + op + " on '" + key + "' failed",
                        cause);
            } catch (TimeoutException e) {
                throw new OpenLatchTimeoutException("condition " + op + " on '" + key
                        + "' response read bound exceeded; the event may have been moved"
                        + " (signal is an event — no blind retry)");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new OpenLatchException("condition " + op + " on '" + key
                        + "' interrupted", e);
            }
            if (status == StatusCode.OK) {
                return;
            }
            if (status == StatusCode.NOT_HELD) {
                // 服务端权威层：与本地 unlock()/signal() 误用同型异常。
                throw new IllegalMonitorStateException("current thread does not hold lock '"
                        + key + "' on server side (condition '" + name + "', " + op
                        + " rejected NOT_HELD)");
            }
            if (status == StatusCode.NOT_LEADER || status == StatusCode.OVERLOADED) {
                continue; // 既有退避改道（无 leader 提示字段，判例 QUEUE/TOPIC 直发车道）
            }
            throw new OpenLatchException(status, "condition " + op + " on '" + key
                    + "' rejected: " + status);
        }
    }

    /**
     * LEAVE 尽力而为：fire-and-forget 摘除等待集登记（按 (会话,
     * await_request_id) 幂等），失败仅记日志——三路回收兜底。
     *
     * @param awaitRequestId 原折叠 ACQUIRE 的请求 id；{@code <= 0} 表示
     *                       未提交（无登记），直接跳过
     */
    private void leaveBestEffort(long awaitRequestId) {
        if (awaitRequestId <= 0) {
            return;
        }
        OpenLatchClient.LatchRoute route = client.latchRoute();
        if (route == null) {
            return; // 连接不可用：登记随会话死亡/换主回收兜底
        }
        Envelope env = OpenLatchClient.conditionEnvelope(route.session().nextRequestId(),
                ConditionOp.CONDITION_OP_LEAVE, key, name, 0L, awaitRequestId);
        route.mux().sendWithId(env, client.config().requestTimeout().toMillis())
                .whenComplete((resp, err) -> {
                    if (err != null) {
                        log.debug("condition LEAVE on '{}' (await rid={}) best-effort failed: {}",
                                key, awaitRequestId, err.toString());
                    }
                });
    }

    /**
     * 自救重获取：常规阻塞获取（排队式 {@code waitMs=-1}，受等待总超时
     * 兜底）重新入锁；授予经既有钩子登记为 1 级重入。本方法对齐 JDK
     * "被唤醒/超时后的重获取不可中断"保真——等待中遭遇的中断仅置标志、
     * 续等至授予，收束时恢复。
     *
     * @param threadId 归属线程标识
     * @throws InterruptedException 不可能抛出（中断仅恢复标志）
     */
    private void reacquireToHold(long threadId) throws InterruptedException {
        AcquireSpec spec = new AcquireSpec(key, lockType, threadId, 0, -1);
        CompletableFuture<LockGrant> future = client.acquireAsync(spec);
        long budgetMs = client.config().defaultWaitTimeout().toMillis()
                + client.config().requestTimeout().toMillis() + SLACK_MS;
        long deadline = System.currentTimeMillis() + budgetMs;
        boolean interrupted = false;
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                throw new LockAcquisitionTimeoutException(
                        "await on condition '" + name + "' timed out re-acquiring lock '" + key
                                + "' — returned/aborted without holding the lock; the server"
                                + " side grant is reclaimed by lease expiry");
            }
            try {
                future.get(remaining, TimeUnit.MILLISECONDS);
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                return;
            } catch (InterruptedException ie) {
                interrupted = true; // 重获取不可中断保真：标志保持、续等授予
            } catch (TimeoutException e) {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                throw new LockAcquisitionTimeoutException(
                        "await on condition '" + name + "' timed out re-acquiring lock '" + key
                                + "' — returned/aborted without holding the lock; the server"
                                + " side grant is reclaimed by lease expiry");
            } catch (ExecutionException e) {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                Throwable cause = unwrap(e);
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                throw new OpenLatchException("await reacquire of '" + key + "' failed", cause);
            }
        }
    }

    /**
     * 本地持有检查（signal 双层权限的第一层、await 的提交门）：未持有
     * 即抛，不产生任何请求。
     *
     * @param threadId 当前线程标识
     */
    private void requireHeldByCurrentThread(long threadId) {
        if (client.heldLockRegistry().get(key, threadId) == null) {
            throw new IllegalMonitorStateException(
                    "current thread does not hold lock '" + key + "' (condition '" + name + "')");
        }
    }

    /**
     * 解包 {@link ExecutionException} 至真实原因。
     *
     * @param t 待解包异常
     * @return 真实原因
     */
    private static Throwable unwrap(Throwable t) {
        Throwable cause = t instanceof ExecutionException && t.getCause() != null
                ? t.getCause() : t;
        return cause instanceof ExecutionException ee && ee.getCause() != null
                ? ee.getCause() : cause;
    }
}
