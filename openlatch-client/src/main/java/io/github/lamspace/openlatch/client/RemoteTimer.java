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

import io.github.lamspace.openlatch.client.internal.LatchNotifyRegistry;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TimerOp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link OTimer} 的集群实现（v11）：直发请求-应答车道 + 等待-通知-重发闭环。
 *
 * <p><b>车道</b>：全部操作经 {@code latchRoute()}（屏障/队列/条件/相位同族的
 * Leader 路由车道，判例 v5–v10——W11 换主窗暴露面与注记口径随行）。装载/撤销
 * 由服务端提交复制日志；等待/撤销等待/读数为 Leader 本地直答（零日志；
 * 到期亦零日志——谓词派生）。
 *
 * <p><b>幂等与重发</b>：装载去重槽（每会话单槽）承载同 (会话,请求) 重发的
 * 回声回放——应答丢失的自动重发不双换代；等待登记幂等（同身份重发即刻
 * 续挂了结）。会话切换语义按操作族分立：变异（SCHEDULE/DISARM）在途不跨
 * 会话自动重放、显式放弃（结果以 {@code isArmed()} 读数核对，判例
 * {@code RemotePhaser.submitOneShot} 口径）；纯等待/读数跨会话重建信封
 * 续发（无复制态副作用，安全）。
 *
 * <p><b>等待重挂（自愈通道）</b>：挂起以分片轮询承载保活语义——每
 * {@value #REHANG_POLL_MS} 毫秒未收唤醒即向当值 Leader 以同 (会话,请求)
 * 重发等待请求：服务端幂等续挂或谓词即刻了结（OK 共见 / DENIED 代终结）。
 * 换主后路由改道即等价重挂——唤醒谓词（当代 armed 且已越过 fireAt）在复制
 * 账簿，重挂无损耗（对照 {@code OCondition} signal 灭失窗的增强面，与
 * {@code RemotePhaser} 同侧证据）。**分片重发恒以原信封原 request_id**——
 * 换 rid 会把装载面打成假象双换代（v10 过程真缺陷教训的 v11 复现钉死）。
 *
 * <p><b>两不可得形态不混读</b>：{@code await(timeout)} 的 {@code false} 仅
 * 承载超时；当代撤销唤醒后的重发收 {@code DENIED} → 本实现映射为
 * {@link OpenLatchException}（"等待不可再满足"异常终态，接口契约成文）。
 */
final class RemoteTimer implements OTimer {

    /** 无活动路由时的会话轮询间隔（毫秒，判例 RemotePhaser）。 */
    private static final long SESSION_POLL_MS = 50;
    /** 应答读界相对请求超时的松弛（毫秒）。 */
    private static final long SLACK_MS = 200;
    /** 挂起分片轮询周期（毫秒）——超时未醒即以同身份重发续挂（保活语义）。 */
    private static final long REHANG_POLL_MS = 5_000;

    /** 所属客户端。 */
    private final OpenLatchClient client;
    /** 延时触发键。 */
    private final String key;

    /**
     * 构造句柄（零网络——timer 无"构造即预装"语义，装载恒显式）。
     *
     * @param client 客户端
     * @param key    延时触发键
     */
    RemoteTimer(OpenLatchClient client, String key) {
        this.client = client;
        this.key = key;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public long schedule(long delay, TimeUnit unit) {
        if (delay < 0) {
            throw new IllegalArgumentException("delay must be >= 0: " + delay);
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        OpenLatchClient.LatchRoute route = requireRoute();
        long rid = route.session().nextRequestId();
        Envelope resp = sendOnce(OpenLatchClient.timerEnvelope(rid,
                TimerOp.TIMER_OP_SCHEDULE, key, unit.toMillis(delay)), "schedule", false);
        return resp.getTimerOpResponse().getGeneration();
    }

    @Override
    public void disarm() {
        OpenLatchClient.LatchRoute route = requireRoute();
        long rid = route.session().nextRequestId();
        sendOnce(OpenLatchClient.timerEnvelope(rid, TimerOp.TIMER_OP_DISARM, key, null),
                "disarm", false);
    }

    @Override
    public void await() throws InterruptedException {
        try {
            boolean seen = awaitInterruptibly(client.config().defaultWaitTimeout().toMillis(),
                    TimeUnit.MILLISECONDS);
            if (!seen) {
                throw new OpenLatchTimeoutException("await of timer '" + key
                        + "' timed out (bounded wait; loop to retry)");
            }
        } catch (TimeoutException e) {
            throw new OpenLatchTimeoutException("await of timer '" + key
                    + "' timed out (bounded wait; loop to retry)");
        }
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        if (timeout <= 0) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        try {
            return awaitInterruptibly(timeout, unit);
        } catch (TimeoutException e) {
            return false; // 超时形态唯一由 false 承载（契约）
        }
    }

    /**
     * 限时等待核心：谓词三态——即刻共见（OK）/ 代终结（DENIED→异常）/
     * 续挂（QUEUED→通知或分片重发，恒原 rid）。
     *
     * @param timeout 等待预算（{@code > 0}）
     * @param unit    时间单位
     * @return {@code true} 预算内见到当代标记
     * @throws InterruptedException 被中断（先尽力撤销挂起）
     * @throws TimeoutException     预算耗尽
     */
    private boolean awaitInterruptibly(long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        LatchNotifyRegistry registry = client.latchNotifies();
        OpenLatchClient.LatchRoute route = client.latchRoute();
        Envelope env = null;
        long envSession = -1;
        long rid = -1;
        try {
            while (true) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    cancelQuietly(envSession, rid);
                    throw new TimeoutException("await of timer '" + key + "' timed out");
                }
                if (route == null) {
                    Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
                    route = client.latchRoute();
                    continue;
                }
                if (env == null || envSession != route.session().sessionId()) {
                    // 纯等待无复制态副作用：跨会话重建信封安全（自愈通道）。
                    envSession = route.session().sessionId();
                    rid = route.session().nextRequestId();
                    env = OpenLatchClient.timerEnvelope(rid, TimerOp.TIMER_OP_AWAIT,
                            key, null);
                }
                CompletableFuture<Void> arrived = registry.register(envSession, rid);
                Envelope resp;
                try {
                    resp = route.mux().sendWithId(env,
                                    Math.min(remaining, client.config().requestTimeout().toMillis()))
                            .get(Math.min(remaining,
                                    client.config().requestTimeout().toMillis() + SLACK_MS),
                                    TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    Throwable cause = unwrap(e);
                    registry.remove(envSession, rid);
                    OpenLatchClient.LatchRoute now = client.latchRoute();
                    if (now != null && now.session().sessionId() == envSession
                            && isTransient(cause)) {
                        route = now;
                        continue;
                    }
                    route = null;
                    env = null;
                    continue; // 换主/瞬态：新路由重发（纯等待安全重挂）
                } catch (TimeoutException e) {
                    continue; // 读界兜底：保持挂起等下一次唤醒/分片重发
                }
                if (!resp.hasTimerOpResponse()) {
                    continue; // 异型码形：同 id 重发（W10 判例）
                }
                StatusCode status = resp.getTimerOpResponse().getStatus();
                if (status == StatusCode.OK) {
                    registry.remove(envSession, rid);
                    return true;
                }
                if (status == StatusCode.DENIED) {
                    // 代终结即刻/唤醒后重评均落此臂：等待不可再满足的异常终态。
                    registry.remove(envSession, rid);
                    throw new OpenLatchException(StatusCode.DENIED,
                            "await of timer '" + key + "' unsatisfiable: generation disarmed");
                }
                if (status == StatusCode.QUEUED) {
                    try {
                        arrived.get(Math.min(deadline - System.currentTimeMillis(),
                                REHANG_POLL_MS), TimeUnit.MILLISECONDS);
                        // 唤醒到达：同 id 重发取终态（谓词当值重评 OK/DENIED）。
                        continue;
                    } catch (TimeoutException e) {
                        // 分片到期（含换主窗通知灭失形态）：以**原信封原 rid**
                        // 重发——登记幂等续挂或谓词即刻了结（换 rid 会把装载面
                        // 打成假象双换代，v10 真缺陷教训的 v11 护栏）。
                        continue;
                    } catch (ExecutionException e) {
                        registry.remove(envSession, rid);
                        throw new OpenLatchException("await of timer '" + key + "' failed",
                                unwrap(e));
                    } catch (InterruptedException e) {
                        registry.remove(envSession, rid);
                        cancelQuietly(envSession, rid);
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
                registry.remove(envSession, rid);
                throw new OpenLatchException(status,
                        "await of timer '" + key + "' rejected: " + status);
            }
        } finally {
            if (rid >= 0) {
                registry.remove(envSession, rid);
            }
        }
    }

    @Override
    public boolean isFired() {
        return query().getTimerOpResponse().getMarked();
    }

    @Override
    public boolean isArmed() {
        return query().getTimerOpResponse().getArmed();
    }

    @Override
    public long getRemainingMillis() {
        Envelope resp = query();
        long fireAt = resp.getTimerOpResponse().getFireAtMs();
        long remaining = fireAt - System.currentTimeMillis();
        return Math.max(0L, remaining);
    }

    /**
     * 一次 QUERY 读数（Leader 本地零日志；advisory 语义见 {@link OTimer}）。
     *
     * @return 应答信封（timer_op_response 已裁决 OK）
     */
    private Envelope query() {
        return sendOnce(OpenLatchClient.timerEnvelope(requestId(),
                TimerOp.TIMER_OP_QUERY, key, null), "query", true);
    }

    /**
     * 请求-应答单发（含同会话瞬态重试；跨会话或明确拒绝即时抛出）。
     * {@code DENIED} 在本通道为撤销等待终态——由 await 核心消化，不进
     * 本方法的拒绝抛出路径（仅 await 携带该状态）。
     *
     * @param env         信封（同会话重试复用同 request_id 幂等）
     * @param verb        日志/异常动词条
     * @param rerouteSafe 跨会话可重建重发（仅纯读数传 true——本方法内不
     *                    重建，false 表示变异族跨会话放弃）
     * @return 应答信封（status=OK/DENIED 之外的拒绝已抛出）
     */
    private Envelope sendOnce(Envelope env, String verb, boolean rerouteSafe) {
        OpenLatchClient.LatchRoute route = requireRoute();
        for (int attempt = 0; attempt < 3; attempt++) {
            long session = route.session().sessionId();
            try {
                Envelope resp = route.mux().sendWithId(env,
                                client.config().requestTimeout().toMillis())
                        .get(client.config().requestTimeout().toMillis() + SLACK_MS,
                                TimeUnit.MILLISECONDS);
                if (!resp.hasTimerOpResponse()) {
                    // 异型/空码形：MUST NOT 读 protobuf 默认实例伪成功（W10 判例）。
                    continue;
                }
                StatusCode status = resp.getTimerOpResponse().getStatus();
                if (status == StatusCode.OK || status == StatusCode.DENIED) {
                    return resp;
                }
                throw new OpenLatchException(status,
                        verb + " on timer '" + key + "' rejected: " + status);
            } catch (ExecutionException e) {
                Throwable cause = unwrap(e);
                OpenLatchClient.LatchRoute now = client.latchRoute();
                boolean sameSession = now != null && now.session().sessionId() == session;
                if (sameSession && isTransient(cause)) {
                    route = now;
                    continue;
                }
                if (cause instanceof OpenLatchException ole) {
                    throw ole;
                }
                throw new OpenLatchException(verb + " on timer '" + key + "' failed", cause);
            } catch (TimeoutException e) {
                OpenLatchClient.LatchRoute now = client.latchRoute();
                if (now != null && now.session().sessionId() == session) {
                    continue; // 同会话超时：同 id 幂等重发
                }
                if (rerouteSafe && now != null) {
                    throw new OpenLatchException(StatusCode.NOT_LEADER,
                            verb + " on timer '" + key + "': leader changed, retry with fresh request");
                }
                throw new OpenLatchTimeoutException(verb + " on timer '" + key
                        + "' timed out (result unknown)");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new OpenLatchException(verb + " on timer '" + key + "' interrupted", e);
            }
        }
        throw new OpenLatchTimeoutException(verb + " on timer '" + key
                + "' timed out after retries (result unknown)");
    }

    /**
     * 尽力撤销挂起（fire-and-forget：失败不致命，三路回收兜底）。
     *
     * @param session 挂起登记所在会话（-1 跳过）
     * @param rid     挂起请求 id
     */
    private void cancelQuietly(long session, long rid) {
        if (session < 0 || rid < 0) {
            return;
        }
        try {
            OpenLatchClient.LatchRoute route = client.latchRoute();
            if (route == null) {
                return;
            }
            long rid2 = route.session().nextRequestId();
            route.mux().sendWithId(OpenLatchClient.timerCancelEnvelope(rid2, key, rid),
                    client.config().requestTimeout().toMillis());
        } catch (RuntimeException ignored) {
            // 尽力而为：ghost 由簿记换主清零与三路回收收敛（契约声明面）
        }
    }

    /**
     * 当前路由（无则抛——与既有句柄口径一致）。
     *
     * @return 活动路由
     */
    private OpenLatchClient.LatchRoute requireRoute() {
        OpenLatchClient.LatchRoute route = client.latchRoute();
        if (route == null) {
            throw new OpenLatchException("no active session for timer operation on '" + key + "'");
        }
        return route;
    }

    /**
     * 新请求 id（当前会话）。
     *
     * @return 请求 id
     */
    private long requestId() {
        return requireRoute().session().nextRequestId();
    }

    /**
     * 瞬态失败判定（同会话重发安全，判例 {@code RemotePhaser}）。
     *
     * @param cause 解包后原因
     * @return 瞬态为 {@code true}
     */
    private static boolean isTransient(Throwable cause) {
        return cause instanceof ServerUnavailableException
                || cause instanceof OpenLatchTimeoutException;
    }

    /**
     * 解包执行异常（判例 {@code RemotePhaser}）。
     *
     * @param e 包装异常
     * @return 根因
     */
    private static Throwable unwrap(Throwable e) {
        Throwable c = e;
        while (c instanceof ExecutionException || c instanceof java.util.concurrent.CompletionException) {
            Throwable inner = c.getCause();
            if (inner == null) {
                break;
            }
            c = inner;
        }
        return c;
    }
}
