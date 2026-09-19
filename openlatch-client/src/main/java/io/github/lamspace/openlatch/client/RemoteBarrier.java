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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link OBarrier} 的远程实现。
 *
 * <p><b>等待编排</b>：{@code BARRIER_AWAIT} 应答 QUEUED 后挂起等待
 * {@code AWAIT_NOTIFY}——通知到达以同一 {@code requestId} 重发（服务端按
 * {@code (会话, 请求)} 世代账簿幂等了结，MUST NOT 重复计次）；应答 OK 即
 * 所属世代合拢放行，BARRIER_BROKEN 即世代破障裁决。执行者形态（应答
 * {@code executor=true}）在本调用栈内执行动作，随后以
 * {@code BARRIER_ACTION_DONE} 终结本等待——该回报按世代幂等，瞬态失败以
 * 同一信封重发至等待界限。与 {@code OCountDownLatch} 的关键差异：到场是
 * 有副作用请求，<b>会话切换 MUST NOT 以新请求重放</b>（旧世代随本会话
 * 消亡已破，重放将污染新世代）——在途传输失败即裁决放弃。
 *
 * <p><b>离场即破障</b>：{@code await(timeout)} 超时、本地中断与动作异常
 * 都经 {@code BARRIER_LEAVE} 通道离场/破障；leave 为尽力投递（写失败不
 * 阻塞本方收场——网络不可达时服务端会话清理兜底破障）。
 *
 * <p><b>无租约</b>：等待者不登记持锁簿记、不启动看门狗——await 全程零
 * {@code LEASE_RENEW} 流量（服务端 awaiter 断连即摘除并连带破障）。
 */
final class RemoteBarrier implements OBarrier {

    /** 断线重连窗口的会话轮询间隔（毫秒）。 */
    private static final long SESSION_POLL_MS = 50;
    /** 应答读界在请求超时之外的余量（毫秒），与 {@code RemoteLock} 口径一致。 */
    private static final long SLACK_MS = 200;

    /** 所属客户端。 */
    private final OpenLatchClient client;
    /** 屏障键。 */
    private final String key;
    /** 定型主张（创建者句柄 {@code > 0}，纯加入句柄 0）。 */
    private final long parties;
    /** barrierAction（可空——空则到场不携带动作）。 */
    private final Runnable barrierAction;
    /** 最近所见裁决：世代以破障收场。 */
    private volatile boolean lastBroken;
    /** 服务端回显的定型许可数（纯加入句柄观测用）。 */
    private volatile long partiesObserved;

    /**
     * 创建远程循环屏障句柄。仅由 {@link OpenLatchClient} 工厂调用。
     *
     * @param client       所属客户端
     * @param key          屏障键
     * @param parties      定型主张（{@code >= 0}，0 为纯加入）
     * @param barrierAction 最后到场者执行的动作，可为 {@code null}
     */
    RemoteBarrier(OpenLatchClient client, String key, long parties, Runnable barrierAction) {
        this.client = client;
        this.key = key;
        this.parties = parties;
        this.barrierAction = barrierAction;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public void await() throws InterruptedException {
        if (!doAwait(client.config().defaultWaitTimeout().toMillis())) {
            throw new OpenLatchTimeoutException(
                    "await of barrier '" + key + "' timed out (generation broken)");
        }
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }
        return doAwait(unit.toMillis(timeout));
    }

    @Override
    public void breakBarrier() {
        OpenLatchClient.LatchRoute route = requireRoute();
        long rid = route.session().nextRequestId();
        Envelope env = OpenLatchClient.barrierLeaveEnvelope(rid, key, 0);
        try {
            Envelope resp = route.mux().sendWithId(env, client.config().requestTimeout().toMillis())
                    .get(client.config().requestTimeout().toMillis() + SLACK_MS,
                            TimeUnit.MILLISECONDS);
            StatusCode status = resp.getBarrierLeaveResponse().getStatus();
            if (status != StatusCode.OK) {
                throw new OpenLatchException(status,
                        "breakBarrier of barrier '" + key + "' failed: " + status);
            }
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof OpenLatchException ole) {
                throw ole;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new OpenLatchException("breakBarrier of barrier '" + key + "' failed", cause);
        } catch (TimeoutException e) {
            throw new OpenLatchTimeoutException(
                    "breakBarrier of barrier '" + key + "' timed out (result unknown; 幂等可重试)");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OpenLatchException("breakBarrier of barrier '" + key + "' interrupted", e);
        }
    }

    @Override
    public boolean isBroken() {
        return lastBroken;
    }

    @Override
    public long getParties() {
        return parties > 0 ? parties : partiesObserved;
    }

    /**
     * 等待公共路径：到场-通知-重发闭环 + 执行者两阶段。
     *
     * @param budgetMs 等待总预算（毫秒）
     * @return 世代合拢放行 {@code true}；预算耗尽（已连带破障）{@code false}
     * @throws InterruptedException  本地中断（已尽力离场，世代破障）
     * @throws OBrokenBarrierException 所属世代已破障
     * @throws OpenLatchException    服务端显式拒绝或在途到场遇会话切换放弃
     */
    private boolean doAwait(long budgetMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + budgetMs;
        LatchNotifyRegistry registry = client.latchNotifies();
        OpenLatchClient.LatchRoute route = client.latchRoute();
        Envelope env = null;
        long envSession = -1;
        long rid = -1;
        try {
            while (true) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    if (rid >= 0 && route != null) {
                        // 超时即破障：离场连带打破所属世代后以超时收场。
                        leaveQuietly(route, rid);
                    }
                    return false;
                }
                if (route == null) {
                    Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
                    route = client.latchRoute();
                    continue;
                }
                if (env == null || envSession != route.session().sessionId()) {
                    envSession = route.session().sessionId();
                    rid = route.session().nextRequestId();
                    env = OpenLatchClient.barrierAwaitEnvelope(rid, key, parties,
                            barrierAction != null);
                }
                CompletableFuture<Void> arrived = registry.register(envSession, rid);
                CompletableFuture<Envelope> response = route.mux().sendWithId(env,
                        Math.min(remaining, client.config().requestTimeout().toMillis()));
                StatusCode status;
                boolean executor;
                long generation;
                try {
                    long waitMs = Math.min(remaining,
                            client.config().requestTimeout().toMillis() + SLACK_MS);
                    Envelope resp = response.get(waitMs, TimeUnit.MILLISECONDS);
                    status = resp.getBarrierAwaitResponse().getStatus();
                    executor = resp.getBarrierAwaitResponse().getExecutor();
                    generation = resp.getBarrierAwaitResponse().getGeneration();
                    partiesObserved = resp.getBarrierAwaitResponse().getParties();
                } catch (ExecutionException e) {
                    Throwable cause = unwrap(e);
                    registry.remove(envSession, rid);
                    OpenLatchClient.LatchRoute now = client.latchRoute();
                    boolean sameSession = now != null && now.session().sessionId() == envSession;
                    boolean expired = cause instanceof OpenLatchException ole
                            && ole.status() == StatusCode.SESSION_EXPIRED;
                    if (!expired && sameSession && isTransient(cause)) {
                        // 同会话超时/瞬态失败：按同 id 重发（世代账簿幂等安全）。
                        route = now;
                        continue;
                    }
                    // 会话切换或断连：到场不跨会话重放（契约）——本方放弃裁决；
                    // 旧世代等待项随该会话的服务端清理连带破障，他方可感知。
                    throw new OpenLatchException("await of barrier '" + key
                            + "' abandoned on session change", cause);
                } catch (TimeoutException e) {
                    // 应答读界兜底（响应 future 自带请求超时，理论先到）。
                    registry.remove(envSession, rid);
                    continue;
                } catch (InterruptedException e) {
                    // 中断即离场：leave 对"无对应到场记录"幂等无操作、对已入账
                    // 的当前世代到场连带破障（JDK 中断破障口径），本方以中断终结。
                    registry.remove(envSession, rid);
                    OpenLatchClient.LatchRoute cur = client.latchRoute();
                    if (cur != null) {
                        leaveQuietly(cur, rid);
                    }
                    throw e;
                }
                if (status == StatusCode.OK) {
                    registry.remove(envSession, rid);
                    lastBroken = false;
                    return true;
                }
                if (status == StatusCode.BARRIER_BROKEN) {
                    registry.remove(envSession, rid);
                    lastBroken = true;
                    throw new OBrokenBarrierException(
                            "barrier '" + key + "' generation " + generation + " was broken");
                }
                if (status == StatusCode.QUEUED) {
                    if (executor) {
                        // 执行者不再等待推送：登记让位于 finally 清理。
                        return runActionAndReport(route, generation, deadline);
                    }
                    // 保持登记在案：AWAIT_NOTIFY 到达即唤醒（唤醒后同 id 重发了结）；
                    // 推送丢失时 requestTimeout 兜底自发重发。MUST NOT 在此前
                    // registry.remove——否则通知永远落空、每代都退化为 5s 兜底。
                    long left = deadline - System.currentTimeMillis();
                    if (left <= 0) {
                        registry.remove(envSession, rid);
                        leaveQuietly(route, rid);
                        return false;
                    }
                    try {
                        arrived.get(Math.min(left,
                                client.config().requestTimeout().toMillis()), TimeUnit.MILLISECONDS);
                    } catch (TimeoutException pushLost) {
                        // 推送丢失兜底：自发幂等重发一次。
                    } catch (InterruptedException ie) {
                        // 中断即离场：尽力摘除本方到场项并连带破障，以中断终结。
                        registry.remove(envSession, rid);
                        leaveQuietly(route, rid);
                        throw ie;
                    } catch (ExecutionException ee) {
                        // arrived 仅被 complete(null)，此分支不可达，防御清场。
                    }
                    continue;
                }
                registry.remove(envSession, rid);
                throw new OpenLatchException(status,
                        "await of barrier '" + key + "' rejected: " + status);
            }
        } finally {
            if (rid >= 0) {
                registry.remove(envSession, rid);
            }
        }
    }

    /**
     * 执行者路径：本调用栈内执行动作→回报 {@code BARRIER_ACTION_DONE}。
     * 动作异常时改发纯破障离场（全体以破障收场，对齐 JDK 动作异常破障），
     * 随后以动作异常终结本等待。
     *
     * @param route      当前路由
     * @param generation 被指定的世代号
     * @param deadline   等待截止（毫秒）
     * @return 世代合拢放行 {@code true}
     * @throws InterruptedException 动作执行或回报等待被中断（尽力离场）
     * @throws OBrokenBarrierException 动作期间世代已被他方打破
     * @throws OpenLatchException    回报不可判定（提交超时等）或动作异常包装
     */
    private boolean runActionAndReport(OpenLatchClient.LatchRoute route, long generation,
            long deadline) throws InterruptedException {
        Throwable actionFailure = null;
        if (barrierAction != null) {
            try {
                barrierAction.run();
            } catch (Throwable t) {
                actionFailure = t;
            }
        }
        if (actionFailure != null) {
            // 动作异常即破障（纯破障主张，无在队身份）。
            leaveQuietly(route, 0);
            if (actionFailure instanceof RuntimeException re) {
                throw re;
            }
            if (actionFailure instanceof Error err) {
                throw err;
            }
            throw new OpenLatchException(
                    "barrierAction of barrier '" + key + "' failed", actionFailure);
        }
        final long envSessionOfDone = route.session().sessionId();
        Envelope env = OpenLatchClient.barrierActionDoneEnvelope(
                route.session().nextRequestId(), key, generation);
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            StatusCode status;
            try {
                Envelope resp = route.mux().sendWithId(env,
                                Math.max(1, Math.min(remaining,
                                        client.config().requestTimeout().toMillis())))
                        .get(Math.max(1, Math.min(remaining,
                                client.config().requestTimeout().toMillis())) + SLACK_MS,
                                TimeUnit.MILLISECONDS);
                status = resp.getBarrierActionDoneResponse().getStatus();
            } catch (ExecutionException e) {
                Throwable cause = unwrap(e);
                OpenLatchClient.LatchRoute now = client.latchRoute();
                boolean sameSession = now != null && now.session().sessionId()
                        == envSessionOfDone;
                if (isTransient(cause)) {
                    if (!sameSession || deadline - System.currentTimeMillis() <= 0) {
                        // 会话已换（执行者挂账随旧会话消亡破障）或界限耗尽：
                        // 结果不确定——回报幂等，重试由调用方以新句柄 await 完成。
                        throw new OpenLatchTimeoutException(
                                "action-done of barrier '" + key + "' timed out (result unknown)");
                    }
                    // 同会话超时：世代了结按 (会话, 世代) 幂等，重发安全。
                    route = now;
                    continue;
                }
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                throw new OpenLatchException("action-done of barrier '" + key + "' failed", cause);
            } catch (TimeoutException e) {
                throw new OpenLatchTimeoutException(
                        "action-done of barrier '" + key + "' timed out (result unknown)");
            }
            if (status == StatusCode.OK) {
                lastBroken = false;
                return true;
            }
            if (status == StatusCode.BARRIER_BROKEN) {
                lastBroken = true;
                throw new OBrokenBarrierException(
                        "barrier '" + key + "' generation " + generation + " broken during action");
            }
            throw new OpenLatchException(status,
                    "action-done of barrier '" + key + "' rejected: " + status);
        }
    }

    /**
     * 尽力离场（fire-and-forget）：写出失败静默——网络不可达时服务端会话
     * 清理兜底破障，本方收场不依赖其送达。
     *
     * @param route     当前路由
     * @param awaitRid  被摘除的到场请求 id（0=纯破障主张）
     */
    private void leaveQuietly(OpenLatchClient.LatchRoute route, long awaitRid) {
        try {
            long rid = route.session().nextRequestId();
            route.mux().sendWithId(OpenLatchClient.barrierLeaveEnvelope(rid, key, awaitRid),
                            client.config().requestTimeout().toMillis())
                    .whenComplete((resp, err) -> {
                        // 送达与否不改变本方收场；失败路径由服务端清扫/会话清理兜底。
                    });
        } catch (RuntimeException ignore) {
            // 路由已失能：无出口可离场，交由服务端侧兜底。
        }
    }

    /**
     * 取可用路由，无活动会话即拒。
     *
     * @return 非空路由
     * @throws ServerUnavailableException 连接未活动
     */
    private OpenLatchClient.LatchRoute requireRoute() {
        OpenLatchClient.LatchRoute route = client.latchRoute();
        if (route == null) {
            throw new ServerUnavailableException("connection is not active");
        }
        return route;
    }

    /**
     * 瞬态失败判定：同会话重发安全的失败（服务端幂等去重兜底）。
     *
     * @param cause 解包后的失败原因
     * @return 瞬态返回 {@code true}
     */
    private static boolean isTransient(Throwable cause) {
        return cause instanceof ServerUnavailableException
                || cause instanceof OpenLatchTimeoutException;
    }

    /**
     * 解包 {@link ExecutionException}/{@link CompletionException}。
     *
     * @param e 包装异常
     * @return 根因
     */
    private static Throwable unwrap(Throwable e) {
        Throwable c = e;
        while (c instanceof ExecutionException || c instanceof CompletionException) {
            Throwable inner = c.getCause();
            if (inner == null) {
                break;
            }
            c = inner;
        }
        return c;
    }
}
