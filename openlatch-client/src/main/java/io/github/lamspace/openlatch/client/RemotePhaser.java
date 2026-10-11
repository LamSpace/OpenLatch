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
import io.github.lamspace.openlatch.protocol.PhaserOp;
import io.github.lamspace.openlatch.protocol.StatusCode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * {@link OPhaser} 的集群实现（v10）：直发请求-应答车道 + 等待-通知-重发闭环。
 *
 * <p><b>车道</b>：全部操作经 {@code latchRoute()}（屏障/队列/条件同族的
 * Leader 路由车道，判例 v5/v7/v8/v9——W11 换主窗暴露面与注记口径随行）。
 * 变异操作（REGISTER/ARRIVE 族）由服务端提交复制日志；等待/撤销/读数为
 * Leader 本地直答（零日志）。
 *
 * <p><b>幂等与重发</b>：单次请求在其会话内以同一 {@code request_id} 重试
 * 瞬态失败（服务端到场去重槽/注册幂等槽/等待登记幂等承载，判例
 * {@code RemoteBarrier.doAwait} 的同 id 重发纪律）。会话切换语义按操作族
 * 分立——变异操作：在途到场不跨会话自动重放，显式放弃（配额/到场已随
 * 提交生效与否不确定，应用层裁决）；纯等待/读数：新会话重建信封续发
 * （无复制态副作用，安全）。
 *
 * <p><b>等待重挂（自愈通道）</b>：挂起以分片轮询承载保活语义——每
 * {@value #REHANG_POLL_MS} 毫秒未收唤醒即向当值 Leader 以同 (会话,请求)
 * 重发等待请求：服务端幂等续挂或谓词即刻了结。换主后路由改道即等价
 * 重挂（唤醒谓词"当前相位 &gt; 已见相位"在复制账簿，无损耗——对照
 * {@code OCondition} 换主窗 signal 灭失窗的增强面，见接口契约注）。
 *
 * <p><b>initialParties 重执</b>：构造携 {@code n > 0} 时，每代会话首个
 * 业务操作前同步提交 {@code REGISTER(n)}（归属本会话）。重连（会话重建）
 * 后重执与死亡隐式摘除咬合自洽：旧会话已摘除→重执恰恢复注册意图；旧会话
 * 未及摘除→registered 短暂超调（方向保守：更难合拢、不会误推进，随旧会话
 * 摘除收敛回正），差异声明见 {@link OPhaser} 契约注。
 */
final class RemotePhaser implements OPhaser {

    /** 无活动路由时的会话轮询间隔（毫秒，判例 RemoteBarrier）。 */
    private static final long SESSION_POLL_MS = 50;
    /** 应答读界相对请求超时的松弛（毫秒）。 */
    private static final long SLACK_MS = 200;
    /** 挂起分片轮询周期（毫秒）——超时未醒即以同身份重发续挂（保活语义）。 */
    private static final long REHANG_POLL_MS = 5_000;

    /** 所属客户端。 */
    private final OpenLatchClient client;
    /** 相位器键。 */
    private final String key;
    /** 初始注册数（0=无，差异声明见类注）。 */
    private final int initialParties;
    /** 已完成初始注册的会话 id 水位（-1=尚无）。 */
    private long registeredSession = -1L;
    /**
     * 构造句柄（零网络——{@code initialParties} 注册延迟至首操作）。
     *
     * @param client         客户端
     * @param key            相位器键
     * @param initialParties 初始注册数（0 表示不预注册）
     */
    RemotePhaser(OpenLatchClient client, String key, int initialParties) {
        this.client = client;
        this.key = key;
        this.initialParties = initialParties;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public long register() {
        return submitOneShot(PhaserOp.PHASER_OP_REGISTER, 1);
    }

    @Override
    public void bulkRegister(int parties) {
        if (parties < 1) {
            throw new IllegalArgumentException("parties must be >= 1: " + parties);
        }
        submitOneShot(PhaserOp.PHASER_OP_REGISTER, parties);
    }

    @Override
    public long arrive() {
        return submitOneShot(PhaserOp.PHASER_OP_ARRIVE, 0);
    }

    @Override
    public long arriveAndDeregister() {
        return submitOneShot(PhaserOp.PHASER_OP_ARRIVE_AND_DEREGISTER, 0);
    }

    @Override
    public long arriveAndAwaitAdvance() throws InterruptedException {
        ensureInitialRegistration();
        long budgetMs = client.config().defaultWaitTimeout().toMillis();
        long deadline = System.currentTimeMillis() + budgetMs;
        LatchNotifyRegistry registry = client.latchNotifies();
        OpenLatchClient.LatchRoute route = client.latchRoute();
        Envelope env = null;
        long envSession = -1;
        long rid = -1;
        Long startSession = null;
        try {
            while (true) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    cancelQuietly(envSession, rid);
                    throw new OpenLatchTimeoutException("arriveAndAwaitAdvance of phaser '"
                            + key + "' timed out (phase not reached)");
                }
                if (route == null) {
                    Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
                    route = client.latchRoute();
                    continue;
                }
                boolean freshSend = false;
                if (env == null || envSession != route.session().sessionId()) {
                    if (startSession != null && startSession != route.session().sessionId()) {
                        // 到场变异半程可能已提交（应答丢失后跨会话）：不跨会话
                        // 重放（JDK 到场计次语义与配额归属域约束），显式放弃。
                        throw new OpenLatchException(StatusCode.SESSION_EXPIRED,
                                "arrive-and-await of phaser '" + key
                                        + "' abandoned on session change; verify with getArrivedParties()");
                    }
                    envSession = route.session().sessionId();
                    if (startSession == null) {
                        startSession = envSession;
                    }
                    rid = route.session().nextRequestId();
                    env = OpenLatchClient.phaserEnvelope(rid,
                            PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, key, 0, null, 0);
                    freshSend = true;
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
                    boolean sameSession = now != null && now.session().sessionId() == envSession;
                    if (sameSession && isTransient(cause)) {
                        route = now;
                        continue;
                    }
                    if (isExplicitReject(cause)) {
                        throw new OpenLatchException(statusOf(cause),
                                "arriveAndAwaitAdvance of phaser '" + key + "' rejected: "
                                        + statusOf(cause));
                    }
                    throw new OpenLatchException("arriveAndAwaitAdvance of phaser '" + key
                            + "' abandoned on session change", cause);
                } catch (TimeoutException e) {
                    // 读界兜底：保持挂起等下一次唤醒/分片重发。
                    continue;
                }
                if (!resp.hasPhaserOpResponse()) {
                    continue; // 异型码形：同 id 重发（W10 判例）
                }
                StatusCode status = resp.getPhaserOpResponse().getStatus();
                long phase = resp.getPhaserOpResponse().getPhase();
                if (status == StatusCode.OK) {
                    registry.remove(envSession, rid);
                    return phase;
                }
                if (status == StatusCode.QUEUED) {
                    try {
                        arrived.get(Math.min(deadline - System.currentTimeMillis(),
                                REHANG_POLL_MS), TimeUnit.MILLISECONDS);
                        // 唤醒到达：同 id 重发取数了结（服务端谓词恒已成立）。
                        continue;
                    } catch (TimeoutException e) {
                        // 分片到期（含换主窗通知灭失形态）：以**原信封原 rid**
                        // 重发——槽幂等/谓词判定即刻续挂或了结（换 rid 会把
                        // 到场半程重复计数，判例屏障同 id 重发纪律）。
                        continue;
                    } catch (ExecutionException e) {
                        registry.remove(envSession, rid);
                        throw new OpenLatchException("await of phaser '" + key + "' failed",
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
                        "arriveAndAwaitAdvance of phaser '" + key + "' rejected: " + status);
            }
        } finally {
            if (rid >= 0) {
                registry.remove(envSession, rid);
            }
        }
    }

    @Override
    public long awaitAdvance(long phase) throws InterruptedException {
        try {
            return awaitAdvanceInterruptibly(phase,
                    client.config().defaultWaitTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            throw new OpenLatchTimeoutException(
                    "awaitAdvance of phaser '" + key + "' timed out (bounded wait; loop to retry)");
        }
    }

    @Override
    public long awaitAdvanceInterruptibly(long phase, long timeout, TimeUnit unit)
            throws InterruptedException, TimeoutException {
        if (timeout <= 0) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        if (phase < 0) {
            throw new IllegalArgumentException("phase must be >= 0: " + phase);
        }
        ensureInitialRegistration();
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
                    throw new TimeoutException(
                            "awaitAdvance of phaser '" + key + "' timed out at phase " + phase);
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
                    env = OpenLatchClient.phaserEnvelope(rid,
                            PhaserOp.PHASER_OP_AWAIT_ADVANCE, key, 0, phase, 0);
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
                    continue;
                }
                if (!resp.hasPhaserOpResponse()) {
                    continue; // 异型码形：同 id 重发（W10 判例）
                }
                StatusCode status = resp.getPhaserOpResponse().getStatus();
                if (status == StatusCode.OK) {
                    registry.remove(envSession, rid);
                    return resp.getPhaserOpResponse().getPhase();
                }
                if (status == StatusCode.QUEUED) {
                    try {
                        arrived.get(Math.min(deadline - System.currentTimeMillis(),
                                REHANG_POLL_MS), TimeUnit.MILLISECONDS);
                        continue;
                    } catch (TimeoutException e) {
                        // 分片保活：原信封原 rid 重发（等待登记幂等、谓词即刻
                        // 判定）；会话变更路径才换 rid（上方 ExecutionException
                        // 分支经 route 刷新重建）。
                        continue;
                    } catch (ExecutionException e) {
                        registry.remove(envSession, rid);
                        throw new OpenLatchException("awaitAdvance of phaser '" + key
                                + "' failed", unwrap(e));
                    } catch (InterruptedException e) {
                        registry.remove(envSession, rid);
                        cancelQuietly(envSession, rid);
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
                registry.remove(envSession, rid);
                throw new OpenLatchException(status,
                        "awaitAdvance of phaser '" + key + "' rejected: " + status);
            }
        } finally {
            if (rid >= 0) {
                registry.remove(envSession, rid);
            }
        }
    }

    @Override
    public long getPhase() {
        Envelope resp = query();
        return resp.getPhaserOpResponse().getPhase();
    }

    @Override
    public int getRegisteredParties() {
        Envelope resp = query();
        return resp.getPhaserOpResponse().getRegistered();
    }

    @Override
    public int getArrivedParties() {
        Envelope resp = query();
        return resp.getPhaserOpResponse().getArrived();
    }

    @Override
    public int getUnarrivedParties() {
        Envelope resp = query();
        return Math.max(0, resp.getPhaserOpResponse().getRegistered()
                - resp.getPhaserOpResponse().getArrived());
    }

    /**
     * 一次 QUERY 读数（Leader 本地零日志；advisory 语义见 {@link OPhaser}）。
     *
     * @return 应答信封（phaser_op_response 已裁决 OK）
     */
    private Envelope query() {
        return sendOnce(OpenLatchClient.phaserEnvelope(
                requestId(), PhaserOp.PHASER_OP_QUERY, key, 0, null, 0), "query", true);
    }

    /**
     * 一次性变异提交（REGISTER/ARRIVE 族）：同会话瞬态同 id 重发；跨会话
     * 放弃（在途不重放，契约）。
     *
     * @param op      操作词
     * @param parties 注册数（仅 REGISTER）
     * @return 相位回显
     */
    private long submitOneShot(PhaserOp op, int parties) {
        ensureInitialRegistration();
        OpenLatchClient.LatchRoute route = requireRoute();
        long session = route.session().sessionId();
        long rid = route.session().nextRequestId();
        Envelope env = OpenLatchClient.phaserEnvelope(rid, op, key, parties, null, 0);
        Envelope resp = sendOnce(env, op.name(), false);
        if (route.session().sessionId() != session) {
            // sendOnce 内跨会话即弃；走到这里说明同会话完成。
            throw new IllegalStateException("unreachable: route changed under one-shot send");
        }
        return resp.getPhaserOpResponse().getPhase();
    }

    /**
     * 请求-应答单发（含同会话瞬态重试；跨会话或明确拒绝即时抛出）。
     *
     * @param env        信封（同会话重试复用同 request_id 幂等）
     * @param verb       日志/异常动词条
     * @param rerouteSafe 跨会话可重建重发（仅纯等待/读数传 true——本方法内
     *                    不重建，false 表示变异族跨会话放弃）
     * @return 应答信封（status=OK）
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
                if (!resp.hasPhaserOpResponse()) {
                    // 异型/空码形：MUST NOT 读 protobuf 默认实例伪成功（W10 判例），
                    // 视作瞬态按同 id 重发。
                    continue;
                }
                StatusCode status = resp.getPhaserOpResponse().getStatus();
                if (status == StatusCode.OK) {
                    return resp;
                }
                throw new OpenLatchException(status,
                        verb + " on phaser '" + key + "' rejected: " + status);
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
                throw new OpenLatchException(verb + " on phaser '" + key + "' failed", cause);
            } catch (TimeoutException e) {
                OpenLatchClient.LatchRoute now = client.latchRoute();
                if (now != null && now.session().sessionId() == session) {
                    continue; // 同会话超时：同 id 幂等重发
                }
                if (rerouteSafe && now != null) {
                    throw new OpenLatchException(StatusCode.NOT_LEADER,
                            verb + " on phaser '" + key + "': leader changed, retry with fresh request");
                }
                throw new OpenLatchTimeoutException(verb + " on phaser '" + key
                        + "' timed out (result unknown)");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new OpenLatchException(verb + " on phaser '" + key + "' interrupted", e);
            }
        }
        throw new OpenLatchTimeoutException(verb + " on phaser '" + key
                + "' timed out after retries (result unknown)");
    }

    /**
     * initialParties 的会话作用域注册（幂等水位 {@link #registeredSession}）。
     */
    private void ensureInitialRegistration() {
        if (initialParties <= 0) {
            return;
        }
        OpenLatchClient.LatchRoute route = requireRoute();
        long session = route.session().sessionId();
        if (registeredSession == session) {
            return;
        }
        long rid = route.session().nextRequestId();
        sendOnce(OpenLatchClient.phaserEnvelope(rid, PhaserOp.PHASER_OP_REGISTER,
                key, initialParties, null, 0), "initial registration", false);
        registeredSession = session;
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
            route.mux().sendWithId(OpenLatchClient.phaserEnvelope(rid2,
                    PhaserOp.PHASER_OP_CANCEL, key, 0, null, rid),
                    client.config().requestTimeout().toMillis());
        } catch (RuntimeException ignored) {
            // 尽力而为：ghost 由护栏与三路回收收敛（契约声明面）
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
            throw new OpenLatchException("no active session for phaser operation on '" + key + "'");
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
     * 是否明确拒绝型异常（携状态码）。
     *
     * @param cause 解包后原因
     * @return 明确拒绝为 {@code true}
     */
    private static boolean isExplicitReject(Throwable cause) {
        return cause instanceof OpenLatchException ole && ole.status() != null;
    }

    /**
     * 取拒绝状态码。
     *
     * @param cause 明确拒绝型异常
     * @return 状态码
     */
    private static StatusCode statusOf(Throwable cause) {
        return ((OpenLatchException) cause).status();
    }

    /**
     * 瞬态失败判定（同会话重发安全，判例 {@code RemoteBarrier}）。
     *
     * @param cause 解包后原因
     * @return 瞬态为 {@code true}
     */
    private static boolean isTransient(Throwable cause) {
        return cause instanceof ServerUnavailableException
                || cause instanceof OpenLatchTimeoutException;
    }

    /**
     * 解包执行异常（判例 {@code RemoteBarrier}）。
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
