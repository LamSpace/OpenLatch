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

package io.github.lamspace.openlatch.server.raft;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 复制网关（详设 §3.2 {@code ReplicationGateway}）：节点内所有复制条目的
 * 唯一提交通道与"提交 → 应用 → 应答"桥（§4.5，design D3/D4/D11/D12）。
 *
 * <p><b>提交与完成路径</b>：{@link #submit} 分配 seq、登记 pending 回执
 * future，经内部 RaftClient 池把条目发往当值 Leader；客户端应答的完成点
 * 是<b>本副本应用线程</b>回调 {@link #onApplied}（Ratis 提交后串行应用，
 * design D10）——因此"应答即多数派确认后"。Ratis 传输层回执本身仅用于
 * 发现"条目根本不会被应用"的失败（NOT_LEADER、超时、服务关停），以
 * {@link RetryableCommitException} 完成（spec"在途请求快速失败"）。
 *
 * <p><b>Leader 侧应用副效应</b>（仅当本节点为当值 Leader，design D9）：
 * 授予出队、"需排队"竞态的排队登记与 QUEUED 改写（§4.5/D3）、按
 * {@code freed_keys} 推进等待队首并推送 {@code AWAIT_NOTIFY}、会话关闭
 * 摘除。Follower 应用同一批条目但跳过全部副效应——等待队列非复制状态，
 * 副本一致性只由影子表/引擎的迁移维持。
 *
 * <p><b>Leadership 边界</b>：{@link #onLeaderChanged} 失去 Leadership 时把
 * 全部未决 future 以可重试错误完成（MUST NOT 悬挂）；当选时清空上一任期
 * 等待队列（任期作用域 FIFO）。
 *
 * <p><b>线程模型</b>：{@link #submit} 任意业务线程可调；{@link #onApplied}
 * 在状态机应用线程回调——内部 MUST NOT 阻塞（应答写回经 Netty 的线程安全
 * {@code writeAndFlush} 投递；{@code pending} 为并发容器）。
 */
public final class ReplicationGateway implements ApplyObserver {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(ReplicationGateway.class);

    /** 装配后的子系统（提交通道与角色查询）。 */
    private final RaftSubsystem subsystem;
    /** 语义内核（影子表供预检查/唤醒消费）。 */
    private final LockStateMachineCore kernel;
    /** Leader 侧等待队列（design D9）。 */
    private final WaitQueue waitQueue;
    /** 连接注册表（AWAIT_NOTIFY 本地投递）。 */
    private final ServerSessionRegistry sessions;
    /** seq → 在途回执（含提交时本节点是否 Leader——决定失去 Leadership 时是否立即失败）。 */
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    /** 条目全局序号发号器。 */
    private final AtomicLong seqGen = new AtomicLong();
    /** 本节点当前是否 Leader（应用副效应与任期队列清理的裁决位）。 */
    private volatile boolean leader;
    /** 到期驱动（P2-09 装配后回挂；null 表示到期复制未启用）。 */
    private volatile LeaseExpiryDriver expiryDriver;
    /** 会话协调器（P2-08 装配后回挂；接收应用/角色事件转发）。 */
    private volatile SessionCoordinator sessionCoordinator;

    /**
     * 构造网关。
     *
     * @param subsystem  Raft 子系统（已 start）
     * @param kernel     状态机内核（与子系统共享同一实例）
     * @param waitQueue  Leader 侧等待队列
     * @param sessions   连接注册表
     */
    public ReplicationGateway(RaftSubsystem subsystem, LockStateMachineCore kernel,
                              WaitQueue waitQueue, ServerSessionRegistry sessions) {
        this.subsystem = subsystem;
        this.kernel = kernel;
        this.waitQueue = waitQueue;
        this.sessions = sessions;
        subsystem.stateMachine().setObserver(this);
    }

    /**
     * 回挂到期驱动（装配后期绑定，P2-09）。
     *
     * @param driver 到期驱动，可为 {@code null}（摘挂）
     */
    public void setExpiryDriver(LeaseExpiryDriver driver) {
        this.expiryDriver = driver;
    }

    /**
     * 回挂会话协调器（装配后期绑定，P2-08）。
     *
     * @param coordinator 协调器，可为 {@code null}（摘挂）
     */
    public void setSessionCoordinator(SessionCoordinator coordinator) {
        this.sessionCoordinator = coordinator;
    }

    /**
     * 提交一条复制条目并返回应用回执 future。
     *
     * <p>条目序号由本网关分配并登记 pending；{@code wall_clock_ms} 取提交
     * 时刻（Leader 发起时刻，§4.2 诊断与条目时刻语义的来源）。
     *
     * @param type    条目类型
     * @param payload 类型对应载荷序列化
     * @return 完成于本副本应用（或失败于可重试原因）的回执 future
     */
    public CompletableFuture<ApplyResult> submit(RaftEntryType type, ByteString payload) {
        long seq = seqGen.incrementAndGet();
        RaftLogEntry entry = RaftLogEntry.newBuilder()
                .setType(type)
                .setSeq(seq)
                .setWallClockMs(System.currentTimeMillis())
                .setCommandPayload(payload)
                .build();
        return submit(entry);
    }

    /**
     * 提交已构造条目（测试与追赶工具入口；seq 必须未被使用过）。
     *
     * @param entry 复制条目
     * @return 应用回执 future
     */
    public CompletableFuture<ApplyResult> submit(RaftLogEntry entry) {
        CompletableFuture<ApplyResult> f = new CompletableFuture<>();
        pending.put(entry.getSeq(), new Pending(f, leader));
        RaftClient client = subsystem.acquireClient();
        if (client == null) {
            failPending(entry.getSeq(), new RetryableCommitException("subsystem not running"));
            return f;
        }
        client.async().send(Message.valueOf(
                        org.apache.ratis.thirdparty.com.google.protobuf.ByteString
                                .copyFrom(entry.toByteArray())))
                .whenComplete((reply, err) -> {
                    if (err != null) {
                        failPending(entry.getSeq(), new RetryableCommitException(err));
                    } else if (!reply.isSuccess()) {
                        failPending(entry.getSeq(), new RetryableCommitException(
                                "raft reply failed: " + reply.getException()));
                    }
                    // reply.isSuccess：应答不取自回执消息——完成点在 onApplied（design D10）。
                });
        return f;
    }

    /**
     * 条目应用回调（状态机应用线程，见 {@link ApplyObserver}）。
     *
     * <p>裁决顺序：①Leader 副效应（授予出队 / 排队改写 / 唤醒 / 摘除）；
     * ②pending 完成（改写后的结果）。非本节点提交的条目（seq 不在表内）
     * 仅执行 Leader 副效应（若本节点恰为当值 Leader）。
     *
     * @param entry  已应用条目
     * @param result 应用回执
     */
    @Override
    public void onApplied(RaftLogEntry entry, ApplyResult result) {
        ApplyResult out = result;
        if (leader) {
            out = leaderSideEffects(entry, result);
        }
        LeaseExpiryDriver driver = expiryDriver;
        if (driver != null) {
            driver.onEntryApplied(entry, out);
        }
        SessionCoordinator coordinator = sessionCoordinator;
        if (coordinator != null) {
            coordinator.onEntryApplied(entry, out);
        }
        Pending p = pending.remove(entry.getSeq());
        if (p != null) {
            p.future().complete(out);
        }
    }

    /**
     * Leadership 变更（状态机事件线程）。失去：未决 future 全部可重试完成；
     * 当选：清空上一任期等待队列并启动到期驱动首扫（P2-09/D9）。
     *
     * @param isLeader 本节点当前是否 Leader
     */
    @Override
    public void onLeaderChanged(boolean isLeader) {
        boolean wasLeader = this.leader;
        this.leader = isLeader;
        if (wasLeader && !isLeader) {
            // 仅终结"本节点以 Leader 身份受理"的在途请求（其条目可能随降级被
            // 截断）；Follower 提交的条目（如 HELLO 的 SESSION_OPEN）由新
            // Leader 照常提交、经本副本应用回执完成，不受本事件影响（spec
            // "Leadership 丧失时在途请求快速失败"的精确口径）。
            for (Long seq : pending.keySet()) {
                Pending p = pending.get(seq);
                if (p != null && p.viaSelfLeader()) {
                    failPending(seq, new RetryableCommitException("leadership lost"));
                }
            }
        }
        if (!wasLeader && isLeader) {
            waitQueue.clear();
            LeaseExpiryDriver driver = expiryDriver;
            if (driver != null) {
                driver.onLeadershipGained();
            }
        }
        SessionCoordinator coordinator = sessionCoordinator;
        if (coordinator != null) {
            coordinator.onLeaderChanged(isLeader);
        }
        log.info("leadership changed on node {}: leader={}",
                subsystem.clusterConfig().nodeId(), isLeader);
    }

    /**
     * Leader 侧应用副效应（design D3/D9 的落点）。
     *
     * @param entry  条目
     * @param result 原始回执
     * @return 改写后的回执（当前仅"预演失效→排队"一处改写）
     */
    private ApplyResult leaderSideEffects(RaftLogEntry entry, ApplyResult result) {
        switch (entry.getType()) {
            case LOCK_ACQUIRE_ENTRY -> {
                if (result.getStatus() == ApplyStatus.OK) {
                    try {
                        var p = entry.getCommandPayload().toByteArray();
                        var ap = io.github.lamspace.openlatch.protocol.raft.AcquirePayload.parseFrom(p);
                        waitQueue.onGranted(ap.getSessionId(), ap.getRequestId());
                    } catch (InvalidProtocolBufferException e) {
                        log.warn("acquire payload unparsable in side effects (seq={})", entry.getSeq());
                    }
                }
            }
            case LEASE_RENEW_ENTRY -> {
                // 续租成功仅刷新队列无关状态（唤醒来源为释放/到期/会话关闭）。
            }
            default -> {
            }
        }
        // 预演失效改写（§4.5/D3）：提交时判定可授予、应用时锁已被占——
        // 原请求愿意排队（wait_ms != 0）则在应用点登记本地队列并回 QUEUED；
        // 立即式保持 DENIED。仅改写本节点在途请求的回执。
        if (result.getStatus() == ApplyStatus.DENIED
                && pending.containsKey(entry.getSeq())) {
            try {
                var ap = io.github.lamspace.openlatch.protocol.raft.AcquirePayload
                        .parseFrom(entry.getCommandPayload().toByteArray());
                if (ap.getRequest().getWaitMs() != 0) {
                    int pos = waitQueue.enqueue(ap.getSessionId(), ap.getRequestId(),
                            ap.getRequest().getKey());
                    if (pos > 0) {
                        return ApplyResult.newBuilder()
                                .setStatus(ApplyStatus.QUEUED)
                                .setQueuePosition(pos)
                                .build();
                    }
                    return ApplyResult.newBuilder()
                            .setStatus(ApplyStatus.QUEUE_FULL)
                            .build();
                }
            } catch (InvalidProtocolBufferException e) {
                log.warn("acquire payload unparsable in rewrite (seq={})", entry.getSeq());
            }
        }
        // 唤醒推进：任何完全空出的 key（释放/到期/会话关闭的 freed_keys）。
        long now = System.currentTimeMillis();
        for (String key : result.getFreedKeysList()) {
            for (WaitQueue.Waiter w : waitQueue.onKeyFreed(key, now)) {
                pushAwaitNotify(w, key);
            }
        }
        if (entry.getType() == RaftEntryType.SESSION_CLOSE) {
            try {
                var sp = io.github.lamspace.openlatch.protocol.raft.SessionPayload
                        .parseFrom(entry.getCommandPayload().toByteArray());
                for (WaitQueue.Waiter w : waitQueue.purgeSession(sp.getSessionId(), now)) {
                    pushAwaitNotify(w, w.key());
                }
            } catch (InvalidProtocolBufferException e) {
                log.warn("session payload unparsable in purge (seq={})", entry.getSeq());
            }
        }
        return result;
    }

    /**
     * 推送 AWAIT_NOTIFY（Leader 本地连接投递；跨接入节点转发挂 S3，design D9）。
     * 应用线程内仅做查表与非阻塞写投递。
     *
     * @param w   待通知的队首等待项
     * @param key 释放的锁键
     */
    private void pushAwaitNotify(WaitQueue.Waiter w, String key) {
        ServerSession session = sessions.get(w.sessionId());
        if (session == null || !session.channel().isActive()) {
            return; // 连接已不存在：等清扫路径兜底（与 Phase 1 静默丢弃同语义）
        }
        Envelope notify = Envelope.newBuilder()
                .setProtocolVersion(session.protocolVersion())
                .setType(MessageType.AWAIT_NOTIFY)
                .setRequestId(0)
                .setAwaitNotify(AwaitNotify.newBuilder().setKey(key).setRequestIdRef(w.requestId()))
                .build();
        session.channel().writeAndFlush(notify);
    }

    /**
     * 以可重试原因完成指定 seq 的在途 future（不存在即已被 onApplied 捷足）。
     *
     * @param seq   条目序号
     * @param cause 可重试原因
     */
    private void failPending(long seq, Throwable cause) {
        Pending p = pending.remove(seq);
        if (p != null) {
            p.future().completeExceptionally(cause);
        }
    }

    /**
     * 在途登记：回执 future + 提交时本节点角色。
     *
     * @param future        回执 future
     * @param viaSelfLeader 提交时本节点是否 Leader（true 者在失去 Leadership 时立即可重试终结）
     */
    private record Pending(CompletableFuture<ApplyResult> future, boolean viaSelfLeader) { }

    /**
     * 关停：以可重试错误完成全部未决 future（spec"关停无悬挂请求"）。
     */
    public void close() {
        for (Long seq : pending.keySet()) {
            failPending(seq, new RetryableCommitException("gateway closing"));
        }
    }

    /**
     * 本网关的等待队列（测试与到期驱动观察）。
     *
     * @return 等待队列
     */
    public WaitQueue waitQueue() {
        return waitQueue;
    }

    /**
     * 本节点当前是否 Leader（应用事件折算的角色标志，预检裁决用；
     * 权威角色以 {@link RaftSubsystem#isLeader()} 为准，本标志允许
     * 毫秒级滞后——其用途仅为"不向非 Leader 副效应路径误入"）。
     *
     * @return Leader 为 {@code true}
     */
    public boolean isLeader() {
        return leader;
    }

    /**
     * 影子表引用（预检查消费）。
     *
     * @return 影子表
     */
    public ShadowTable shadow() {
        return kernel.shadow();
    }

    /**
     * 权威角色查询（Ratis division 实况，写请求拒入预检使用；
     * 区别于 {@link #isLeader()} 的事件折算标志）。
     *
     * @return 本节点当前是否 Leader
     */
    public boolean isLeaderAuthoritative() {
        return subsystem.isLeader();
    }

    /**
     * 可重试提交失败：条目未获得应用回执（未提交/失去 Leadership/关停）。
     * 调用方 MUST 以可重试错误应答客户端（§6.3 快速失败优先，S3 语义的 S2 承载）。
     */
    public static final class RetryableCommitException extends RuntimeException {
        /**
         * 构造。
         *
         * @param message 原因说明
         */
        public RetryableCommitException(String message) {
            super(message);
        }

        /**
         * 构造。
         *
         * @param cause 底层原因
         */
        public RetryableCommitException(Throwable cause) {
            super(cause);
        }
    }
}
