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
import io.github.lamspace.openlatch.protocol.raft.QueueOpPayload;
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
 * 复制网关：节点内所有复制条目的
 * 唯一提交通道与"提交 → 应用 → 应答"桥。
 *
 * <p><b>提交与完成路径</b>：{@link #submit} 分配 seq、登记 pending 回执
 * future，经内部 RaftClient 池把条目发往当值 Leader；客户端应答的完成点
 * 是<b>本副本应用线程</b>回调 {@link #onApplied}（Ratis 提交后串行应用）——
 * 因此"应答即多数派确认后"。Ratis 传输层回执本身仅用于
 * 发现"条目根本不会被应用"的失败（NOT_LEADER、超时、服务关停），以
 * {@link RetryableCommitException} 完成（在途请求快速失败）。
 *
 * <p><b>Leader 侧应用副效应</b>（仅当本节点为当值 Leader）：
 * 授予出队、"需排队"竞态的排队登记与 QUEUED 改写、按
 * {@code freed_keys} 推进等待队首并推送 {@code AWAIT_NOTIFY}、会话关闭
 * 摘除（含 v9 条件登记与 topic 订阅）、授予应用点按 (会话,线程) 收口
 * 陈旧条件登记（v9）。Follower 应用同一批条目但跳过全部副效应——等待队列非复制状态，
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
    /** Leader 侧等待队列。 */
    private final WaitQueue waitQueue;
    /** 连接注册表（AWAIT_NOTIFY 本地投递）。 */
    private final ServerSessionRegistry sessions;
    /** seq → 在途回执（含提交时本节点是否 Leader——决定失去 Leadership 时是否立即失败）。 */
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    /** 条目全局序号发号器。 */
    private final AtomicLong seqGen = new AtomicLong();
    /** 本节点当前是否 Leader（应用副效应与任期队列清理的裁决位）。 */
    private volatile boolean leader;
    /** 到期驱动（装配后回挂；null 表示到期复制未启用）。 */
    private volatile LeaseExpiryDriver expiryDriver;
    /** 会话协调器（装配后回挂；接收应用/角色事件转发）。 */
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
     * 回挂到期驱动（装配后期绑定）。
     *
     * @param driver 到期驱动，可为 {@code null}（摘挂）
     */
    public void setExpiryDriver(LeaseExpiryDriver driver) {
        this.expiryDriver = driver;
    }

    /**
     * 回挂队列就绪驱动（v7，装配后期绑定）。
     *
     * @param driver 就绪驱动，可为 {@code null}（摘挂）
     */
    public void setQueueReadyDriver(QueueReadyDriver driver) {
        this.queueReadyDriver = driver;
    }

    /** v8 topic 登记表（会话摘除与换主清零钩子；可为未挂载）。 */
    private volatile io.github.lamspace.openlatch.server.topic.TopicRegistry topicRegistry;

    /**
     * 回挂 topic 登记表（v8，装配后期绑定）：本网关在 {@code SESSION_CLOSE}
     * 应用点摘除该会话的订阅登记/缓冲/去重槽，并在当选事件清零登记表
     * （判例 {@code WaitQueue} 换主清零）。
     *
     * @param registry 登记表，可为 {@code null}（摘挂）
     */
    public void setTopicRegistry(io.github.lamspace.openlatch.server.topic.TopicRegistry registry) {
        this.topicRegistry = registry;
    }

    /** v9 条件等待登记表（会话摘除、换主清零与授予侧收口钩子；可为未挂载）。 */
    private volatile io.github.lamspace.openlatch.server.condition.ConditionRegistry
            conditionRegistry;

    /**
     * 回挂条件等待登记表（v9，装配后期绑定）：本网关在 {@code SESSION_CLOSE}
     * 应用点摘除该会话的全部条件登记（死亡不吞锁——持有/租约/队列零触碰），
     * 在当选事件清零登记表（等待集随换主清零、客户端 ACQUIRE 车道迁移重挂
     * 补登记，判例 {@code WaitQueue}/{@code TopicRegistry}），并在
     * {@code LOCK_ACQUIRE_ENTRY} 授予应用点按 (会话,线程) 收口陈旧登记
     * （await 终结的授予侧摘除）。
     *
     * @param registry 登记表，可为 {@code null}（摘挂）
     */
    public void setConditionRegistry(
            io.github.lamspace.openlatch.server.condition.ConditionRegistry registry) {
        this.conditionRegistry = registry;
    }

    /** v7 队列就绪驱动（当选首扫钩子；可为未挂载）。 */
    private volatile QueueReadyDriver queueReadyDriver;

    /**
     * 回挂会话协调器（装配后期绑定）。
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
     * 时刻（Leader 发起时刻，诊断与条目时刻语义的来源）。
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
                    // reply.isSuccess：应答不取自回执消息——完成点在 onApplied。
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
     * 当选：清空上一任期等待队列、topic 登记表与条件等待登记表（进程本地态
     * 不跨任期存续，等待项经客户端车道迁移重挂补登记）并启动到期驱动首扫。
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
            io.github.lamspace.openlatch.server.topic.TopicRegistry tr = topicRegistry;
            if (tr != null) {
                // v8：订阅登记随换主清零——客户端 home 迁移后自动重订阅
                // （判例挂起者清零重挂；登记表无复制来源，新任期从零开始）。
                tr.clear();
            }
            io.github.lamspace.openlatch.server.condition.ConditionRegistry cr =
                    conditionRegistry;
            if (cr != null) {
                // v9：条件等待集随换主清零——进程本地态无快照/日志来源，
                // 等待项经客户端 ACQUIRE 车道迁移重挂以重发折叠 ACQUIRE
                // 补登记（幂等接纳；判例 WaitQueue/TopicRegistry 换主清零）。
                cr.clear();
            }
            LeaseExpiryDriver driver = expiryDriver;
            if (driver != null) {
                driver.onLeadershipGained();
            }
            QueueReadyDriver qdriver = queueReadyDriver;
            if (qdriver != null) {
                qdriver.onLeadershipGained();
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
     * Leader 侧应用副效应。
     *
     * @param entry  条目
     * @param result 原始回执
     * @return 改写后的回执（当前仅"预演失效→排队"一处改写）
     */
    private ApplyResult leaderSideEffects(RaftLogEntry entry, ApplyResult result) {
        // v7：队列侧效（回弹重挂 + 双轨唤醒）独立分支即返——其 DENIED 回弹
        // 与锁"预演失效改写"同型但判据不同（轨道、满足性谓词），MUST NOT
        // 落入下方锁改写臂。
        if (entry.getType() == RaftEntryType.QUEUE_OP_ENTRY) {
            return queueSideEffects(entry, result);
        }
        switch (entry.getType()) {
            case LOCK_ACQUIRE_ENTRY -> {
                if (result.getStatus() == ApplyStatus.OK) {
                    try {
                        var p = entry.getCommandPayload().toByteArray();
                        var ap = io.github.lamspace.openlatch.protocol.raft.AcquirePayload.parseFrom(p);
                        waitQueue.onGranted(ap.getSessionId(), ap.getRequestId());
                        if (!ap.getRequest().hasCondition()) {
                            // v9 授予侧收口：普通/唤醒后重发（condition 已清除）的
                            // 授予按 (会话,线程) 归属摘除该等待者可能残留的陈旧条件
                            // 登记（同一线程不可能既持锁又条件等待，判例
                            // LockEntry 授予侧 purge）。折叠 ACQUIRE 自身的 OK 是
                            // "释放半程守卫通过"而非授予——其登记刚在受理预检点
                            // 生效，MUST NOT 被本臂误摘（"登记先于释放可见"不变式）。
                            io.github.lamspace.openlatch.server.condition.ConditionRegistry
                                    cr = conditionRegistry;
                            if (cr != null) {
                                cr.purgeOwner(ap.getSessionId(),
                                        ap.getRequest().getThreadId());
                            }
                        }
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
        // 预演失效改写：提交时判定可授予、应用时锁已被占——
        // 原请求愿意排队（wait_ms != 0）则在应用点登记本地队列并回 QUEUED；
        // 立即式保持 DENIED。仅改写本节点在途请求的回执。
        // v7 注记：类型守卫收口——队列的 DENIED 回弹经 queueSideEffects 独立
        // 承载，MUST NOT 被本臂以 AcquirePayload 误解析。
        if (entry.getType() == RaftEntryType.LOCK_ACQUIRE_ENTRY
                && result.getStatus() == ApplyStatus.DENIED
                && pending.containsKey(entry.getSeq())) {
            try {
                var ap = io.github.lamspace.openlatch.protocol.raft.AcquirePayload
                        .parseFrom(entry.getCommandPayload().toByteArray());
                if (ap.getRequest().getWaitMs() != 0) {
                    int pos = waitQueue.enqueue(ap.getSessionId(), ap.getRequestId(),
                            ap.getRequest().getKey(),
                            io.github.lamspace.openlatch.server.dispatch.RequestDispatcher
                                    .normalizedPermits(ap.getRequest().getPermits()),
                            System.currentTimeMillis());
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
        // 唤醒推进：任何空出/归还的 key（释放/到期/会话关闭的 freed_keys）。
        // 许可感知：Semaphore 按影子表可用数判定队首是否满足，
        // 锁与条目已消失场景等价于"无限量"（队首恒可推进）。
        long now = System.currentTimeMillis();
        for (String key : result.getFreedKeysList()) {
            for (WaitQueue.Waiter w : waitQueue.onKeyFreedPermits(key, now,
                    kernel.shadow().permitsAvailable(key))) {
                pushAwaitNotify(w, key);
            }
        }
        // 部分释放的 Semaphore（条目未空、池已归还）同样驱动队首检查。
        if (entry.getType() == RaftEntryType.LOCK_RELEASE_ENTRY
                && result.getStatus() == ApplyStatus.OK
                && result.getFullyReleased() == false) {
            try {
                var rp = io.github.lamspace.openlatch.protocol.raft.ReleasePayload
                        .parseFrom(entry.getCommandPayload().toByteArray());
                String skey = rp.getRequest().getKey();
                if (kernel.shadow().isSemaphore(skey)) {
                    for (WaitQueue.Waiter w : waitQueue.onKeyFreedPermits(skey, now,
                            kernel.shadow().permitsAvailable(skey))) {
                        pushAwaitNotify(w, skey);
                    }
                }
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                log.warn("release payload unparsable in wake (seq={})", entry.getSeq());
            }
        }
        // 屏障归零：全体 awaiter 广播放行。
        if (entry.getType() == RaftEntryType.LATCH_COUNT_DOWN_ENTRY
                && result.getStatus() == ApplyStatus.OK && result.getLatchRemaining() == 0) {
            try {
                var lp = io.github.lamspace.openlatch.protocol.raft.LatchCountDownPayload
                        .parseFrom(entry.getCommandPayload().toByteArray());
                String lkey = lp.getRequest().getKey();
                for (WaitQueue.Waiter w : waitQueue.broadcastKey(lkey, now)) {
                    pushAwaitNotify(w, lkey);
                }
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                log.warn("latch payload unparsable in broadcast (seq={})", entry.getSeq());
            }
        }
        // v5：循环屏障 Leader 侧簿记——推送簿记登记（普通等待者入队、了结/
        // 离场出队）与世代终态广播（barrier_settled 位驱动，含
        // SESSION_CLOSE 的破障传播 barrier_released_keys）。位次与队列满
        // 护栏由引擎条目队列在应用点权威裁决（回执 queue_position 透传），
        // 本处 WaitQueue 仅承担"谁需要收 AWAIT_NOTIFY"的连接簿记。
        switch (entry.getType()) {
            case BARRIER_AWAIT_ENTRY -> {
                try {
                    var bp = io.github.lamspace.openlatch.protocol.raft.BarrierAwaitPayload
                            .parseFrom(entry.getCommandPayload().toByteArray());
                    String bkey = bp.getRequest().getKey();
                    if (result.getStatus() == ApplyStatus.QUEUED && !result.getBarrierExecutor()) {
                        if (waitQueue.enqueue(bp.getSessionId(), bp.getRequestId(), bkey, now) < 0) {
                            // 引擎护栏更严格时理论不可达；兜底不登记（该等待者
                            // 经重发自愈，与推送丢失同口径）。
                            log.warn("barrier waiter enqueue overflow (seq={}, key={})",
                                    entry.getSeq(), bkey);
                        }
                    }
                    if (result.getStatus() == ApplyStatus.OK
                            || result.getStatus() == ApplyStatus.BARRIER_BROKEN) {
                        waitQueue.onGranted(bp.getSessionId(), bp.getRequestId());
                    }
                    if (result.getBarrierSettled()) {
                        for (WaitQueue.Waiter w : waitQueue.broadcastKey(bkey, now)) {
                            pushAwaitNotify(w, bkey);
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.warn("barrier await payload unparsable in side effects (seq={})",
                            entry.getSeq());
                }
            }
            case BARRIER_LEAVE_ENTRY -> {
                try {
                    var lp = io.github.lamspace.openlatch.protocol.raft.BarrierLeavePayload
                            .parseFrom(entry.getCommandPayload().toByteArray());
                    String lkey = lp.getRequest().getKey();
                    if (result.getStatus() == ApplyStatus.OK) {
                        waitQueue.onGranted(lp.getSessionId(), lp.getRequest().getAwaitRequestId());
                        if (result.getBarrierSettled()) {
                            for (WaitQueue.Waiter w : waitQueue.broadcastKey(lkey, now)) {
                                pushAwaitNotify(w, lkey);
                            }
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.warn("barrier leave payload unparsable in side effects (seq={})",
                            entry.getSeq());
                }
            }
            case BARRIER_ACTION_DONE_ENTRY -> {
                try {
                    var dp = io.github.lamspace.openlatch.protocol.raft.BarrierActionDonePayload
                            .parseFrom(entry.getCommandPayload().toByteArray());
                    String dkey = dp.getRequest().getKey();
                    if (result.getBarrierSettled()) {
                        for (WaitQueue.Waiter w : waitQueue.broadcastKey(dkey, now)) {
                            pushAwaitNotify(w, dkey);
                        }
                    }
                } catch (InvalidProtocolBufferException e) {
                    log.warn("barrier action-done payload unparsable in side effects (seq={})",
                            entry.getSeq());
                }
            }
            default -> {
            }
        }
        if (entry.getType() == RaftEntryType.SESSION_CLOSE) {
            try {
                var sp = io.github.lamspace.openlatch.protocol.raft.SessionPayload
                        .parseFrom(entry.getCommandPayload().toByteArray());
                for (WaitQueue.Waiter w : waitQueue.purgeSession(sp.getSessionId(), now)) {
                    pushAwaitNotify(w, w.key());
                }
                io.github.lamspace.openlatch.server.topic.TopicRegistry tr = topicRegistry;
                if (tr != null) {
                    // v8：死亡即退订——摘除该会话全部订阅登记、缓冲与去重槽
                    //（防泄漏三路回收之一；失联探针补发的 SESSION_CLOSE 同径）。
                    tr.removeSession(sp.getSessionId());
                }
                io.github.lamspace.openlatch.server.condition.ConditionRegistry cr =
                        conditionRegistry;
                if (cr != null) {
                    // v9：死亡不吞锁——摘除该会话全部条件登记（三路回收之
                    // SESSION_CLOSE 双路同径：本节点断连传播与失联探针补发）；
                    // 锁持有/租约/等待队列由条目侧既有簿记各自收口。
                    cr.removeSession(sp.getSessionId());
                }
                // 离场即破障经会话关闭传播：被破世代的存活等待者收放行通知。
                for (String bkey : result.getBarrierReleasedKeysList()) {
                    for (WaitQueue.Waiter w : waitQueue.broadcastKey(bkey, now)) {
                        pushAwaitNotify(w, bkey);
                    }
                }
            } catch (InvalidProtocolBufferException e) {
                log.warn("session payload unparsable in purge (seq={})", entry.getSeq());
            }
        }
        return result;
    }

    /**
     * 队列条目 Leader 侧效（v7）。两件事——
     * <ol>
     *   <li><b>回弹重挂</b>：预检放行提交、应用点 {@code DENIED}（并发竞态
     *       使元素被抢先摘走/容量被抢先占满）且原请求携 {@code blocking}
     *       时，把该 (会话,请求) 挂回对应轨道并改写回执为 {@code QUEUED}
     *       （位次为轨内新位——判例锁"预演失效改写"；挂满回
     *       {@code QUEUE_FULL}，由分发层映射 OVERLOADED 终结本次重发）；
     *       立即式保持 DENIED 原样透传。</li>
     *   <li><b>双轨唤醒</b>：{@code PUT} 落地后若队首元素可消费（QUEUE 形态
     *       有深度即可；DELAY 形态须队首到期不晚于本时刻）唤醒 take 轨队首；
     *       {@code TAKE}/{@code DRAIN} 交付后按影子表深度与容量的空位判定
     *       唤醒 put 轨队首。谓词全部基于影子表（复制状态镜像）判定，
     *       推送仅提示、消费仍经提交路径在应用点终判。</li>
     * </ol>
     * 非本节点在途的条目（Follower 提交或转发而来）不改写回执，仅执行
     * 唤醒推送（挂起登记属当值 Leader 车道，与锁改写臂的 pending 守卫同理）。
     *
     * @param entry  已应用条目（{@link RaftEntryType#QUEUE_OP_ENTRY}）
     * @param result 原始回执
     * @return 改写后的回执（非回弹路径原样返回）
     */
    private ApplyResult queueSideEffects(RaftLogEntry entry, ApplyResult result) {
        long now = System.currentTimeMillis();
        try {
            var qp = QueueOpPayload.parseFrom(entry.getCommandPayload().toByteArray());
            var req = qp.getRequest();
            String qkey = req.getKey();
            var op = req.getOp();
            if (result.getStatus() == ApplyStatus.DENIED && req.getBlocking()
                    && pending.containsKey(entry.getSeq())) {
                int track = op == io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT
                        ? 1 : 2;
                int pos = waitQueue.enqueueTrack(qp.getSessionId(), qp.getRequestId(),
                        qkey, track, now);
                if (pos > 0) {
                    return ApplyResult.newBuilder()
                            .setStatus(ApplyStatus.QUEUED)
                            .setQueuePosition(pos)
                            .build();
                }
                return ApplyResult.newBuilder().setStatus(ApplyStatus.QUEUE_FULL).build();
            }
            if (result.getStatus() == ApplyStatus.OK) {
                // 交付/入队成功即摘本 (会话,请求) 的挂起项（判例锁授予出队）——
                // 残留陈旧队首会挡住同轨后继者的唤醒推进。
                waitQueue.onGranted(qp.getSessionId(), qp.getRequestId());
                if (op == io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT) {
                    if (queueElementVisible(qkey, now)) {
                        for (WaitQueue.Waiter w : waitQueue.onElementReady(qkey, now, true)) {
                            pushAwaitNotify(w, qkey);
                        }
                    }
                } else if (op == io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_TAKE
                        || op == io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_DRAIN) {
                    boolean free = kernel.shadow().queueDepth(qkey)
                            < kernel.shadow().queueCapacity(qkey);
                    for (WaitQueue.Waiter w : waitQueue.onCapacityFreed(qkey, now, free)) {
                        pushAwaitNotify(w, qkey);
                    }
                }
            }
        } catch (InvalidProtocolBufferException e) {
            log.warn("queue op payload unparsable in side effects (seq={})", entry.getSeq());
        }
        return result;
    }

    /**
     * 队首元素可见性判定（唤醒谓词，影子表口径）：QUEUE 形态队首到期恒 0
     * ——有深度即可见；DELAY 形态要求队首绝对到期时刻不晚于给定时刻。
     *
     * @param key 队列键
     * @param now 判定时刻（epoch 毫秒，与条目折算的到期时刻同域）
     * @return 队首可消费为 {@code true}
     */
    private boolean queueElementVisible(String key, long now) {
        var shadow = kernel.shadow();
        if (shadow.queueDepth(key) <= 0) {
            return false;
        }
        long expiry = shadow.queueHeadExpiryMs(key);
        return expiry == 0 || expiry <= now;
    }

    /**
     * 推送 AWAIT_NOTIFY（Leader 本地连接投递；跨接入节点转发不在本方法职责）。
     * 应用线程内仅做查表与非阻塞写投递。
     *
     * @param w   待通知的队首等待项
     * @param key 释放的锁键
     */
    private void pushAwaitNotify(WaitQueue.Waiter w, String key) {
        ServerSession session = sessions.get(w.sessionId());
        if (session == null || !session.channel().isActive()) {
            return; // 连接已不存在：等清扫路径兜底（与单机静默丢弃同语义）
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
     * 关停：以可重试错误完成全部未决 future（关停无悬挂请求）。
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
    /**
     * 已通知队首超时清扫（Leader 侧周期驱动）：摘除超时未重发
     * 的队首并推进新队首通知；Semaphore 的新队首须许可足量方可推送，不足
     * 则撤销其已通知标记（{@code deferHead}），由下一轮清扫/下一次归还
     * 重新评估——与单机 {@code SemaphoreEntry} 的队首检查语义等价。
     *
     * @param now 当前时刻（毫秒）
     */
    public void sweepWaitQueue(long now) {
        if (!isLeaderAuthoritative()) {
            return;
        }
        for (WaitQueue.Waiter w : waitQueue.sweepNotified(now)) {
            if (kernel.shadow().isSemaphore(w.key())
                    && kernel.shadow().permitsAvailable(w.key()) < w.permits()) {
                waitQueue.deferHead(w.key());
                continue;
            }
            pushAwaitNotify(w, w.key());
        }
    }

    /**
     * 队列就绪扫描（v7，{@code QueueReadyDriver} 与当选首扫的共用体）：
     * 遍历存在挂起等待者的队列键（由等待队列反查，非全表扫描影子表），
     * take 轨按队首可见性（DELAY 形态须到期不晚于当前时刻）、put 轨按
     * 容量空位（兼作事件唤醒丢失的自愈兜底）推进队首并推送
     * {@code AWAIT_NOTIFY}。零日志、零状态变更——推送只是重发提示，
     * 消费与创建的终判恒在应用点；已通知窗口内不重复推送。
     *
     * @param now 扫描时刻（epoch 毫秒，与条目折算到期时刻同域）
     */
    public void sweepQueueReady(long now) {
        if (!isLeaderAuthoritative()) {
            return;
        }
        for (String key : waitQueue.trackKeys(2)) {
            if (!queueElementVisible(key, now)) {
                continue;
            }
            for (WaitQueue.Waiter w : waitQueue.onElementReady(key, now, true)) {
                pushAwaitNotify(w, key);
            }
        }
        for (String key : waitQueue.trackKeys(1)) {
            boolean free = kernel.shadow().queueDepth(key) < kernel.shadow().queueCapacity(key);
            for (WaitQueue.Waiter w : waitQueue.onCapacityFreed(key, now, free)) {
                pushAwaitNotify(w, key);
            }
        }
    }

    /**
     * Leader 侧等待队列（测试与指标观测口）。
     *
     * @return 等待队列实例
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
     * 调用方 MUST 以可重试错误应答客户端（快速失败优先）。
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
