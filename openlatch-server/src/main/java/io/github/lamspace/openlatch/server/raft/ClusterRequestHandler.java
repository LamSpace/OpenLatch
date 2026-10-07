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
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.AtomicOpResponse;
import io.github.lamspace.openlatch.protocol.BarrierActionDoneResponse;
import io.github.lamspace.openlatch.protocol.BarrierAwaitResponse;
import io.github.lamspace.openlatch.protocol.BarrierLeaveResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LeaseRenewResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.QueueOp;
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.QueueOpResponse;
import io.github.lamspace.openlatch.protocol.ReleaseResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.raft.QueueOpPayload;
import io.github.lamspace.openlatch.protocol.raft.AcquirePayload;
import io.github.lamspace.openlatch.protocol.raft.AtomicOpPayload;
import io.github.lamspace.openlatch.protocol.raft.BarrierActionDonePayload;
import io.github.lamspace.openlatch.protocol.raft.BarrierAwaitPayload;
import io.github.lamspace.openlatch.protocol.raft.BarrierLeavePayload;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.ReleasePayload;
import io.github.lamspace.openlatch.protocol.raft.RenewPayload;
import io.github.lamspace.openlatch.protocol.raft.LatchCountDownPayload;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.metrics.ServerMetrics;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 集群模式写请求处理器：单机模式的"同步函数
 * 调用即应答"在这里被替换为"预检查 → 提交 → 应用后应答"三段，本类承载
 * 协议侧的裁决与映射：
 *
 * <ul>
 *   <li><b>预检查（快速失败通道）</b>：类型/键合法性与会话登记校验不过直接
 *       同步错误应答，MUST NOT 写日志；锁被占或队列非空时——可排队请求在
 *       本地 {@link WaitQueue} 登记并即时回 QUEUED，立即式回 DENIED，两者
 *       均不写日志（排队不是复制状态）；</li>
 *   <li><b>授予/释放/续租</b>：构造 {@link RaftEntryType} 条目提交
 *       {@link ReplicationGateway}，应答在完成回调中写回连接所属
 *       EventLoop；</li>
 *   <li><b>回执 → 协议映射</b>：{@link ApplyStatus} 全表映射（与单机
 *       {@code RequestDispatcher} 的映射表语义逐项对齐，错误码不复用）。
 * </ul>
 *
 * <p><b>Follower 分车道</b>：
 * <ul>
 *   <li><b>ACQUIRE（新授予/排队）</b>：排队登记与 {@code AWAIT_NOTIFY} 是
 *       Leader 本地态，Follower 受理无法保证通知送达——角色门命中即同步回
 *       {@code NOT_LEADER} 并随附 {@link LeaderTracker} 提示（选举空窗
 *       nodeId 为 -1），不产生条目、不动等待队列；</li>
 *   <li><b>RELEASE / RENEW（存量操作）</b>：纯 token/归属校验、无 Leader
 *       本地态依赖——Follower 摘除角色门照常提交，经内部提交通道转发至
 *       当值 Leader 复制执行（与 {@code SESSION_OPEN} 同车道），应答与
 *       客户端直发 Leader 结果一致；会话已被清理的在 Follower 本地预检即
 *       {@code SESSION_EXPIRED}（零转发），Leader 应用点 {@code REJECT_SESSION}
 *       为权威兜底。</li>
 * </ul>
 * 提交失败按语义拆分：可重试的提交失败（含降级在途终结）以
 * {@code NOT_LEADER} + 当时提示应答；其余内部失败以 {@code INTERNAL_ERROR}
 * 应答——不再共享一个混叠码。
 *
 * <p><b>线程模型</b>：入站处理在连接 EventLoop；应答完成在状态机应用线程，
 * 经 {@code channel.eventLoop().execute} 弹回写回——单连接的请求序由
 * EventLoop 串行保证，跨连接并发经日志全序仲裁。
 *
 * <p><b>指标埋点</b>（与单机路径共用
 * {@link ServerMetrics}）：每条受理并应答的请求恰记一次，收口于
 * {@link #writeSync}/{@link #respondAsync} 两个写回出口；耗时口径为
 * 受理至应答生成（集群档含 Raft 提交等待）。
 * {@code metrics} 可为 {@code null}（测试夹具装配），此时零记录。
 */
public final class ClusterRequestHandler {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(ClusterRequestHandler.class);

    /** 复制网关（提交通道）。 */
    private final ReplicationGateway gateway;
    /** 服务配置（键长限额等本地预检）。 */
    private final ServerConfig config;
    /** Leader 侧等待队列（预检查排队路径）。 */
    private final WaitQueue waitQueue;
    /** 语义内核（会话登记预检消费影子表）。 */
    private final LockStateMachineCore kernel;
    /** Leader 提示单源（NOT_LEADER 应答随附提示）。 */
    private final LeaderTracker leaderTracker;
    /** 指标词表门面；{@code null} 表示不埋点（测试夹具装配形态）。 */
    private final ServerMetrics metrics;
    /** v8 topic 登记表（Leader 本地态）；{@code null}=夹具无 topic 面。 */
    private final io.github.lamspace.openlatch.server.topic.TopicRegistry topics;
    /** v9 条件等待登记表（Leader 本地态）；{@code null}=夹具无条件面。 */
    private final io.github.lamspace.openlatch.server.condition.ConditionRegistry conditions;
    /**
     * v10：phaser 等待簿记（Leader 本地态，账簿本体经复制状态机——纯等待/
     * 撤销/读数在本簿记与影子表直答，变异操作经提交；{@code null}=夹具无
     * phaser 面，抵达回 {@code INTERNAL_ERROR}，判例 conditions 同形态）。
     */
    private final io.github.lamspace.openlatch.server.phaser.PhaserRegistry phasers;
    /**
     * v11：timer 等待簿记（Leader 本地态，账簿本体经复制状态机——等待/撤销/
     * 读数在本簿记与影子表直答，装载/撤销经提交；{@code null}=夹具无 timer 面，
     * 抵达回 {@code INTERNAL_ERROR}，判例 phasers 同形态）。
     */
    private final io.github.lamspace.openlatch.server.timer.TimerRegistry timers;

    /**
     * 构造处理器（不埋点，既有测试夹具形态）。
     *
     * @param gateway       复制网关
     * @param kernel        状态机内核（会话预检）
     * @param waitQueue     等待队列
     * @param config        服务配置
     * @param leaderTracker Leader 提示单源
     */
    public ClusterRequestHandler(ReplicationGateway gateway, LockStateMachineCore kernel,
                                 WaitQueue waitQueue, ServerConfig config,
                                 LeaderTracker leaderTracker) {
        this(gateway, kernel, waitQueue, config, leaderTracker, null);
    }

    /**
     * 构造处理器（生产形态：写回出口经 {@code metrics} 记入服务端指标词表）。
     *
     * @param gateway       复制网关
     * @param kernel        状态机内核（会话预检）
     * @param waitQueue     等待队列
     * @param config        服务配置
     * @param leaderTracker Leader 提示单源
     * @param metrics       指标门面，可为 {@code null}（不埋点）
     */
    public ClusterRequestHandler(ReplicationGateway gateway, LockStateMachineCore kernel,
                                 WaitQueue waitQueue, ServerConfig config,
                                 LeaderTracker leaderTracker, ServerMetrics metrics) {
        this(gateway, kernel, waitQueue, config, leaderTracker, metrics, null);
    }

    /**
     * 构造集群请求处理器（v8 兼容形态：无条件面装配，等价 {@code conditions=null}
     * ——CONDITION_OP 与带 {@code condition} 的 ACQUIRE 抵达时回同型
     * {@code INTERNAL_ERROR}，判例 topics 同形态）。
     *
     * @param gateway       复制网关
     * @param kernel        状态机内核（会话预检与撞 key 影子读）
     * @param waitQueue     等待队列
     * @param config        服务配置
     * @param leaderTracker Leader 提示单源
     * @param metrics       指标门面，可为 {@code null}（不埋点）
     * @param topics        topic 登记表（Leader 本地态）；{@code null}=夹具
     *                      无 topic 面（TOPIC_OP 抵达时回 {@code INTERNAL_ERROR}）
     */
    public ClusterRequestHandler(ReplicationGateway gateway, LockStateMachineCore kernel,
                                 WaitQueue waitQueue, ServerConfig config,
                                 LeaderTracker leaderTracker, ServerMetrics metrics,
                                 io.github.lamspace.openlatch.server.topic.TopicRegistry topics) {
        this(gateway, kernel, waitQueue, config, leaderTracker, metrics, topics, null);
    }

    /**
     * 构造集群请求处理器（v9 全参形态）。
     *
     * @param gateway       复制网关
     * @param kernel        状态机内核（会话预检、撞 key 影子读与 signal 权限权威读）
     * @param waitQueue     等待队列
     * @param config        服务配置
     * @param leaderTracker Leader 提示单源
     * @param metrics       指标门面，可为 {@code null}（不埋点）
     * @param topics        topic 登记表（Leader 本地态）；{@code null}=夹具
     *                      无 topic 面（TOPIC_OP 抵达时回 {@code INTERNAL_ERROR}）
     * @param conditions    条件等待登记表（Leader 本地态）；{@code null}=夹具
     *                      无条件面（CONDITION_OP 与折叠 ACQUIRE 抵达时回同型
     *                      {@code INTERNAL_ERROR}，判例 topics 同形态）
     */
    public ClusterRequestHandler(ReplicationGateway gateway, LockStateMachineCore kernel,
                                 WaitQueue waitQueue, ServerConfig config,
                                 LeaderTracker leaderTracker, ServerMetrics metrics,
                                 io.github.lamspace.openlatch.server.topic.TopicRegistry topics,
                                 io.github.lamspace.openlatch.server.condition.ConditionRegistry
                                         conditions) {
        this(gateway, kernel, waitQueue, config, leaderTracker, metrics, topics, conditions, null);
    }

    /**
     * 构造集群请求处理器（v10 全参形态：追加 phaser 等待簿记装配）。
     *
     * @param gateway       复制网关
     * @param kernel        状态机内核（影子读与提交路径装配）
     * @param waitQueue     等待队列
     * @param config        服务配置
     * @param leaderTracker Leader 提示单源
     * @param metrics       指标门面，可为 {@code null}（不埋点）
     * @param topics        topic 登记表（Leader 本地态）
     * @param conditions    条件等待登记表（Leader 本地态）
     * @param phasers       phaser 等待簿记（Leader 本地态）；{@code null}=夹具
     *                      无 phaser 面（PHASER_OP 抵达时回 {@code INTERNAL_ERROR}）
     */
    public ClusterRequestHandler(ReplicationGateway gateway, LockStateMachineCore kernel,
                                 WaitQueue waitQueue, ServerConfig config,
                                 LeaderTracker leaderTracker, ServerMetrics metrics,
                                 io.github.lamspace.openlatch.server.topic.TopicRegistry topics,
                                 io.github.lamspace.openlatch.server.condition.ConditionRegistry
                                         conditions,
                                 io.github.lamspace.openlatch.server.phaser.PhaserRegistry
                                         phasers) {
        this(gateway, kernel, waitQueue, config, leaderTracker, metrics, topics, conditions,
                phasers, null);
    }

    /**
     * 构造集群请求处理器（v11 全参形态：追加 timer 等待簿记装配）。
     *
     * @param gateway       复制网关
     * @param kernel        状态机内核（影子读与提交路径装配）
     * @param waitQueue     等待队列
     * @param config        服务配置
     * @param leaderTracker Leader 提示单源
     * @param metrics       指标门面，可为 {@code null}（不埋点）
     * @param topics        topic 登记表（Leader 本地态）
     * @param conditions    条件等待登记表（Leader 本地态）
     * @param phasers       phaser 等待簿记（Leader 本地态）
     * @param timers        timer 等待簿记（Leader 本地态）；{@code null}=夹具
     *                      无 timer 面（TIMER_OP 抵达时回 {@code INTERNAL_ERROR}）
     */
    public ClusterRequestHandler(ReplicationGateway gateway, LockStateMachineCore kernel,
                                 WaitQueue waitQueue, ServerConfig config,
                                 LeaderTracker leaderTracker, ServerMetrics metrics,
                                 io.github.lamspace.openlatch.server.topic.TopicRegistry topics,
                                 io.github.lamspace.openlatch.server.condition.ConditionRegistry
                                         conditions,
                                 io.github.lamspace.openlatch.server.phaser.PhaserRegistry
                                         phasers,
                                 io.github.lamspace.openlatch.server.timer.TimerRegistry
                                         timers) {
        this.gateway = gateway;
        this.kernel = kernel;
        this.waitQueue = waitQueue;
        this.config = config;
        this.leaderTracker = leaderTracker;
        this.metrics = metrics;
        this.topics = topics;
        this.conditions = conditions;
        this.phasers = phasers;
        this.timers = timers;
    }

    /**
     * ACQUIRE 集群路径：预检查通过后提交，应答于应用后写回。
     *
     * <p>判定顺序（同步分支立即写回，异步分支接管在途记账）：
     * 角色（Follower 即 {@code NOT_LEADER}+提示改连，分车道）→
     * 载荷与键合法性 → 会话登记 → 排队裁决（预演）。
     *
     * @param session 已握手会话（携带逻辑 sessionId）
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleAcquire(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, true);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        var req = msg.getAcquireRequest();
        if (req.getLockType() == io.github.lamspace.openlatch.protocol.LockType.UNRECOGNIZED) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        // v3 门控：与单机分发器同规则——v3 专属类型对低版本会话
        // 消息级拒绝、不断连，先于排队预检与提案（MUST NOT 进入复制日志）。
        if (session.protocolVersion() < 3 && RequestDispatcher.isV3OnlyLockType(req.getLockType())) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        // 许可参数合法性（与单机分发器共用判定）：非法不入日志。
        StatusCode permitBad = RequestDispatcher.validateAcquirePermits(req);
        if (permitBad != null) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, permitBad));
            return;
        }
        // v9 门控：ACQUIRE 携带 condition 字段（await 折叠形态）仅对 v9 会话开放
        // （v≤8 消息级拒绝、不断连，判例 v3-v8 门），先于折叠受理路径。
        if (req.hasCondition()) {
            if (session.protocolVersion() < 9) {
                writeSync(ctx, session, startNanos,
                        RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
                return;
            }
            handleAcquireFold(session, msg, ctx, startNanos, req);
            return;
        }
        boolean queueWanted = req.getWaitMs() != 0;
        boolean semaphore = req.getLockType() == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_SEMAPHORE;
        boolean held = kernel.shadow().isHeld(req.getKey());
        // 重入豁免：请求归属已在持有集内时不得被 busy 拦截——
        // 与单机引擎"重入先于队列规则"对齐；重入判定读无锁快照（可旧不可错，
        // 误放行由应用路径裁决）。
        boolean reentrantHold = held && kernel.shadow().isHeldBy(
                session.sessionId(), req.getThreadId(), req.getKey());
        // 许可感知 busy：Semaphore 的"占用"是池不足而非有人持有
        // （多持有者共存是常态）；条目已回收视同池满量（重发带断言重建）。
        int permits = RequestDispatcher.normalizedPermits(req.getPermits());
        boolean poolShort = kernel.shadow().isSemaphore(req.getKey())
                && kernel.shadow().permitsAvailable(req.getKey()) < permits;
        // 队首重发且授予条件成立（锁：无人持有；Semaphore：池足量）：自推进
        // 走复制授予路径（AWAIT_NOTIFY 后重发，与直接受理语义等价；
        // onGranted 负责出队）。
        boolean selfPromotion = waitQueue.isHead(
                session.sessionId(), msg.getRequestId(), req.getKey())
                && (semaphore ? !poolShort : !held);
        boolean busy = (semaphore ? poolShort : (held && !reentrantHold))
                || (!selfPromotion && waitQueue.hasWaiters(req.getKey()));
        if (busy) {
            if (!queueWanted) {
                writeSync(ctx, session, startNanos, acquireErrorResponse(msg, StatusCode.DENIED));
                return;
            }
            int pos = waitQueue.enqueue(session.sessionId(), msg.getRequestId(), req.getKey(),
                    permits, System.currentTimeMillis());
            if (pos < 0) {
                writeSync(ctx, session, startNanos, acquireErrorResponse(msg, StatusCode.OVERLOADED));
                return;
            }
            writeSync(ctx, session, startNanos, Envelope.newBuilder()
                    .setProtocolVersion(msg.getProtocolVersion())
                    .setType(MessageType.LOCK_ACQUIRE)
                    .setRequestId(msg.getRequestId())
                    .setAcquireResponse(AcquireResponse.newBuilder()
                            .setStatus(StatusCode.QUEUED).setQueuePosition(pos))
                    .build());
            return;
        }
        // 可授予预演 → 复制路径。应用结果为准：预演失效时 Leader 在应用点
        // 补登记并改写回执（gateway.leaderSideEffects）。
        ByteString payload = AcquirePayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequestId(msg.getRequestId())
                .setRequest(req)
                .build().toByteString();
        gateway.submit(RaftEntryType.LOCK_ACQUIRE_ENTRY, payload)
                .whenComplete((r, err) -> respondAsync(ctx, session, startNanos,
                        err == null ? mapAcquire(msg, r) : commitFailure(msg, err)));
    }

    /**
     * await 折叠 ACQUIRE 的集群受理（v9，ACQUIRE 提交通道，无新条目类型）：
     * 登记半程在 Leader 受理预检点先于提交完成——此刻持有归属仍是 awaiter，
     * 任何第三方 SIGNAL 必被权限检查拒绝（"登记先于释放可见"不变式，丢唤醒窗
     * 为零）；释放半程随 {@code LOCK_ACQUIRE_ENTRY} 条目经复制在各副本确定
     * 重放（{@code LockStateMachineCore.applyAcquire} 折叠分支）。判定顺序：
     * 折叠形状（复用接入层唯一裁决 {@link RequestDispatcher#acquireFoldShapeValid}，
     * {@code condition} 另须非空且 UTF-8 字节数 ≤ {@code maxKeyLength}——空串/
     * 超长属形状违例，判例"判定唯一在接入层"）→
     * 装配守卫（无登记表=夹具形态，同型 {@code INTERNAL_ERROR}）→
     * 合并深度护栏（等待队列深度 + 登记集合计达
     * {@code max-queue-depth-per-key} 即 {@code OVERLOADED} 且登记零发生）→
     * 幂等登记（同 (会话, request_id) 重挂跳过登记与护栏、保留原登记时刻）→
     * 既有 {@code AcquirePayload} 提交通道。应答经 {@link #mapAcquireFold}
     * 改写；提交失败按 {@link #commitFailure} 既有拆分（此时登记随换主/
     * 会话生命周期收口，客户端重发同信封幂等命中）。
     *
     * @param session    已握手会话
     * @param msg        请求信封（{@code acquire_request} 携带 {@code condition}）
     * @param ctx        连接上下文
     * @param startNanos 受理起始时刻（自 {@code handleAcquire} 入口透传）
     * @param req        获取请求
     */
    private void handleAcquireFold(ServerSession session, Envelope msg,
                                   ChannelHandlerContext ctx, long startNanos,
                                   io.github.lamspace.openlatch.protocol.AcquireRequest req) {
        if (!RequestDispatcher.acquireFoldShapeValid(req)
                || !conditionNameValid(req.getCondition())) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        if (conditions == null) {
            log.warn("folded ACQUIRE handled without condition registry assembly");
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INTERNAL_ERROR));
            return;
        }
        String key = req.getKey();
        if (!conditions.isRegistered(session.sessionId(), msg.getRequestId(), key)) {
            // 合并深度护栏：本 key 等待项统一口径（等待队列 + 全部条件集）。
            // 幂等重挂（应答丢失自愈/换主车道迁移）跳过护栏与登记——
            // 同身份已在集，重发不得挤压位次或刷新登记时刻。
            if (waitQueue.waitCount(key) + conditions.count(key)
                    >= config.maxQueueDepthPerKey()) {
                writeSync(ctx, session, startNanos,
                        acquireErrorResponse(msg, StatusCode.OVERLOADED));
                return;
            }
            conditions.register(session.sessionId(), msg.getRequestId(), req.getThreadId(),
                    key, req.getCondition(), System.currentTimeMillis());
        }
        ByteString payload = AcquirePayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequestId(msg.getRequestId())
                .setRequest(req)
                .build().toByteString();
        gateway.submit(RaftEntryType.LOCK_ACQUIRE_ENTRY, payload)
                .whenComplete((r, err) -> respondAsync(ctx, session, startNanos,
                        err == null ? mapAcquireFold(msg, r) : commitFailure(msg, err)));
    }

    /**
     * {@link ApplyResult} → 折叠 await 应答改写（v9）：应用点 {@code OK} 表示
     * 释放半程守卫通过（恒含非持有零操作的重挂形态），登记半程已在受理预检点
     * 生效——回执改写为 {@code QUEUED}、位次=本 key 等待项合计读数（等待队列 +
     * 条件集，与单机 {@code LockEntry.awaitFold} 的位次口径同型）；其余状态
     * （会话失效/家族不匹配/内部）透传 {@link #mapAcquire} 既有映射。
     * {@code AcquireResponse} 零新字段（协议 v9 增量纪律）。
     *
     * @param msg    原请求信封
     * @param result 应用回执
     * @return 应答信封
     */
    private Envelope mapAcquireFold(Envelope msg, ApplyResult result) {
        if (result.getStatus() != ApplyStatus.OK) {
            return mapAcquire(msg, result);
        }
        String key = msg.getAcquireRequest().getKey();
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(msg.getRequestId())
                .setAcquireResponse(AcquireResponse.newBuilder()
                        .setStatus(StatusCode.QUEUED)
                        .setQueuePosition(waitQueue.waitCount(key) + conditions.count(key)))
                .build();
    }

    /**
     * RELEASE 集群路径（转发车道）：无排队裁决、不设角色门——Follower 亦
     * 照常提交，经内部通道由当值 Leader 复制执行；应答于应用后写回。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleRelease(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, false);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        // 归还数为负属参数非法（与单机分发器同规则），不入日志。
        if (msg.getReleaseRequest().getPermits() < 0) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        ByteString payload = ReleasePayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequest(msg.getReleaseRequest())
                .build().toByteString();
        gateway.submit(RaftEntryType.LOCK_RELEASE_ENTRY, payload)
                .whenComplete((r, err) -> respondAsync(ctx, session, startNanos,
                        err == null ? mapRelease(msg, r) : commitFailure(msg, err)));
    }

    /**
     * RENEW 集群路径（转发车道）：不设角色门，Follower 亦照常提交、由
     * 当值 Leader 复制执行；应答于应用后写回。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleRenew(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, false);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        ByteString payload = RenewPayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequest(msg.getLeaseRenewRequest())
                .build().toByteString();
        gateway.submit(RaftEntryType.LEASE_RENEW_ENTRY, payload)
                .whenComplete((r, err) -> respondAsync(ctx, session, startNanos,
                        err == null ? mapRenew(msg, r) : commitFailure(msg, err)));
    }

    /**
     * 公共预检：载荷合法性（类型匹配、键非空、UTF-8 长度）与会话登记；
     * {@code requireLeader=true}（ACQUIRE 车道）时先过权威角色门——非 Leader
     * 回 {@code NOT_LEADER} 并随附 {@link LeaderTracker} 当时的提示。
     * 转发车道（RELEASE/RENEW）不设角色门：条目经内部通道抵达当值 Leader，
     * 权威判定在应用点。会话登记预检两车道同规则：连接 sid 于握手完成前已
     * 在本副本应用，本地判 {@code SESSION_EXPIRED} 与 Leader 判定
     * 结果一致且省一次转发。返回非 {@code null} 即为应立即写回的同步错误。
     *
     * @param msg          请求信封
     * @param session      已握手会话
     * @param requireLeader 是否要求本节点为当值 Leader（ACQUIRE 车道 true）
     * @return 需立即写回的错误应答；通过预检为 {@code null}
     */
    /**
     * LATCH_COUNT_DOWN 集群路径（转发车道）：计数变更属复制状态，
     * 与 RELEASE 同车道不设角色门——Follower 提交经内部通道由当值 Leader
     * 复制执行；等待队列不在日志内。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleLatchCountDown(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, false);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 3) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        var req = msg.getLatchCountDownRequest();
        if (req.getCount() < 0 || req.getTotal() < 0) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        ByteString payload = LatchCountDownPayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequest(req)
                .build().toByteString();
        gateway.submit(RaftEntryType.LATCH_COUNT_DOWN_ENTRY, payload)
                .whenComplete((r, err) -> respondAsync(ctx, session, startNanos,
                        err == null ? mapLatchCountDown(msg, r) : commitFailure(msg, err)));
    }

    /**
     * LATCH_AWAIT 集群路径（ACQUIRE 车道 + Leader 本地裁决）：
     * await MUST NOT 进日志——但携带 {@code total} 的定型创建属计数变更，
     * 先经 {@code LATCH_COUNT_DOWN_ENTRY(count=0)} 复制定型，
     * 提交确认后回到本地裁决；已归零直接放行（幂等重发抵达处），
     * 未归零在 Leader 内存队列挂起（归零广播经 {@code AWAIT_NOTIFY}）。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleLatchAwait(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, true);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 3) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        var req = msg.getLatchAwaitRequest();
        if (req.getTotal() < 0) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        var shadow = kernel.shadow();
        // 家族误用预检（与引擎家族判定对齐）：key 已有他家族条目时直接
        // INVALID_REQUEST，不进入定型创建也不入队。
        if (!shadow.hasLatch(req.getKey()) && shadow.isHeld(req.getKey())) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        if (!shadow.hasLatch(req.getKey()) && req.getTotal() > 0) {
            // 定型创建：纯初始化条目进日志，应用确认后回本地 await 裁决。
            ByteString payload = LatchCountDownPayload.newBuilder()
                    .setSessionId(session.sessionId())
                    .setRequest(io.github.lamspace.openlatch.protocol.LatchCountDownRequest.newBuilder()
                            .setKey(req.getKey()).setCount(0).setTotal(req.getTotal()))
                    .build().toByteString();
            gateway.submit(RaftEntryType.LATCH_COUNT_DOWN_ENTRY, payload)
                    .whenComplete((r, err) -> {
                        if (err != null) {
                            respondAsync(ctx, session, startNanos, commitFailure(msg, err));
                            return;
                        }
                        if (r.getStatus() != ApplyStatus.OK) {
                            respondAsync(ctx, session, startNanos, mapLatchAwait(msg, r));
                            return;
                        }
                        latchAwaitLocal(session, msg, ctx, startNanos);
                    });
            return;
        }
        latchAwaitLocal(session, msg, ctx, startNanos);
    }

    /**
     * await 的 Leader 本地裁决：屏障不存在（含纯加入）回
     * {@code INVALID_REQUEST}；已归零回 {@code OK}（顺带出队重发抵达的
     * 等待项）；未归零挂本地队列回 {@code QUEUED} 位次。
     *
     * @param session     已握手会话
     * @param msg         请求信封
     * @param ctx         连接上下文
     * @param startNanos  受理起始时刻（自 {@code handleLatchAwait} 入口或
     *                    定型创建提交回调透传，指标耗时含创建提交段）
     */
    private void latchAwaitLocal(ServerSession session, Envelope msg, ChannelHandlerContext ctx,
                                 long startNanos) {
        var req = msg.getLatchAwaitRequest();
        var shadow = kernel.shadow();
        long count = shadow.latchCount(req.getKey());
        if (count < 0) {
            writeSync(ctx, session, startNanos, latchAwaitResponse(msg, StatusCode.INVALID_REQUEST, 0));
            return;
        }
        if (count == 0) {
            waitQueue.onGranted(session.sessionId(), msg.getRequestId());
            writeSync(ctx, session, startNanos, latchAwaitResponse(msg, StatusCode.OK, 0));
            return;
        }
        int pos = waitQueue.enqueue(session.sessionId(), msg.getRequestId(), req.getKey(),
                System.currentTimeMillis());
        if (pos < 0) {
            writeSync(ctx, session, startNanos, latchAwaitResponse(msg, StatusCode.OVERLOADED, 0));
            return;
        }
        writeSync(ctx, session, startNanos, latchAwaitResponse(msg, StatusCode.QUEUED, pos));
    }

    /**
     * 屏障等待应答构造。
     *
     * @param msg    原请求
     * @param status 状态码
     * @param pos    队列位次（QUEUED 有效）
     * @return 应答信封
     */
    private static Envelope latchAwaitResponse(Envelope msg, StatusCode status, int pos) {
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.LATCH_AWAIT)
                .setRequestId(msg.getRequestId())
                .setLatchAwaitResponse(io.github.lamspace.openlatch.protocol.LatchAwaitResponse
                        .newBuilder().setStatus(status).setQueuePosition(pos))
                .build();
    }

    /**
     * {@link ApplyResult} → LatchCountDownResponse（码形与单机
     * {@code toLatchStatus} 对齐；OK 携带剩余计数）。
     *
     * @param msg    原请求
     * @param result 应用回执
     * @return 应答信封
     */
    static Envelope mapLatchCountDown(Envelope msg, ApplyResult result) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.LATCH_COUNT_DOWN)
                .setRequestId(msg.getRequestId())
                .setLatchCountDownResponse(io.github.lamspace.openlatch.protocol.LatchCountDownResponse
                        .newBuilder().setStatus(st).setRemaining(result.getLatchRemaining()))
                .build();
    }

    /**
     * {@link ApplyResult} → LatchAwaitResponse（定型创建提交失败路径的
     * 回执翻译；本地裁决不经此）。
     *
     * @param msg    原请求
     * @param result 应用回执
     * @return 应答信封
     */
    static Envelope mapLatchAwait(Envelope msg, ApplyResult result) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        return latchAwaitResponse(msg, st, 0);
    }

    /**
     * ATOMIC_OP 集群路径（转发车道）：值变更与 GET 读数皆属复制状态裁决，
     * 与 LATCH_COUNT_DOWN 同车道不设角色门——Follower 提交经内部通道由当值
     * Leader 复制执行（GET 亦进日志：线性一致读数，见 v4 设计裁决，重放零迁移）。
     * v4 门控与形状合法性先于提交——非法请求零入日志（与单机分发器共用
     * {@link RequestDispatcher#validateAtomicRequest} 判定）。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleAtomicOp(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, false);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 4) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        var req = msg.getAtomicOpRequest();
        if (req.getLockType()
                == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_REFERENCE
                && session.protocolVersion() < 6) {
            // v6 专属语义：低版本会话引用形态消息级拒绝、不断连（判例各门）。
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        StatusCode shapeBad = RequestDispatcher.validateAtomicRequest(req);
        if (shapeBad == null) {
            // v6 载荷入口钳制：超限命令 MUST NOT 入日志（判定唯一在接入层，
            // 引擎/条目侧不复核——防节点配置漂移引入回放分歧）。
            shapeBad = RequestDispatcher.validateRefPayloadClamp(req, config.maxValueBytes());
        }
        if (shapeBad != null) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, shapeBad));
            return;
        }
        ByteString payload = AtomicOpPayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequestId(msg.getRequestId())
                .setRequest(req)
                .build().toByteString();
        gateway.submit(RaftEntryType.ATOMIC_OP_ENTRY, payload)
                .whenComplete((r, err) -> {
                    Envelope resp = err == null ? mapAtomicOp(msg, r) : commitFailure(msg, err);
                    if (metrics != null) {
                        metrics.recordAtomic(req.getLockType(), req.getOp(),
                                resp.getAtomicOpResponse().getStatus());
                    }
                    respondAsync(ctx, session, startNanos, resp);
                });
    }

    /**
     * QUEUE_OP 集群路径（v7，Leader 权威车道）：挂起是 Leader 本地
     * {@code WaitQueue} 双轨态、预检与挂起登记仅在当值 Leader 发生，故与
     * LATCH_AWAIT 同型设角色门——非 Leader 回 {@code NOT_LEADER} 随附提示，
     * 客户端改道既有车道。门控/形状/入口钳制先于一切（非法请求零日志，
     * 钳定后的 DRAIN 上限随条目入日志保回放一致）。阻塞式预检读影子表：
     * PUT 按容量空位判定（key 缺席视为可满足——定型创建经提交落地）、
     * TAKE 按队首可见性判定（DELAY 要求队首到期不晚于当前时刻）；不可满足
     * 则入轨回 {@code QUEUED}（零日志）。预检与提交间竞态致应用点
     * {@code DENIED} 的回弹重挂由 {@code ReplicationGateway.queueSideEffects}
     * 改写回执（判例锁预演失效改写）；唤醒推送同在该处。
     *
     * @param session 已握手会话
     * @param msg     请求信封（{@code queue_op_request} 分支）
     * @param ctx     连接上下文
     */
    public void handleQueueOp(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, true);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 7) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        QueueOpRequest req = msg.getQueueOpRequest();
        StatusCode shapeBad = RequestDispatcher.validateQueueShape(req);
        RequestDispatcher.QueueClamp clamp = shapeBad == null
                ? RequestDispatcher.clampQueueRequest(req, config.maxValueBytes(),
                        config.maxQueueCapacity(), config.maxDrainBytes())
                : new RequestDispatcher.QueueClamp(shapeBad, null);
        if (clamp.error() != null) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, clamp.error()));
            return;
        }
        QueueOpRequest finalReq = clamp.request();
        long now = System.currentTimeMillis();
        if (finalReq.getBlocking()) {
            var shadow = kernel.shadow();
            String key = finalReq.getKey();
            int track = finalReq.getOp() == QueueOp.QUEUE_OP_PUT ? 1 : 2;
            // 重发队首门：已挂起且非队首的早到重发不提交（防插队，判例锁
            // "队首已通知窗口禁止越过"）——回显其位次继续等待唤醒事件。
            int parkedPos = waitQueue.trackPosition(session.sessionId(), msg.getRequestId(),
                    key, track);
            if (parkedPos > 1) {
                writeSync(ctx, session, startNanos,
                        queueOpLocalResponse(msg, StatusCode.QUEUED, parkedPos, finalReq));
                return;
            }
            boolean satisfiable;
            if (finalReq.getOp() == QueueOp.QUEUE_OP_PUT) {
                satisfiable = !shadow.isQueue(key)
                        || shadow.queueDepth(key) < shadow.queueCapacity(key);
            } else {
                long expiry = shadow.queueHeadExpiryMs(key);
                satisfiable = shadow.queueDepth(key) > 0 && (expiry == 0 || expiry <= now);
            }
            if (!satisfiable) {
                // 队首重发但不满足经幂等命中续约通知窗口（判例锁重发抵达）。
                int pos = waitQueue.enqueueTrack(session.sessionId(), msg.getRequestId(),
                        key, track, now);
                StatusCode st = pos < 0 ? StatusCode.OVERLOADED : StatusCode.QUEUED;
                writeSync(ctx, session, startNanos, queueOpLocalResponse(msg, st, pos, finalReq));
                return;
            }
        }
        ByteString payload = QueueOpPayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequestId(msg.getRequestId())
                .setRequest(finalReq)
                .build().toByteString();
        gateway.submit(RaftEntryType.QUEUE_OP_ENTRY, payload)
                .whenComplete((r, err) -> {
                    Envelope resp = err == null ? mapQueueOp(msg, r, finalReq)
                            : commitFailure(msg, err);
                    if (metrics != null) {
                        metrics.recordQueue(finalReq.getOp(),
                                resp.getQueueOpResponse().getStatus());
                    }
                    respondAsync(ctx, session, startNanos, resp);
                });
    }

    /**
     * {@link ApplyResult} → QueueOpResponse（v7）：状态码映射沿既有口径
     * （{@code QUEUE_FULL}→{@code OVERLOADED} 判例锁深度护栏）；交付字段
     * 按显式 presence 透传（空串元素=零长度存在，与"无元素"的缺省两态
     * 可辨）；容量回显取影子表弱一致读数（仅便利，非裁决）。
     *
     * @param msg    原请求
     * @param result 应用回执
     * @param req    钳定后的请求（op 回显用）
     * @return 应答信封
     */
    private Envelope mapQueueOp(Envelope msg, ApplyResult result, QueueOpRequest req) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case QUEUED -> StatusCode.QUEUED;
            case DENIED -> StatusCode.DENIED;
            case QUEUE_FULL -> StatusCode.OVERLOADED;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        QueueOpResponse.Builder b = QueueOpResponse.newBuilder()
                .setStatus(st)
                .setOp(req.getOp())
                .setQueuePosition(result.getQueuePosition());
        if (result.hasQueueElementBytes()) {
            b.setElementBytes(result.getQueueElementBytes());
        }
        b.addAllDrainedBytes(result.getQueueDrainedBytesList());
        if (result.getQueueSize() != 0) {
            b.setSize(result.getQueueSize());
        }
        if (st == StatusCode.OK || st == StatusCode.QUEUED || st == StatusCode.DENIED) {
            b.setCapacity(kernel.shadow().queueCapacity(req.getKey()));
        }
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.QUEUE_OP)
                .setRequestId(msg.getRequestId())
                .setQueueOpResponse(b)
                .build();
    }

    /**
     * 本地挂起/拒绝的队列应答构造（不经 ApplyResult 的同步路径，
     * 复用 {@link #mapQueueOp} 装配形态）。
     *
     * @param msg      原请求
     * @param st       协议状态码（QUEUED/OVERLOADED）
     * @param position 轨内位次（QUEUED 有效）
     * @param req      钳定后的请求
     * @return 应答信封
     */
    private Envelope queueOpLocalResponse(Envelope msg, StatusCode st, int position,
            QueueOpRequest req) {
        ApplyStatus as = st == StatusCode.QUEUED ? ApplyStatus.QUEUED : ApplyStatus.QUEUE_FULL;
        return mapQueueOp(msg, ApplyResult.newBuilder()
                .setStatus(as)
                .setQueuePosition(position)
                .build(), req);
    }

    /**
     * TOPIC_OP 集群路径（v8，<b>零复制日志</b>——topic 无复制状态，登记表为
     * Leader 本地易失态，判例 {@code WaitQueue} 换主清零）：统一预检
     * （角色门/载荷/键长/会话在场）后由 Leader 本地直接受理：v8 门、形状
     * 互斥、{@code maxValueBytes} 钳制、撞 key 影子表只读探测（内部裁决
     * {@code REJECT_TYPE_MISMATCH}、线路送达 {@code INVALID_REQUEST}）逐层
     * 前置于登记表调用——全部拒绝态零登记、零扇出、零日志。受理恒同步
     * 回执（无挂起面）：SUBSCRIBE 幂等登记（达上限 {@code REJECT_SUBSCRIBERS}）、
     * UNSUBSCRIBE 幂等摘除、PUBLISH 经 (key, 会话) 去重槽受理并即时 fan-out。
     * Follower 抵达的请求在 {@code validateEnvelope} 角色门即被同型
     * {@code NOT_LEADER} 拒绝，绝不触达登记表。
     *
     * @param session 已握手会话
     * @param msg     请求信封（{@code topic_op_request} 分支）
     * @param ctx     连接上下文
     */
    public void handleTopicOp(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, true);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 8) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        if (topics == null) {
            log.warn("TOPIC_OP handled without topic registry assembly");
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INTERNAL_ERROR));
            return;
        }
        io.github.lamspace.openlatch.protocol.TopicOpRequest req = msg.getTopicOpRequest();
        StatusCode shapeBad = RequestDispatcher.validateTopicShape(req);
        if (shapeBad != null) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(msg, shapeBad));
            return;
        }
        if (req.hasPayloadBytes() && req.getPayloadBytes().size() > config.maxValueBytes()) {
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(
                    msg, StatusCode.INVALID_REQUEST));
            return;
        }
        String key = req.getKey();
        boolean mutating = req.getOp() != io.github.lamspace.openlatch.protocol.TopicOp.TOPIC_OP_UNSUBSCRIBE;
        if (mutating && kernel.shadow().adminEntry(key) != null) {
            // 撞 key 尽力而为探测（与并发建条目存在竞态窗——"一 key 一形态"
            // 应用侧契约）；退订不探测（幂等摘除无建立副作用）。
            writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(
                    msg, StatusCode.INVALID_REQUEST));
            return;
        }
        long now = System.currentTimeMillis();
        io.github.lamspace.openlatch.protocol.TopicOpResponse.Builder out =
                io.github.lamspace.openlatch.protocol.TopicOpResponse.newBuilder()
                        .setOp(req.getOp());
        switch (req.getOp()) {
            case TOPIC_OP_SUBSCRIBE -> {
                var r = topics.subscribe(session.sessionId(), key, now);
                out.setStatus(r.status()).setSubscriptionId(r.subscriptionId());
            }
            case TOPIC_OP_UNSUBSCRIBE -> {
                topics.unsubscribe(session.sessionId(), key);
                out.setStatus(StatusCode.OK);
            }
            case TOPIC_OP_PUBLISH -> {
                var r = topics.publish(session.sessionId(), key, req.getOpSeq(),
                        req.getPayloadBytes().toByteArray(), now);
                out.setStatus(StatusCode.OK).setTopicSeq(r.topicSeq());
            }
            default -> {
                writeSync(ctx, session, startNanos, RequestDispatcher.errorResponse(
                        msg, StatusCode.INVALID_REQUEST));
                return;
            }
        }
        if (metrics != null) {
            metrics.recordTopic(req.getOp(), out.getStatus());
        }
        writeSync(ctx, session, startNanos, Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.TOPIC_OP)
                .setRequestId(msg.getRequestId())
                .setTopicOpResponse(out)
                .build());
    }

    /**
     * CONDITION_OP 集群路径（v9，<b>Leader 本地直裁决、零日志</b>——signal 家族
     * 只搬运 Leader 本地条件等待集→本地等待队列、发本地推送，不触碰任何复制态，
     * "signal 是事件不是状态"，判例 v8 topic 零日志豁免的类目化）：统一预检
     * （角色门/载荷/键长/会话在场）后由 Leader 直接受理：v9 门、op×字段形状矩阵、
     * {@code condition} 名非空与长度钳制逐层前置于搬运——全部拒绝态零搬运、
     * 零摘除、零日志。判定顺序与裁决权威：SIGNAL/SIGNAL_ALL 先形态守卫
     * （影子表该 key 有持有但其形态非互斥三型 REENTRANT/SIMPLE/FAIR 时
     * {@code INVALID_REQUEST}——读侧尽力而为，与并发建条目存在竞态窗，判例
     * topic 撞 key 探测），再权限权威检查（影子表 (会话,线程) 恰为持有归属，
     * 否则 {@code NOT_HELD}——判例释放校验）；通过后按条件名搬运非调用归属
     * 等待项入 {@link WaitQueue}（到达序；搬运恒发生于锁被持时点，唤醒由后续
     * 释放/到期/会话关闭应用点的队首通知接力——事件驱动闭环，零新定时器）；
     * 搬运项入队溢出仅记 WARN（等待者经重发自愈，判例 barrier 溢出臂）。
     * LEAVE 幂等摘除（未存在的登记无错误态、恒 {@code OK}）。空集/无此条件/
     * 无该 key 均 {@code OK} 无操作（JDK 对齐，ghost 无害面）。Follower 抵达的
     * 请求在 {@code validateEnvelope} 角色门即被同型 {@code NOT_LEADER} 拒绝
     * 且零副作用（无 leader 提示字段，判例 QUEUE/TOPIC 直发车道）。
     *
     * @param session 已握手会话
     * @param msg     请求信封（{@code condition_op_request} 分支）
     * @param ctx     连接上下文
     */
    public void handleConditionOp(ServerSession session, Envelope msg,
                                  ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, true);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 9) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        if (conditions == null) {
            log.warn("CONDITION_OP handled without condition registry assembly");
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INTERNAL_ERROR));
            return;
        }
        io.github.lamspace.openlatch.protocol.ConditionOpRequest req = msg.getConditionOpRequest();
        if (RequestDispatcher.conditionOpShapeInvalid(req)
                || !conditionNameValid(req.getCondition())) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        String key = req.getKey();
        StatusCode status;
        switch (req.getOp()) {
            case CONDITION_OP_SIGNAL, CONDITION_OP_SIGNAL_ALL -> {
                // 形态守卫双源：heldRef（有写侧持有时可见）之外再查 adminEntry
                // （全部条目观察视图，弱一致拒绝性读，判例撞 key 探测）——空闲的
                // LATCH/ATOMIC/QUEUE/SEMAPHORE 形态 key 同判 INVALID_REQUEST，与
                // 单机 core 门面家族判定对称；READ/WRITE 属 LOCK 家族、单机条目
                // 不携带形态位，其 signal 拒绝面为集群 adminView 尽力而为
                // （request 侧折叠形状两拓扑均已拒，SDK 对读/写锁 newCondition 抛
                // UnsupportedOperationException，裸协议窗在此声明）。
                var view = kernel.shadow().adminEntry(key);
                int lt = view != null ? view.lockType()
                        : (kernel.shadow().heldRef(key) != null
                                ? kernel.shadow().heldRef(key).lockType() : -1);
                if (lt >= 0 && lt != io.github.lamspace.openlatch.protocol
                        .LockType.LOCK_TYPE_REENTRANT.getNumber()
                        && lt != io.github.lamspace.openlatch.protocol
                        .LockType.LOCK_TYPE_SIMPLE.getNumber()
                        && lt != io.github.lamspace.openlatch.protocol
                        .LockType.LOCK_TYPE_FAIR.getNumber()) {
                    status = StatusCode.INVALID_REQUEST;
                    break;
                }
                if (!kernel.shadow().isHeldBy(session.sessionId(), req.getThreadId(), key)) {
                    status = StatusCode.NOT_HELD;
                    break;
                }
                long now = System.currentTimeMillis();
                java.util.List<io.github.lamspace.openlatch.server.condition
                        .ConditionRegistry.Promoted> promoted =
                        req.getOp() == io.github.lamspace.openlatch.protocol
                                .ConditionOp.CONDITION_OP_SIGNAL
                                ? conditions.promoteFirst(key, req.getCondition(),
                                        session.sessionId(), req.getThreadId())
                                        .map(java.util.List::of)
                                        .orElse(java.util.List.of())
                                : conditions.promoteAll(key, req.getCondition(),
                                        session.sessionId(), req.getThreadId());
                for (var p : promoted) {
                    if (waitQueue.enqueue(p.sessionId(), p.requestId(), key, 1, now) < 0) {
                        // 护栏合并口径下理论可撞等待队列独立上限：溢出仅记
                        // WARN 不登记（该等待者经重发自愈，判例 barrier 溢出臂）。
                        log.warn("condition promoted waiter enqueue overflow "
                                + "(session={}, request={}, key={})",
                                p.sessionId(), p.requestId(), key);
                    }
                }
                status = StatusCode.OK;
            }
            case CONDITION_OP_LEAVE -> {
                conditions.leave(session.sessionId(), req.getAwaitRequestId());
                status = StatusCode.OK;
            }
            default -> {
                writeSync(ctx, session, startNanos,
                        RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
                return;
            }
        }
        if (metrics != null) {
            metrics.recordCondition(req.getOp(), status);
        }
        writeSync(ctx, session, startNanos, Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.CONDITION_OP)
                .setRequestId(msg.getRequestId())
                .setConditionOpResponse(io.github.lamspace.openlatch.protocol
                        .ConditionOpResponse.newBuilder()
                        .setStatus(status)
                        .setOp(req.getOp()))
                .build());
    }

    /**
     * 条件名合法性（接入层唯一裁决的组成部分，判例键长纪律）：非空且 UTF-8
     * 字节数不超过 {@code maxKeyLength}（与键同界，协议 v9 增量条款钉定）。
     *
     * @param condition 条件名（协议 {@code string} 字段值）
     * @return 合法返回 {@code true}
     */
    private boolean conditionNameValid(String condition) {
        return !condition.isEmpty()
                && condition.getBytes(StandardCharsets.UTF_8).length <= config.maxKeyLength();
    }

    /**
     * PHASER_OP 集群路径（v10，Leader 权威车道双轨分派——判例
     * {@code handleQueueOp}/{@code handleBarrierAwait} 的合成形态）：统一预检
     * （角色门/载荷/键长/会话在场经 {@code validateEnvelope}，v10 门与形状
     * 互斥矩阵前置，全部拒绝态零提交、簿记零扰动）后按操作词分两轨：
     *
     * <ul>
     *   <li><b>变异操作</b>（REGISTER/ARRIVE/ARRIVE_AND_AWAIT/
     *   ARRIVE_AND_DEREGISTER）经 {@code PHASER_OP_ENTRY} 提交——账簿迁移在
     *   应用点确定性重放；受理点仅做<b>配额上限预检</b>（影子表 registered +
     *   parties 与 config 上限、家族/无条目在带拒绝，超限零条目——上限判定
     *   唯一在受理点，应用侧不复核）与 {@code ARRIVE_AND_AWAIT} 的簿记联动
     *   （回执 QUEUED 的等待登记、OK 的簿记摘除与推进唤醒广播均在
     *   {@code ReplicationGateway} 应用副作用完成，与提交串行点同线程）。</li>
     *   <li><b>本地操作词</b>（AWAIT_ADVANCE/CANCEL/QUERY）零日志直答——
     *   等待谓词读影子表账簿（{@code expected < phase} 即刻了结；挂起入
     *   {@code PhaserRegistry} 簿记并受合并深度护栏）；簿记为 Leader 本地态，
     *   Follower 抵达已在角色门同型拒绝。</li>
     * </ul>
     *
     * @param session 已握手会话
     * @param msg     请求信封（{@code phaser_op_request} 分支）
     * @param ctx     连接上下文
     */
    public void handlePhaserOp(ServerSession session, Envelope msg,
                               ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, true);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 10) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        if (phasers == null) {
            log.warn("PHASER_OP handled without phaser registry assembly");
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INTERNAL_ERROR));
            return;
        }
        io.github.lamspace.openlatch.protocol.PhaserOpRequest req = msg.getPhaserOpRequest();
        if (RequestDispatcher.phaserShapeInvalid(req)) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        String key = req.getKey();
        var view = kernel.shadow().adminEntry(key);
        boolean isPhaser = view != null && ShadowTable.isPhaserType(view.lockType());
        var op = req.getOp();
        // 家族/无条目在带裁决（零条目、零簿记扰动；判例 signal 形态守卫双源）。
        if (view != null && !isPhaser) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        switch (op) {
            case PHASER_OP_REGISTER, PHASER_OP_ARRIVE, PHASER_OP_ARRIVE_AND_AWAIT,
                    PHASER_OP_ARRIVE_AND_DEREGISTER -> {
                if (op == io.github.lamspace.openlatch.protocol.PhaserOp.PHASER_OP_REGISTER) {
                    int registered = isPhaser ? view.phaserRegistered() : 0;
                    if ((long) registered + req.getParties() > config.maxPartiesPerPhaser()) {
                        respondPhaser(ctx, session, startNanos, msg,
                                StatusCode.OVERLOADED, 0, 0, 0);
                        return;
                    }
                } else if (!isPhaser) {
                    writeSync(ctx, session, startNanos,
                            RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
                    return;
                }
                io.github.lamspace.openlatch.protocol.raft.PhaserOpPayload payload =
                        io.github.lamspace.openlatch.protocol.raft.PhaserOpPayload.newBuilder()
                        .setSessionId(session.sessionId())
                        .setRequestId(msg.getRequestId())
                        .setRequest(req)
                        .build();
                gateway.submit(io.github.lamspace.openlatch.protocol.raft.RaftEntryType
                        .PHASER_OP_ENTRY, payload.toByteString())
                        .whenComplete((r, err) -> {
                            Envelope resp = err == null
                                    ? mapPhaserOp(msg, r)
                                    : commitFailure(msg, err);
                            if (metrics != null && resp.hasPhaserOpResponse()) {
                                metrics.recordPhaser(op,
                                        resp.getPhaserOpResponse().getStatus());
                            }
                            respondAsync(ctx, session, startNanos, resp);
                        });
            }
            case PHASER_OP_AWAIT_ADVANCE -> {
                if (!isPhaser) {
                    writeSync(ctx, session, startNanos,
                            RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
                    return;
                }
                long now = System.currentTimeMillis();
                long expected = req.getExpectedPhase();
                if (view.phaserPhase() > expected) {
                    // 谓词已越（含唤醒后的了结重发与换主重挂）：即刻了结。
                    phasers.remove(key, session.sessionId(), msg.getRequestId());
                    respondPhaser(ctx, session, startNanos, msg, StatusCode.OK,
                            view.phaserPhase(), view.phaserRegistered(),
                            view.phaserArrived());
                    return;
                }
                if (phasers.count(key) >= config.maxQueueDepthPerKey()) {
                    respondPhaser(ctx, session, startNanos, msg, StatusCode.OVERLOADED,
                            0, 0, 0);
                    return;
                }
                phasers.register(key, session.sessionId(), msg.getRequestId(), expected, now);
                respondPhaser(ctx, session, startNanos, msg, StatusCode.QUEUED,
                        view.phaserPhase(), view.phaserRegistered(), view.phaserArrived());
            }
            case PHASER_OP_CANCEL -> {
                phasers.remove(key, session.sessionId(), req.getAwaitRequestId());
                respondPhaser(ctx, session, startNanos, msg, StatusCode.OK,
                        isPhaser ? view.phaserPhase() : 0,
                        isPhaser ? view.phaserRegistered() : 0,
                        isPhaser ? view.phaserArrived() : 0);
            }
            case PHASER_OP_QUERY -> {
                if (!isPhaser) {
                    writeSync(ctx, session, startNanos,
                            RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
                    return;
                }
                respondPhaser(ctx, session, startNanos, msg, StatusCode.OK,
                        view.phaserPhase(), view.phaserRegistered(), view.phaserArrived());
            }
            default -> writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
        }
    }

    /**
     * TIMER_OP 集群路径（v11，Leader 权威车道双轨分派——判例
     * {@code handlePhaserOp}）。SCHEDULE/DISARM 为**变异操作**，经
     * {@code TIMER_OP_ENTRY} 提交（受理点预检=门控/形状/horizon/家族/无条目，
     * 越界与违例零条目在带拒绝）；AWAIT/CANCEL/QUERY 为 **Leader 本地操作**，
     * 直读影子账簿谓词判定与簿记登记（"等待是订阅不是状态"——到期共见的
     * 判定基于复制态镜像 + Leader 本地时钟，MUST NOT 触达提交通道）。
     * 非 Leader 节点的全量同型 {@code NOT_LEADER} 拒绝在入口门
     * （{@code validateEnvelope} 后按角色分派）承载，无提示字段判例族。
     *
     * @param session 已握手会话（v11 协商）
     * @param msg     请求信封（{@code timer_op_request} 分支）
     * @param ctx     连接上下文
     */
    public void handleTimerOp(ServerSession session, Envelope msg,
                              ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, true);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 11) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        if (timers == null) {
            log.warn("TIMER_OP handled without timer registry assembly");
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INTERNAL_ERROR));
            return;
        }
        io.github.lamspace.openlatch.protocol.TimerOpRequest req = msg.getTimerOpRequest();
        if (RequestDispatcher.timerShapeInvalid(req)) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        String key = req.getKey();
        var view = kernel.shadow().adminEntry(key);
        boolean isTimer = view != null && ShadowTable.isTimerType(view.lockType());
        // 家族/无条目在带裁决（零条目、零簿记扰动；判例 phaser 双源守卫）。
        if (view != null && !isTimer) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        var op = req.getOp();
        switch (op) {
            case TIMER_OP_SCHEDULE -> {
                if (!req.hasDelayMs() || req.getDelayMs() < 0
                        || req.getDelayMs() > config.maxTimerHorizonMs()) {
                    // horizon 判定唯一在受理点（越界零条目；条目应用侧不复核）。
                    writeSync(ctx, session, startNanos,
                            RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
                    return;
                }
                io.github.lamspace.openlatch.protocol.raft.TimerOpPayload payload =
                        io.github.lamspace.openlatch.protocol.raft.TimerOpPayload.newBuilder()
                        .setSessionId(session.sessionId())
                        .setRequestId(msg.getRequestId())
                        .setRequest(req)
                        .build();
                gateway.submit(io.github.lamspace.openlatch.protocol.raft.RaftEntryType
                        .TIMER_OP_ENTRY, payload.toByteString())
                        .whenComplete((r, err) -> {
                            Envelope resp = err == null
                                    ? mapTimerOp(msg, r)
                                    : commitFailure(msg, err);
                            if (metrics != null && resp.hasTimerOpResponse()) {
                                metrics.recordTimer(op,
                                        resp.getTimerOpResponse().getStatus());
                            }
                            respondAsync(ctx, session, startNanos, resp);
                        });
            }
            case TIMER_OP_DISARM -> {
                if (!isTimer) {
                    writeSync(ctx, session, startNanos,
                            RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
                    return;
                }
                io.github.lamspace.openlatch.protocol.raft.TimerOpPayload payload =
                        io.github.lamspace.openlatch.protocol.raft.TimerOpPayload.newBuilder()
                        .setSessionId(session.sessionId())
                        .setRequestId(msg.getRequestId())
                        .setRequest(req)
                        .build();
                gateway.submit(io.github.lamspace.openlatch.protocol.raft.RaftEntryType
                        .TIMER_OP_ENTRY, payload.toByteString())
                        .whenComplete((r, err) -> {
                            Envelope resp = err == null
                                    ? mapTimerOp(msg, r)
                                    : commitFailure(msg, err);
                            if (metrics != null && resp.hasTimerOpResponse()) {
                                metrics.recordTimer(op,
                                        resp.getTimerOpResponse().getStatus());
                            }
                            respondAsync(ctx, session, startNanos, resp);
                        });
            }
            case TIMER_OP_AWAIT -> {
                if (!isTimer) {
                    writeSync(ctx, session, startNanos,
                            RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
                    return;
                }
                long now = System.currentTimeMillis();
                if (!view.timerArmed()) {
                    // 代终结即刻 DENIED（等待不可满足的显式终态——撤销唤醒后的
                    // 重发同样落此臂，终态幂等）。
                    timers.remove(key, session.sessionId(), msg.getRequestId());
                    respondTimer(ctx, session, startNanos, msg, StatusCode.DENIED,
                            view.timerGeneration(), false, view.timerFireAtMs(), false);
                    return;
                }
                if (now >= view.timerFireAtMs()) {
                    // 谓词已越（含唤醒后的了结重发与换主重挂）：即刻共见。
                    timers.remove(key, session.sessionId(), msg.getRequestId());
                    respondTimer(ctx, session, startNanos, msg, StatusCode.OK,
                            view.timerGeneration(), true, view.timerFireAtMs(), true);
                    return;
                }
                if (timers.count(key) >= config.maxQueueDepthPerKey()) {
                    respondTimer(ctx, session, startNanos, msg, StatusCode.OVERLOADED,
                            0, false, 0, false);
                    return;
                }
                timers.register(key, session.sessionId(), msg.getRequestId(), now);
                respondTimer(ctx, session, startNanos, msg, StatusCode.QUEUED,
                        view.timerGeneration(), true, view.timerFireAtMs(), false);
            }
            case TIMER_OP_CANCEL -> {
                timers.remove(key, session.sessionId(), req.getAwaitRequestId());
                respondTimer(ctx, session, startNanos, msg, StatusCode.OK,
                        isTimer ? view.timerGeneration() : 0,
                        isTimer && view.timerArmed(),
                        isTimer ? view.timerFireAtMs() : 0, false);
            }
            case TIMER_OP_QUERY -> {
                if (!isTimer) {
                    writeSync(ctx, session, startNanos,
                            RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
                    return;
                }
                boolean marked = view.timerArmed()
                        && System.currentTimeMillis() >= view.timerFireAtMs();
                respondTimer(ctx, session, startNanos, msg, StatusCode.OK,
                        view.timerGeneration(), view.timerArmed(), view.timerFireAtMs(), marked);
            }
            default -> writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
        }
    }

    /**
     * TIMER 变异操作回执映射：应用回执 → 协议应答（零值形拒绝态不携三元组；
     * marked 在读回侧按判定时刻折算——条目回执无 marked 位，派生裁决的
     * 应答线呈现）。
     *
     * @param msg 原请求信封
     * @param r   应用回执
     * @return 应答信封
     */
    private Envelope mapTimerOp(Envelope msg,
            io.github.lamspace.openlatch.protocol.raft.ApplyResult r) {
        StatusCode status = switch (r.getStatus()) {
            case OK -> StatusCode.OK;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        boolean marked = status == StatusCode.OK
                && msg.getTimerOpRequest().getOp()
                        == io.github.lamspace.openlatch.protocol.TimerOp.TIMER_OP_SCHEDULE
                && r.getTimerArmed()
                && System.currentTimeMillis() >= r.getTimerFireAtMs();
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.TIMER_OP)
                .setRequestId(msg.getRequestId())
                .setTimerOpResponse(io.github.lamspace.openlatch.protocol
                        .TimerOpResponse.newBuilder()
                        .setStatus(status)
                        .setOp(msg.getTimerOpRequest().getOp())
                        .setGeneration(r.getTimerGeneration())
                        .setArmed(r.getTimerArmed())
                        .setFireAtMs(r.getTimerFireAtMs())
                        .setMarked(marked))
                .build();
    }

    /**
     * TIMER 本地操作词的即时应答写回（簿记/影子直答路径专用，op 回显与
     * 三元组+marked 择用；拒绝态零值形）。
     *
     * @param ctx        连接上下文
     * @param session    会话
     * @param startNanos 起始时刻（写回耗时埋点）
     * @param msg        原请求信封
     * @param status     应答状态码
     * @param generation 代次回显（拒绝态 0）
     * @param armed      装载态回显（拒绝态 false）
     * @param fireAtMs   绝对到期时刻回显（拒绝态 0）
     * @param marked     到期共见读数
     */
    private void respondTimer(ChannelHandlerContext ctx, ServerSession session,
            long startNanos, Envelope msg, StatusCode status, long generation,
            boolean armed, long fireAtMs, boolean marked) {
        Envelope resp = Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.TIMER_OP)
                .setRequestId(msg.getRequestId())
                .setTimerOpResponse(io.github.lamspace.openlatch.protocol
                        .TimerOpResponse.newBuilder()
                        .setStatus(status)
                        .setOp(msg.getTimerOpRequest().getOp())
                        .setGeneration(generation)
                        .setArmed(armed)
                        .setFireAtMs(fireAtMs)
                        .setMarked(marked))
                .build();
        if (metrics != null) {
            metrics.recordTimer(msg.getTimerOpRequest().getOp(), status);
        }
        writeSync(ctx, session, startNanos, resp);
    }

    /**
     * PHASER 变异操作回执映射：应用回执 → 协议应答（零值形拒绝态不携计数；
     * {@code QUEUE_FULL} 对 phaser 恒不可达——上限判定在受理点、条目应用侧
     * 不复核，防御映射为 {@code OVERLOADED}）。
     *
     * @param msg 原请求信封
     * @param r   应用回执
     * @return 应答信封
     */
    private Envelope mapPhaserOp(Envelope msg,
            io.github.lamspace.openlatch.protocol.raft.ApplyResult r) {
        StatusCode status = switch (r.getStatus()) {
            case OK -> StatusCode.OK;
            case QUEUED -> StatusCode.QUEUED;
            case QUEUE_FULL -> StatusCode.OVERLOADED;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.PHASER_OP)
                .setRequestId(msg.getRequestId())
                .setPhaserOpResponse(io.github.lamspace.openlatch.protocol
                        .PhaserOpResponse.newBuilder()
                        .setStatus(status)
                        .setOp(msg.getPhaserOpRequest().getOp())
                        .setPhase(r.getPhaserPhase())
                        .setRegistered(r.getPhaserRegistered())
                        .setArrived(r.getPhaserArrived()))
                .build();
    }

    /**
     * PHASER 本地操作词的即时应答写回（簿记/影子直答路径专用，op 回显与
     * 三计数择用）。
     *
     * @param ctx        连接上下文
     * @param session    会话
     * @param startNanos 起始时刻（写回耗时埋点）
     * @param msg        原请求信封
     * @param status     应答状态码
     * @param phase      相位回显（拒绝态 0）
     * @param registered 注册总数回显（拒绝态 0）
     * @param arrived    到场数回显（拒绝态 0）
     */
    private void respondPhaser(ChannelHandlerContext ctx, ServerSession session,
            long startNanos, Envelope msg, StatusCode status, long phase, int registered,
            int arrived) {
        Envelope resp = Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.PHASER_OP)
                .setRequestId(msg.getRequestId())
                .setPhaserOpResponse(io.github.lamspace.openlatch.protocol
                        .PhaserOpResponse.newBuilder()
                        .setStatus(status)
                        .setOp(msg.getPhaserOpRequest().getOp())
                        .setPhase(phase)
                        .setRegistered(registered)
                        .setArrived(arrived))
                .build();
        if (metrics != null) {
            metrics.recordPhaser(msg.getPhaserOpRequest().getOp(), status);
        }
        writeSync(ctx, session, startNanos, resp);
    }

    /**
     * BARRIER_AWAIT 集群路径（ACQUIRE 车道 + 复制提交）：到场改变复制状态
     * （到场账簿、合拢与执行者指定、世代号），MUST 经日志——与 Latch
     * "await 零日志"的边界差异系设计使然（屏障到场是状态迁移事件）。
     * 角色门同 ACQUIRE 车道（Leader 权威裁决；Leader 侧
     * {@code onApplied} 完成等待队列登记与合拢/破障广播）。
     * v5 门控与形状合法性先于提交——非法请求零入日志。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleBarrierAwait(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, true);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 5) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        var req = msg.getBarrierAwaitRequest();
        if (req.getParties() < 0) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        ByteString payload = BarrierAwaitPayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequestId(msg.getRequestId())
                .setRequest(req)
                .build().toByteString();
        gateway.submit(RaftEntryType.BARRIER_AWAIT_ENTRY, payload)
                .whenComplete((r, err) -> {
                    Envelope resp = err == null ? mapBarrierAwait(msg, r) : commitFailure(msg, err);
                    if (metrics != null && resp.hasBarrierAwaitResponse()) {
                        metrics.recordBarrier("await", resp.getBarrierAwaitResponse().getStatus());
                    }
                    respondAsync(ctx, session, startNanos, resp);
                });
    }

    /**
     * BARRIER_LEAVE 集群路径（转发车道）：离场/破障是复制状态迁移，
     * 与 LATCH_COUNT_DOWN 同车道不设角色门——Follower 提交经内部通道由
     * 当值 Leader 复制执行；破障广播经 Leader 侧 {@code onApplied}。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleBarrierLeave(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, false);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 5) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        var req = msg.getBarrierLeaveRequest();
        if (req.getAwaitRequestId() < 0) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        ByteString payload = BarrierLeavePayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequest(req)
                .build().toByteString();
        gateway.submit(RaftEntryType.BARRIER_LEAVE_ENTRY, payload)
                .whenComplete((r, err) -> {
                    Envelope resp = err == null ? mapBarrierLeave(msg, r) : commitFailure(msg, err);
                    if (metrics != null && resp.hasBarrierLeaveResponse()) {
                        metrics.recordBarrier("leave", resp.getBarrierLeaveResponse().getStatus());
                    }
                    respondAsync(ctx, session, startNanos, resp);
                });
    }

    /**
     * BARRIER_ACTION_DONE 集群路径（转发车道）：动作了结裁决在应用点
     * （执行者指定校验入引擎，非指定执行者拒），合拢广播经 Leader 侧
     * {@code onApplied}。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleBarrierActionDone(ServerSession session, Envelope msg,
                                        ChannelHandlerContext ctx) {
        long startNanos = System.nanoTime();
        Envelope bad = validateEnvelope(msg, session, false);
        if (bad != null) {
            writeSync(ctx, session, startNanos, bad);
            return;
        }
        if (session.protocolVersion() < 5) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        var req = msg.getBarrierActionDoneRequest();
        if (req.getGeneration() <= 0) {
            writeSync(ctx, session, startNanos,
                    RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        ByteString payload = BarrierActionDonePayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequest(req)
                .build().toByteString();
        gateway.submit(RaftEntryType.BARRIER_ACTION_DONE_ENTRY, payload)
                .whenComplete((r, err) -> {
                    Envelope resp = err == null ? mapBarrierActionDone(msg, r)
                            : commitFailure(msg, err);
                    if (metrics != null && resp.hasBarrierActionDoneResponse()) {
                        metrics.recordBarrier("action_done",
                                resp.getBarrierActionDoneResponse().getStatus());
                    }
                    respondAsync(ctx, session, startNanos, resp);
                });
    }

    /**
     * {@link ApplyResult} → BarrierAwaitResponse（码形与单机对齐；
     * OK/QUEUED/BARRIER_BROKEN 携带世代/位次/执行者/定型回显）。
     *
     * @param msg    原请求
     * @param result 应用回执
     * @return 应答信封
     */
    static Envelope mapBarrierAwait(Envelope msg, ApplyResult result) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case QUEUED -> StatusCode.QUEUED;
            case QUEUE_FULL -> StatusCode.OVERLOADED;
            case BARRIER_BROKEN -> StatusCode.BARRIER_BROKEN;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.BARRIER_AWAIT)
                .setRequestId(msg.getRequestId())
                .setBarrierAwaitResponse(BarrierAwaitResponse.newBuilder()
                        .setStatus(st)
                        .setQueuePosition(result.getQueuePosition())
                        .setGeneration(result.getBarrierGeneration())
                        .setExecutor(result.getBarrierExecutor())
                        .setParties(result.getBarrierParties()))
                .build();
    }

    /**
     * {@link ApplyResult} → BarrierLeaveResponse。
     *
     * @param msg    原请求
     * @param result 应用回执
     * @return 应答信封
     */
    static Envelope mapBarrierLeave(Envelope msg, ApplyResult result) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.BARRIER_LEAVE)
                .setRequestId(msg.getRequestId())
                .setBarrierLeaveResponse(BarrierLeaveResponse.newBuilder().setStatus(st))
                .build();
    }

    /**
     * {@link ApplyResult} → BarrierActionDoneResponse。
     *
     * @param msg    原请求
     * @param result 应用回执
     * @return 应答信封
     */
    static Envelope mapBarrierActionDone(Envelope msg, ApplyResult result) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case BARRIER_BROKEN -> StatusCode.BARRIER_BROKEN;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.BARRIER_ACTION_DONE)
                .setRequestId(msg.getRequestId())
                .setBarrierActionDoneResponse(BarrierActionDoneResponse.newBuilder()
                        .setStatus(st))
                .build();
    }

    /**
     * {@link ApplyResult} → AtomicOpResponse（码形与单机
     * {@code toAtomicOpResponse} 对齐；OK 携带应答四元组，
     * CAS 家族成败经 {@code atomic_applied} 透传）。
     *
     * @param msg    原请求
     * @param result 应用回执
     * @return 应答信封
     */
    static Envelope mapAtomicOp(Envelope msg, ApplyResult result) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        AtomicOpResponse.Builder b = AtomicOpResponse.newBuilder()
                .setStatus(st)
                .setOp(msg.getAtomicOpRequest().getOp());
        boolean reference = msg.getAtomicOpRequest().getLockType()
                == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_REFERENCE;
        if (result.getStatus() == ApplyStatus.OK) {
            b.setApplied(result.getAtomicApplied())
                    .setVersion(result.getAtomicVersion());
            if (reference) {
                // v6 引用形态：载荷读数按回执 presence 回显（缺省=null 态、
                // EMPTY=零长度），标量值位恒不出现。
                if (result.hasAtomicOldValueBytes()) {
                    b.setOldValueBytes(result.getAtomicOldValueBytes());
                }
                if (result.hasAtomicValueBytes()) {
                    b.setValueBytes(result.getAtomicValueBytes());
                }
            } else {
                b.setOldValue(result.getAtomicOldValue())
                        .setValue(result.getAtomicValue());
            }
        }
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.ATOMIC_OP)
                .setRequestId(msg.getRequestId())
                .setAtomicOpResponse(b)
                .build();
    }

    /**
     * 写请求统一预检：载荷在场性、键长、会话登记；{@code requireLeader} 为
     * 真时先过角色门（ACQUIRE 车道——排队裁决与通知是 Leader 本地态）。
     * 通过返回 {@code null}，否则返回应即写回的拒绝信封。
     *
     * @param msg           请求信封
     * @param session       会话
     * @param requireLeader 是否要求当值 Leader
     * @return 拒绝信封；通过为 {@code null}
     */
    private Envelope validateEnvelope(Envelope msg, ServerSession session, boolean requireLeader) {
        if (requireLeader && !gateway.isLeaderAuthoritative()) {
            // ACQUIRE 车道角色门：用权威角色而非事件标志——降级空窗内拒绝
            // 受理，杜绝"无多数派仍提交"；提示来自单源视图，选举空窗为 -1。
            return notLeaderEnvelope(msg, leaderTracker.snapshot());
        }
        boolean hasPayload = switch (msg.getType()) {
            case LOCK_ACQUIRE -> msg.hasAcquireRequest();
            case LOCK_RELEASE -> msg.hasReleaseRequest();
            case LEASE_RENEW -> msg.hasLeaseRenewRequest();
            case LATCH_COUNT_DOWN -> msg.hasLatchCountDownRequest();
            case LATCH_AWAIT -> msg.hasLatchAwaitRequest();
            case ATOMIC_OP -> msg.hasAtomicOpRequest();
            case BARRIER_AWAIT -> msg.hasBarrierAwaitRequest();
            case BARRIER_LEAVE -> msg.hasBarrierLeaveRequest();
            case BARRIER_ACTION_DONE -> msg.hasBarrierActionDoneRequest();
            case QUEUE_OP -> msg.hasQueueOpRequest();
            case TOPIC_OP -> msg.hasTopicOpRequest();
            case CONDITION_OP -> msg.hasConditionOpRequest();
            case PHASER_OP -> msg.hasPhaserOpRequest();
            case TIMER_OP -> msg.hasTimerOpRequest();
            default -> false;
        };
        if (!hasPayload) {
            return RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        String key = switch (msg.getType()) {
            case LOCK_ACQUIRE -> msg.getAcquireRequest().getKey();
            case LOCK_RELEASE -> msg.getReleaseRequest().getKey();
            case LATCH_COUNT_DOWN -> msg.getLatchCountDownRequest().getKey();
            case LATCH_AWAIT -> msg.getLatchAwaitRequest().getKey();
            case ATOMIC_OP -> msg.getAtomicOpRequest().getKey();
            case BARRIER_AWAIT -> msg.getBarrierAwaitRequest().getKey();
            case BARRIER_LEAVE -> msg.getBarrierLeaveRequest().getKey();
            case BARRIER_ACTION_DONE -> msg.getBarrierActionDoneRequest().getKey();
            case QUEUE_OP -> msg.getQueueOpRequest().getKey();
            case TOPIC_OP -> msg.getTopicOpRequest().getKey();
            case CONDITION_OP -> msg.getConditionOpRequest().getKey();
            case PHASER_OP -> msg.getPhaserOpRequest().getKey();
            case TIMER_OP -> msg.getTimerOpRequest().getKey();
            default -> msg.getLeaseRenewRequest().getKey();
        };
        if (key.isEmpty()) {
            return RequestDispatcher.errorResponse(msg, StatusCode.KEY_EMPTY);
        }
        if (key.getBytes(StandardCharsets.UTF_8).length > config.maxKeyLength()) {
            return RequestDispatcher.errorResponse(msg, StatusCode.KEY_TOO_LONG);
        }
        if (!kernel.shadow().hasSession(session.sessionId())) {
            return RequestDispatcher.errorResponse(msg, StatusCode.SESSION_EXPIRED);
        }
        return null;
    }

    /**
     * 同步写回（预检/排队快速路径）：写完成终结该请求的在途记账；
     * 写回前经指标出口记录一次。
     *
     * @param ctx         连接上下文
     * @param session     连接簿记（endRequest 目标）
     * @param startNanos  受理起始时刻（{@code System.nanoTime()} 基准）
     * @param resp        应答信封
     */
    private void writeSync(ChannelHandlerContext ctx, ServerSession session,
                           long startNanos, Envelope resp) {
        recordDispatch(resp, startNanos);
        ctx.writeAndFlush(resp).addListener(f -> session.endRequest());
    }

    /**
     * 异步应答弹回连接 EventLoop 写回（断连后 writeAndFlush 自动丢弃，
     * 写完成终结在途记账）；指标出口在弹回前记录——耗时样本自受理线程读
     * 起算、含 Raft 提交等待。
     *
     * @param ctx         连接上下文
     * @param session     连接簿记（endRequest 目标）
     * @param startNanos  受理起始时刻
     * @param resp        应答信封
     */
    private void respondAsync(ChannelHandlerContext ctx, ServerSession session,
                              long startNanos, Envelope resp) {
        recordDispatch(resp, startNanos);
        ctx.channel().eventLoop().execute(
                () -> ctx.writeAndFlush(resp).addListener(f -> session.endRequest()));
    }

    /**
     * 指标出口：按应答信封家族计数并记耗时样本；测试夹具装配
     * （{@code metrics == null}）时零记录。
     *
     * @param resp       应答信封
     * @param startNanos 受理起始时刻
     */
    private void recordDispatch(Envelope resp, long startNanos) {
        if (metrics != null) {
            metrics.recordDispatch(resp, System.nanoTime() - startNanos);
        }
    }

    /**
     * 提交失败的应答拆分（不再共享混叠码）：
     * {@link ReplicationGateway.RetryableCommitException}（提交失败、降级在途
     * 终结、子系统未就绪等在途可重试原因）以 {@code NOT_LEADER} + 当时提示
     * 应答，客户端按提示改道或退避；其余异常为预期外的内部失败，记 WARN
     * 并以 {@code INTERNAL_ERROR} 应答。
     *
     * @param msg 原请求信封
     * @param err 提交失败原因
     * @return 错误应答信封
     */
    private Envelope commitFailure(Envelope msg, Throwable err) {
        if (err instanceof ReplicationGateway.RetryableCommitException) {
            return notLeaderEnvelope(msg, leaderTracker.snapshot());
        }
        log.warn("unexpected commit failure for request {} (type {})",
                msg.getRequestId(), msg.getType(), err);
        return RequestDispatcher.errorResponse(msg, StatusCode.INTERNAL_ERROR);
    }

    /**
     * 随附 Leader 提示的 {@code NOT_LEADER} 拒绝应答：载荷 MUST 与请求消息
     * 类型同型（"拒绝状态码线路可见"不变式）——每类客户端接入车道都以其
     * 对应的 Response 自述 {@code NOT_LEADER}，杜绝任何车道把拒绝读成
     * protobuf 默认实例的"成功空应答"（{@code StatusCode.OK} 为枚举零值）。
     * {@code leader_node_id} 取 {@link LeaderTracker} 单源当时值（选举空窗
     * -1）、{@code leader_address} 未配置地址映射时为空串——提示字段仅由
     * v2 三类应答（Acquire/Release/LeaseRenew）承载，其余类型（AtomicOp/
     * Barrier/Latch/QueueOp）沿无提示判例，改道由客户端 Leader 发现机制
     * 兜底。未配 case 的新类型落 default：记 WARN 暴露漏配并以 acquire
     * 形态安全落位（协议扩展时 MUST 补同型 case，表驱动门禁测试守门）。
     *
     * @param msg    原请求信封
     * @param leader 当时 Leader 快照（单源，含选举空窗 -1 形态）
     * @return 带提示的拒绝应答
     */
    static Envelope notLeaderEnvelope(Envelope msg, LeaderTracker.Snapshot leader) {
        Envelope.Builder b = Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(msg.getType())
                .setRequestId(msg.getRequestId());
        switch (msg.getType()) {
            case LOCK_RELEASE -> b.setReleaseResponse(ReleaseResponse.newBuilder()
                    .setStatus(StatusCode.NOT_LEADER)
                    .setLeaderNodeId(leader.leaderNodeId())
                    .setLeaderAddress(leader.leaderAddress()));
            case LEASE_RENEW -> b.setLeaseRenewResponse(LeaseRenewResponse.newBuilder()
                    .setStatus(StatusCode.NOT_LEADER)
                    .setLeaderNodeId(leader.leaderNodeId())
                    .setLeaderAddress(leader.leaderAddress()));
            case ATOMIC_OP -> b.setAtomicOpResponse(AtomicOpResponse.newBuilder()
                    .setStatus(StatusCode.NOT_LEADER)
                    .setOp(msg.getAtomicOpRequest().getOp()));
            case BARRIER_AWAIT -> b.setBarrierAwaitResponse(BarrierAwaitResponse.newBuilder()
                    .setStatus(StatusCode.NOT_LEADER)
                    .setQueuePosition(0));
            case BARRIER_LEAVE -> b.setBarrierLeaveResponse(BarrierLeaveResponse.newBuilder()
                    .setStatus(StatusCode.NOT_LEADER));
            case BARRIER_ACTION_DONE -> b.setBarrierActionDoneResponse(
                    BarrierActionDoneResponse.newBuilder().setStatus(StatusCode.NOT_LEADER));
            // v3：LATCH 同型拒绝——默认实例的 OK 会被成型为"已破障/remaining=0"
            // 假绿，码形违例在此杜绝。
            case LATCH_COUNT_DOWN -> b.setLatchCountDownResponse(
                    io.github.lamspace.openlatch.protocol.LatchCountDownResponse.newBuilder()
                            .setStatus(StatusCode.NOT_LEADER));
            case LATCH_AWAIT -> b.setLatchAwaitResponse(
                    io.github.lamspace.openlatch.protocol.LatchAwaitResponse.newBuilder()
                            .setStatus(StatusCode.NOT_LEADER));
            // v7：QUEUE_OP 同型拒绝——op 回显沿 ATOMIC_OP 判例；默认实例的
            // OK 会被成型为"take 交付空元素/size 读 0"伪成功（W10 根因）。
            case QUEUE_OP -> b.setQueueOpResponse(QueueOpResponse.newBuilder()
                    .setStatus(StatusCode.NOT_LEADER)
                    .setOp(msg.getQueueOpRequest().getOp()));
            // v8：TOPIC_OP 同型拒绝——op 回显沿 QUEUE_OP 判例；默认实例的
            // OK 会被成型为"订阅登记成功（subscription_id=0）/发布受理
            // （topic_seq=0）"伪成功，码形违例在此杜绝。
            case TOPIC_OP -> b.setTopicOpResponse(
                    io.github.lamspace.openlatch.protocol.TopicOpResponse.newBuilder()
                            .setStatus(StatusCode.NOT_LEADER)
                            .setOp(msg.getTopicOpRequest().getOp()));
            // v9：CONDITION_OP 同型拒绝——op 回显沿 TOPIC_OP 判例；默认实例的
            // OK 会被成型为"signal 搬运成功/leave 摘除成功"伪成功，码形违例
            // 在此杜绝（Follower 零副作用拒绝的线路可见面）。
            case PHASER_OP -> b.setPhaserOpResponse(
                    io.github.lamspace.openlatch.protocol.PhaserOpResponse.newBuilder()
                            .setStatus(StatusCode.NOT_LEADER)
                            .setOp(msg.getPhaserOpRequest().getOp()));
            case CONDITION_OP -> b.setConditionOpResponse(
                    io.github.lamspace.openlatch.protocol.ConditionOpResponse.newBuilder()
                            .setStatus(StatusCode.NOT_LEADER)
                            .setOp(msg.getConditionOpRequest().getOp()));
            // v11：TIMER_OP 同型拒绝——op 回显沿 PHASER_OP 判例；默认实例的 OK
            // 会被成型为"装载成功（generation=0）/共见读数"伪成功，码形违例
            // 在此杜绝（W10 常驻门禁扩展）。
            case TIMER_OP -> b.setTimerOpResponse(
                    io.github.lamspace.openlatch.protocol.TimerOpResponse.newBuilder()
                            .setStatus(StatusCode.NOT_LEADER)
                            .setOp(msg.getTimerOpRequest().getOp()));
            default -> {
                log.warn("NOT_LEADER reject has no same-shape case for type {} — "
                        + "add the matching payload branch on protocol extension "
                        + "(reject-codec table gate)", msg.getType());
                b.setAcquireResponse(AcquireResponse.newBuilder()
                        .setStatus(StatusCode.NOT_LEADER)
                        .setLeaderNodeId(leader.leaderNodeId())
                        .setLeaderAddress(leader.leaderAddress()));
            }
        }
        return b.build();
    }

    /**
     * 无 payload 的 acquire 错误响应（键校验/拒绝路径）。
     *
     * @param msg    原请求信封
     * @param status 错误状态码
     * @return 应答信封
     */
    private static Envelope acquireErrorResponse(Envelope msg, StatusCode status) {
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(msg.getRequestId())
                .setAcquireResponse(AcquireResponse.newBuilder().setStatus(status))
                .build();
    }

    /**
     * {@link ApplyResult} → AcquireResponse（映射表与单机
     * {@code RequestDispatcher#toAcquireResponse} 逐项对齐）。
     *
     * @param msg    原请求（回显版本与 request_id）
     * @param result 应用回执
     * @return 应答信封
     */
    static Envelope mapAcquire(Envelope msg, ApplyResult result) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case QUEUED -> StatusCode.QUEUED;
            case DENIED -> StatusCode.DENIED;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case QUEUE_FULL -> StatusCode.OVERLOADED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        AcquireResponse.Builder b = AcquireResponse.newBuilder().setStatus(st);
        if (st == StatusCode.OK) {
            b.setLeaseToken(result.getLeaseToken())
                    .setLeaseExpiresAtMs(result.getLeaseExpiresAtMs())
                    .setGrantedLeaseMs(result.getGrantedLeaseMs());
        } else if (st == StatusCode.QUEUED) {
            b.setQueuePosition(result.getQueuePosition());
        }
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(msg.getRequestId())
                .setAcquireResponse(b)
                .build();
    }

    /**
     * {@link ApplyResult} → ReleaseResponse。
     *
     * @param msg    原请求
     * @param result 应用回执
     * @return 应答信封
     */
    static Envelope mapRelease(Envelope msg, ApplyResult result) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case NOT_HELD -> StatusCode.NOT_HELD;
            case INVALID_TOKEN -> StatusCode.INVALID_TOKEN;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case INVALID_REQUEST -> StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(msg.getRequestId())
                .setReleaseResponse(ReleaseResponse.newBuilder()
                        .setStatus(st).setFullyReleased(result.getFullyReleased()))
                .build();
    }

    /**
     * {@link ApplyResult} → LeaseRenewResponse。
     *
     * @param msg    原请求
     * @param result 应用回执
     * @return 应答信封
     */
    static Envelope mapRenew(Envelope msg, ApplyResult result) {
        StatusCode st = switch (result.getStatus()) {
            case OK -> StatusCode.OK;
            case NOT_HELD -> StatusCode.NOT_HELD;
            case INVALID_TOKEN -> StatusCode.INVALID_TOKEN;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            default -> StatusCode.INTERNAL_ERROR;
        };
        LeaseRenewResponse.Builder b = LeaseRenewResponse.newBuilder().setStatus(st);
        if (st == StatusCode.OK) {
            b.setLeaseExpiresAtMs(result.getLeaseExpiresAtMs());
        }
        return Envelope.newBuilder()
                .setProtocolVersion(msg.getProtocolVersion())
                .setType(MessageType.LEASE_RENEW)
                .setRequestId(msg.getRequestId())
                .setLeaseRenewResponse(b)
                .build();
    }
}
