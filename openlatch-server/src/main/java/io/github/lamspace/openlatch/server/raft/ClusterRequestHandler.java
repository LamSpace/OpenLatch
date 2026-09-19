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
import io.github.lamspace.openlatch.protocol.ReleaseResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
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
        this.gateway = gateway;
        this.kernel = kernel;
        this.waitQueue = waitQueue;
        this.config = config;
        this.leaderTracker = leaderTracker;
        this.metrics = metrics;
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
        StatusCode shapeBad = RequestDispatcher.validateAtomicRequest(req);
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
        if (result.getStatus() == ApplyStatus.OK) {
            b.setApplied(result.getAtomicApplied())
                    .setOldValue(result.getAtomicOldValue())
                    .setValue(result.getAtomicValue())
                    .setVersion(result.getAtomicVersion());
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
            return notLeaderEnvelope(msg);
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
            return notLeaderEnvelope(msg);
        }
        log.warn("unexpected commit failure for request {} (type {})",
                msg.getRequestId(), msg.getType(), err);
        return RequestDispatcher.errorResponse(msg, StatusCode.INTERNAL_ERROR);
    }

    /**
     * 随附 Leader 提示的 {@code NOT_LEADER} 应答：按原请求类型选载荷
     * （Acquire/Release/LeaseRenew），{@code leader_node_id} 取
     * {@link LeaderTracker} 单源当时值（选举空窗 -1），{@code leader_address}
     * 未配置地址映射时为空串（客户端种子发现兜底）。
     *
     * @param msg 原请求信封
     * @return 带提示的拒绝应答
     */
    private Envelope notLeaderEnvelope(Envelope msg) {
        LeaderTracker.Snapshot leader = leaderTracker.snapshot();
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
            default -> b.setAcquireResponse(AcquireResponse.newBuilder()
                    .setStatus(StatusCode.NOT_LEADER)
                    .setLeaderNodeId(leader.leaderNodeId())
                    .setLeaderAddress(leader.leaderAddress()));
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
