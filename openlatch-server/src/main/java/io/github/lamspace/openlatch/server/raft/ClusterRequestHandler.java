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
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LeaseRenewResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.raft.AcquirePayload;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.ReleasePayload;
import io.github.lamspace.openlatch.protocol.raft.RenewPayload;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 集群模式写请求处理器（详设 §4.5，design D3/D9）：Phase 1 的"同步函数
 * 调用即应答"在这里被替换为"预检查 → 提交 → 应用后应答"三段，本类承载
 * 协议侧的裁决与映射：
 *
 * <ul>
 *   <li><b>预检查（快速失败通道）</b>：类型/键合法性与会话登记校验不过直接
 *       同步错误应答，MUST NOT 写日志；锁被占或队列非空时——可排队请求在
 *       本地 {@link WaitQueue} 登记并即时回 QUEUED，立即式回 DENIED，两者
 *       均不写日志（§4.5"排队不是复制状态"）；</li>
 *   <li><b>授予/释放/续租</b>：构造 {@link RaftEntryType} 条目提交
 *       {@link ReplicationGateway}，应答在完成回调中写回连接所属
 *       EventLoop（design D4）；</li>
 *   <li><b>回执 → 协议映射</b>：{@link ApplyStatus} 全表映射（与单机
 *       {@code RequestDispatcher} 的映射表语义逐项对齐，错误码不复用）。
 * </ul>
 *
 * <p><b>Follower 分车道（S3/P2-12，spec"Follower 写请求分车道"）</b>：
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
    /** Leader 提示单源（NOT_LEADER 应答随附提示，s3 design D3）。 */
    private final LeaderTracker leaderTracker;

    /**
     * 构造处理器。
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
        this.gateway = gateway;
        this.kernel = kernel;
        this.waitQueue = waitQueue;
        this.config = config;
        this.leaderTracker = leaderTracker;
    }

    /**
     * ACQUIRE 集群路径：预检查通过后提交，应答于应用后写回。
     *
     * <p>判定顺序（同步分支立即写回，异步分支接管在途记账）：
     * 角色（Follower 即 {@code NOT_LEADER}+提示改连，S3 分车道）→
     * 载荷与键合法性 → 会话登记 → 排队裁决（§4.5 预演）。
     *
     * @param session 已握手会话（携带逻辑 sessionId）
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleAcquire(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        Envelope bad = validateEnvelope(msg, session, true);
        if (bad != null) {
            writeSync(ctx, session, bad);
            return;
        }
        var req = msg.getAcquireRequest();
        if (req.getLockType() == io.github.lamspace.openlatch.protocol.LockType.UNRECOGNIZED) {
            writeSync(ctx, session, RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        boolean queueWanted = req.getWaitMs() != 0;
        boolean held = kernel.shadow().isHeld(req.getKey());
        // 队首重发且锁已空出：自推进走复制授予路径（AWAIT_NOTIFY 后重发的
        // Phase 1 语义在集群路径的等价形态；onGranted 负责出队）。
        boolean selfPromotion = !held && waitQueue.isHead(
                session.sessionId(), msg.getRequestId(), req.getKey());
        boolean busy = held || (!selfPromotion && waitQueue.hasWaiters(req.getKey()));
        if (busy) {
            if (!queueWanted) {
                writeSync(ctx, session, acquireErrorResponse(msg, StatusCode.DENIED));
                return;
            }
            int pos = waitQueue.enqueue(session.sessionId(), msg.getRequestId(), req.getKey());
            if (pos < 0) {
                writeSync(ctx, session, acquireErrorResponse(msg, StatusCode.OVERLOADED));
                return;
            }
            writeSync(ctx, session, Envelope.newBuilder()
                    .setProtocolVersion(msg.getProtocolVersion())
                    .setType(MessageType.LOCK_ACQUIRE)
                    .setRequestId(msg.getRequestId())
                    .setAcquireResponse(AcquireResponse.newBuilder()
                            .setStatus(StatusCode.QUEUED).setQueuePosition(pos))
                    .build());
            return;
        }
        // 可授予预演 → 复制路径。应用结果为准：预演失效时 Leader 在应用点
        // 补登记并改写回执（gateway.leaderSideEffects，design D3）。
        ByteString payload = AcquirePayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequestId(msg.getRequestId())
                .setRequest(req)
                .build().toByteString();
        gateway.submit(RaftEntryType.LOCK_ACQUIRE_ENTRY, payload)
                .whenComplete((r, err) -> respondAsync(ctx, session,
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
        Envelope bad = validateEnvelope(msg, session, false);
        if (bad != null) {
            writeSync(ctx, session, bad);
            return;
        }
        ByteString payload = ReleasePayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequest(msg.getReleaseRequest())
                .build().toByteString();
        gateway.submit(RaftEntryType.LOCK_RELEASE_ENTRY, payload)
                .whenComplete((r, err) -> respondAsync(ctx, session,
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
        Envelope bad = validateEnvelope(msg, session, false);
        if (bad != null) {
            writeSync(ctx, session, bad);
            return;
        }
        ByteString payload = RenewPayload.newBuilder()
                .setSessionId(session.sessionId())
                .setRequest(msg.getLeaseRenewRequest())
                .build().toByteString();
        gateway.submit(RaftEntryType.LEASE_RENEW_ENTRY, payload)
                .whenComplete((r, err) -> respondAsync(ctx, session,
                        err == null ? mapRenew(msg, r) : commitFailure(msg, err)));
    }

    /**
     * 公共预检：载荷合法性（类型匹配、键非空、UTF-8 长度）与会话登记；
     * {@code requireLeader=true}（ACQUIRE 车道）时先过权威角色门——非 Leader
     * 回 {@code NOT_LEADER} 并随附 {@link LeaderTracker} 当时的提示。
     * 转发车道（RELEASE/RENEW）不设角色门：条目经内部通道抵达当值 Leader，
     * 权威判定在应用点。会话登记预检两车道同规则：连接 sid 于握手完成前已
     * 在本副本应用（D12），本地判 {@code SESSION_EXPIRED} 与 Leader 判定
     * 结果一致且省一次转发。返回非 {@code null} 即为应立即写回的同步错误。
     *
     * @param msg          请求信封
     * @param session      已握手会话
     * @param requireLeader 是否要求本节点为当值 Leader（ACQUIRE 车道 true）
     * @return 需立即写回的错误应答；通过预检为 {@code null}
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
            default -> false;
        };
        if (!hasPayload) {
            return RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        String key = switch (msg.getType()) {
            case LOCK_ACQUIRE -> msg.getAcquireRequest().getKey();
            case LOCK_RELEASE -> msg.getReleaseRequest().getKey();
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
     * 同步写回（预检/排队快速路径）：写完成终结该请求的在途记账。
     *
     * @param ctx     连接上下文
     * @param session 连接簿记（endRequest 目标）
     * @param resp    应答信封
     */
    private void writeSync(ChannelHandlerContext ctx, ServerSession session, Envelope resp) {
        ctx.writeAndFlush(resp).addListener(f -> session.endRequest());
    }

    /**
     * 异步应答弹回连接 EventLoop 写回（design D4；断连后 writeAndFlush 自动丢弃，
     * 写完成终结在途记账）。
     *
     * @param ctx     连接上下文
     * @param session 连接簿记（endRequest 目标）
     * @param resp    应答信封
     */
    private void respondAsync(ChannelHandlerContext ctx, ServerSession session, Envelope resp) {
        ctx.channel().eventLoop().execute(
                () -> ctx.writeAndFlush(resp).addListener(f -> session.endRequest()));
    }

    /**
     * 提交失败的应答拆分（S3/P2-12，不再共享混叠码）：
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
     * 未配置地址映射时为空串（客户端种子发现兜底，design D4）。
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
