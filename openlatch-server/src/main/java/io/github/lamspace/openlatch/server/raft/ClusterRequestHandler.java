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
 * <p><b>角色前提</b>：本类只在当值 Leader 上被路由到（Follower 侧写请求的
 * NOT_LEADER 重定向归 S3/P2-12；非 Leader 抵达此处按可重试错误应答）。
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

    /**
     * 构造处理器。
     *
     * @param gateway   复制网关
     * @param kernel    状态机内核（会话预检）
     * @param waitQueue 等待队列
     * @param config    服务配置
     */
    public ClusterRequestHandler(ReplicationGateway gateway, LockStateMachineCore kernel,
                                 WaitQueue waitQueue, ServerConfig config) {
        this.gateway = gateway;
        this.kernel = kernel;
        this.waitQueue = waitQueue;
        this.config = config;
    }

    /**
     * ACQUIRE 集群路径：预检查通过后提交，应答于应用后写回。
     *
     * <p>判定顺序（同步分支立即写回，异步分支接管在途记账）：
     * 角色 → 载荷与键合法性 → 会话登记 → 排队裁决（§4.5 预演）。
     *
     * @param session 已握手会话（携带逻辑 sessionId）
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleAcquire(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        Envelope bad = validateEnvelope(msg, session);
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
                        err == null ? mapAcquire(msg, r) : retryableError(msg)));
    }

    /**
     * RELEASE 集群路径：无排队裁决，直接提交，应答于应用后写回。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleRelease(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        Envelope bad = validateEnvelope(msg, session);
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
                        err == null ? mapRelease(msg, r) : retryableError(msg)));
    }

    /**
     * RENEW 集群路径：直接提交，应答于应用后写回。
     *
     * @param session 已握手会话
     * @param msg     请求信封
     * @param ctx     连接上下文
     */
    public void handleRenew(ServerSession session, Envelope msg, ChannelHandlerContext ctx) {
        Envelope bad = validateEnvelope(msg, session);
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
                        err == null ? mapRenew(msg, r) : retryableError(msg)));
    }

    /**
     * 公共预检：角色与载荷合法性（类型匹配、键非空、UTF-8 长度、会话登记）。
     * 返回非 {@code null} 即为应立即写回的同步错误。
     *
     * @param msg     请求信封
     * @param session 已握手会话
     * @return 需立即写回的错误应答；通过预检为 {@code null}
     */
    private Envelope validateEnvelope(Envelope msg, ServerSession session) {
        if (!gateway.isLeaderAuthoritative()) {
            // S2 兜底：非 Leader 抵达此处（S3 未上线的重定向空窗）按可重试错误。
            // 用权威角色而非事件标志：降级空窗内拒绝写入，杜绝"无多数派仍提交"。
            return RequestDispatcher.errorResponse(msg, StatusCode.NOT_LEADER);
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
     * 可重试提交失败的统一错误（S3 前以 NOT_LEADER 呈现语义最近的拒绝）。
     *
     * @param msg 原请求信封
     * @return 错误应答信封
     */
    private Envelope retryableError(Envelope msg) {
        return RequestDispatcher.errorResponse(msg, StatusCode.NOT_LEADER);
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
