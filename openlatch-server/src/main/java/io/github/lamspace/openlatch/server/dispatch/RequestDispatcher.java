package io.github.lamspace.openlatch.server.dispatch;

import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.ReleaseResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.session.ServerSession;

import java.util.Objects;

/**
 * 请求分发（设计说明书 §5.4）：{@code Envelope} → core 命令，core 结果 → {@code Envelope}。
 * 映射为纯函数（design.md D5），可脱离 Netty 单测；{@link #dispatch} 为入口。
 */
public final class RequestDispatcher {

    private final CoreEngine core;

    public RequestDispatcher(CoreEngine core) {
        this.core = Objects.requireNonNull(core);
    }

    /**
     * 分发一条已握手连接上的业务消息。返回要写回的响应；{@code PING} 返回
     * {@code null}（不回复）。未知类型与 payload 不匹配回 {@code INVALID_REQUEST}，
     * 不断连（规格"消息合法性校验"）。
     */
    public Envelope dispatch(ServerSession session, Envelope msg) {
        return switch (msg.getType()) {
            case LOCK_ACQUIRE -> msg.hasAcquireRequest()
                    ? dispatchAcquire(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case LOCK_RELEASE -> msg.hasReleaseRequest()
                    ? dispatchRelease(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case LEASE_RENEW -> msg.hasLeaseRenewRequest()
                    ? dispatchRenew(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case PING -> null;
            default -> errorResponse(msg, StatusCode.INVALID_REQUEST);
        };
    }

    private Envelope dispatchAcquire(ServerSession session, Envelope msg) {
        AcquireRequest req = msg.getAcquireRequest();
        LockType lockType = toCoreLockType(req.getLockType());
        if (lockType == null) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        AcquireCommand cmd = new AcquireCommand(
                session.sessionId(),
                msg.getRequestId(),
                req.getKey(),
                lockType,
                req.getThreadId(),
                req.getLeaseMs(),
                req.getWaitMs() != 0);   // wait_ms == 0 立即式；-1 与 >0 均可排队（设计说明书 §3.2.2）
        AcquireResult result = core.acquire(cmd);
        return toAcquireResponse(msg, result, System.currentTimeMillis());
    }

    private Envelope dispatchRelease(ServerSession session, Envelope msg) {
        ReleaseRequest req = msg.getReleaseRequest();
        ReleaseCommand cmd = new ReleaseCommand(
                session.sessionId(), req.getKey(), req.getLeaseToken(), req.getThreadId());
        return toReleaseResponse(msg, core.release(cmd));
    }

    private Envelope dispatchRenew(ServerSession session, Envelope msg) {
        LeaseRenewRequest req = msg.getLeaseRenewRequest();
        RenewCommand cmd = new RenewCommand(
                session.sessionId(), req.getKey(), req.getLeaseToken(), req.getLeaseMs());
        return toRenewResponse(msg, core.renew(cmd));
    }

    /** 协议锁类型 → core 锁类型；未知值返回 null。 */
    static LockType toCoreLockType(io.github.lamspace.openlatch.protocol.LockType type) {
        return switch (type) {
            case LOCK_TYPE_REENTRANT -> LockType.REENTRANT;
            case LOCK_TYPE_SIMPLE -> LockType.SIMPLE;
            case LOCK_TYPE_READ -> LockType.READ;
            case LOCK_TYPE_WRITE -> LockType.WRITE;
            default -> null;
        };
    }

    /** core 授予结果 → 协议响应（design.md D5 全表映射）。 */
    static Envelope toAcquireResponse(Envelope request, AcquireResult result, long nowMs) {
        AcquireResponse.Builder resp = AcquireResponse.newBuilder()
                .setStatus(toAcquireStatus(result.outcome()));
        if (result.outcome() == Outcome.GRANTED) {
            resp.setLeaseToken(result.leaseToken())
                    .setGrantedLeaseMs(result.grantedLeaseMs())
                    .setLeaseExpiresAtMs(nowMs + result.grantedLeaseMs());
        } else if (result.outcome() == Outcome.QUEUED) {
            resp.setQueuePosition(result.queuePosition());
        }
        return envelope(request, MessageType.LOCK_ACQUIRE, b -> b.setAcquireResponse(resp));
    }

    static StatusCode toAcquireStatus(Outcome outcome) {
        return switch (outcome) {
            case GRANTED -> StatusCode.OK;
            case QUEUED -> StatusCode.QUEUED;
            case DENIED -> StatusCode.DENIED;
            case REJECT_KEY_EMPTY -> StatusCode.KEY_EMPTY;
            case REJECT_KEY_TOO_LONG -> StatusCode.KEY_TOO_LONG;
            case REJECT_QUEUE_FULL -> StatusCode.OVERLOADED;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
        };
    }

    /** core 释放结果 → 协议响应。 */
    static Envelope toReleaseResponse(Envelope request, ReleaseResult result) {
        ReleaseResponse.Builder resp = ReleaseResponse.newBuilder()
                .setStatus(toCommonStatus(result.status()))
                .setFullyReleased(result.fullyReleased());
        return envelope(request, MessageType.LOCK_RELEASE, b -> b.setReleaseResponse(resp));
    }

    /** core 续租结果 → 协议响应。 */
    static Envelope toRenewResponse(Envelope request, RenewResult result) {
        LeaseRenewResponse.Builder resp = LeaseRenewResponse.newBuilder()
                .setStatus(toCommonStatus(result.status()));
        if (result.status() == ReleaseStatus.OK) {
            resp.setLeaseExpiresAtMs(result.newExpiresAtMs());
        }
        return envelope(request, MessageType.LEASE_RENEW, b -> b.setLeaseRenewResponse(resp));
    }

    static StatusCode toCommonStatus(ReleaseStatus status) {
        return switch (status) {
            case OK -> StatusCode.OK;
            case INVALID_TOKEN -> StatusCode.INVALID_TOKEN;
            case NOT_HELD -> StatusCode.NOT_HELD;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
        };
    }

    private static Envelope envelope(Envelope request, MessageType type,
                                     java.util.function.Consumer<Envelope.Builder> payload) {
        Envelope.Builder b = Envelope.newBuilder()
                .setProtocolVersion(request.getProtocolVersion())
                .setType(type)
                .setRequestId(request.getRequestId());
        payload.accept(b);
        return b.build();
    }

    /**
     * 构造与请求类型对应的最小错误响应，回显 {@code request_id}。
     * 未知类型返回无 payload 的信封（仍可被客户端按 {@code request_id} 关联）。
     */
    public static Envelope errorResponse(Envelope request, StatusCode status) {
        Envelope.Builder b = Envelope.newBuilder()
                .setProtocolVersion(request.getProtocolVersion())
                .setType(request.getType())
                .setRequestId(request.getRequestId());
        switch (request.getType()) {
            case HELLO -> b.setHelloResponse(HelloResponse.newBuilder().setStatus(status));
            case LOCK_ACQUIRE -> b.setAcquireResponse(AcquireResponse.newBuilder().setStatus(status));
            case LOCK_RELEASE -> b.setReleaseResponse(ReleaseResponse.newBuilder().setStatus(status));
            case LEASE_RENEW -> b.setLeaseRenewResponse(LeaseRenewResponse.newBuilder().setStatus(status));
            default -> {
            }
        }
        return b.build();
    }
}
