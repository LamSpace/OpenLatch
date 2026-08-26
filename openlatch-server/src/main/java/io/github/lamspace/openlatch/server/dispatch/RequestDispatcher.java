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

    /** 锁语义核心，全部业务命令均委托其执行。 */
    private final CoreEngine core;

    /**
     * 构造分发器。
     *
     * @param core 锁语义核心
     */
    public RequestDispatcher(CoreEngine core) {
        this.core = Objects.requireNonNull(core);
    }

    /**
     * 分发一条已握手连接上的业务消息。返回要写回的响应；{@code PING} 返回
     * {@code null}（不回复）。未知类型与 payload 不匹配回 {@code INVALID_REQUEST}，
     * 不断连（规格"消息合法性校验"）。
     *
     * @param session 已握手会话（提供 sessionId）
     * @param msg     入站消息信封
     * @return 要写回的响应；{@code PING} 返回 {@code null}
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

    /**
     * 分发获取锁请求：协议 {@code AcquireRequest} → core {@code AcquireCommand}
     * → 结果映射为协议响应。{@code wait_ms == 0} 映射为立即式（不排队），
     * {@code -1} 与正数均映射为可排队（设计说明书 §3.2.2）；租约到期时刻
     * 以映射时的 {@code System.currentTimeMillis()} 计算。
     *
     * @param session 已握手会话（提供 sessionId）
     * @param msg     入站消息信封（已确认携带 {@code AcquireRequest}）
     * @return 协议响应信封
     */
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

    /**
     * 分发释放锁请求：协议 {@code ReleaseRequest} → core {@code ReleaseCommand}
     * → 结果映射为协议响应（含 {@code fullyReleased} 标志）。
     *
     * @param session 已握手会话（提供 sessionId）
     * @param msg     入站消息信封（已确认携带 {@code ReleaseRequest}）
     * @return 协议响应信封
     */
    private Envelope dispatchRelease(ServerSession session, Envelope msg) {
        ReleaseRequest req = msg.getReleaseRequest();
        ReleaseCommand cmd = new ReleaseCommand(
                session.sessionId(), req.getKey(), req.getLeaseToken(), req.getThreadId());
        return toReleaseResponse(msg, core.release(cmd));
    }

    /**
     * 分发续租请求：协议 {@code LeaseRenewRequest} → core {@code RenewCommand}
     * → 结果映射为协议响应（成功时携带新到期时刻）。
     *
     * @param session 已握手会话（提供 sessionId）
     * @param msg     入站消息信封（已确认携带 {@code LeaseRenewRequest}）
     * @return 协议响应信封
     */
    private Envelope dispatchRenew(ServerSession session, Envelope msg) {
        LeaseRenewRequest req = msg.getLeaseRenewRequest();
        RenewCommand cmd = new RenewCommand(
                session.sessionId(), req.getKey(), req.getLeaseToken(), req.getLeaseMs());
        return toRenewResponse(msg, core.renew(cmd));
    }

    /**
     * 协议锁类型 → core 锁类型；未知值返回 null。
     *
     * @param type 协议锁类型
     * @return core 锁类型；未知值返回 {@code null}
     */
    static LockType toCoreLockType(io.github.lamspace.openlatch.protocol.LockType type) {
        return switch (type) {
            case LOCK_TYPE_REENTRANT -> LockType.REENTRANT;
            case LOCK_TYPE_SIMPLE -> LockType.SIMPLE;
            case LOCK_TYPE_READ -> LockType.READ;
            case LOCK_TYPE_WRITE -> LockType.WRITE;
            default -> null;
        };
    }

    /**
     * core 授予结果 → 协议响应（design.md D5 全表映射）。
     *
     * @param request 原请求信封（回显 protocolVersion 与 requestId）
     * @param result  core 获取结果
     * @param nowMs   当前时刻，用于计算租约到期时刻（毫秒）
     * @return 协议响应信封
     */
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

    /**
     * core 获取结果状态 → 协议状态码（全表映射）。
     *
     * @param outcome core 获取结果状态
     * @return 协议状态码
     */
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

    /**
     * core 释放结果 → 协议响应。
     *
     * @param request 原请求信封（回显 protocolVersion 与 requestId）
     * @param result  core 释放结果
     * @return 协议响应信封
     */
    static Envelope toReleaseResponse(Envelope request, ReleaseResult result) {
        ReleaseResponse.Builder resp = ReleaseResponse.newBuilder()
                .setStatus(toCommonStatus(result.status()))
                .setFullyReleased(result.fullyReleased());
        return envelope(request, MessageType.LOCK_RELEASE, b -> b.setReleaseResponse(resp));
    }

    /**
     * core 续租结果 → 协议响应。
     *
     * @param request 原请求信封（回显 protocolVersion 与 requestId）
     * @param result  core 续租结果
     * @return 协议响应信封
     */
    static Envelope toRenewResponse(Envelope request, RenewResult result) {
        LeaseRenewResponse.Builder resp = LeaseRenewResponse.newBuilder()
                .setStatus(toCommonStatus(result.status()));
        if (result.status() == ReleaseStatus.OK) {
            resp.setLeaseExpiresAtMs(result.newExpiresAtMs());
        }
        return envelope(request, MessageType.LEASE_RENEW, b -> b.setLeaseRenewResponse(resp));
    }

    /**
     * core 释放/续租状态 → 协议状态码（全表映射）。
     *
     * @param status core 释放/续租状态
     * @return 协议状态码
     */
    static StatusCode toCommonStatus(ReleaseStatus status) {
        return switch (status) {
            case OK -> StatusCode.OK;
            case INVALID_TOKEN -> StatusCode.INVALID_TOKEN;
            case NOT_HELD -> StatusCode.NOT_HELD;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
        };
    }

    /**
     * 构造响应信封骨架：回显请求的 {@code protocolVersion} 与 {@code requestId}，
     * 设置响应类型，再交由调用方填充具体 payload。
     *
     * @param request 原请求信封（回显来源）
     * @param type    响应消息类型
     * @param payload payload 填充器
     * @return 协议响应信封
     */
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
     *
     * @param request 原请求信封
     * @param status  错误状态码
     * @return 错误响应信封
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
