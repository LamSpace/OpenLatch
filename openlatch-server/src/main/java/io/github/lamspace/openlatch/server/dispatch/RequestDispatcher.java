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
import io.github.lamspace.openlatch.core.command.LatchAwaitCommand;
import io.github.lamspace.openlatch.core.command.LatchCountDownCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.LatchAwaitResult;
import io.github.lamspace.openlatch.core.result.LatchCountDownResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse;
import io.github.lamspace.openlatch.protocol.AdminListKeysResponse;
import io.github.lamspace.openlatch.protocol.AdminListSessionsResponse;
import io.github.lamspace.openlatch.protocol.AdminSummaryResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.LatchAwaitResponse;
import io.github.lamspace.openlatch.protocol.LatchCountDownResponse;
import io.github.lamspace.openlatch.protocol.ReleaseResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.metrics.ServerMetrics;
import io.github.lamspace.openlatch.server.session.ServerSession;

import java.util.Objects;

/**
 * 请求分发（设计说明书 §5.4）：{@code Envelope} → core 命令，core 结果 → {@code Envelope}。
 * 映射为纯函数（design.md D5），可脱离 Netty 单测；{@link #dispatch} 为入口。
 *
 * <p><b>线程模型</b>：{@link #dispatch} 由 {@code ServerSessionHandler} 在
 * 连接所属 EventLoop 线程上同步调用（含 {@code core.acquire}/{@code release}/
 * {@code renew} 的委托执行亦在该线程完成）。本类无可变状态（仅持有 final 的
 * core 与指标引用），可被多连接线程并发进入；单 key 状态的串行性由
 * {@code CoreEngine} 的条目锁保证，不属本类职责。
 *
 * <p><b>指标埋点</b>（Phase 3 T2，详设 §3.4 勘误后口径）：每条产出应答的
 * 请求在 {@link #dispatch} 收口记录一次（计数按应答状态码、获取类消息附带
 * dispatch 起止耗时）；{@code metrics} 可为 {@code null}（既有测试夹具的
 * 直接构造），此时零记录、行为不变。
 */
public final class RequestDispatcher {

    /** 锁语义核心，全部业务命令均委托其执行。 */
    private final CoreEngine core;
    /** 指标词表门面；{@code null} 表示不埋点（测试夹具形态）。 */
    private final ServerMetrics metrics;

    /**
     * 构造分发器（不埋点，既有测试夹具形态）。
     *
     * @param core 锁语义核心
     */
    public RequestDispatcher(CoreEngine core) {
        this(core, null);
    }

    /**
     * 构造分发器（生产形态：应答经 {@code metrics} 记入服务端指标词表）。
     *
     * @param core    锁语义核心
     * @param metrics 指标门面，可为 {@code null}（不埋点）
     */
    public RequestDispatcher(CoreEngine core, ServerMetrics metrics) {
        this.core = Objects.requireNonNull(core);
        this.metrics = metrics;
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
        long startNanos = metrics != null ? System.nanoTime() : 0L;
        Envelope resp = switch (msg.getType()) {
            case LOCK_ACQUIRE -> msg.hasAcquireRequest()
                    ? dispatchAcquire(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case LOCK_RELEASE -> msg.hasReleaseRequest()
                    ? dispatchRelease(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case LEASE_RENEW -> msg.hasLeaseRenewRequest()
                    ? dispatchRenew(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case LATCH_COUNT_DOWN -> msg.hasLatchCountDownRequest()
                    ? dispatchLatchCountDown(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case LATCH_AWAIT -> msg.hasLatchAwaitRequest()
                    ? dispatchLatchAwait(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case PING -> null;
            default -> errorResponse(msg, StatusCode.INVALID_REQUEST);
        };
        if (metrics != null) {
            metrics.recordDispatch(resp, System.nanoTime() - startNanos);
        }
        return resp;
    }

    /**
     * 分发屏障倒计数（Phase 3 P3-05，详设 §2.4）：负参数在协议层拒绝；
     * v3 门控——LATCH 消息对握版本 &lt;3 的会话消息级拒绝、不断连。
     *
     * @param session 已握手会话
     * @param msg     入站消息信封
     * @return 协议响应信封
     */
    private Envelope dispatchLatchCountDown(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 3) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        var req = msg.getLatchCountDownRequest();
        if (req.getCount() < 0 || req.getTotal() < 0) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        var result = core.countDown(new LatchCountDownCommand(
                session.sessionId(), req.getKey(), req.getCount(), req.getTotal()));
        return envelope(msg, MessageType.LATCH_COUNT_DOWN, b -> b.setLatchCountDownResponse(
                LatchCountDownResponse.newBuilder()
                        .setStatus(toLatchStatus(result.outcome()))
                        .setRemaining(result.remaining())));
    }

    /**
     * 分发屏障等待（Phase 3 P3-05）：判定与门控同上；QUEUED 携带位次，
     * 归零经 {@code AWAIT_NOTIFY} 推送后由客户端同 id 重发。
     *
     * @param session 已握手会话
     * @param msg     入站消息信封
     * @return 协议响应信封
     */
    private Envelope dispatchLatchAwait(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 3) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        var req = msg.getLatchAwaitRequest();
        if (req.getTotal() < 0) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        LatchAwaitResult result = core.latchAwait(new LatchAwaitCommand(
                session.sessionId(), msg.getRequestId(), req.getKey(), req.getTotal()));
        return envelope(msg, MessageType.LATCH_AWAIT, b -> b.setLatchAwaitResponse(
                LatchAwaitResponse.newBuilder()
                        .setStatus(toLatchStatus(result.outcome()))
                        .setQueuePosition(result.queuePosition())));
    }

    /**
     * 屏障命令结果 → 协议状态码（映射表与 {@link #toAcquireStatus} 同规则：
     * GRANTED=OK、拒绝细分同码，design D3 协议面不新增状态码）。
     *
     * @param outcome 屏障命令结果状态
     * @return 协议状态码
     */
    static StatusCode toLatchStatus(Outcome outcome) {
        return switch (outcome) {
            case GRANTED -> StatusCode.OK;
            case QUEUED -> StatusCode.QUEUED;
            case DENIED -> StatusCode.DENIED;
            case REJECT_KEY_EMPTY -> StatusCode.KEY_EMPTY;
            case REJECT_KEY_TOO_LONG -> StatusCode.KEY_TOO_LONG;
            case REJECT_QUEUE_FULL -> StatusCode.OVERLOADED;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case REJECT_TYPE_MISMATCH -> StatusCode.INVALID_REQUEST;
            case REJECT_SEMAPHORE_TOTAL -> StatusCode.INVALID_REQUEST;
            case REJECT_LATCH_TOTAL -> StatusCode.INVALID_REQUEST;
        };
    }

    /**
     * 分发获取锁请求：协议 {@code AcquireRequest} → core {@code AcquireCommand}
     * → 结果映射为协议响应。{@code wait_ms == 0} 映射为立即式（不排队），
     * {@code -1} 与正数均映射为可排队（设计说明书 §3.2.2）；租约到期时刻
     * 以映射时的 {@code System.currentTimeMillis()} 计算。v3 门控与许可参数
     * 合法性（{@link #validateAcquirePermits}）先于命令构造，许可数经
     * {@link #normalizedPermits} 归一后传入 core。
     *
     * @param session 已握手会话（提供 sessionId）
     * @param msg     入站消息信封（已确认携带 {@code AcquireRequest}）
     * @return 协议响应信封
     */
    private Envelope dispatchAcquire(ServerSession session, Envelope msg) {
        AcquireRequest req = msg.getAcquireRequest();
        // v3 门控（详设 §6）：新锁类型仅对握手中声明 v3 的会话开放，v1/v2
        // 会话消息级拒绝、不断连（请求形状错误，非安全事件）。
        if (session.protocolVersion() < 3 && isV3OnlyLockType(req.getLockType())) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        StatusCode permitBad = validateAcquirePermits(req);
        if (permitBad != null) {
            return errorResponse(msg, permitBad);
        }
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
                req.getWaitMs() != 0,   // wait_ms == 0 立即式；-1 与 >0 均可排队（设计说明书 §3.2.2）
                normalizedPermits(req.getPermits()),
                req.getPermitsTotal());
        AcquireResult result = core.acquire(cmd);
        return toAcquireResponse(msg, result, System.currentTimeMillis());
    }

    /**
     * 获取请求的许可参数合法性（Phase 3 详设 §2.1 / P3-03）：
     * {@code permits}/{@code permits_total} 为负，或非 SEMAPHORE 类型携带
     * {@code permits > 1} / {@code permits_total != 0} 时非法。合法返回
     * {@code null}；非法返回映射状态码（统一 {@code INVALID_REQUEST}）。
     * "SEMAPHORE 建条目必填总量 / 既有条目断言匹配"依赖条目存在性，由
     * core 侧判定（{@link io.github.lamspace.openlatch.core.result.Outcome#REJECT_SEMAPHORE_TOTAL}）。
     *
     * @param req 获取请求
     * @return 非法时的状态码；合法为 {@code null}
     */
    public static StatusCode validateAcquirePermits(AcquireRequest req) {
        if (req.getPermits() < 0 || req.getPermitsTotal() < 0) {
            return StatusCode.INVALID_REQUEST;
        }
        if (req.getLockType() != io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_SEMAPHORE
                && (req.getPermits() > 1 || req.getPermitsTotal() != 0)) {
            return StatusCode.INVALID_REQUEST;
        }
        return null;
    }

    /**
     * 许可数归一：proto3 缺省 0 按"单许可"处理（协议注释口径，
     * server 层统一折算，core 只见 {@code >= 1}）。
     *
     * @param permits 协议携带值
     * @return 归一后的许可数（{@code 0 → 1}，其余原值）
     */
    public static int normalizedPermits(int permits) {
        return permits == 0 ? 1 : permits;
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
        if (req.getPermits() < 0) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        ReleaseCommand cmd = new ReleaseCommand(
                session.sessionId(), req.getKey(), req.getLeaseToken(), req.getThreadId(),
                normalizedPermits(req.getPermits()));
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
            case LOCK_TYPE_FAIR -> LockType.FAIR;
            case LOCK_TYPE_SEMAPHORE -> LockType.SEMAPHORE;
            default -> null;
        };
    }

    /**
     * 是否 v3 专属协议锁类型：握版本 &lt;3 的会话请求这些类型 MUST 被消息级
     * 拒绝（详设 §6 兼容性策略）。已接入：{@code FAIR}（P3-02）、
     * {@code SEMAPHORE}（P3-03）；{@code LATCH} 随 P3-05 增列。
     *
     * @param type 协议锁类型
     * @return v3 专属返回 {@code true}
     */
    public static boolean isV3OnlyLockType(io.github.lamspace.openlatch.protocol.LockType type) {
        return type == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_FAIR
                || type == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_SEMAPHORE
                || type == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_LATCH;
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
     * core 获取结果状态 → 协议状态码（全表映射，与屏障命令共用
     * {@link #toLatchStatus} 单表——Outcome 对两类命令的码形语义一致）。
     *
     * @param outcome core 获取结果状态
     * @return 协议状态码
     */
    static StatusCode toAcquireStatus(Outcome outcome) {
        return toLatchStatus(outcome);
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
            // 超额归还是请求参数与持有不符，非租约问题（design D3）。
            case OVER_RELEASE -> StatusCode.INVALID_REQUEST;
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
     * 请求 {@code type} 为协议未定义数值（Protobuf 解析为 {@code UNRECOGNIZED}）时，
     * 响应 type 以 {@code MESSAGE_TYPE_UNKNOWN} 占位并返回无 payload 的信封——
     * MUST NOT 因回显未知类型抛异常而使请求静默悬挂（规格"消息合法性校验"）。
     * 其余未知类型（{@code PING}/{@code AWAIT_NOTIFY}）同样返回无 payload 信封，
     * 仍可被客户端按 {@code request_id} 关联。
     *
     * @param request 原请求信封
     * @param status  错误状态码
     * @return 错误响应信封
     */
    public static Envelope errorResponse(Envelope request, StatusCode status) {
        MessageType requestType = request.getType();
        Envelope.Builder b = Envelope.newBuilder()
                .setProtocolVersion(request.getProtocolVersion())
                .setType(requestType == MessageType.UNRECOGNIZED
                        ? MessageType.MESSAGE_TYPE_UNKNOWN : requestType)
                .setRequestId(request.getRequestId());
        switch (requestType) {
            case HELLO -> b.setHelloResponse(HelloResponse.newBuilder().setStatus(status));
            case LOCK_ACQUIRE -> b.setAcquireResponse(AcquireResponse.newBuilder().setStatus(status));
            case LOCK_RELEASE -> b.setReleaseResponse(ReleaseResponse.newBuilder().setStatus(status));
            case LEASE_RENEW -> b.setLeaseRenewResponse(LeaseRenewResponse.newBuilder().setStatus(status));
            // v2：CLUSTER_VIEW 自带 status，拒绝路径状态码在线路可见（空成员表）。
            case CLUSTER_VIEW -> b.setClusterView(
                    io.github.lamspace.openlatch.protocol.ClusterView.newBuilder().setStatus(status));
            // v3：LATCH 消息同规则——拒绝状态码在线路可见（客户端裁决依赖）。
            case LATCH_COUNT_DOWN -> b.setLatchCountDownResponse(
                    LatchCountDownResponse.newBuilder().setStatus(status));
            case LATCH_AWAIT -> b.setLatchAwaitResponse(
                    LatchAwaitResponse.newBuilder().setStatus(status));
            // v3-T3：ADMIN 消息同规则——认证/门控/限额拒绝的状态码在线路可见
            // （控制台裁决依赖；被拒应答仅带状态码，观察字段留零值）。
            case ADMIN_SUMMARY -> b.setAdminSummaryResponse(
                    AdminSummaryResponse.newBuilder().setStatus(status));
            case ADMIN_LIST_KEYS -> b.setAdminListKeysResponse(
                    AdminListKeysResponse.newBuilder().setStatus(status));
            case ADMIN_KEY_DETAIL -> b.setAdminKeyDetailResponse(
                    AdminKeyDetailResponse.newBuilder().setStatus(status));
            case ADMIN_LIST_SESSIONS -> b.setAdminListSessionsResponse(
                    AdminListSessionsResponse.newBuilder().setStatus(status));
            default -> {
            }
        }
        return b.build();
    }
}
