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
import io.github.lamspace.openlatch.core.AtomicOp;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.AtomicOpCommand;
import io.github.lamspace.openlatch.core.command.BarrierActionDoneCommand;
import io.github.lamspace.openlatch.core.command.BarrierAwaitCommand;
import io.github.lamspace.openlatch.core.command.BarrierLeaveCommand;
import io.github.lamspace.openlatch.core.result.BarrierActionDoneResult;
import io.github.lamspace.openlatch.core.result.BarrierAwaitResult;
import io.github.lamspace.openlatch.core.result.BarrierLeaveResult;
import io.github.lamspace.openlatch.core.command.LatchAwaitCommand;
import io.github.lamspace.openlatch.core.command.LatchCountDownCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.AtomicOpResult;
import io.github.lamspace.openlatch.core.result.LatchAwaitResult;
import io.github.lamspace.openlatch.core.result.LatchCountDownResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.BarrierActionDoneRequest;
import io.github.lamspace.openlatch.protocol.BarrierActionDoneResponse;
import io.github.lamspace.openlatch.protocol.BarrierAwaitRequest;
import io.github.lamspace.openlatch.protocol.BarrierAwaitResponse;
import io.github.lamspace.openlatch.protocol.BarrierLeaveRequest;
import io.github.lamspace.openlatch.protocol.BarrierLeaveResponse;
import io.github.lamspace.openlatch.protocol.AtomicOpRequest;
import io.github.lamspace.openlatch.protocol.AtomicOpResponse;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse;
import io.github.lamspace.openlatch.protocol.AdminListKeysResponse;
import io.github.lamspace.openlatch.protocol.AdminListSessionsResponse;
import io.github.lamspace.openlatch.protocol.AdminSummaryResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.QueueOp;
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.QueueOpResponse;
import io.github.lamspace.openlatch.protocol.TopicOp;
import io.github.lamspace.openlatch.protocol.TopicOpRequest;
import io.github.lamspace.openlatch.protocol.TopicOpResponse;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.LatchAwaitResponse;
import io.github.lamspace.openlatch.protocol.LatchCountDownResponse;
import io.github.lamspace.openlatch.protocol.ReleaseResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.metrics.ServerMetrics;
import io.github.lamspace.openlatch.server.session.ServerSession;

import java.util.Objects;

/**
 * 请求分发：{@code Envelope} → core 命令，core 结果 → {@code Envelope}。
 * 映射为纯函数，可脱离 Netty 单测；{@link #dispatch} 为入口。
 *
 * <p><b>线程模型</b>：{@link #dispatch} 由 {@code ServerSessionHandler} 在
 * 连接所属 EventLoop 线程上同步调用（含 {@code core.acquire}/{@code release}/
 * {@code renew} 的委托执行亦在该线程完成）。本类无可变状态（仅持有 final 的
 * core 与指标引用），可被多连接线程并发进入；单 key 状态的串行性由
 * {@code CoreEngine} 的条目锁保证，不属本类职责。
 *
 * <p><b>指标埋点</b>：每条产出应答的
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
     * 有值引用载荷字节上限（单机分发入口钳制判定用；引擎/条目侧不复核，
     * 钳制点唯一在接入层）。
     */
    private final int maxValueBytes;

    /**
     * v7 队列定型容量上限（单机分发入口钳制；引擎/条目侧不复核，钳制点
     * 唯一在接入层，判例 {@code maxValueBytes}）。
     */
    private final int maxQueueCapacity;

    /**
     * v7 drainTo 应答字节预算（单机分发入口钳制，钳定后的 DRAIN 上限
     * 随命令入引擎）。
     */
    private final long maxDrainBytes;

    /**
     * 构造分发器（不埋点，既有测试夹具形态；载荷上限取内置默认 4096）。
     *
     * @param core 锁语义核心
     */
    public RequestDispatcher(CoreEngine core) {
        this(core, null);
    }

    /**
     * 构造分发器（生产形态：应答经 {@code metrics} 记入服务端指标词表；
     * 载荷上限取内置默认 4096）。
     *
     * @param core    锁语义核心
     * @param metrics 指标门面，可为 {@code null}（不埋点）
     */
    public RequestDispatcher(CoreEngine core, ServerMetrics metrics) {
        this(core, metrics, io.github.lamspace.openlatch.server.ServerConfig.DEFAULT_MAX_VALUE_BYTES);
    }

    /**
     * 构造分发器（v6 三参形态：队列限额取内置默认，既有装配与测试夹具
     * 兼容）。
     *
     * @param core          锁语义核心
     * @param metrics       指标门面，可为 {@code null}（不埋点）
     * @param maxValueBytes 有值引用载荷字节上限（{@code >= 1}）
     */
    public RequestDispatcher(CoreEngine core, ServerMetrics metrics, int maxValueBytes) {
        this(core, metrics, maxValueBytes,
                io.github.lamspace.openlatch.server.ServerConfig.DEFAULT_MAX_QUEUE_CAPACITY,
                io.github.lamspace.openlatch.server.ServerConfig.DEFAULT_MAX_DRAIN_BYTES);
    }

    /**
     * 构造分发器（全参形态：单机分发按 {@code maxValueBytes} 对有值引用载荷
     * 与队列元素做入口钳制、按 {@code maxQueueCapacity}/{@code maxDrainBytes}
     * 对队列容量主张与 drain 上限做入口钳制——钳制点唯一在接入层，
     * 引擎/条目侧不复核）。
     *
     * @param core             锁语义核心
     * @param metrics          指标门面，可为 {@code null}（不埋点）
     * @param maxValueBytes    有值引用载荷/队列元素字节上限（{@code >= 1}）
     * @param maxQueueCapacity 队列定型容量上限（{@code >= 1}）
     * @param maxDrainBytes    drainTo 应答字节预算（{@code >= 1}）
     */
    public RequestDispatcher(CoreEngine core, ServerMetrics metrics, int maxValueBytes,
            int maxQueueCapacity, long maxDrainBytes) {
        this(core, metrics, maxValueBytes, maxQueueCapacity, maxDrainBytes,
                io.github.lamspace.openlatch.server.ServerConfig.DEFAULT_MAX_KEY_LENGTH, null);
    }

    /**
     * v8 topic 登记表；{@code null}=装配不含 topic 面（既有夹具形态，
     * TOPIC_OP 抵达时回 {@code INTERNAL_ERROR} 并记 WARN）。
     */
    private final io.github.lamspace.openlatch.server.topic.TopicRegistry topics;
    /** 锁键长度上限（UTF-8 字节；topic 路径不经引擎，键校验在本层执行）。 */
    private final int maxKeyLength;

    /**
     * 构造分发器（v8 全参形态）：追加 topic 登记表与键长上限——
     * TOPIC_OP 单机路径由本层完成门控、形状、键长与 {@code maxValueBytes}
     * 钳制后交 {@code topics}（零日志：不经引擎、不进状态机）。
     *
     * @param core             锁语义核心
     * @param metrics          指标门面，可为 {@code null}（不埋点）
     * @param maxValueBytes    有值引用载荷/队列元素/topic 消息体字节上限（{@code >= 1}）
     * @param maxQueueCapacity 队列定型容量上限（{@code >= 1}）
     * @param maxDrainBytes    drainTo 应答字节预算（{@code >= 1}）
     * @param maxKeyLength     锁键长度上限（{@code >= 1}，topic 键校验用）
     * @param topics           topic 登记表，可为 {@code null}（夹具无 topic 面）
     */
    public RequestDispatcher(CoreEngine core, ServerMetrics metrics, int maxValueBytes,
            int maxQueueCapacity, long maxDrainBytes, int maxKeyLength,
            io.github.lamspace.openlatch.server.topic.TopicRegistry topics) {
        this.core = Objects.requireNonNull(core);
        this.metrics = metrics;
        this.maxValueBytes = maxValueBytes;
        this.maxQueueCapacity = maxQueueCapacity;
        this.maxDrainBytes = maxDrainBytes;
        this.maxKeyLength = maxKeyLength;
        this.topics = topics;
    }

    /**
     * topic 登记表只读口（装配与测试观察）；夹具形态为 {@code null}。
     *
     * @return 登记表或 {@code null}
     */
    public io.github.lamspace.openlatch.server.topic.TopicRegistry topicRegistry() {
        return topics;
    }

    /**
     * 单机路径会话关闭钩子：摘除该会话的 topic 登记、缓冲与去重槽
     * （死亡即退订）。由接入层断连清理在 {@code core.sessionClosed} 之后
     * 调用；{@code topics} 为 {@code null} 时为空操作。
     *
     * @param sessionId 关闭的会话 id
     */
    public void onSessionClosedTopics(long sessionId) {
        if (topics != null) {
            topics.removeSession(sessionId);
        }
    }

    /**
     * 分发一条已握手连接上的业务消息。返回要写回的响应；{@code PING} 返回
     * {@code null}（不回复）。未知类型与 payload 不匹配回 {@code INVALID_REQUEST}，
     * 不断连。
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
            case ATOMIC_OP -> msg.hasAtomicOpRequest()
                    ? dispatchAtomicOp(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case BARRIER_AWAIT -> msg.hasBarrierAwaitRequest()
                    ? dispatchBarrierAwait(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case BARRIER_LEAVE -> msg.hasBarrierLeaveRequest()
                    ? dispatchBarrierLeave(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case BARRIER_ACTION_DONE -> msg.hasBarrierActionDoneRequest()
                    ? dispatchBarrierActionDone(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case QUEUE_OP -> msg.hasQueueOpRequest()
                    ? dispatchQueueOp(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case TOPIC_OP -> msg.hasTopicOpRequest()
                    ? dispatchTopicOp(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case CONDITION_OP -> msg.hasConditionOpRequest()
                    ? dispatchConditionOp(session, msg)
                    : errorResponse(msg, StatusCode.INVALID_REQUEST);
            case PHASER_OP -> msg.hasPhaserOpRequest()
                    ? dispatchPhaserOp(session, msg)
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
     * 分发屏障倒计数：负参数在协议层拒绝；
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
     * 分发屏障等待：判定与门控同上；QUEUED 携带位次，
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
     * GRANTED=OK、拒绝细分同码，协议面不新增状态码；v4 起原子通道的
     * 初值断言与值域越界拒绝亦并入本表，CAS 家族成败不经状态码——
     * 由应答 {@code applied} 承载）。
     *
     * @param outcome 屏障/原子命令结果状态
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
            // v4：初值断言与值域越界同族同码（形状非法，非租约/会话问题）。
            case REJECT_ATOMIC_INIT -> StatusCode.INVALID_REQUEST;
            case REJECT_ATOMIC_RANGE -> StatusCode.INVALID_REQUEST;
            // v5：parties 断言与动作回报不被受理同属形状非法；破障是在带裁决。
            case REJECT_BARRIER_PARTIES -> StatusCode.INVALID_REQUEST;
            case REJECT_BARRIER_ACTION -> StatusCode.INVALID_REQUEST;
            // v7：队列容量断言不成立同属形状非法（非零主张判例）。
            case REJECT_QUEUE_CAPACITY -> StatusCode.INVALID_REQUEST;
            // v10：phaser 配额上限/离场透支/无条目——上限骑资源护栏码，其余
            // 形状非法（判例 toPhaserStatus 同映射，两超限以线路 op 分轨）。
            case REJECT_PHASER_PARTIES -> StatusCode.OVERLOADED;
            case REJECT_PHASER_QUOTA -> StatusCode.INVALID_REQUEST;
            case REJECT_PHASER_NO_ENTRY -> StatusCode.INVALID_REQUEST;
            case BARRIER_BROKEN -> StatusCode.BARRIER_BROKEN;
        };
    }

    /**
     * 分发原子变量操作（v4）：形状合法性（{@link #validateAtomicRequest}）与
     * v4 门控先于命令构造——非法请求零入核、零扰动；低版本会话消息级拒绝、
     * 不断连（判例：LATCH 消息对 v3 门）。应答构造见 {@link #toAtomicOpResponse}；
     * 单机路径直调引擎，集群路径的对应车道在
     * {@code ClusterRequestHandler.handleAtomicOp}。
     *
     * @param session 已握手会话
     * @param msg     入站消息信封
     * @return 协议响应信封
     */
    private Envelope dispatchAtomicOp(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 4) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        AtomicOpRequest req = msg.getAtomicOpRequest();
        if (req.getLockType()
                == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_REFERENCE
                && session.protocolVersion() < 6) {
            // v6 专属语义：低版本会话引用形态消息级拒绝、不断连（判例各门）。
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        StatusCode shapeBad = validateAtomicRequest(req);
        if (shapeBad == null) {
            shapeBad = validateRefPayloadClamp(req, maxValueBytes);
        }
        if (shapeBad != null) {
            return errorResponse(msg, shapeBad);
        }
        if (req.getLockType()
                == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_REFERENCE) {
            io.github.lamspace.openlatch.core.result.AtomicRefOpResult refResult =
                    core.atomicRefOp(toAtomicRefCommand(session.sessionId(),
                            msg.getRequestId(), req));
            if (metrics != null) {
                metrics.recordAtomic(req.getLockType(), req.getOp(),
                        toLatchStatus(refResult.outcome()));
            }
            return toAtomicRefOpResponse(msg, refResult);
        }
        AtomicOpResult result = core.atomicOp(toAtomicCommand(session.sessionId(),
                msg.getRequestId(), req));
        if (metrics != null) {
            metrics.recordAtomic(req.getLockType(), req.getOp(), toLatchStatus(result.outcome()));
        }
        return toAtomicOpResponse(msg, result);
    }

    /**
     * 原子请求形状合法性（单机与集群共用判定，先于日志提交）：
     * 形态非三原子类型、op 未知数值、写操作缺 {@code op_seq}（客户端义务）、
     * 负 {@code expected_version}、负 {@code initial_value}、布尔形态的
     * 落值/期望值越出 {0,1} 或携带 ADD——任一命中回
     * {@code INVALID_REQUEST}。合法返回 {@code null}。core 侧另有权威兜底
     * （{@code REJECT_ATOMIC_RANGE}），本判定只为"非法不入日志"。
     *
     * @param req 原子操作请求
     * @return 非法时的状态码；合法为 {@code null}
     */
    public static StatusCode validateAtomicRequest(AtomicOpRequest req) {
        io.github.lamspace.openlatch.protocol.LockType kind = req.getLockType();
        boolean reference = kind
                == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_REFERENCE;
        if (!reference && kind != io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_LONG
                && kind != io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_INTEGER
                && kind != io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_BOOLEAN) {
            return StatusCode.INVALID_REQUEST;
        }
        AtomicOp op = toCoreAtomicOp(req.getOp());
        if (op == null) {
            return StatusCode.INVALID_REQUEST;
        }
        if (op != AtomicOp.GET && req.getOpSeq() < 1) {
            return StatusCode.INVALID_REQUEST;
        }
        if (op == AtomicOp.GET && req.getOpSeq() != 0) {
            return StatusCode.INVALID_REQUEST;
        }
        if (req.getExpectedVersion() < 0) {
            return StatusCode.INVALID_REQUEST;
        }
        if (reference) {
            // 有值引用形态：ADD 值域外（字节串无加法，判例布尔 ADD）；
            // 标量槽位 MUST 为 0（操作数全走 optional bytes 通道）。
            if (op == AtomicOp.ADD || req.getOperand() != 0 || req.getExpected() != 0
                    || req.getInitialValue() != 0) {
                return StatusCode.INVALID_REQUEST;
            }
            return null;
        }
        if (req.getInitialValue() < 0) {
            return StatusCode.INVALID_REQUEST;
        }
        // 标量形态 MUST NOT 携带引用载荷字段（presence 即形状违例）。
        if (req.hasOperandBytes() || req.hasExpectedBytes() || req.hasInitialBytes()) {
            return StatusCode.INVALID_REQUEST;
        }
        if (kind == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_BOOLEAN) {
            boolean valuesOk = (op != AtomicOp.CAS && op != AtomicOp.CAS_STAMPED)
                    || req.getExpected() == 0 || req.getExpected() == 1;
            boolean operandOk = op == AtomicOp.ADD
                    || req.getOperand() == 0 || req.getOperand() == 1;
            if (op == AtomicOp.ADD || !valuesOk || !operandOk) {
                return StatusCode.INVALID_REQUEST;
            }
        }
        return null;
    }

    /**
     * 有值引用载荷入口钳制（v6，尺寸判定唯一发生在接入层——超限命令
     * MUST NOT 入日志/触引擎）：引用形态请求任一 {@code optional bytes}
     * 字段（operand/expected/initial）字节长度超过 {@code maxValueBytes}
     * 时回 {@code INVALID_REQUEST}；非引用形态恒合法（其载荷 presence
     * 属形状违例，由 {@link #validateAtomicRequest} 拦截）。
     *
     * @param req           原子操作请求（形状已先行校验）
     * @param maxValueBytes 本节点载荷上限
     * @return 超限时的状态码；合法为 {@code null}
     */
    public static StatusCode validateRefPayloadClamp(AtomicOpRequest req, int maxValueBytes) {
        if (req.getLockType()
                != io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_REFERENCE) {
            return null;
        }
        if ((req.hasOperandBytes() && req.getOperandBytes().size() > maxValueBytes)
                || (req.hasExpectedBytes() && req.getExpectedBytes().size() > maxValueBytes)
                || (req.hasInitialBytes() && req.getInitialBytes().size() > maxValueBytes)) {
            return StatusCode.INVALID_REQUEST;
        }
        return null;
    }

    /**
     * 队列请求形状校验（v7，配置无关的形状矩阵，单机分发与集群 Leader
     * 入口共用——违例以 {@code INVALID_REQUEST} 消息级拒绝、不断连、零扰动）：
     * {@code lock_type} MUST 为队列双形态之一；元素仅 PUT 携带且 PUT MUST
     * 携带（元素不可为 null——缺省 presence 即违例，与引用形态的 null
     * 语义刻意不同）；{@code delay_ms} 仅 DELAY_QUEUE 形态的 PUT 可携非零
     * （负值违例）；{@code max_elements} 仅 DRAIN 可携非零；{@code blocking}
     * 仅 PUT/TAKE 可携 {@code true}；写类 op {@code op_seq ≥ 1}、读类
     * （PEEK/SIZE）{@code op_seq} MUST 为 0；容量断言 MUST 非负。
     * 尺寸钳制（元素字节/容量上限/drain 预算）经 {@link #clampQueueRequest}
     * 另行执行，不含在本矩阵内。
     *
     * @param req 队列请求
     * @return 非法时 {@code INVALID_REQUEST}；合法为 {@code null}
     */
    public static StatusCode validateQueueShape(QueueOpRequest req) {
        io.github.lamspace.openlatch.protocol.LockType kind = req.getLockType();
        if (kind != io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_QUEUE
                && kind != io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_DELAY_QUEUE) {
            return StatusCode.INVALID_REQUEST;
        }
        QueueOp op = req.getOp();
        if (op == QueueOp.UNRECOGNIZED) {
            return StatusCode.INVALID_REQUEST;
        }
        boolean put = op == QueueOp.QUEUE_OP_PUT;
        boolean take = op == QueueOp.QUEUE_OP_TAKE;
        boolean drain = op == QueueOp.QUEUE_OP_DRAIN;
        boolean read = op == QueueOp.QUEUE_OP_PEEK || op == QueueOp.QUEUE_OP_SIZE;
        if (put != req.hasElementBytes()) {
            return StatusCode.INVALID_REQUEST;
        }
        if (req.getBlocking() && !(put || take)) {
            return StatusCode.INVALID_REQUEST;
        }
        if (req.getCapacity() < 0 || req.getDelayMs() < 0 || req.getMaxElements() < 0) {
            return StatusCode.INVALID_REQUEST;
        }
        if (req.getDelayMs() > 0 && !(put && kind
                == io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_DELAY_QUEUE)) {
            return StatusCode.INVALID_REQUEST;
        }
        if (req.getMaxElements() > 0 && !drain) {
            return StatusCode.INVALID_REQUEST;
        }
        if (read ? req.getOpSeq() != 0 : req.getOpSeq() < 1) {
            return StatusCode.INVALID_REQUEST;
        }
        return null;
    }

    /**
     * 队列入口钳制结果（v7）：拒绝时 {@code error} 非空、{@code request}
     * 为 {@code null}；放行时 {@code request} 为钳定后的最终请求——DRAIN
     * 提取上限折算为正值随条目入日志（全副本应用一致，apply 不读本地
     * 配置，判例 v6 钳制唯一在接入层）。
     *
     * @param error   入口拒绝状态码（{@code null}=放行）
     * @param request 钳定后的请求
     */
    public record QueueClamp(StatusCode error, QueueOpRequest request) {
    }

    /**
     * 队列入口钳制（v7）：PUT 元素字节数超 {@code maxValueBytes} 拒；容量
     * 主张超 {@code maxQueueCapacity} 拒；DRAIN 提取上限钳定到
     * {@code min(主张值(0=取派生上限), maxQueueCapacity, floor(maxDrainBytes /
     * maxValueBytes))}（下限 1）回写请求。超限 MUST 入口拒绝且零日志/零
     * 引擎调用；钳制下调不追溯存量元素（既有大元素照常可读可消费）。
     *
     * @param req              队列请求（已经 {@link #validateQueueShape} 放行）
     * @param maxValueBytes    单元素载荷上限
     * @param maxQueueCapacity 容量主张上限
     * @param maxDrainBytes    drain 应答字节预算
     * @return 钳制结果
     */
    public static QueueClamp clampQueueRequest(QueueOpRequest req, int maxValueBytes,
            long maxQueueCapacity, long maxDrainBytes) {
        if (req.hasElementBytes() && req.getElementBytes().size() > maxValueBytes) {
            return new QueueClamp(StatusCode.INVALID_REQUEST, null);
        }
        if (req.getCapacity() > maxQueueCapacity) {
            return new QueueClamp(StatusCode.INVALID_REQUEST, null);
        }
        QueueOpRequest out = req;
        if (req.getOp() == QueueOp.QUEUE_OP_DRAIN) {
            long derived = Math.max(1L, Math.min(Math.max(1L, maxDrainBytes)
                            / Math.max(1, maxValueBytes),
                    Math.max(1L, maxQueueCapacity)));
            int limit = req.getMaxElements();
            if (limit <= 0 || limit > derived) {
                out = req.toBuilder()
                        .setMaxElements((int) Math.min(derived, Integer.MAX_VALUE))
                        .build();
            }
        }
        return new QueueClamp(null, out);
    }

    /**
     * 协议队列请求 → core 命令（形态与操作映射；{@code blocking} 位原样
     * 承载——单机路径由条目裁决挂起，集群 apply 侧固定立即式）。
     *
     * @param sessionId 引擎内部或单机会话 id
     * @param requestId 信封请求 id
     * @param req       队列请求（已过形状与钳制）
     * @return core 命令
     */
    public static io.github.lamspace.openlatch.core.command.QueueOpCommand toQueueCommand(
            long sessionId, long requestId, QueueOpRequest req) {
        return new io.github.lamspace.openlatch.core.command.QueueOpCommand(
                sessionId, requestId, req.getKey(),
                io.github.lamspace.openlatch.server.raft.LockStateMachineCore
                        .toCoreQueueKind(req.getLockType().getNumber()),
                io.github.lamspace.openlatch.server.raft.LockStateMachineCore
                        .toCoreQueueOp(req.getOp()),
                req.getBlocking(), req.getCapacity(),
                req.hasElementBytes() ? req.getElementBytes().toByteArray() : null,
                req.getDelayMs(), req.getMaxElements(), req.getOpSeq());
    }

    /**
     * core 队列结果 → 协议应答（v7）：GRANTED 择用交付字段（TAKE/PEEK 显式
     * presence——空队/未到期回缺省；DRAIN 空列表为合法结果；SIZE 读数），
     * QUEUED 携位次，拒绝态交付字段全缺省；{@code capacity} 恒回显条目
     * 定型值（key 尚不存在为 0）。
     *
     * @param msg 原请求信封
     * @param r   core 结果
     * @return 应答信封
     */
    public static Envelope toQueueOpResponse(Envelope msg,
            io.github.lamspace.openlatch.core.result.QueueOpResult r) {
        QueueOpResponse.Builder b = QueueOpResponse.newBuilder()
                .setStatus(toLatchStatus(r.outcome()))
                .setOp(msg.getQueueOpRequest().getOp())
                .setQueuePosition(r.queuePosition())
                .setSize(r.size())
                .setCapacity(r.capacity());
        if (r.element() != null) {
            b.setElementBytes(com.google.protobuf.ByteString.copyFrom(r.element()));
        }
        for (byte[] element : r.drained()) {
            b.addDrainedBytes(com.google.protobuf.ByteString.copyFrom(element));
        }
        return envelope(msg, MessageType.QUEUE_OP, x -> x.setQueueOpResponse(b));
    }

    /**
     * 协议原子请求 → core 命令（形态与操作映射；{@code UNRECOGNIZED}
     * 已由 {@link #validateAtomicRequest} 拦截，此处仅收口编译穷尽）。
     *
     * @param sessionId 会话 id
     * @param requestId 连接内请求 id
     * @param req       协议请求
     * @return core 命令
     */
    static AtomicOpCommand toAtomicCommand(long sessionId, long requestId, AtomicOpRequest req) {
        return new AtomicOpCommand(sessionId, requestId, req.getKey(),
                toCoreAtomicKind(req.getLockType().getNumber()),
                toCoreAtomicOp(req.getOp()), req.getOperand(), req.getExpected(),
                req.getExpectedVersion(), req.getInitialValue(), req.getOpSeq());
    }

    /**
     * 协议原子请求（引用形态）→ core 引用命令。{@code optional bytes}
     * 按显式 presence 还原：缺省={@code null}、零长度=空字节串。仅在
     * {@link #validateAtomicRequest} 与 {@link #validateRefPayloadClamp}
     * 通过后调用。
     *
     * @param sessionId 会话 id
     * @param requestId 连接内请求 id
     * @param req       协议请求（引用形态）
     * @return core 引用命令
     */
    static io.github.lamspace.openlatch.core.command.AtomicRefOpCommand toAtomicRefCommand(
            long sessionId, long requestId, AtomicOpRequest req) {
        return new io.github.lamspace.openlatch.core.command.AtomicRefOpCommand(
                sessionId, requestId, req.getKey(), toCoreAtomicOp(req.getOp()),
                req.hasOperandBytes() ? req.getOperandBytes().toByteArray() : null,
                req.hasExpectedBytes() ? req.getExpectedBytes().toByteArray() : null,
                req.getExpectedVersion(),
                req.hasInitialBytes() ? req.getInitialBytes().toByteArray() : null,
                req.getOpSeq());
    }

    /**
     * core 引用结果 → 协议应答（v6）：{@code GRANTED} 携带载荷应答四元组
     * （null 态=bytes 字段缺省、零长度=EMPTY）与 op 回显，标量值位恒 0；
     * 拒绝态四元组留零值形（bytes 全缺省）。
     *
     * @param msg    原请求信封
     * @param result core 引用结果
     * @return 应答信封
     */
    static Envelope toAtomicRefOpResponse(Envelope msg,
            io.github.lamspace.openlatch.core.result.AtomicRefOpResult result) {
        AtomicOpResponse.Builder resp = AtomicOpResponse.newBuilder()
                .setStatus(toLatchStatus(result.outcome()))
                .setOp(msg.getAtomicOpRequest().getOp());
        if (result.outcome() == Outcome.GRANTED) {
            resp.setApplied(result.applied()).setVersion(result.version());
            if (result.oldValue() != null) {
                resp.setOldValueBytes(com.google.protobuf.ByteString.copyFrom(result.oldValue()));
            }
            if (result.value() != null) {
                resp.setValueBytes(com.google.protobuf.ByteString.copyFrom(result.value()));
            }
        }
        return envelope(msg, MessageType.ATOMIC_OP, b -> b.setAtomicOpResponse(resp));
    }

    /**
     * core 原子操作结果 → 协议应答（状态码经 {@link #toLatchStatus} 共表映射；
     * {@code GRANTED} 携带应答四元组与 op 回显，拒绝态四元组留零值形）。
     *
     * @param msg    原请求信封
     * @param result core 结果
     * @return 应答信封
     */
    static Envelope toAtomicOpResponse(Envelope msg, AtomicOpResult result) {
        AtomicOpResponse.Builder resp = AtomicOpResponse.newBuilder()
                .setStatus(toLatchStatus(result.outcome()))
                .setOp(msg.getAtomicOpRequest().getOp());
        if (result.outcome() == Outcome.GRANTED) {
            resp.setApplied(result.applied())
                    .setOldValue(result.oldValue())
                    .setValue(result.value())
                    .setVersion(result.version());
        }
        return envelope(msg, MessageType.ATOMIC_OP, b -> b.setAtomicOpResponse(resp));
    }

    /**
     * 分发循环屏障到场（v5）：v5 门控与形状合法性先于引擎调用——低版本
     * 会话消息级拒绝、不断连（判例：LATCH v3 门、ATOMIC v4 门）；
     * {@code parties < 0} 属参数非法。应答携带世代/位次/执行者标记与
     * 定型回显；单机路径的放行/破障广播经引擎监听点直投
     * {@code AWAIT_NOTIFY}（与 Latch 同通道）。
     *
     * @param session 已握手会话
     * @param msg     入站消息信封
     * @return 协议响应信封
     */
    private Envelope dispatchBarrierAwait(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 5) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        BarrierAwaitRequest req = msg.getBarrierAwaitRequest();
        if (req.getParties() < 0) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        BarrierAwaitResult r = core.barrierAwait(new BarrierAwaitCommand(
                session.sessionId(), msg.getRequestId(), req.getKey(),
                req.getParties(), req.getCarriesAction()));
        StatusCode status = toLatchStatus(r.outcome());
        if (metrics != null) {
            metrics.recordBarrier("await", status);
        }
        return envelope(msg, MessageType.BARRIER_AWAIT, b -> b.setBarrierAwaitResponse(
                BarrierAwaitResponse.newBuilder()
                        .setStatus(status)
                        .setQueuePosition(r.queuePosition())
                        .setGeneration(r.generation())
                        .setExecutor(r.executor())
                        .setParties(r.parties())));
    }

    /**
     * 分发循环屏障离场（v5，超时/中断/显式破障共用通道）：门控同上；
     * {@code await_request_id < 0} 属参数非法；OK 即离场完成（离场
     * 连带破障经其余等待者的重发了结可见，幂等无操作亦回 OK）。
     *
     * @param session 已握手会话
     * @param msg     入站消息信封
     * @return 协议响应信封
     */
    private Envelope dispatchBarrierLeave(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 5) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        BarrierLeaveRequest req = msg.getBarrierLeaveRequest();
        if (req.getAwaitRequestId() < 0) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        BarrierLeaveResult r = core.barrierLeave(new BarrierLeaveCommand(
                session.sessionId(), req.getKey(), req.getAwaitRequestId()));
        StatusCode status = toLatchStatus(r.outcome());
        if (metrics != null) {
            metrics.recordBarrier("leave", status);
        }
        return envelope(msg, MessageType.BARRIER_LEAVE, b -> b.setBarrierLeaveResponse(
                BarrierLeaveResponse.newBuilder().setStatus(status)));
    }

    /**
     * 分发循环屏障动作了结（v5）：门控同上；{@code generation <= 0} 属
     * 参数非法（世代号自 1 起）；非指定执行者/未知世代回
     * {@code INVALID_REQUEST}，世代已破回在带裁决 {@code BARRIER_BROKEN}。
     *
     * @param session 已握手会话
     * @param msg     入站消息信封
     * @return 协议响应信封
     */
    private Envelope dispatchBarrierActionDone(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 5) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        BarrierActionDoneRequest req = msg.getBarrierActionDoneRequest();
        if (req.getGeneration() <= 0) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        BarrierActionDoneResult r = core.barrierActionDone(new BarrierActionDoneCommand(
                session.sessionId(), req.getKey(), req.getGeneration()));
        StatusCode status = toLatchStatus(r.outcome());
        if (metrics != null) {
            metrics.recordBarrier("action_done", status);
        }
        return envelope(msg, MessageType.BARRIER_ACTION_DONE, b -> b.setBarrierActionDoneResponse(
                BarrierActionDoneResponse.newBuilder().setStatus(status)));
    }

    /**
     * 分发队列操作（v7）：v7 门控与形状校验、入口钳制先于核心调用——
     * 非法请求零入核、消息级拒绝不断连（判例各门与原子通道钳制）。
     * 单机路径的挂起由条目双轨承载（引擎条目内 awaiters，判例 Latch
     * 单机形态），唤醒经引擎事件出口推送、延时到期由本机调度周期扫描
     * 驱动。应答构造见 {@link #toQueueOpResponse}。
     *
     * @param session 已握手会话
     * @param msg     请求信封（{@code queue_op_request} 分支）
     * @return 应答信封
     */
    private Envelope dispatchQueueOp(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 7) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        QueueOpRequest req = msg.getQueueOpRequest();
        StatusCode shapeBad = validateQueueShape(req);
        QueueClamp clamp = shapeBad == null
                ? clampQueueRequest(req, maxValueBytes, maxQueueCapacity, maxDrainBytes)
                : new QueueClamp(shapeBad, null);
        if (clamp.error() != null) {
            return errorResponse(msg, clamp.error());
        }
        io.github.lamspace.openlatch.core.result.QueueOpResult r =
                core.queueOp(toQueueCommand(session.sessionId(), msg.getRequestId(),
                        clamp.request()));
        if (metrics != null) {
            metrics.recordQueue(clamp.request().getOp(), toLatchStatus(r.outcome()));
        }
        return toQueueOpResponse(msg, r);
    }

    /**
     * topic 请求形状互斥矩阵（v8，两路共用）：PUBLISH 必携 {@code
     * payload_bytes}（缺省即违例，判例 v7 元素纪律）且 {@code op_seq >= 1}；
     * SUBSCRIBE/UNSUBSCRIBE 不携带载荷且 {@code op_seq == 0}；
     * {@code UNRECOGNIZED} 拒绝。判定唯一在接入层，登记表不复核。
     *
     * @param req topic 请求
     * @return 非法时 {@code INVALID_REQUEST}；合法为 {@code null}
     */
    public static StatusCode validateTopicShape(TopicOpRequest req) {
        TopicOp op = req.getOp();
        if (op == TopicOp.UNRECOGNIZED) {
            return StatusCode.INVALID_REQUEST;
        }
        return switch (op) {
            case TOPIC_OP_PUBLISH -> req.hasPayloadBytes() && req.getOpSeq() >= 1
                    ? null : StatusCode.INVALID_REQUEST;
            case TOPIC_OP_SUBSCRIBE, TOPIC_OP_UNSUBSCRIBE ->
                    !req.hasPayloadBytes() && req.getOpSeq() == 0
                    ? null : StatusCode.INVALID_REQUEST;
            default -> StatusCode.INVALID_REQUEST;
        };
    }

    /**
     * topic 键校验（v8，两路共用）：空键回 {@code KEY_EMPTY}、超长按
     * {@code KEY_TOO_LONG}——与引擎 acquire 的键纪律同码形（topic 不经引擎，
     * 校验上收到接入层）。
     *
     * @param key          topic 键
     * @param maxKeyLength 键长上限（UTF-8 字节）
     * @return 非法状态码；合法为 {@code null}
     */
    public static StatusCode validateTopicKey(String key, int maxKeyLength) {
        if (key.isEmpty()) {
            return StatusCode.KEY_EMPTY;
        }
        if (key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > maxKeyLength) {
            return StatusCode.KEY_TOO_LONG;
        }
        return null;
    }

    /**
     * 分发 topic 操作（v8，单机路径）：v8 门、形状互斥、键校验、载荷
     * {@code maxValueBytes} 钳制、撞 key 只读探测（引擎条目在场即拒，
     * 内部裁决 {@code REJECT_TYPE_MISMATCH}、线路送达 {@code INVALID_REQUEST}，
     * 判例引擎形状拒绝映射）先于登记表调用——全部拒绝态零登记、零扇出。
     * 受理成功后按操作分派：SUBSCRIBE 幂等登记（达上限回
     * {@code REJECT_SUBSCRIBERS}）、UNSUBSCRIBE 幂等摘除、PUBLISH 经去重槽
     * 受理并入 fan-out（至多一次：{@code OK} 不承诺交付）。计数线在
     * 受理点记录（形状非法不计数，杜绝维度伪造，判例 atomic/queue）。
     *
     * @param session 已握手会话（握手门闩保证会话在场）
     * @param msg     请求信封（{@code topic_op_request} 分支）
     * @return 应答信封（同型 topic_op_response）
     */
    private Envelope dispatchTopicOp(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 8) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        if (topics == null) {
            java.util.logging.Logger.getLogger(RequestDispatcher.class.getName())
                    .warning("TOPIC_OP dispatched without topic registry assembly");
            return errorResponse(msg, StatusCode.INTERNAL_ERROR);
        }
        TopicOpRequest req = msg.getTopicOpRequest();
        StatusCode shapeBad = validateTopicShape(req);
        if (shapeBad != null) {
            return errorResponse(msg, shapeBad);
        }
        StatusCode keyBad = validateTopicKey(req.getKey(), maxKeyLength);
        if (keyBad != null) {
            return errorResponse(msg, keyBad);
        }
        if (req.hasPayloadBytes() && req.getPayloadBytes().size() > maxValueBytes) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        long nowMs = System.currentTimeMillis();
        String key = req.getKey();
        TopicOpResponse.Builder out = TopicOpResponse.newBuilder().setOp(req.getOp());
        switch (req.getOp()) {
            case TOPIC_OP_SUBSCRIBE -> {
                if (core.inspectKey(key) != null) {
                    return errorResponse(msg, StatusCode.INVALID_REQUEST);
                }
                var r = topics.subscribe(session.sessionId(), key, nowMs);
                out.setStatus(r.status()).setSubscriptionId(r.subscriptionId());
            }
            case TOPIC_OP_UNSUBSCRIBE -> {
                topics.unsubscribe(session.sessionId(), key);
                out.setStatus(StatusCode.OK);
            }
            case TOPIC_OP_PUBLISH -> {
                if (core.inspectKey(key) != null) {
                    return errorResponse(msg, StatusCode.INVALID_REQUEST);
                }
                var r = topics.publish(session.sessionId(), key, req.getOpSeq(),
                        req.getPayloadBytes().toByteArray(), nowMs);
                out.setStatus(StatusCode.OK).setTopicSeq(r.topicSeq());
            }
            default -> {
                return errorResponse(msg, StatusCode.INVALID_REQUEST);
            }
        }
        if (metrics != null) {
            metrics.recordTopic(req.getOp(), out.getStatus());
        }
        return envelope(msg, MessageType.TOPIC_OP, b -> b.setTopicOpResponse(out));
    }

    /**
     * 协议形态数值 → core 原子形态（枚举序对齐 7/8/9）；其余回 {@code null}。
     *
     * @param number 协议 {@code LockType} 数值
     * @return core 形态，非原子为 {@code null}
     */
    static LockType toCoreAtomicKind(int number) {
        return switch (number) {
            case 7 -> LockType.ATOMIC_LONG;
            case 8 -> LockType.ATOMIC_INTEGER;
            case 9 -> LockType.ATOMIC_BOOLEAN;
            case 11 -> LockType.ATOMIC_REFERENCE;
            default -> null;
        };
    }

    /**
     * 协议操作枚举 → core {@link AtomicOp}；未知数值回 {@code null}。
     *
     * @param wireOp 协议枚举值
     * @return core 操作，未知为 {@code null}
     */
    static AtomicOp toCoreAtomicOp(io.github.lamspace.openlatch.protocol.AtomicOp wireOp) {
        return switch (wireOp) {
            case ATOMIC_GET -> AtomicOp.GET;
            case ATOMIC_SET -> AtomicOp.SET;
            case ATOMIC_GET_AND_SET -> AtomicOp.GET_AND_SET;
            case ATOMIC_ADD -> AtomicOp.ADD;
            case ATOMIC_CAS -> AtomicOp.CAS;
            case ATOMIC_CAS_STAMPED -> AtomicOp.CAS_STAMPED;
            default -> null;
        };
    }

    /**
     * 分发获取锁请求：协议 {@code AcquireRequest} → core {@code AcquireCommand}
     * → 结果映射为协议响应。{@code wait_ms == 0} 映射为立即式（不排队），
     * {@code -1} 与正数均映射为可排队；租约到期时刻
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
        // v3 门控：新锁类型仅对握手中声明 v3 的会话开放，v1/v2
        // 会话消息级拒绝、不断连（请求形状错误，非安全事件）。
        if (session.protocolVersion() < 3 && isV3OnlyLockType(req.getLockType())) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        // v9 门控：ACQUIRE 携带 condition 字段仅对 v9 会话开放（v≤8 消息级拒绝、
        // 不断连，判例 v3-v8 门；唤醒后的重发信封按协议纪律清除该字段，走普通获取）。
        if (req.hasCondition() && session.protocolVersion() < 9) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        // v9 折叠形状唯一裁决（判定在接入层，判例 validateAcquirePermits）：
        // 立即式携带 condition 违例；lock_type 限互斥三型；condition 名须非空
        // 且 UTF-8 字节数 ≤ maxKeyLength（空串/超长同型拒绝、零扰动——与集群
        // ClusterRequestHandler.handleAcquireFold 的接入层裁决逐字对称，core
        // 折叠路径对条件名不做二次裁决）。
        if (req.hasCondition() && (!acquireFoldShapeValid(req)
                || req.getCondition().isEmpty()
                || req.getCondition().getBytes(java.nio.charset.StandardCharsets.UTF_8)
                        .length > maxKeyLength)) {
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
                req.getWaitMs() != 0,   // wait_ms == 0 立即式；-1 与 >0 均可排队
                normalizedPermits(req.getPermits()),
                req.getPermitsTotal(),
                req.hasCondition() ? req.getCondition() : null);   // v9 折叠形态
        AcquireResult result = core.acquire(cmd);
        return toAcquireResponse(msg, result, System.currentTimeMillis());
    }

    /**
     * v9 await 折叠形状合法性（判定唯一在接入层，判例 {@link #validateAcquirePermits}）：
     * {@code wait_ms == 0}（立即式）携带 condition 违例——await 恒为挂起形态，
     * {@code >0} 的本地计时窗与 {@code -1} 对服务端同判；{@code lock_type} 限
     * REENTRANT/SIMPLE/FAIR 三互斥形态（v1 支持面，读写形态与其余家族携带属违例）。
     * 条件名非空与长度上限（UTF-8 字节数 ≤ {@code maxKeyLength}）由本类
     * {@code dispatchAcquire} 门与集群 {@code ClusterRequestHandler.handleAcquireFold}
     * 同点裁决（空串/超长同型 {@code INVALID_REQUEST}、零扰动；core 折叠路径
     * 对条件名不再二次裁决——判定唯一在接入层）。
     *
     * @param req 获取请求（已确认 {@code hasCondition()}）
     * @return 形状合法返回 true
     */
    public static boolean acquireFoldShapeValid(AcquireRequest req) {
        if (req.getWaitMs() == 0) {
            return false;
        }
        return switch (req.getLockType()) {
            case LOCK_TYPE_REENTRANT, LOCK_TYPE_SIMPLE, LOCK_TYPE_FAIR -> true;
            default -> false;
        };
    }

    /**
     * 分发条件 signal 家族操作（v9，单机形态=常驻权威）：门控与 op×字段携带矩阵
     * 唯一裁决在接入层（违例同型 {@code INVALID_REQUEST}、等待集零扰动）；裁决后
     * 交 core 门面即时回执（AWAIT 不经本通道——它是 ACQUIRE 携带 condition 的
     * 折叠形态）。唤醒通知经既有队首监听器出口推送。
     *
     * @param session 已握手会话
     * @param msg     入站消息信封（已确认携带 {@code ConditionOpRequest}）
     * @return 协议响应信封（status + op 回显）
     */
    private Envelope dispatchConditionOp(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 9) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        var req = msg.getConditionOpRequest();
        if (conditionOpShapeInvalid(req)) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        io.github.lamspace.openlatch.core.result.ConditionOpResult result =
                core.conditionOp(new io.github.lamspace.openlatch.core.command.ConditionOpCommand(
                        session.sessionId(), req.getThreadId(), req.getKey(),
                        req.getCondition(), toCoreConditionOp(req.getOp()),
                        req.getAwaitRequestId()));
        StatusCode status = toConditionStatus(result.status());
        if (metrics != null) {
            metrics.recordCondition(req.getOp(), status);
        }
        return envelope(msg, MessageType.CONDITION_OP, b -> b.setConditionOpResponse(
                io.github.lamspace.openlatch.protocol.ConditionOpResponse.newBuilder()
                        .setStatus(status)
                        .setOp(req.getOp())));
    }

    /**
     * v9 signal 家族 op×字段携带矩阵违例判定（接入层唯一裁决，判例 topic 形状矩阵）：
     * SIGNAL/SIGNAL_ALL 携非零 {@code await_request_id} 违例；LEAVE 必携非零
     * {@code await_request_id} 且 {@code thread_id} 为 0、条件名非空；
     * 矩阵外组合一律违例。
     *
     * @param req 条件操作请求
     * @return 违例返回 true
     */
    public static boolean conditionOpShapeInvalid(
            io.github.lamspace.openlatch.protocol.ConditionOpRequest req) {
        return switch (req.getOp()) {
            case CONDITION_OP_SIGNAL, CONDITION_OP_SIGNAL_ALL -> req.getAwaitRequestId() != 0;
            case CONDITION_OP_LEAVE -> req.getAwaitRequestId() == 0 || req.getThreadId() != 0
                    || req.getCondition().isEmpty();
            default -> true;
        };
    }

    /**
     * 协议条件操作词 → core 词表。
     *
     * @param op 协议操作词
     * @return core 操作词
     */
    static io.github.lamspace.openlatch.core.command.ConditionOp toCoreConditionOp(
            io.github.lamspace.openlatch.protocol.ConditionOp op) {
        return switch (op) {
            case CONDITION_OP_SIGNAL -> io.github.lamspace.openlatch.core.command.ConditionOp.SIGNAL;
            case CONDITION_OP_SIGNAL_ALL ->
                    io.github.lamspace.openlatch.core.command.ConditionOp.SIGNAL_ALL;
            case CONDITION_OP_LEAVE -> io.github.lamspace.openlatch.core.command.ConditionOp.LEAVE;
            default -> throw new IllegalArgumentException("unknown condition op: " + op);
        };
    }

    /**
     * core 条件裁决状态 → 协议状态码（既有词表承载、零新增，判例
     * {@code toLatchStatus} 的映射纪律）：key/条件名/家族违例统一
     * {@code INVALID_REQUEST}，会话失效 {@code SESSION_EXPIRED}，
     * signal 权限 {@code NOT_HELD} 原词送达。
     *
     * @param status core 裁决状态
     * @return 协议状态码
     */
    /**
     * 分发相位器操作（v10 单机路径）：v10 门 → 形状互斥矩阵 →
     * {@code core.phaserOp}（受理通道——配额上限与合并等待深度在引擎
     * 门面判定，唤醒随条目锁外经监听器推送，判例 {@code dispatchQueueOp}
     * 的受理即裁决形态；单机恒"登记等待项于条目"——集群双拓扑见
     * {@code ClusterRequestHandler.handlePhaserOp}）。
     *
     * @param session 已握手会话
     * @param msg     入站消息信封
     * @return 协议响应信封
     */
    private Envelope dispatchPhaserOp(ServerSession session, Envelope msg) {
        if (session.protocolVersion() < 10) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        io.github.lamspace.openlatch.protocol.PhaserOpRequest req = msg.getPhaserOpRequest();
        if (phaserShapeInvalid(req)) {
            return errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        io.github.lamspace.openlatch.core.result.PhaserOpResult r =
                core.phaserOp(toPhaserCommand(session.sessionId(), msg.getRequestId(), req));
        StatusCode status = toPhaserStatus(r.outcome());
        if (metrics != null) {
            metrics.recordPhaser(req.getOp(), status);
        }
        return envelope(msg, MessageType.PHASER_OP, b -> b.setPhaserOpResponse(
                io.github.lamspace.openlatch.protocol.PhaserOpResponse.newBuilder()
                        .setStatus(status)
                        .setOp(req.getOp())
                        .setPhase(r.phase())
                        .setRegistered(r.registered())
                        .setArrived(r.arrived())));
    }

    /**
     * 协议 phaser 请求 → core 命令（两车道共用；形状已由
     * {@link #phaserShapeInvalid} 放行，{@code expected_phase} presence
     * 按主张透传）。
     *
     * @param sessionId 会话 id（单机引擎内部 sid）
     * @param requestId 信封请求 id
     * @param req       协议请求
     * @return core 命令
     */
    public static io.github.lamspace.openlatch.core.command.PhaserOpCommand toPhaserCommand(
            long sessionId, long requestId,
            io.github.lamspace.openlatch.protocol.PhaserOpRequest req) {
        return new io.github.lamspace.openlatch.core.command.PhaserOpCommand(sessionId,
                requestId, req.getKey(),
                io.github.lamspace.openlatch.server.raft.LockStateMachineCore
                        .toCorePhaserOp(req.getOp()),
                req.getParties(),
                req.hasExpectedPhase() ? req.getExpectedPhase() : null,
                req.getAwaitRequestId());
    }

    /**
     * phaser 请求形状互斥矩阵（接入层唯一裁决，判例
     * {@code validateQueueShape}/{@code conditionOpShapeInvalid}，集群路径
     * 复用本静态）：{@code parties} 仅 REGISTER 且 ≥1；
     * {@code expected_phase} 仅 AWAIT_ADVANCE 且 ≥0（presence 必携）；
     * {@code await_request_id} 仅 CANCEL 且 >0。矩阵外组合回
     * {@code true}（违例）。
     *
     * @param req 协议请求
     * @return 形状违例为 {@code true}
     */
    public static boolean phaserShapeInvalid(
            io.github.lamspace.openlatch.protocol.PhaserOpRequest req) {
        boolean hasExpected = req.hasExpectedPhase();
        return switch (req.getOp()) {
            case PHASER_OP_REGISTER -> req.getParties() < 1 || hasExpected
                    || req.getAwaitRequestId() != 0;
            case PHASER_OP_ARRIVE, PHASER_OP_ARRIVE_AND_AWAIT,
                    PHASER_OP_ARRIVE_AND_DEREGISTER -> req.getParties() != 0
                    || hasExpected || req.getAwaitRequestId() != 0;
            case PHASER_OP_AWAIT_ADVANCE -> req.getParties() != 0 || !hasExpected
                    || req.getExpectedPhase() < 0 || req.getAwaitRequestId() != 0;
            case PHASER_OP_CANCEL -> req.getParties() != 0 || hasExpected
                    || req.getAwaitRequestId() <= 0;
            case PHASER_OP_QUERY -> req.getParties() != 0 || hasExpected
                    || req.getAwaitRequestId() != 0;
            default -> true;
        };
    }

    /**
     * core phaser 裁决 → 协议状态码（两超限同落 {@code OVERLOADED}、以线路
     * op 回显分轨；配额透支与无条目两拒绝共 {@code INVALID_REQUEST}——
     * 判例 {@code toQueueStatus} 的映射纪律）。
     *
     * @param outcome core 裁决
     * @return 协议状态码
     */
    static StatusCode toPhaserStatus(io.github.lamspace.openlatch.core.result.Outcome outcome) {
        return switch (outcome) {
            case GRANTED -> StatusCode.OK;
            case QUEUED -> StatusCode.QUEUED;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case REJECT_KEY_EMPTY -> StatusCode.KEY_EMPTY;
            case REJECT_KEY_TOO_LONG -> StatusCode.KEY_TOO_LONG;
            case REJECT_QUEUE_FULL, REJECT_PHASER_PARTIES -> StatusCode.OVERLOADED;
            case REJECT_PHASER_QUOTA, REJECT_PHASER_NO_ENTRY, REJECT_TYPE_MISMATCH ->
                    StatusCode.INVALID_REQUEST;
            default -> StatusCode.INTERNAL_ERROR;
        };
    }

    /**
     * 条件裁决 → 协议状态码（signal 权限 {@code NOT_HELD} 原词送达）。
     *
     * @param status core 条件裁决状态
     * @return 协议状态码
     */
    static StatusCode toConditionStatus(
            io.github.lamspace.openlatch.core.result.ConditionOpResult.Status status) {
        return switch (status) {
            case OK -> StatusCode.OK;
            case NOT_HELD -> StatusCode.NOT_HELD;
            case REJECT_SESSION -> StatusCode.SESSION_EXPIRED;
            case REJECT_KEY_EMPTY, REJECT_KEY_TOO_LONG, REJECT_TYPE_MISMATCH ->
                    StatusCode.INVALID_REQUEST;
        };
    }

    /**
     * 获取请求的许可参数合法性：
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
     * 拒绝（v3 兼容性策略）。适用类型：{@code FAIR}、
     * {@code SEMAPHORE}、{@code LATCH}。
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
     * core 授予结果 → 协议响应（全表映射）。
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
            // 超额归还是请求参数与持有不符，非租约问题。
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
     * MUST NOT 因回显未知类型抛异常而使请求静默悬挂。
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
            // v3：ADMIN 消息同规则——认证/门控/限额拒绝的状态码在线路可见
            // （控制台裁决依赖；被拒应答仅带状态码，观察字段留零值）。
            case ADMIN_SUMMARY -> b.setAdminSummaryResponse(
                    AdminSummaryResponse.newBuilder().setStatus(status));
            case ADMIN_LIST_KEYS -> b.setAdminListKeysResponse(
                    AdminListKeysResponse.newBuilder().setStatus(status));
            case ADMIN_KEY_DETAIL -> b.setAdminKeyDetailResponse(
                    AdminKeyDetailResponse.newBuilder().setStatus(status));
            case ADMIN_LIST_SESSIONS -> b.setAdminListSessionsResponse(
                    AdminListSessionsResponse.newBuilder().setStatus(status));
            // v5：BARRIER 消息同规则——拒绝状态码在线路可见（客户端裁决依赖）。
            case BARRIER_AWAIT -> b.setBarrierAwaitResponse(
                    BarrierAwaitResponse.newBuilder().setStatus(status));
            case BARRIER_LEAVE -> b.setBarrierLeaveResponse(
                    BarrierLeaveResponse.newBuilder().setStatus(status));
            case BARRIER_ACTION_DONE -> b.setBarrierActionDoneResponse(
                    BarrierActionDoneResponse.newBuilder().setStatus(status));
            // v4：ATOMIC 消息同规则——拒绝状态码在线路可见（客户端裁决依赖）。
            case ATOMIC_OP -> b.setAtomicOpResponse(
                    AtomicOpResponse.newBuilder().setStatus(status));
            // v7：QUEUE_OP 同规则——门控/形状/钳制拒绝的状态码在线路可见
            // （SDK 的"不可重试→显式异常"分类依赖应答码形）。
            case QUEUE_OP -> b.setQueueOpResponse(
                    QueueOpResponse.newBuilder().setStatus(status));
            // v8：TOPIC_OP 同规则——门控/形状/钳制/撞 key 拒绝的状态码在线路
            // 可见且 op 回显（判例 QUEUE_OP；默认实例形态的 OK 伪成功违例）。
            case TOPIC_OP -> b.setTopicOpResponse(TopicOpResponse.newBuilder()
                    .setStatus(status)
                    .setOp(request.getTopicOpRequest().getOp()));
            // v9：CONDITION_OP 同规则——门控/形状/权限拒绝的状态码在线路可见且
            // op 回显（判例 TOPIC_OP/QUEUE_OP；默认实例形态的 OK 伪成功违例）。
            case CONDITION_OP -> b.setConditionOpResponse(
                    io.github.lamspace.openlatch.protocol.ConditionOpResponse.newBuilder()
                            .setStatus(status)
                            .setOp(request.getConditionOpRequest().getOp()));
            // v10：PHASER_OP 同规则——门控/形状/家族/无条目拒绝的状态码在线路
            // 可见且 op 回显（判例 CONDITION_OP；默认实例形态的 OK 伪成功违例）。
            case PHASER_OP -> b.setPhaserOpResponse(
                    io.github.lamspace.openlatch.protocol.PhaserOpResponse.newBuilder()
                            .setStatus(status)
                            .setOp(request.getPhaserOpRequest().getOp()));
            default -> {
            }
        }
        return b.build();
    }
}
