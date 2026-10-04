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

import io.github.lamspace.openlatch.protocol.AdminKeyDetailRequest;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.AtomicOpRequest;
import io.github.lamspace.openlatch.protocol.AtomicOpResponse;
import io.github.lamspace.openlatch.protocol.BarrierActionDoneRequest;
import io.github.lamspace.openlatch.protocol.BarrierActionDoneResponse;
import io.github.lamspace.openlatch.protocol.BarrierAwaitRequest;
import io.github.lamspace.openlatch.protocol.BarrierAwaitResponse;
import io.github.lamspace.openlatch.protocol.BarrierLeaveRequest;
import io.github.lamspace.openlatch.protocol.BarrierLeaveResponse;
import io.github.lamspace.openlatch.protocol.ClusterView;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LatchAwaitRequest;
import io.github.lamspace.openlatch.protocol.LatchAwaitResponse;
import io.github.lamspace.openlatch.protocol.LatchCountDownRequest;
import io.github.lamspace.openlatch.protocol.LatchCountDownResponse;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewResponse;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.QueueOp;
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.QueueOpResponse;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.ReleaseResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 拒绝码形表驱动门禁：客户端接入车道的每一请求 {@code MessageType}，两类
 * 拒绝码形路径（单机 {@code RequestDispatcher.errorResponse} 门控拒绝、
 * 集群 {@code ClusterRequestHandler.notLeaderEnvelope} 非权威拒绝）的应答
 * MUST 携带与请求<b>同型</b>的 Response 载荷并自述状态码——"拒绝状态码
 * 线路可见"不变式的机械防线。
 *
 * <p><b>防复发机制</b>：客户端各车道按 oneof 取自身应答（如
 * {@code getQueueOpResponse()}），异型/空载荷应答在 protobuf 下读为默认
 * 实例——{@code StatusCode.OK} 恰为枚举零值，拒绝会被静默成型为"成功空
 * 应答"（换主窗 {@code take} 交付空串、{@code size} 读 0 的根因形态）。
 * 新增消息类型未配同型 case 时，本测试对表立即红，MUST 补 case 方可合入。
 *
 * <p>纯编解码器直调（判例 {@code applyLevelSessionRejectedMapsToSessionExpired}），
 * 无需集群基座、零时序依赖。
 */
class RejectCodecTableTest {

    /** 门禁行：请求类型 + 请求侧最小合法载荷 + 应答同型在场性判据。 */
    private record RejectCase(MessageType type, int protocolVersion,
                              java.util.function.Consumer<Envelope.Builder> requestPayload,
                              Predicate<Envelope> sameShapePresent) {
    }

    /**
     * 写家族请求类型（经角色门或提交失败达 {@code notLeaderEnvelope} 的
     * 全集）——HELLO/CLUSTER_VIEW/ADMIN/PING/AWAIT_NOTIFY 为任意节点可答的
     * 观察/控制面，不经非权威拒绝码形，不入本集。
     */
    private static final java.util.Set<MessageType> NOT_LEADER_REACHABLE =
            java.util.Set.of(MessageType.LOCK_ACQUIRE, MessageType.LOCK_RELEASE,
                    MessageType.LEASE_RENEW, MessageType.LATCH_COUNT_DOWN,
                    MessageType.LATCH_AWAIT, MessageType.ATOMIC_OP,
                    MessageType.BARRIER_AWAIT, MessageType.BARRIER_LEAVE,
                    MessageType.BARRIER_ACTION_DONE, MessageType.QUEUE_OP);

    /** 客户端接入车道全部请求类型（PING/AWAIT_NOTIFY 无拒绝应答语义，除外）。 */
    private static RejectCase[] allCases() {
        return new RejectCase[] {
            new RejectCase(MessageType.LOCK_ACQUIRE, 1,
                    b -> b.setAcquireRequest(AcquireRequest.newBuilder()
                            .setKey("k").setLockType(LockType.LOCK_TYPE_REENTRANT)
                            .setThreadId(1L).setLeaseMs(1000L)),
                    Envelope::hasAcquireResponse),
            new RejectCase(MessageType.LOCK_RELEASE, 1,
                    b -> b.setReleaseRequest(ReleaseRequest.newBuilder()
                            .setKey("k").setLeaseToken(1L).setThreadId(1L)),
                    Envelope::hasReleaseResponse),
            new RejectCase(MessageType.LEASE_RENEW, 1,
                    b -> b.setLeaseRenewRequest(LeaseRenewRequest.newBuilder()
                            .setKey("k").setLeaseToken(1L).setLeaseMs(1000L)),
                    Envelope::hasLeaseRenewResponse),
            new RejectCase(MessageType.CLUSTER_VIEW, 2,
                    b -> b.setClusterView(ClusterView.newBuilder()),
                    Envelope::hasClusterView),
            new RejectCase(MessageType.LATCH_COUNT_DOWN, 3,
                    b -> b.setLatchCountDownRequest(LatchCountDownRequest.newBuilder()
                            .setKey("k").setCount(1).setTotal(1)),
                    Envelope::hasLatchCountDownResponse),
            new RejectCase(MessageType.LATCH_AWAIT, 3,
                    b -> b.setLatchAwaitRequest(LatchAwaitRequest.newBuilder()
                            .setKey("k").setTotal(1)),
                    Envelope::hasLatchAwaitResponse),
            new RejectCase(MessageType.ADMIN_KEY_DETAIL, 3,
                    b -> b.setAdminKeyDetailRequest(AdminKeyDetailRequest.newBuilder()
                            .setToken("t").setKey("k")),
                    Envelope::hasAdminKeyDetailResponse),
            new RejectCase(MessageType.ATOMIC_OP, 4,
                    b -> b.setAtomicOpRequest(AtomicOpRequest.newBuilder()
                            .setKey("k").setOp(io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_GET)
                            .setLockType(LockType.LOCK_TYPE_ATOMIC_LONG)),
                    Envelope::hasAtomicOpResponse),
            new RejectCase(MessageType.BARRIER_AWAIT, 5,
                    b -> b.setBarrierAwaitRequest(BarrierAwaitRequest.newBuilder()
                            .setKey("k").setParties(2)),
                    Envelope::hasBarrierAwaitResponse),
            new RejectCase(MessageType.BARRIER_LEAVE, 5,
                    b -> b.setBarrierLeaveRequest(BarrierLeaveRequest.newBuilder()
                            .setKey("k")),
                    Envelope::hasBarrierLeaveResponse),
            new RejectCase(MessageType.BARRIER_ACTION_DONE, 5,
                    b -> b.setBarrierActionDoneRequest(BarrierActionDoneRequest.newBuilder()
                            .setKey("k").setGeneration(1)),
                    Envelope::hasBarrierActionDoneResponse),
            new RejectCase(MessageType.QUEUE_OP, 7,
                    b -> b.setQueueOpRequest(QueueOpRequest.newBuilder()
                            .setKey("k").setOp(QueueOp.QUEUE_OP_SIZE)
                            .setLockType(LockType.LOCK_TYPE_QUEUE)),
                    Envelope::hasQueueOpResponse),
        };
    }

    /** 构造某类型的请求信封（回显 version/requestId 供两码形使用）。 */
    private static Envelope request(RejectCase c) {
        Envelope.Builder b = Envelope.newBuilder()
                .setProtocolVersion(c.protocolVersion())
                .setType(c.type())
                .setRequestId(42L);
        c.requestPayload().accept(b);
        return b.build();
    }

    /**
     * 单机门控拒绝码形：{@code errorResponse(msg, INVALID_REQUEST)} 对每类型
     * MUST 回显请求类型并携同型载荷、状态码线路可见。
     */
    @Test
    void singleNodeErrorRejectionCarriesSameShapePayload() {
        for (RejectCase c : allCases()) {
            Envelope msg = request(c);
            Envelope resp = io.github.lamspace.openlatch.server.dispatch
                    .RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST);
            assertThat(resp.getType())
                    .as("%s 拒绝应答 type 回显", c.type())
                    .isEqualTo(c.type());
            assertThat(resp.getRequestId())
                    .as("%s 拒绝应答回显 request_id", c.type())
                    .isEqualTo(msg.getRequestId());
            assertThat(c.sameShapePresent().test(resp))
                    .as("%s 拒绝应答 MUST 携同型载荷（默认实例会被读成伪 OK）", c.type())
                    .isTrue();
            assertThat(statusOf(resp, c.type()))
                    .as("%s 拒绝状态码线路可见", c.type())
                    .isEqualTo(StatusCode.INVALID_REQUEST);
        }
    }

    /**
     * 集群非权威拒绝码形：{@code notLeaderEnvelope(msg, snapshot)} 对每类型
     * MUST 携同型载荷并自述 {@code NOT_LEADER}（选举空窗快照 -1/空串）。
     */
    @Test
    void clusterNotLeaderRejectionCarriesSameShapePayload() {
        LeaderTracker.Snapshot unknown = new LeaderTracker.Snapshot(-1, "");
        // 守卫：非权威可达集须覆盖全部经门/提交失败达码形的写家族类型。
        assertThat(NOT_LEADER_REACHABLE)
                .as("集群拒绝覆盖集 = 写家族全集（CLUSTER_VIEW/ADMIN 本地作答除外）")
                .containsExactlyInAnyOrderElementsOf(
                        java.util.Arrays.stream(allCases())
                                .map(RejectCase::type)
                                .filter(t -> t != MessageType.CLUSTER_VIEW
                                        && t != MessageType.ADMIN_KEY_DETAIL)
                                .collect(java.util.stream.Collectors.toSet()));
        for (RejectCase c : allCases()) {
            if (!NOT_LEADER_REACHABLE.contains(c.type())) {
                continue;
            }
            Envelope msg = request(c);
            Envelope resp = ClusterRequestHandler.notLeaderEnvelope(msg, unknown);
            assertThat(resp.getType())
                    .as("%s 非权威拒绝应答 type 回显", c.type())
                    .isEqualTo(c.type());
            assertThat(resp.getRequestId())
                    .as("%s 非权威拒绝回显 request_id", c.type())
                    .isEqualTo(msg.getRequestId());
            assertThat(c.sameShapePresent().test(resp))
                    .as("%s 非权威拒绝 MUST 携同型载荷——异型/空载荷会被客户端读成 "
                            + "protobuf 默认实例的伪 OK（换主窗 take 空串/size 0 根因）", c.type())
                    .isTrue();
            assertThat(statusOf(resp, c.type()))
                    .as("%s 非权威拒绝状态码线路可见", c.type())
                    .isEqualTo(StatusCode.NOT_LEADER);
        }
    }

    /**
     * op 回显守门（写形态拒绝的调用方裁决依赖）：ATOMIC_OP 与 QUEUE_OP 的
     * 非权威拒绝 MUST 携请求 op——客户端 {@code recordQueue}/{@code recordAtomic}
     * 与车道日志按 op 分派，缺回显即分派失真（队列拒绝此前被指标记为 ok 的
     * 派生症状同样杜绝）。
     */
    @Test
    void clusterNotLeaderEchoesOpForOpCarryingRequests() {
        LeaderTracker.Snapshot unknown = new LeaderTracker.Snapshot(-1, "");
        for (RejectCase c : allCases()) {
            if (c.type() != MessageType.ATOMIC_OP && c.type() != MessageType.QUEUE_OP) {
                continue;
            }
            Envelope resp = ClusterRequestHandler.notLeaderEnvelope(request(c), unknown);
            if (c.type() == MessageType.QUEUE_OP) {
                assertThat(resp.getQueueOpResponse().getOp())
                        .as("QUEUE_OP 拒绝须回显 op（指标 recordQueue 消费同表达式）")
                        .isEqualTo(QueueOp.QUEUE_OP_SIZE);
            } else {
                assertThat(resp.getAtomicOpResponse().getOp())
                        .as("ATOMIC_OP 拒绝须回显 op")
                        .isEqualTo(io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_GET);
            }
        }
    }

    /** v2 三类应答携 Leader 提示字段（其余类型同型无提示判例的反向守门）。 */
    @Test
    void hintFieldsStayOnV2ShapedResponses() {
        LeaderTracker.Snapshot known = new LeaderTracker.Snapshot(3, "127.0.0.1:9090");
        assertThat(ClusterRequestHandler
                .notLeaderEnvelope(request(caseOf(MessageType.LOCK_ACQUIRE)), known)
                .getAcquireResponse().getLeaderNodeId()).isEqualTo(3);
        assertThat(ClusterRequestHandler
                .notLeaderEnvelope(request(caseOf(MessageType.LOCK_RELEASE)), known)
                .getReleaseResponse().getLeaderAddress()).isEqualTo("127.0.0.1:9090");
        assertThat(ClusterRequestHandler
                .notLeaderEnvelope(request(caseOf(MessageType.LEASE_RENEW)), known)
                .getLeaseRenewResponse().getLeaderNodeId()).isEqualTo(3);
        // 队列拒绝沿无提示判例（QueueOpResponse 结构不含 leader 字段）：
        // 自述 NOT_LEADER + op 回显即足，改道由客户端发现机制兜底。
        Envelope queueReject = ClusterRequestHandler
                .notLeaderEnvelope(request(caseOf(MessageType.QUEUE_OP)), known);
        assertThat(queueReject.getQueueOpResponse().getStatus())
                .isEqualTo(StatusCode.NOT_LEADER);
        assertThat(queueReject.getQueueOpResponse().getOp())
                .isEqualTo(QueueOp.QUEUE_OP_SIZE);
    }

    /** default 安全落位：未知类型不抛错，落 acquire 形态且状态码可见（记 WARN 不判死）。 */
    @Test
    void unknownTypeFallsBackSafelyWithoutThrowing() {
        LeaderTracker.Snapshot unknown = new LeaderTracker.Snapshot(-1, "");
        Envelope msg = Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.AWAIT_NOTIFY)
                .setRequestId(7L)
                .build();
        Envelope resp = ClusterRequestHandler.notLeaderEnvelope(msg, unknown);
        assertThat(resp.hasAcquireResponse()).isTrue();
        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
    }

    /** 按类型取门禁行（提示/回显用例的便捷定位）。 */
    private static RejectCase caseOf(MessageType type) {
        for (RejectCase c : allCases()) {
            if (c.type() == type) {
                return c;
            }
        }
        throw new IllegalArgumentException("no case: " + type);
    }

    /**
     * 从同型应答提取自述状态码——MUST 在载荷在场性确认之后调用（缺载荷的
     * 默认实例读 {@code getStatus()} 恒 OK，恰是本门禁杜绝的伪形态）。
     */
    private static StatusCode statusOf(Envelope resp, MessageType type) {
        return switch (type) {
            case LOCK_ACQUIRE -> resp.getAcquireResponse().getStatus();
            case LOCK_RELEASE -> resp.getReleaseResponse().getStatus();
            case LEASE_RENEW -> resp.getLeaseRenewResponse().getStatus();
            case CLUSTER_VIEW -> resp.getClusterView().getStatus();
            case LATCH_COUNT_DOWN -> resp.getLatchCountDownResponse().getStatus();
            case LATCH_AWAIT -> resp.getLatchAwaitResponse().getStatus();
            case ADMIN_KEY_DETAIL -> resp.getAdminKeyDetailResponse().getStatus();
            case ATOMIC_OP -> resp.getAtomicOpResponse().getStatus();
            case BARRIER_AWAIT -> resp.getBarrierAwaitResponse().getStatus();
            case BARRIER_LEAVE -> resp.getBarrierLeaveResponse().getStatus();
            case BARRIER_ACTION_DONE -> resp.getBarrierActionDoneResponse().getStatus();
            case QUEUE_OP -> resp.getQueueOpResponse().getStatus();
            default -> throw new IllegalArgumentException("no codec: " + type);
        };
    }
}
