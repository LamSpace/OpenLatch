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

package io.github.lamspace.openlatch.protocol;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 两类协议编解码测试：全消息类型 round-trip 与未知字段容忍。
 */
class ProtocolCodecTest {

    /**
     * 序列化→反序列化回环：{@code toByteArray()} 后 {@code parseFrom} 还原。
     * 解码失败（{@link InvalidProtocolBufferException}）转为 {@link AssertionError}，
     * 使调用方直接对返回值做等值断言而无需处理受检异常。
     *
     * @param envelope 待回环的原信封
     * @return 回环后的信封（应与入参等值）
     */
    private static Envelope roundTrip(Envelope envelope) {
        try {
            return Envelope.parseFrom(envelope.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("decode failed", e);
        }
    }

    /** 场景：HELLO 请求信封回环——协议版本、请求标识与客户端字段整体等值，还原后 payload 分支仍为 HELLO_REQUEST。 */
    @Test
    void helloRequestRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.HELLO)
                .setRequestId(42L)
                .setHelloRequest(HelloRequest.newBuilder()
                        .setClientProtocolVersion(1)
                        .setClientName("app-1")
                        .setAuthToken("")
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getType()).isEqualTo(MessageType.HELLO);
        assertThat(parsed.getPayloadCase()).isEqualTo(Envelope.PayloadCase.HELLO_REQUEST);
    }

    /** 场景：HELLO 响应信封回环——会话 ID、服务端协议版本、默认租约与 leader hint 字段等值保留。 */
    @Test
    void helloResponseRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.HELLO)
                .setRequestId(42L)
                .setHelloResponse(HelloResponse.newBuilder()
                        .setStatus(StatusCode.OK)
                        .setSessionId(99L)
                        .setServerProtocolVersion(1)
                        .setDefaultLeaseMs(30_000L)
                        .setLeaderHint(0L)
                        .build())
                .build();

        assertThat(roundTrip(envelope)).isEqualTo(envelope);
    }

    /** 场景：LOCK_ACQUIRE 请求信封回环（含 wait_ms=-1 排队语义）——整体等值，key 与锁类型还原后逐字段可读。 */
    @Test
    void acquireRequestRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(7L)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("my-lock")
                        .setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1234L)
                        .setLeaseMs(30_000L)
                        .setWaitMs(-1L)
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getAcquireRequest().getKey()).isEqualTo("my-lock");
        assertThat(parsed.getAcquireRequest().getLockType()).isEqualTo(LockType.LOCK_TYPE_REENTRANT);
    }

    /** 场景：LOCK_ACQUIRE 的 QUEUED 响应信封回环——位次 3 与凭证/到期字段取 0 的口径等值保留。 */
    @Test
    void acquireResponseRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(7L)
                .setAcquireResponse(AcquireResponse.newBuilder()
                        .setStatus(StatusCode.QUEUED)
                        .setLeaseToken(0L)
                        .setLeaseExpiresAtMs(0L)
                        .setQueuePosition(3)
                        .setGrantedLeaseMs(0L)
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(parsed.getAcquireResponse().getQueuePosition()).isEqualTo(3);
    }

    /** 场景：LOCK_RELEASE 请求信封回环——key、lease token 与 thread_id 等值保留。 */
    @Test
    void releaseRequestRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(8L)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey("my-lock")
                        .setLeaseToken(555L)
                        .setThreadId(1234L)
                        .build())
                .build();

        assertThat(roundTrip(envelope)).isEqualTo(envelope);
    }

    /** 场景：LOCK_RELEASE 的 OK 响应信封回环——整体等值且 fullyReleased=true 布尔字段保留。 */
    @Test
    void releaseResponseRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(8L)
                .setReleaseResponse(ReleaseResponse.newBuilder()
                        .setStatus(StatusCode.OK)
                        .setFullyReleased(true)
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getReleaseResponse().getFullyReleased()).isTrue();
    }

    /** 场景：LEASE_RENEW 请求与响应各自回环——含 leaseExpiresAtMs 毫秒纪元大值不截断，均保持等值。 */
    @Test
    void leaseRenewRoundTrip() {
        Envelope request = Envelope.newBuilder()
                .setType(MessageType.LEASE_RENEW)
                .setRequestId(9L)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder()
                        .setKey("my-lock")
                        .setLeaseToken(555L)
                        .setLeaseMs(30_000L)
                        .build())
                .build();
        assertThat(roundTrip(request)).isEqualTo(request);

        Envelope response = Envelope.newBuilder()
                .setType(MessageType.LEASE_RENEW)
                .setRequestId(9L)
                .setLeaseRenewResponse(LeaseRenewResponse.newBuilder()
                        .setStatus(StatusCode.OK)
                        .setLeaseExpiresAtMs(1_700_000_000_000L)
                        .build())
                .build();
        assertThat(roundTrip(response)).isEqualTo(response);
    }

    /** 场景：无 payload 的 PING 信封回环——仅 type 与 request_id 承载信息，还原后 payload 分支为 PAYLOAD_NOT_SET。 */
    @Test
    void pingRoundTrip() {
        // PING 无 payload：Envelope 仅有 type 与 request_id。
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.PING)
                .setRequestId(10L)
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed.getType()).isEqualTo(MessageType.PING);
        assertThat(parsed.getRequestId()).isEqualTo(10L);
        assertThat(parsed.getPayloadCase()).isEqualTo(Envelope.PayloadCase.PAYLOAD_NOT_SET);
    }

    /** 场景：服务端推送 AWAIT_NOTIFY 信封回环——request_id=0、经 request_id_ref 关联原请求，两语义还原后不变。 */
    @Test
    void awaitNotifyPushRoundTrip() {
        // 服务端推送：Envelope.request_id 为 0，通过 request_id_ref 关联原请求。
        Envelope envelope = Envelope.newBuilder()
                .setType(MessageType.AWAIT_NOTIFY)
                .setRequestId(0L)
                .setAwaitNotify(AwaitNotify.newBuilder()
                        .setKey("my-lock")
                        .setRequestIdRef(7L)
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.getRequestId()).isZero();
        assertThat(parsed.getAwaitNotify().getRequestIdRef()).isEqualTo(7L);
    }

    /** 场景：未知字段前向兼容——带未知字段（field 999, varint）的字节流可解析、已知字段不受损，且未知字段跨再序列化仍保留。 */
    @Test
    void unknownFieldsAreToleratedAndPreserved() throws InvalidProtocolBufferException {
        Envelope original = Envelope.newBuilder()
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(7L)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("my-lock")
                        .setThreadId(1234L)
                        .build())
                .build();

        // 附加一个模式未知的字段（field 999, varint 12345）。
        UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
                .addField(999, UnknownFieldSet.Field.newBuilder()
                        .addVarint(12345L)
                        .build())
                .build();

        byte[] withUnknown = Envelope.parseFrom(original.toByteArray())
                .toBuilder()
                .setUnknownFields(unknown)
                .build()
                .toByteArray();

        Envelope parsed = Envelope.parseFrom(withUnknown);
        assertThat(parsed.getRequestId()).isEqualTo(7L);
        assertThat(parsed.getAcquireRequest().getKey()).isEqualTo("my-lock");
        // 未知字段被保留。
        assertThat(parsed.getUnknownFields().hasField(999)).isTrue();

        // 再次序列化后未知字段仍保留。
        Envelope reparsed = Envelope.parseFrom(parsed.toByteArray());
        assertThat(reparsed.getUnknownFields().hasField(999)).isTrue();
    }

    /** 场景：MessageType/LockType/StatusCode 枚举编号与线格式契约逐项核对（抽查关键取值）。 */
    @Test
    void enumValuesMatchDesignSpecification() {
        // 字段/枚举取值与线格式契约逐项一致（抽查关键取值）。
        assertThat(MessageType.HELLO.getNumber()).isEqualTo(1);
        assertThat(MessageType.LOCK_ACQUIRE.getNumber()).isEqualTo(2);
        assertThat(MessageType.LOCK_RELEASE.getNumber()).isEqualTo(3);
        assertThat(MessageType.LEASE_RENEW.getNumber()).isEqualTo(4);
        assertThat(MessageType.PING.getNumber()).isEqualTo(5);
        assertThat(MessageType.AWAIT_NOTIFY.getNumber()).isEqualTo(6);
        // v2：CLUSTER_VIEW 启用。
        assertThat(MessageType.CLUSTER_VIEW.getNumber()).isEqualTo(7);

        assertThat(LockType.LOCK_TYPE_REENTRANT.getNumber()).isZero();
        assertThat(LockType.LOCK_TYPE_SIMPLE.getNumber()).isEqualTo(1);
        assertThat(LockType.LOCK_TYPE_READ.getNumber()).isEqualTo(2);
        assertThat(LockType.LOCK_TYPE_WRITE.getNumber()).isEqualTo(3);

        assertThat(StatusCode.OK.getNumber()).isZero();
        assertThat(StatusCode.INVALID_TOKEN.getNumber()).isEqualTo(3);
        assertThat(StatusCode.OVERLOADED.getNumber()).isEqualTo(6);
        assertThat(StatusCode.KEY_EMPTY.getNumber()).isEqualTo(8);
        assertThat(StatusCode.INVALID_REQUEST.getNumber()).isEqualTo(9);
        // v1 预留、v2 启用：编号不变。
        assertThat(StatusCode.NOT_LEADER.getNumber()).isEqualTo(10);

        // v7：队列消息对/形态/操作编号钉定。
        assertThat(MessageType.QUEUE_OP.getNumber()).isEqualTo(18);
        assertThat(LockType.LOCK_TYPE_QUEUE.getNumber()).isEqualTo(12);
        assertThat(LockType.LOCK_TYPE_DELAY_QUEUE.getNumber()).isEqualTo(13);
        assertThat(QueueOp.QUEUE_OP_PUT.getNumber()).isZero();
        assertThat(QueueOp.QUEUE_OP_TAKE.getNumber()).isEqualTo(1);
        assertThat(QueueOp.QUEUE_OP_DRAIN.getNumber()).isEqualTo(2);
        assertThat(QueueOp.QUEUE_OP_PEEK.getNumber()).isEqualTo(3);
        assertThat(QueueOp.QUEUE_OP_SIZE.getNumber()).isEqualTo(4);
    }

    /** 场景：v2 CLUSTER_VIEW 响应信封回环——成员表逐项等值，payload 分支为 cluster_view。 */
    @Test
    void clusterViewResponseRoundTrip() {
        Envelope envelope = Envelope.newBuilder()
                .setProtocolVersion(2)
                .setType(MessageType.CLUSTER_VIEW)
                .setRequestId(5L)
                .setClusterView(ClusterView.newBuilder()
                        .setStatus(StatusCode.OK)
                        .addNodes(NodeInfo.newBuilder()
                                .setNodeId(1).setAddress("10.0.0.1:9410").setIsLeader(true))
                        .addNodes(NodeInfo.newBuilder()
                                .setNodeId(2).setAddress("10.0.0.2:9410").setIsLeader(false))
                        .addNodes(NodeInfo.newBuilder()
                                .setNodeId(3).setAddress("").setIsLeader(false))
                        .build())
                .build();

        Envelope parsed = roundTrip(envelope);
        assertThat(parsed).isEqualTo(envelope);
        assertThat(parsed.hasClusterView()).isTrue();
        assertThat(parsed.getClusterView().getNodesCount()).isEqualTo(3);
    }

    /** 场景：v2 leader 提示字段回环——HELLO 提示与三类写响应随附的 hint 字段整体等值。 */
    @Test
    void v2LeaderHintFieldsRoundTrip() {
        Envelope hello = Envelope.newBuilder()
                .setProtocolVersion(2)
                .setType(MessageType.HELLO)
                .setRequestId(1L)
                .setHelloResponse(HelloResponse.newBuilder()
                        .setStatus(StatusCode.OK)
                        .setSessionId(7L)
                        .setServerProtocolVersion(2)
                        .setDefaultLeaseMs(30_000L)
                        .setLeaderHint(-1L)
                        .setLeaderAddress("127.0.0.1:9410")
                        .build())
                .build();
        assertThat(roundTrip(hello).getHelloResponse().getLeaderHint()).isEqualTo(-1L);
        assertThat(roundTrip(hello)).isEqualTo(hello);

        Envelope acquire = Envelope.newBuilder()
                .setProtocolVersion(2)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(2L)
                .setAcquireResponse(AcquireResponse.newBuilder()
                        .setStatus(StatusCode.NOT_LEADER)
                        .setLeaderNodeId(3L)
                        .setLeaderAddress("10.0.0.3:9410")
                        .build())
                .build();
        assertThat(roundTrip(acquire).getAcquireResponse().getLeaderNodeId()).isEqualTo(3L);
        assertThat(roundTrip(acquire)).isEqualTo(acquire);

        Envelope release = Envelope.newBuilder()
                .setProtocolVersion(2)
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(3L)
                .setReleaseResponse(ReleaseResponse.newBuilder()
                        .setStatus(StatusCode.NOT_LEADER)
                        .setLeaderNodeId(-1L)
                        .build())
                .build();
        assertThat(roundTrip(release)).isEqualTo(release);

        Envelope renew = Envelope.newBuilder()
                .setProtocolVersion(2)
                .setType(MessageType.LEASE_RENEW)
                .setRequestId(4L)
                .setLeaseRenewResponse(LeaseRenewResponse.newBuilder()
                        .setStatus(StatusCode.NOT_LEADER)
                        .setLeaderNodeId(2L)
                        .setLeaderAddress("10.0.0.2:9410")
                        .build())
                .build();
        assertThat(roundTrip(renew)).isEqualTo(renew);
    }

    /** 场景：v5 BARRIER 三对消息回环——到场请求/应答（含执行者标记与世代）、离场、动作了结全字段等值。 */
    @Test
    void v5BarrierMessagesRoundTrip() {
        Envelope awaitReq = Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_AWAIT)
                .setRequestId(11L)
                .setBarrierAwaitRequest(BarrierAwaitRequest.newBuilder()
                        .setKey("phase")
                        .setParties(3L)
                        .setCarriesAction(true)
                        .build())
                .build();
        Envelope awaitParsed = roundTrip(awaitReq);
        assertThat(awaitParsed).isEqualTo(awaitReq);
        assertThat(awaitParsed.hasBarrierAwaitRequest()).isTrue();
        assertThat(MessageType.BARRIER_AWAIT.getNumber()).isEqualTo(15);
        assertThat(LockType.LOCK_TYPE_BARRIER.getNumber()).isEqualTo(10);
        assertThat(StatusCode.BARRIER_BROKEN.getNumber()).isEqualTo(12);

        Envelope awaitResp = Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_AWAIT)
                .setRequestId(11L)
                .setBarrierAwaitResponse(BarrierAwaitResponse.newBuilder()
                        .setStatus(StatusCode.QUEUED)
                        .setQueuePosition(2)
                        .setGeneration(4L)
                        .setExecutor(true)
                        .setParties(3L)
                        .build())
                .build();
        assertThat(roundTrip(awaitResp)).isEqualTo(awaitResp);

        Envelope brokenResp = awaitResp.toBuilder()
                .setBarrierAwaitResponse(awaitResp.getBarrierAwaitResponse().toBuilder()
                        .setStatus(StatusCode.BARRIER_BROKEN)
                        .setQueuePosition(0)
                        .setExecutor(false))
                .build();
        assertThat(roundTrip(brokenResp).getBarrierAwaitResponse().getStatus())
                .isEqualTo(StatusCode.BARRIER_BROKEN);

        Envelope leaveReq = Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_LEAVE)
                .setRequestId(12L)
                .setBarrierLeaveRequest(BarrierLeaveRequest.newBuilder()
                        .setKey("phase")
                        .setAwaitRequestId(11L)
                        .build())
                .build();
        assertThat(roundTrip(leaveReq)).isEqualTo(leaveReq);
        assertThat(roundTrip(leaveReq.toBuilder()
                .setBarrierLeaveRequest(BarrierLeaveRequest.newBuilder()
                        .setKey("phase").setAwaitRequestId(0L).build())
                .build())
                .getBarrierLeaveRequest().getAwaitRequestId()).isZero();

        Envelope leaveResp = Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_LEAVE)
                .setRequestId(12L)
                .setBarrierLeaveResponse(BarrierLeaveResponse.newBuilder()
                        .setStatus(StatusCode.OK)
                        .build())
                .build();
        assertThat(roundTrip(leaveResp)).isEqualTo(leaveResp);

        Envelope doneReq = Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_ACTION_DONE)
                .setRequestId(13L)
                .setBarrierActionDoneRequest(BarrierActionDoneRequest.newBuilder()
                        .setKey("phase")
                        .setGeneration(4L)
                        .build())
                .build();
        assertThat(roundTrip(doneReq)).isEqualTo(doneReq);

        Envelope doneResp = Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_ACTION_DONE)
                .setRequestId(13L)
                .setBarrierActionDoneResponse(BarrierActionDoneResponse.newBuilder()
                        .setStatus(StatusCode.BARRIER_BROKEN)
                        .build())
                .build();
        assertThat(roundTrip(doneResp)).isEqualTo(doneResp);
    }

    /**
     * v6 载荷字段回环：{@code optional bytes} 三形态（缺省/零长度/非空）presence
     * 逐格可判别，4KB 二进制字节级保真；含请求、应答与 raft 侧回执/快照承载。
     */
    @Test
    void atomicReferencePayloadRoundTrip() {
        byte[] big = new byte[4096];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 31 + 7);
        }

        // 请求侧三形态：缺省（null）/ 零长度（""）/ 4KB 二进制
        AtomicOpRequest absent = AtomicOpRequest.newBuilder()
                .setKey("ref").setOp(AtomicOp.ATOMIC_SET)
                .setLockType(LockType.LOCK_TYPE_ATOMIC_REFERENCE)
                .setOpSeq(1L).build();
        AtomicOpRequest empty = absent.toBuilder()
                .setOperandBytes(com.google.protobuf.ByteString.EMPTY).build();
        AtomicOpRequest binary = absent.toBuilder()
                .setOperandBytes(com.google.protobuf.ByteString.copyFrom(big))
                .setExpectedBytes(com.google.protobuf.ByteString.copyFromUtf8("x"))
                .setInitialBytes(com.google.protobuf.ByteString.copyFromUtf8("y"))
                .build();

        Envelope req = Envelope.newBuilder().setProtocolVersion(6)
                .setType(MessageType.ATOMIC_OP).setRequestId(21L)
                .setAtomicOpRequest(binary).build();
        Envelope parsedReq = roundTrip(req);
        assertThat(parsedReq.getAtomicOpRequest()).isEqualTo(binary);
        assertThat(parsedReq.getAtomicOpRequest().toByteArray()).isEqualTo(binary.toByteArray());
        assertThat(parsedReq.getAtomicOpRequest().hasOperandBytes()).isTrue();
        assertThat(parsedReq.getAtomicOpRequest().getOperandBytes().toByteArray())
                .containsExactly(big);
        // presence 判别：缺省态无字段、零长度态有字段且为空——两态序列化互异
        assertThat(absent.hasOperandBytes()).isFalse();
        assertThat(empty.hasOperandBytes()).isTrue();
        assertThat(empty.getOperandBytes().isEmpty()).isTrue();
        assertThat(absent.toByteArray()).isNotEqualTo(empty.toByteArray());

        // 应答侧三形态
        AtomicOpResponse respNull = AtomicOpResponse.newBuilder()
                .setStatus(StatusCode.OK).setOp(AtomicOp.ATOMIC_GET)
                .setApplied(false).setVersion(3L).build();
        AtomicOpResponse respEmpty = respNull.toBuilder()
                .setValueBytes(com.google.protobuf.ByteString.EMPTY).build();
        AtomicOpResponse respBinary = respNull.toBuilder()
                .setOldValueBytes(com.google.protobuf.ByteString.copyFrom(big))
                .setValueBytes(com.google.protobuf.ByteString.copyFromUtf8("v2")).build();
        assertThat(roundTripEnvelope(respNull).getAtomicOpResponse()).isEqualTo(respNull);
        assertThat(roundTripEnvelope(respBinary).getAtomicOpResponse()).isEqualTo(respBinary);
        assertThat(respEmpty.hasOldValueBytes()).isFalse();
        assertThat(respEmpty.hasValueBytes()).isTrue();

        // raft 侧：ApplyResult 载荷回执与 SnapshotLock ref 字段回环
        io.github.lamspace.openlatch.protocol.raft.ApplyResult apply =
                io.github.lamspace.openlatch.protocol.raft.ApplyResult.newBuilder()
                        .setStatus(io.github.lamspace.openlatch.protocol.raft.ApplyStatus.OK)
                        .setAtomicApplied(true).setAtomicVersion(4L)
                        .setAtomicOldValueBytes(com.google.protobuf.ByteString.copyFrom(big))
                        .setAtomicValueBytes(com.google.protobuf.ByteString.copyFromUtf8("w"))
                        .build();
        io.github.lamspace.openlatch.protocol.raft.ApplyResult parsedApply = parseApply(apply);
        assertThat(parsedApply).isEqualTo(apply);
        assertThat(parsedApply.getAtomicOldValueBytes().toByteArray()).containsExactly(big);
        assertThat(parsedApply.hasAtomicOldValueBytes()).isTrue();

        io.github.lamspace.openlatch.protocol.raft.SnapshotLock refLock =
                io.github.lamspace.openlatch.protocol.raft.SnapshotLock.newBuilder()
                        .setKey("ref").setLockType(LockType.LOCK_TYPE_ATOMIC_REFERENCE)
                        .setAtomicVersion(9L)
                        .setAtomicRefValue(com.google.protobuf.ByteString.copyFrom(big))
                        .setAtomicRefSlotValue(com.google.protobuf.ByteString.EMPTY)
                        .build();
        assertThat(refLock.toBuilder().clearAtomicRefSlotValue().build().hasAtomicRefSlotValue()).isFalse();
        assertThat(refLock.hasAtomicRefInitial()).isFalse();
        assertThat(refLock.hasAtomicRefSlotValue()).isTrue();
        assertThat(refLock.getAtomicRefSlotValue().isEmpty()).isTrue();
    }

    /** 应答信封包裹回环（复用 {@link #roundTrip(Envelope)} 于 ATOMIC_OP 通道）。 */
    private static Envelope roundTripEnvelope(AtomicOpResponse resp) {
        return roundTrip(Envelope.newBuilder().setProtocolVersion(6)
                .setType(MessageType.ATOMIC_OP).setRequestId(22L)
                .setAtomicOpResponse(resp).build());
    }

    /** ApplyResult 序列化回环。 */
    private static io.github.lamspace.openlatch.protocol.raft.ApplyResult parseApply(
            io.github.lamspace.openlatch.protocol.raft.ApplyResult apply) {
        try {
            return io.github.lamspace.openlatch.protocol.raft.ApplyResult
                    .parseFrom(apply.toByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("apply decode failed", e);
        }
    }

    /**
     * 场景：v7 队列消息对回环——请求形状矩阵（PUT 全字段/TAKE 阻塞/DRAIN 批量/
     * 读类零 op_seq）、元素 presence 三形态（缺省/零长度/4KB 二进制）判别与字节级
     * 保真、应答 QUEUED/交付/两态可区分、raft 侧 QueueOpPayload/ApplyResult/
     * SnapshotLock 队列字段（元素序、到期时刻、交付槽）整体等值。
     */
    @Test
    void queueOperationRoundTrip() throws InvalidProtocolBufferException {
        byte[] big = new byte[4096];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 131 + 7);
        }

        // 请求形状：PUT 全字段（阻塞+容量主张+4KB 元素+写序号）
        QueueOpRequest put = QueueOpRequest.newBuilder()
                .setKey("q").setOp(QueueOp.QUEUE_OP_PUT)
                .setLockType(LockType.LOCK_TYPE_QUEUE)
                .setBlocking(true).setCapacity(8)
                .setElementBytes(com.google.protobuf.ByteString.copyFrom(big))
                .setOpSeq(5L).build();
        Envelope envPut = Envelope.newBuilder().setProtocolVersion(7)
                .setType(MessageType.QUEUE_OP).setRequestId(31L)
                .setQueueOpRequest(put).build();
        Envelope parsedPut = roundTrip(envPut);
        assertThat(parsedPut.hasQueueOpRequest()).isTrue();
        assertThat(parsedPut.getQueueOpRequest()).isEqualTo(put);
        assertThat(parsedPut.getQueueOpRequest().getElementBytes().toByteArray())
                .containsExactly(big);

        // 元素 presence 三形态：缺省（违例形）/零长度（合法空串元素）/4KB——序列化互异
        QueueOpRequest absent = put.toBuilder().clearElementBytes().build();
        QueueOpRequest emptyEl = put.toBuilder()
                .setElementBytes(com.google.protobuf.ByteString.EMPTY).build();
        assertThat(absent.hasElementBytes()).isFalse();
        assertThat(emptyEl.hasElementBytes()).isTrue();
        assertThat(emptyEl.getElementBytes().isEmpty()).isTrue();
        assertThat(absent.toByteArray()).isNotEqualTo(emptyEl.toByteArray());

        // DELAY 形态注入 + DRAIN 批量 + 读类零序号
        QueueOpRequest delayed = QueueOpRequest.newBuilder()
                .setKey("dq").setOp(QueueOp.QUEUE_OP_PUT)
                .setLockType(LockType.LOCK_TYPE_DELAY_QUEUE)
                .setCapacity(16).setDelayMs(3000)
                .setElementBytes(com.google.protobuf.ByteString.copyFromUtf8("late"))
                .setOpSeq(1L).build();
        QueueOpRequest drain = QueueOpRequest.newBuilder()
                .setKey("q").setOp(QueueOp.QUEUE_OP_DRAIN)
                .setLockType(LockType.LOCK_TYPE_QUEUE)
                .setMaxElements(64).setOpSeq(6L).build();
        QueueOpRequest size = QueueOpRequest.newBuilder()
                .setKey("q").setOp(QueueOp.QUEUE_OP_SIZE)
                .setLockType(LockType.LOCK_TYPE_QUEUE).build();
        assertThat(QueueOpRequest.parseFrom(delayed.toByteArray()).getDelayMs()).isEqualTo(3000);
        assertThat(QueueOpRequest.parseFrom(drain.toByteArray()).getMaxElements()).isEqualTo(64);
        assertThat(size.getOpSeq()).isZero();

        // 应答形状：QUEUED 位次 / TAKE 交付 / PEEK 无元素两态 / DRAIN 有序列表 / SIZE+容量回显
        QueueOpResponse queued = QueueOpResponse.newBuilder()
                .setStatus(StatusCode.QUEUED).setOp(QueueOp.QUEUE_OP_PUT).setQueuePosition(3).build();
        QueueOpResponse take = QueueOpResponse.newBuilder()
                .setStatus(StatusCode.OK).setOp(QueueOp.QUEUE_OP_TAKE)
                .setElementBytes(com.google.protobuf.ByteString.copyFrom(big))
                .setCapacity(8).build();
        QueueOpResponse peekAbsent = QueueOpResponse.newBuilder()
                .setStatus(StatusCode.OK).setOp(QueueOp.QUEUE_OP_PEEK).build();
        QueueOpResponse peekEmptyEl = peekAbsent.toBuilder()
                .setElementBytes(com.google.protobuf.ByteString.EMPTY).build();
        assertThat(peekAbsent.hasElementBytes()).isFalse();
        assertThat(peekEmptyEl.hasElementBytes()).isTrue();
        assertThat(peekAbsent.toByteArray()).isNotEqualTo(peekEmptyEl.toByteArray());
        QueueOpResponse drained = QueueOpResponse.newBuilder()
                .setStatus(StatusCode.OK).setOp(QueueOp.QUEUE_OP_DRAIN)
                .addDrainedBytes(com.google.protobuf.ByteString.copyFromUtf8("a"))
                .addDrainedBytes(com.google.protobuf.ByteString.EMPTY)
                .addDrainedBytes(com.google.protobuf.ByteString.copyFrom(big))
                .build();
        QueueOpResponse sized = QueueOpResponse.newBuilder()
                .setStatus(StatusCode.OK).setOp(QueueOp.QUEUE_OP_SIZE)
                .setSize(3).setCapacity(8).build();
        Envelope envTake = Envelope.newBuilder().setProtocolVersion(7)
                .setType(MessageType.QUEUE_OP).setRequestId(32L)
                .setQueueOpResponse(take).build();
        assertThat(roundTrip(envTake).getQueueOpResponse().getElementBytes().toByteArray())
                .containsExactly(big);
        assertThat(parseQueueResponse(drained).getDrainedBytesList())
                .containsExactly(com.google.protobuf.ByteString.copyFromUtf8("a"),
                        com.google.protobuf.ByteString.EMPTY,
                        com.google.protobuf.ByteString.copyFrom(big));
        assertThat(parseQueueResponse(queued).getQueuePosition()).isEqualTo(3);
        assertThat(parseQueueResponse(sized).getSize()).isEqualTo(3);

        // raft 侧：QueueOpPayload 包裹 RaftLogEntry 回环
        io.github.lamspace.openlatch.protocol.raft.QueueOpPayload payload =
                io.github.lamspace.openlatch.protocol.raft.QueueOpPayload.newBuilder()
                        .setSessionId(0x100000001L).setRequestId(31L).setRequest(put).build();
        io.github.lamspace.openlatch.protocol.raft.RaftLogEntry entry =
                io.github.lamspace.openlatch.protocol.raft.RaftLogEntry.newBuilder()
                        .setType(io.github.lamspace.openlatch.protocol.raft.RaftEntryType.QUEUE_OP_ENTRY)
                        .setSeq(77L).setWallClockMs(123456789L)
                        .setCommandPayload(payload.toByteString()).build();
        try {
            io.github.lamspace.openlatch.protocol.raft.RaftLogEntry parsedEntry =
                    io.github.lamspace.openlatch.protocol.raft.RaftLogEntry.parseFrom(entry.toByteArray());
            assertThat(parsedEntry).isEqualTo(entry);
            assertThat(io.github.lamspace.openlatch.protocol.raft.QueueOpPayload
                    .parseFrom(parsedEntry.getCommandPayload()).getRequest()).isEqualTo(put);
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("raft entry decode failed", e);
        }

        // ApplyResult 队列回执字段回环（20/21/22）
        io.github.lamspace.openlatch.protocol.raft.ApplyResult apply =
                io.github.lamspace.openlatch.protocol.raft.ApplyResult.newBuilder()
                        .setStatus(io.github.lamspace.openlatch.protocol.raft.ApplyStatus.OK)
                        .setQueueElementBytes(com.google.protobuf.ByteString.copyFrom(big))
                        .addQueueDrainedBytes(com.google.protobuf.ByteString.copyFromUtf8("x"))
                        .setQueueSize(9L).build();
        io.github.lamspace.openlatch.protocol.raft.ApplyResult parsedApply = parseApply(apply);
        assertThat(parsedApply).isEqualTo(apply);
        assertThat(parsedApply.hasQueueElementBytes()).isTrue();
        assertThat(parsedApply.getQueueElementBytes().toByteArray()).containsExactly(big);

        // SnapshotLock 队列字段回环：元素序与到期时刻、交付槽（TAKE 单份/DRAIN 列表）
        io.github.lamspace.openlatch.protocol.raft.SnapshotLock qLock =
                io.github.lamspace.openlatch.protocol.raft.SnapshotLock.newBuilder()
                        .setKey("q").setLockType(LockType.LOCK_TYPE_DELAY_QUEUE)
                        .setQueueCapacity(16)
                        .addQueueElements(io.github.lamspace.openlatch.protocol.raft.SnapshotQueueElement
                                .newBuilder().setPayload(
                                        com.google.protobuf.ByteString.copyFromUtf8("head"))
                                .setExpiresAtMs(1000L))
                        .addQueueElements(io.github.lamspace.openlatch.protocol.raft.SnapshotQueueElement
                                .newBuilder().setPayload(com.google.protobuf.ByteString.EMPTY)
                                .setExpiresAtMs(2000L))
                        .addQueueDedupSlots(io.github.lamspace.openlatch.protocol.raft.SnapshotQueueSlot
                                .newBuilder().setSessionId(1L).setOpSeq(4L).setOp(1)
                                .setElementBytes(com.google.protobuf.ByteString.copyFrom(big)))
                        .addQueueDedupSlots(io.github.lamspace.openlatch.protocol.raft.SnapshotQueueSlot
                                .newBuilder().setSessionId(2L).setOpSeq(9L).setOp(2)
                                .addDrainedBytes(com.google.protobuf.ByteString.copyFromUtf8("a"))
                                .addDrainedBytes(com.google.protobuf.ByteString.copyFromUtf8("b")))
                        .build();
        try {
            io.github.lamspace.openlatch.protocol.raft.SnapshotLock parsedLock =
                    io.github.lamspace.openlatch.protocol.raft.SnapshotLock
                            .parseFrom(qLock.toByteArray());
            assertThat(parsedLock).isEqualTo(qLock);
            assertThat(parsedLock.getQueueElementsList()).extracting(
                    io.github.lamspace.openlatch.protocol.raft.SnapshotQueueElement::getExpiresAtMs)
                    .containsExactly(1000L, 2000L);
            assertThat(parsedLock.getQueueDedupSlots(1).getDrainedBytesList()).hasSize(2);
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError("snapshot lock decode failed", e);
        }
    }

    /** 队列应答信封包裹回环（复用 {@link #roundTrip(Envelope)} 于 QUEUE_OP 通道）。 */
    private static QueueOpResponse parseQueueResponse(QueueOpResponse resp) {
        return roundTrip(Envelope.newBuilder().setProtocolVersion(7)
                .setType(MessageType.QUEUE_OP).setRequestId(33L)
                .setQueueOpResponse(resp).build()).getQueueOpResponse();
    }
}
