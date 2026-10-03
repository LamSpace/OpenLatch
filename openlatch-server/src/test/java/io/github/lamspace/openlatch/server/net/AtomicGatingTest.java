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

package io.github.lamspace.openlatch.server.net;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.SystemClock;
import io.github.lamspace.openlatch.protocol.AtomicOp;
import io.github.lamspace.openlatch.protocol.AtomicOpRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 协议层用例：ATOMIC_OP 的 v4 门控（低版本会话消息级拒绝、不断连）、
 * ACQUIRE 携带原子类型的合法性拒绝、请求形状矩阵（写序号必填、布尔值域、
 * 负断言）、六操作单机线路往返与初值主张冲突零扰动。
 */
class AtomicGatingTest {

    /** 共享会话注册表。 */
    private final ServerSessionRegistry registry = new ServerSessionRegistry();
    /** 共享核心（原子家族无通知路径，监听器恒静默）。 */
    private final CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
            (sessionId, requestId, key) -> {
            });
    /** 共享分发器。 */
    private final RequestDispatcher dispatcher = new RequestDispatcher(core);

    /** 建立已握手的嵌入通道。 */
    private EmbeddedChannel handshake(int version) {
        EmbeddedChannel ch = new EmbeddedChannel(new ServerSessionHandler(
                core, ServerConfig.defaults(), registry, dispatcher));
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(version)
                .setType(MessageType.HELLO)
                .setRequestId(1)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(version))
                .build());
        assertThat(((Envelope) ch.readOutbound()).getHelloResponse().getStatus())
                .isEqualTo(StatusCode.OK);
        return ch;
    }

    /** 构造原子操作信封（默认 long 形态）。 */
    private static Envelope atomic(long rid, int version, String key, AtomicOp op,
            long operand, long expected, long expectedVersion, long initial, long opSeq) {
        return atomic(rid, version, key, op, LockType.LOCK_TYPE_ATOMIC_LONG,
                operand, expected, expectedVersion, initial, opSeq);
    }

    /** 构造原子操作信封（指定形态）。 */
    private static Envelope atomic(long rid, int version, String key, AtomicOp op, LockType kind,
            long operand, long expected, long expectedVersion, long initial, long opSeq) {
        return Envelope.newBuilder()
                .setProtocolVersion(version)
                .setType(MessageType.ATOMIC_OP)
                .setRequestId(rid)
                .setAtomicOpRequest(AtomicOpRequest.newBuilder()
                        .setKey(key).setOp(op).setLockType(kind)
                        .setOperand(operand).setExpected(expected)
                        .setExpectedVersion(expectedVersion).setInitialValue(initial)
                        .setOpSeq(opSeq))
                .build();
    }

    /** 读一条出站。 */
    private static Envelope out(EmbeddedChannel ch) {
        return (Envelope) ch.readOutbound();
    }

    @Test
    void sixOpsRoundTripWithStampedReads() {
        EmbeddedChannel ch = handshake(4);
        ch.writeInbound(atomic(2, 4, "k", AtomicOp.ATOMIC_SET, 5, 0, 0, 100, 1));
        Envelope set = out(ch);
        assertThat(set.getType()).isEqualTo(MessageType.ATOMIC_OP);
        assertThat(set.getRequestId()).isEqualTo(2);
        assertThat(set.getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(set.getAtomicOpResponse().getApplied()).isTrue();
        assertThat(set.getAtomicOpResponse().getOldValue()).isEqualTo(100);
        assertThat(set.getAtomicOpResponse().getValue()).isEqualTo(5);
        assertThat(set.getAtomicOpResponse().getVersion()).isEqualTo(1);

        ch.writeInbound(atomic(3, 4, "k", AtomicOp.ATOMIC_GET, 0, 0, 0, 0, 0));
        Envelope get = out(ch);
        assertThat(get.getAtomicOpResponse().getApplied()).isFalse();
        assertThat(get.getAtomicOpResponse().getValue()).isEqualTo(5);
        assertThat(get.getAtomicOpResponse().getVersion()).isEqualTo(1);

        ch.writeInbound(atomic(4, 4, "k", AtomicOp.ATOMIC_CAS, 9, 5, 0, 0, 2));
        assertThat(out(ch).getAtomicOpResponse().getVersion()).isEqualTo(2);
        ch.writeInbound(atomic(5, 4, "k", AtomicOp.ATOMIC_CAS, 7, 5, 0, 0, 3));
        Envelope miss = out(ch);
        assertThat(miss.getAtomicOpResponse().getApplied()).isFalse();
        assertThat(miss.getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(miss.getAtomicOpResponse().getVersion()).isEqualTo(2);
        ch.writeInbound(atomic(6, 4, "k", AtomicOp.ATOMIC_ADD, 3, 0, 2, 0, 4));
        assertThat(out(ch).getAtomicOpResponse().getVersion()).isEqualTo(3);
        ch.writeInbound(atomic(7, 4, "k", AtomicOp.ATOMIC_GET_AND_SET, 42, 0, 0, 0, 5));
        Envelope gas = out(ch);
        assertThat(gas.getAtomicOpResponse().getOldValue()).isEqualTo(12);
        assertThat(gas.getAtomicOpResponse().getValue()).isEqualTo(42);
    }

    @Test
    void atomicOpRejectedForV3SessionWithoutDisconnect() {
        EmbeddedChannel ch = handshake(3);
        ch.writeInbound(atomic(2, 3, "k", AtomicOp.ATOMIC_SET, 1, 0, 0, 0, 1));
        Envelope resp = out(ch);
        assertThat(resp.getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        // 低版本判别不伤既有类型：屏障与锁照常。
        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LATCH_COUNT_DOWN)
                .setRequestId(3).setLatchCountDownRequest(
                        io.github.lamspace.openlatch.protocol.LatchCountDownRequest.newBuilder()
                                .setKey("l").setCount(0).setTotal(2))
                .build());
        assertThat(out(ch).getLatchCountDownResponse().getStatus()).isEqualTo(StatusCode.OK);
    }

    @Test
    void acquireCarryingAtomicLockTypeRejected() {
        EmbeddedChannel ch = handshake(4);
        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(4).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(2).setAcquireRequest(
                        io.github.lamspace.openlatch.protocol.AcquireRequest.newBuilder()
                                .setKey("k").setLockType(LockType.LOCK_TYPE_ATOMIC_LONG)
                                .setWaitMs(0))
                .build());
        Envelope resp = out(ch);
        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void shapeViolationsRejectedWithoutStateDisturbance() {
        EmbeddedChannel ch = handshake(4);
        ch.writeInbound(atomic(2, 4, "s", AtomicOp.ATOMIC_SET, 7, 0, 0, 0, 1));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 写操作缺 op_seq（客户端义务违例）。
        ch.writeInbound(atomic(3, 4, "s", AtomicOp.ATOMIC_SET, 8, 0, 0, 0, 0));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 布尔形态值域越界与 ADD 不适用。
        ch.writeInbound(atomic(4, 4, "s", AtomicOp.ATOMIC_SET,
                LockType.LOCK_TYPE_ATOMIC_BOOLEAN, 2, 0, 0, 0, 2));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(atomic(5, 4, "s", AtomicOp.ATOMIC_ADD,
                LockType.LOCK_TYPE_ATOMIC_BOOLEAN, 1, 0, 0, 0, 2));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 负断言数值。
        ch.writeInbound(atomic(6, 4, "s", AtomicOp.ATOMIC_ADD, 1, 0, -3, 0, 2));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 初值主张冲突（条目定型于 SET 无主张=初值 0，非零主张被拒）。
        ch.writeInbound(atomic(7, 4, "s", AtomicOp.ATOMIC_SET, 9, 0, 0, 100, 2));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 原值逐项不变。
        ch.writeInbound(atomic(8, 4, "s", AtomicOp.ATOMIC_GET, 0, 0, 0, 0, 0));
        Envelope get = out(ch);
        assertThat(get.getAtomicOpResponse().getValue()).isEqualTo(7);
        assertThat(get.getAtomicOpResponse().getVersion()).isEqualTo(1);
    }

    @Test
    void sessionCloseLeavesValueForOtherSessions() {
        EmbeddedChannel a = handshake(4);
        EmbeddedChannel b = handshake(4);
        a.writeInbound(atomic(2, 4, "x", AtomicOp.ATOMIC_SET, 42, 0, 0, 0, 1));
        assertThat(out(a).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        a.close(); // 触发断连会话清理
        b.writeInbound(atomic(3, 4, "x", AtomicOp.ATOMIC_GET, 0, 0, 0, 0, 0));
        Envelope get = out(b);
        assertThat(get.getAtomicOpResponse().getValue()).isEqualTo(42);
        assertThat(get.getAtomicOpResponse().getVersion()).isEqualTo(1);
        b.writeInbound(atomic(4, 4, "x", AtomicOp.ATOMIC_ADD, 1, 0, 0, 0, 1));
        assertThat(out(b).getAtomicOpResponse().getValue()).isEqualTo(43);
    }

    // ===================== v6 有值引用：门控、形状矩阵与入口钳制 =====================

    /** 引用形态信封构造（三载荷按 presence 逐格传 null/给定数组）。 */
    private static Envelope atomicRef(long rid, int version, String key, AtomicOp op,
            byte[] operand, byte[] expected, byte[] initial, long opSeq) {
        AtomicOpRequest.Builder rb = AtomicOpRequest.newBuilder()
                .setKey(key).setOp(op).setLockType(LockType.LOCK_TYPE_ATOMIC_REFERENCE)
                .setOpSeq(opSeq);
        if (operand != null) {
            rb.setOperandBytes(com.google.protobuf.ByteString.copyFrom(operand));
        }
        if (expected != null) {
            rb.setExpectedBytes(com.google.protobuf.ByteString.copyFrom(expected));
        }
        if (initial != null) {
            rb.setInitialBytes(com.google.protobuf.ByteString.copyFrom(initial));
        }
        return Envelope.newBuilder().setProtocolVersion(version)
                .setType(MessageType.ATOMIC_OP).setRequestId(rid)
                .setAtomicOpRequest(rb).build();
    }

    /** 引用形态落值信封（缺省参数=不携带该 optional 字段）。 */
    private static Envelope refSet(long rid, int version, String key, byte[] operand) {
        return atomicRef(rid, version, key, AtomicOp.ATOMIC_SET, operand, null, null, 1);
    }

    @Test
    void refFormRoundTripWithPresenceStates() {
        EmbeddedChannel ch = handshake(6);
        ch.writeInbound(refSet(2, 6, "r", new byte[] {1, 2, 3}));
        Envelope set = out(ch);
        assertThat(set.getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(set.getAtomicOpResponse().getApplied()).isTrue();
        assertThat(set.getAtomicOpResponse().hasValueBytes()).isTrue();
        assertThat(set.getAtomicOpResponse().getValueBytes().toByteArray())
                .containsExactly(1, 2, 3);
        assertThat(set.getAtomicOpResponse().getVersion()).isEqualTo(1);

        ch.writeInbound(atomicRef(3, 6, "r", AtomicOp.ATOMIC_GET, null, null, null, 0));
        assertThat(out(ch).getAtomicOpResponse().getValueBytes().toByteArray())
                .containsExactly(1, 2, 3);

        // 落 null 态：applied=true 且 value_bytes 缺省（presence=null 的线路表达）。
        ch.writeInbound(atomicRef(4, 6, "r", AtomicOp.ATOMIC_SET, null, null, null, 2));
        Envelope cleared = out(ch);
        assertThat(cleared.getAtomicOpResponse().getApplied()).isTrue();
        assertThat(cleared.getAtomicOpResponse().getOldValueBytes().toByteArray())
                .containsExactly(1, 2, 3);
        assertThat(cleared.getAtomicOpResponse().hasValueBytes()).isFalse();

        // 期望 null 的 CAS 命中；空字节串以 EMPTY presence 区分。
        ch.writeInbound(atomicRef(5, 6, "r", AtomicOp.ATOMIC_CAS, new byte[0], null, null, 3));
        Envelope cas = out(ch);
        assertThat(cas.getAtomicOpResponse().getApplied()).isTrue();
        assertThat(cas.getAtomicOpResponse().getValueBytes().isEmpty()).isTrue();
    }

    @Test
    void v5SessionRefRejectedWithoutDisconnectScalarRegressionUnchanged() {
        EmbeddedChannel ch = handshake(5);
        // v5 会话引用形态：消息级拒绝、不断连。
        ch.writeInbound(refSet(2, 5, "r", new byte[] {1}));
        Envelope resp = out(ch);
        assertThat(resp.getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        // 同会话标量原子照常。
        ch.writeInbound(atomic(3, 5, "s", AtomicOp.ATOMIC_SET, 5, 0, 0, 0, 1));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // v6 会话标量亦照常（门只拦引用形态）。
        EmbeddedChannel ch6 = handshake(6);
        ch6.writeInbound(atomic(2, 6, "s2", AtomicOp.ATOMIC_ADD, 1, 0, 0, 0, 1));
        assertThat(out(ch6).getAtomicOpResponse().getValue()).isEqualTo(1);
    }

    @Test
    void acquireAndShapeMatrixRejectReferenceForm() {
        EmbeddedChannel ch = handshake(6);
        // ACQUIRE 携带 11 号形态：消息级拒绝、不断连。
        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(6)
                .setType(MessageType.LOCK_ACQUIRE).setRequestId(2)
                .setAcquireRequest(io.github.lamspace.openlatch.protocol.AcquireRequest
                        .newBuilder().setKey("k")
                        .setLockType(LockType.LOCK_TYPE_ATOMIC_REFERENCE).build())
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        // 引用形态携非零标量槽位：形状违例。
        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(6)
                .setType(MessageType.ATOMIC_OP).setRequestId(3)
                .setAtomicOpRequest(AtomicOpRequest.newBuilder()
                        .setKey("k").setOp(AtomicOp.ATOMIC_SET)
                        .setLockType(LockType.LOCK_TYPE_ATOMIC_REFERENCE)
                        .setOperand(5)
                        .setOperandBytes(com.google.protobuf.ByteString.copyFromUtf8("x"))
                        .setOpSeq(1))
                .build());
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 引用形态携带 ADD：值域外（形状层拦截，不入引擎）。
        ch.writeInbound(atomicRef(4, 6, "k", AtomicOp.ATOMIC_ADD, new byte[] {1},
                null, null, 1));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 标量形态携带 bytes 字段：形状违例。
        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(6)
                .setType(MessageType.ATOMIC_OP).setRequestId(5)
                .setAtomicOpRequest(AtomicOpRequest.newBuilder()
                        .setKey("k").setOp(AtomicOp.ATOMIC_SET)
                        .setLockType(LockType.LOCK_TYPE_ATOMIC_LONG)
                        .setOperandBytes(com.google.protobuf.ByteString.copyFromUtf8("x"))
                        .setOpSeq(1))
                .build());
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 引用形态写缺 op_seq：同标量规则。
        ch.writeInbound(atomicRef(6, 6, "k", AtomicOp.ATOMIC_SET, new byte[] {1},
                null, null, 0));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
    }

    @Test
    void payloadClampExactlyAtLimitPassesOversizeRejectedZeroTouch() {
        // 专用小上限夹具：maxValueBytes=64。
        CoreEngine clampCore = new CoreEngine(new CoreConfig(), new SystemClock(),
                (sessionId, requestId, key) -> {
                });
        RequestDispatcher clampDispatcher = new RequestDispatcher(clampCore, null, 64);
        ServerSessionRegistry clampRegistry = new ServerSessionRegistry();
        EmbeddedChannel ch = new EmbeddedChannel(new ServerSessionHandler(
                clampCore, ServerConfig.defaults(), clampRegistry, clampDispatcher));
        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(6).setType(MessageType.HELLO)
                .setRequestId(1)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(6)).build());
        assertThat(out(ch).getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);

        // 恰限 64B 放行。
        byte[] exact = new byte[64];
        ch.writeInbound(refSet(2, 6, "r", exact));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.OK);

        // 65B 超限拒绝，条目零扰动（值与版本不变）。
        ch.writeInbound(refSet(3, 6, "r", new byte[65]));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 超限的 expected 亦拒（CAS 携带 65B 期望值）。
        ch.writeInbound(atomicRef(4, 6, "r", AtomicOp.ATOMIC_CAS, new byte[] {9},
                new byte[65], null, 2));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 超限的 initial 主张亦拒。
        ch.writeInbound(atomicRef(5, 6, "r2", AtomicOp.ATOMIC_SET, new byte[] {1},
                null, new byte[65], 1));
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);

        // 原值逐项不变（64B 空字节数组、版本 1——拒绝请求未占槽未推版本）。
        ch.writeInbound(atomicRef(6, 6, "r", AtomicOp.ATOMIC_GET, null, null, null, 0));
        Envelope get = out(ch);
        assertThat(get.getAtomicOpResponse().getValueBytes().toByteArray()).hasSize(64);
        assertThat(get.getAtomicOpResponse().getVersion()).isEqualTo(1);
        // r2 未建条目（GET 回 null 态、版本 0）。
        ch.writeInbound(atomicRef(7, 6, "r2", AtomicOp.ATOMIC_GET, null, null, null, 0));
        assertThat(out(ch).getAtomicOpResponse().hasValueBytes()).isFalse();
    }
}
