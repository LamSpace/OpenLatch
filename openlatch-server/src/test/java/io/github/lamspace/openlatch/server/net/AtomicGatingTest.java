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
}
