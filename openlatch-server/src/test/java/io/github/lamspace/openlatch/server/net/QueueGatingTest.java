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
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.QueueOp;
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 协议层用例（v7）：QUEUE_OP 的 v7 门控（v≤6 会话消息级拒绝、不断连，
 * 既有能力回归不变）与单机入口钳制矩阵——元素超 {@code maxValueBytes}
 * 拒且零入核（恰限放行）、容量主张超 {@code maxQueueCapacity} 拒、
 * DRAIN 提取上限钳定生效（钳定值随命令入引擎）、钳制下调存量照常可读
 * 可消费、新写按新限拒（判例 {@code AtomicGatingTest}）。
 */
class QueueGatingTest {

    /** 元素钳制上限（测试用，小值便于越界构造）。 */
    private static final int VALUE_CAP = 64;
    /** 容量主张上限（测试用）。 */
    private static final int QUEUE_CAPACITY_CAP = 8;
    /** drain 应答字节预算（测试用）。 */
    private static final long DRAIN_BUDGET = 128L;

    /** 共享会话注册表。 */
    private final ServerSessionRegistry registry = new ServerSessionRegistry();
    /** 共享核心（唤醒桥恒静默）。 */
    private final CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
            (sessionId, requestId, key) -> {
            });
    /** 默认限额分发器（v7 内置默认）。 */
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

    /** 建立钳制限额分发器的通道（v7）。 */
    private EmbeddedChannel clampedChannel() {
        RequestDispatcher clampDispatcher = new RequestDispatcher(core, null,
                VALUE_CAP, QUEUE_CAPACITY_CAP, DRAIN_BUDGET);
        EmbeddedChannel ch = new EmbeddedChannel(new ServerSessionHandler(
                core, ServerConfig.defaults(), registry, clampDispatcher));
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(7).setType(MessageType.HELLO).setRequestId(1)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(7))
                .build());
        assertThat(((Envelope) ch.readOutbound()).getHelloResponse().getStatus())
                .isEqualTo(StatusCode.OK);
        return ch;
    }

    /** 构造队列写请求信封。 */
    private static Envelope put(long rid, int version, String key, LockType kind, long capacity,
            byte[] element, long delayMs) {
        QueueOpRequest.Builder rb = QueueOpRequest.newBuilder()
                .setKey(key).setOp(QueueOp.QUEUE_OP_PUT).setLockType(kind)
                .setCapacity(capacity).setDelayMs(delayMs).setOpSeq(rid * 100);
        if (element != null) {
            rb.setElementBytes(com.google.protobuf.ByteString.copyFrom(element));
        }
        return Envelope.newBuilder().setProtocolVersion(version)
                .setType(MessageType.QUEUE_OP).setRequestId(rid).setQueueOpRequest(rb).build();
    }

    /** 构造队列通用操作信封。 */
    private static Envelope qop(long rid, int version, String key, LockType kind, QueueOp op,
            int maxElements) {
        return Envelope.newBuilder().setProtocolVersion(version)
                .setType(MessageType.QUEUE_OP).setRequestId(rid)
                .setQueueOpRequest(QueueOpRequest.newBuilder()
                        .setKey(key).setOp(op).setLockType(kind)
                        .setMaxElements(maxElements)
                        .setOpSeq(op == QueueOp.QUEUE_OP_PEEK || op == QueueOp.QUEUE_OP_SIZE
                                ? 0 : rid * 100))
                .build();
    }

    private static byte[] bytes(int n) {
        return new byte[n];
    }

    /** 读一条出站。 */
    private static Envelope out(EmbeddedChannel ch) {
        return (Envelope) ch.readOutbound();
    }

    @Test
    void v6SessionQueueRejectedWithoutDisconnectExistingCapabilitiesIntact() {
        EmbeddedChannel ch = handshake(6);
        ch.writeInbound(put(2, 6, "q", LockType.LOCK_TYPE_QUEUE, 4, bytes(4), 0));
        Envelope resp = out(ch);
        assertThat(resp.getType()).isEqualTo(MessageType.QUEUE_OP);
        assertThat(resp.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        // v≤5 既有回归：v6 会话的引用形态原子与锁操作照常（门互不波及）。
        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(6)
                .setType(MessageType.LOCK_ACQUIRE).setRequestId(3)
                .setAcquireRequest(io.github.lamspace.openlatch.protocol.AcquireRequest
                        .newBuilder().setKey("lk").setWaitMs(0))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(core.inspectKey("q")).isNull(); // 被拒请求零入核
    }

    @Test
    void elementClampBoundaryMatrix() {
        EmbeddedChannel ch = clampedChannel();
        // 恰限放行。
        ch.writeInbound(put(2, 7, "q", LockType.LOCK_TYPE_QUEUE, QUEUE_CAPACITY_CAP,
                bytes(VALUE_CAP), 0));
        assertThat(out(ch).getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 超一字节拒且零入核（key 深度保持 1）。
        ch.writeInbound(put(3, 7, "q", LockType.LOCK_TYPE_QUEUE, QUEUE_CAPACITY_CAP,
                bytes(VALUE_CAP + 1), 0));
        Envelope oversize = out(ch);
        assertThat(oversize.getQueueOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(core.inspectKey("q").queueDepth()).isEqualTo(1);
        // 容量主张超上限拒。
        ch.writeInbound(put(4, 7, "q2", LockType.LOCK_TYPE_QUEUE, QUEUE_CAPACITY_CAP + 1,
                bytes(1), 0));
        assertThat(out(ch).getQueueOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(core.inspectKey("q2")).isNull();
    }

    @Test
    void drainLimitClampedIntoCommand() {
        EmbeddedChannel ch = clampedChannel();
        // 灌入 5 个 1 字节元素（预算 128/64 → 派生上限 2）。
        for (int i = 0; i < 5; i++) {
            ch.writeInbound(put(10 + i, 7, "dq", LockType.LOCK_TYPE_QUEUE, QUEUE_CAPACITY_CAP,
                    bytes(1), 0));
            assertThat(out(ch).getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        }
        // 请求 max_elements=100 → 钳到 2：恰摘 2 项。
        ch.writeInbound(qop(20, 7, "dq", LockType.LOCK_TYPE_QUEUE, QueueOp.QUEUE_OP_DRAIN, 100));
        Envelope drained = out(ch);
        assertThat(drained.getQueueOpResponse().getDrainedBytesCount()).isEqualTo(2);
        assertThat(core.inspectKey("dq").queueDepth()).isEqualTo(3);
        // 请求 0（缺省派生上限）同样钳 2。
        ch.writeInbound(qop(21, 7, "dq", LockType.LOCK_TYPE_QUEUE, QueueOp.QUEUE_OP_DRAIN, 0));
        assertThat(out(ch).getQueueOpResponse().getDrainedBytesCount()).isEqualTo(2);
    }

    @Test
    void clampDownKeepsExistingReadableAndRejectsNewWrites() {
        EmbeddedChannel ch = clampedChannel();
        ch.writeInbound(put(2, 7, "q", LockType.LOCK_TYPE_QUEUE, QUEUE_CAPACITY_CAP,
                bytes(VALUE_CAP), 0));
        assertThat(out(ch).getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 钳制下调后的分发器：既有大元素照常可读可消费，新超限写拒。
        RequestDispatcher lowered = new RequestDispatcher(core, null, 16, QUEUE_CAPACITY_CAP,
                DRAIN_BUDGET);
        EmbeddedChannel ch2 = new EmbeddedChannel(new ServerSessionHandler(
                core, ServerConfig.defaults(), registry, lowered));
        ch2.writeInbound(Envelope.newBuilder().setProtocolVersion(7).setType(MessageType.HELLO)
                .setRequestId(1)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(7)).build());
        out(ch2);
        ch2.writeInbound(qop(3, 7, "q", LockType.LOCK_TYPE_QUEUE, QueueOp.QUEUE_OP_PEEK, 0));
        Envelope peeked = out(ch2);
        assertThat(peeked.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(peeked.getQueueOpResponse().getElementBytes().size()).isEqualTo(VALUE_CAP);
        ch2.writeInbound(put(4, 7, "q", LockType.LOCK_TYPE_QUEUE, QUEUE_CAPACITY_CAP,
                bytes(17), 0));
        assertThat(out(ch2).getQueueOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch2.writeInbound(qop(5, 7, "q", LockType.LOCK_TYPE_QUEUE, QueueOp.QUEUE_OP_TAKE, 0));
        assertThat(out(ch2).getQueueOpResponse().getElementBytes().size()).isEqualTo(VALUE_CAP);
    }

    @Test
    void capacityAssertionMismatchZeroDisturb() {
        EmbeddedChannel ch = handshake(7);
        ch.writeInbound(put(2, 7, "q", LockType.LOCK_TYPE_QUEUE, 4, bytes(1), 0));
        assertThat(out(ch).getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 主张不符（8≠4）：INVALID_REQUEST 映射、元素零扰动。
        ch.writeInbound(put(3, 7, "q", LockType.LOCK_TYPE_QUEUE, 8, bytes(1), 0));
        assertThat(out(ch).getQueueOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(core.inspectKey("q").queueDepth()).isEqualTo(1);
    }
}
