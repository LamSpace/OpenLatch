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
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TopicOp;
import io.github.lamspace.openlatch.protocol.TopicOpRequest;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.github.lamspace.openlatch.server.topic.TopicRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 协议层用例（v8）：TOPIC_OP 的 v8 门控（v≤7 会话消息级拒绝、不断连）、
 * 形状互斥矩阵入口拒绝、{@code maxValueBytes} 入口钳制（超限零 fan-out）、
 * 键校验码形、撞 key 只读探测拒绝（线路送达 INVALID_REQUEST）、单机
 * 订阅-发布-交付闭环、退订停交付与断连即退订（死亡即摘除）。
 * 判例 {@code QueueGatingTest}/{@code AtomicGatingTest}。
 */
class TopicGatingTest {

    /** 载荷钳制上限（测试用小值）。 */
    private static final int VALUE_CAP = 64;

    /** 共享会话注册表。 */
    private final ServerSessionRegistry registry = new ServerSessionRegistry();
    /** 共享核心（唤醒桥恒静默）。 */
    private final CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
            (sessionId, requestId, key) -> {
            });
    /** topic 登记表（订阅上限 8、缓冲 16——交付断言用）。 */
    private final TopicRegistry topics = new TopicRegistry(registry, 8, 16);
    /** 默认限额分发器（含 topic 面）。 */
    private final RequestDispatcher dispatcher = new RequestDispatcher(core, null,
            ServerConfig.DEFAULT_MAX_VALUE_BYTES, ServerConfig.DEFAULT_MAX_QUEUE_CAPACITY,
            ServerConfig.DEFAULT_MAX_DRAIN_BYTES, ServerConfig.DEFAULT_MAX_KEY_LENGTH, topics);

    /**
     * 建立已握手的嵌入通道。
     *
     * @param version 客户端协议版本
     * @return 通道
     */
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

    /** 构造 topic 请求信封。 */
    private static Envelope topicOp(long rid, int version, TopicOpRequest req) {
        return Envelope.newBuilder().setProtocolVersion(version)
                .setType(MessageType.TOPIC_OP).setRequestId(rid)
                .setTopicOpRequest(req).build();
    }

    /**
     * 发送并读出应答信封。发布者自身即订阅者时，{@code TOPIC_MESSAGE}
     * 推送先于应答写入同一连接（同 EventLoop FIFO）——推送按类型
     * 收集（{@code request_id=0}，客户端按类型路由的判例 AWAIT_NOTIFY），
     * 应答按 {@code request_id} 匹配返回。
     */
    private static Envelope ask(EmbeddedChannel ch, Envelope req) {
        return ask(ch, req, new java.util.ArrayList<>());
    }

    /** 收集途中推送的 ask 形态。 */
    private static Envelope ask(EmbeddedChannel ch, Envelope req,
            java.util.List<Envelope> pushes) {
        ch.writeInbound(req);
        while (true) {
            Object out = ch.readOutbound();
            assertThat(out).isInstanceOf(Envelope.class);
            Envelope e = (Envelope) out;
            if (e.getType() == MessageType.TOPIC_MESSAGE) {
                pushes.add(e);
                continue;
            }
            assertThat(e.getRequestId()).isEqualTo(req.getRequestId());
            return e;
        }
    }

    @Test
    void v7SessionTopicOpRejectedWithoutDisconnect() {
        EmbeddedChannel ch = handshake(7);

        Envelope resp = ask(ch, topicOp(2, 7, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_SUBSCRIBE).build()));
        // 同型拒绝：payload 为 topic_op_response、op 回显、连接保持（判例各门）。
        assertThat(resp.getType()).isEqualTo(MessageType.TOPIC_OP);
        assertThat(resp.hasTopicOpResponse()).isTrue();
        assertThat(resp.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(resp.getTopicOpResponse().getOp()).isEqualTo(TopicOp.TOPIC_OP_SUBSCRIBE);
        assertThat(ch.isOpen()).isTrue();
        assertThat(topics.topicKeyCount()).isZero();

        // 既有能力回归：v7 会话的 ACQUIRE 照常服务。
        Envelope acq = ask(ch, Envelope.newBuilder().setProtocolVersion(7)
                .setType(MessageType.LOCK_ACQUIRE).setRequestId(3)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("lk").setLockType(LockType.LOCK_TYPE_REENTRANT).build()).build());
        assertThat(acq.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
    }

    @Test
    void v8SubscribePublishDeliversTopicMessage() {
        EmbeddedChannel ch = handshake(8);

        Envelope sub = ask(ch, topicOp(2, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_SUBSCRIBE).build()));
        assertThat(sub.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        long subscriptionId = sub.getTopicOpResponse().getSubscriptionId();
        assertThat(subscriptionId).isPositive();

        java.util.List<Envelope> pushes = new java.util.ArrayList<>();
        Envelope pub = ask(ch, topicOp(3, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_PUBLISH).setOpSeq(1L)
                .setPayloadBytes(com.google.protobuf.ByteString
                        .copyFromUtf8("hello-topic")).build()), pushes);
        assertThat(pub.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(pub.getTopicOpResponse().getTopicSeq()).isEqualTo(1L);

        // 交付推送：request_id=0、全字段可读、字节级保真。
        assertThat(pushes).hasSize(1);
        Envelope push = pushes.get(0);
        assertThat(push.getType()).isEqualTo(MessageType.TOPIC_MESSAGE);
        assertThat(push.getRequestId()).isZero();
        assertThat(push.getProtocolVersion()).isEqualTo(8);
        assertThat(push.getTopicMessage().getKey()).isEqualTo("t");
        assertThat(push.getTopicMessage().getSubscriptionId()).isEqualTo(subscriptionId);
        assertThat(push.getTopicMessage().getTopicSeq()).isEqualTo(1L);
        assertThat(push.getTopicMessage().getPublisherSid()).isPositive();
        assertThat(push.getTopicMessage().getPublishTsMs()).isPositive();
        assertThat(push.getTopicMessage().getPayloadBytes().toStringUtf8())
                .isEqualTo("hello-topic");
    }

    @Test
    void shapeMatrixRejectedAtEntry() {
        EmbeddedChannel ch = handshake(8);
        long rid = 10;

        // PUBLISH 缺载荷（违例，判例 v7 元素纪律）。
        assertThat(ask(ch, topicOp(rid++, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_PUBLISH).setOpSeq(1L).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // PUBLISH 零序号。
        assertThat(ask(ch, topicOp(rid++, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_PUBLISH).setOpSeq(0L)
                .setPayloadBytes(com.google.protobuf.ByteString.copyFromUtf8("x")).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // SUBSCRIBE 携载荷。
        assertThat(ask(ch, topicOp(rid++, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_SUBSCRIBE)
                .setPayloadBytes(com.google.protobuf.ByteString.copyFromUtf8("x")).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // SUBSCRIBE 携非零序号。
        assertThat(ask(ch, topicOp(rid++, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_SUBSCRIBE).setOpSeq(3L).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 零长度载荷的 PUBLISH 是合法空消息（判例 v7）。
        assertThat(ask(ch, topicOp(rid, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_PUBLISH).setOpSeq(9L)
                .setPayloadBytes(com.google.protobuf.ByteString.EMPTY).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(ch.isOpen()).isTrue();
        assertThat(topics.topicKeyCount()).isZero(); // 违例路径零登记、零扰动
    }

    @Test
    void oversizedPayloadRejectedZeroFanout() {
        RequestDispatcher clamped = new RequestDispatcher(core, null, VALUE_CAP,
                ServerConfig.DEFAULT_MAX_QUEUE_CAPACITY, ServerConfig.DEFAULT_MAX_DRAIN_BYTES,
                ServerConfig.DEFAULT_MAX_KEY_LENGTH, topics);
        EmbeddedChannel ch = new EmbeddedChannel(new ServerSessionHandler(
                core, ServerConfig.defaults(), registry, clamped));
        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(8).setType(MessageType.HELLO)
                .setRequestId(1).setHelloRequest(HelloRequest.newBuilder()
                        .setClientProtocolVersion(8)).build());
        assertThat(((Envelope) ch.readOutbound()).getHelloResponse().getStatus())
                .isEqualTo(StatusCode.OK);

        assertThat(ask(ch, topicOp(2, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_SUBSCRIBE).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        byte[] big = new byte[VALUE_CAP + 1];
        java.util.Arrays.fill(big, (byte) 7);
        assertThat(ask(ch, topicOp(3, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_PUBLISH).setOpSeq(1L)
                .setPayloadBytes(com.google.protobuf.ByteString.copyFrom(big)).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 恰限放行。
        java.util.List<Envelope> pushes = new java.util.ArrayList<>();
        assertThat(ask(ch, topicOp(4, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_PUBLISH).setOpSeq(2L)
                .setPayloadBytes(com.google.protobuf.ByteString.copyFrom(
                        java.util.Arrays.copyOf(big, VALUE_CAP))).build()), pushes)
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(topics.droppedTotal()).isZero();
        // 超限消息未被 fan-out 亦未占序号：恰限条为首个受理 seq=1。
        assertThat(pushes).hasSize(1);
        assertThat(pushes.get(0).getTopicMessage().getTopicSeq()).isEqualTo(1L);
        assertThat((Object) ch.readOutbound()).isNull();
    }

    @Test
    void keyValidationCodes() {
        EmbeddedChannel ch = handshake(8);

        assertThat(ask(ch, topicOp(2, 8, TopicOpRequest.newBuilder()
                .setOp(TopicOp.TOPIC_OP_SUBSCRIBE).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.KEY_EMPTY);
        String longKey = "k".repeat(ServerConfig.DEFAULT_MAX_KEY_LENGTH + 1);
        assertThat(ask(ch, topicOp(3, 8, TopicOpRequest.newBuilder()
                .setKey(longKey).setOp(TopicOp.TOPIC_OP_SUBSCRIBE).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.KEY_TOO_LONG);
    }

    @Test
    void collidingKeyRejectedWithoutRegistration() {
        EmbeddedChannel ch = handshake(8);
        // 先把 k 建成锁条目（引擎家族占据）。
        assertThat(ask(ch, Envelope.newBuilder().setProtocolVersion(8)
                .setType(MessageType.LOCK_ACQUIRE).setRequestId(2)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("k").setLockType(LockType.LOCK_TYPE_REENTRANT).build()).build())
                .getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

        // 撞 key 探测命中：内部裁决 REJECT_TYPE_MISMATCH、线路送达 INVALID_REQUEST。
        Envelope resp = ask(ch, topicOp(3, 8, TopicOpRequest.newBuilder()
                .setKey("k").setOp(TopicOp.TOPIC_OP_SUBSCRIBE).build()));
        assertThat(resp.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(topics.subscriberCount("k")).isZero();
        assertThat(topics.topicKeyCount()).isZero();
    }

    @Test
    void unsubscribeAndSessionDeathStopDelivery() {
        EmbeddedChannel ch = handshake(8);
        assertThat(ask(ch, topicOp(2, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_SUBSCRIBE).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);

        // 退订幂等：两次 UNSUBSCRIBE 均 OK、零扰动。
        for (long rid = 3; rid <= 4; rid++) {
            assertThat(ask(ch, topicOp(rid, 8, TopicOpRequest.newBuilder()
                    .setKey("t").setOp(TopicOp.TOPIC_OP_UNSUBSCRIBE).build()))
                    .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        }
        assertThat(topics.topicKeyCount()).isZero();

        // 重订阅后断连：死亡即退订（登记表三路回收之单机路径）。
        assertThat(ask(ch, topicOp(5, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_SUBSCRIBE).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(topics.subscriberCount("t")).isEqualTo(1);
        ch.close();
        assertThat(topics.subscriberCount("t")).isZero();
        assertThat(topics.topicKeyCount()).isZero();
    }

    @Test
    void zeroLengthMessagePayloadRoundTrip() {
        EmbeddedChannel ch = handshake(8);
        assertThat(ask(ch, topicOp(2, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_SUBSCRIBE).build()))
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        java.util.List<Envelope> pushes = new java.util.ArrayList<>();
        assertThat(ask(ch, topicOp(3, 8, TopicOpRequest.newBuilder()
                .setKey("t").setOp(TopicOp.TOPIC_OP_PUBLISH).setOpSeq(1L)
                .setPayloadBytes(com.google.protobuf.ByteString.EMPTY).build()), pushes)
                .getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(pushes).hasSize(1);
        // 交付侧恒存在语义：零长度=合法空消息（proto3 bytes 无 presence 位，
        // 空即消息本体；请求侧 presence 纪律由 TopicOpRequest.optional 承载）。
        assertThat(pushes.get(0).getTopicMessage().getPayloadBytes().isEmpty()).isTrue();
    }
}
