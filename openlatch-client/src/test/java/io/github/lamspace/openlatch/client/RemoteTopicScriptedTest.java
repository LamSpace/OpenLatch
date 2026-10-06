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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.client.internal.ScriptedServer;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TopicMessage;
import io.github.lamspace.openlatch.protocol.TopicOp;
import io.github.lamspace.openlatch.protocol.TopicOpRequest;
import io.github.lamspace.openlatch.protocol.TopicOpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RemoteTopic} 车道单测（v8，{@link ScriptedServer} 直驱）：工厂
 * 参数守门、SUBSCRIBE/PUBLISH/UNSUBSCRIBE 请求形状装配（presence 纪律与
 * op_seq 分配）、应答回显、NOT_LEADER 同信封同序号重试、不可重试拒绝显式
 * 抛错零重发、{@code TOPIC_MESSAGE} 推送经 (会话, 路由键) 命中句柄串行交付、
 * 同任期 seq gap 计入 {@code droppedCount()}、退订后停止交付与幂等。
 * 异型/空载荷拒绝不得读作成功（W10 码形纪律在客户端侧的对偶断言）。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RemoteTopicScriptedTest {

    /** HELLO 应答（v8 服务端形态）。 */
    private static Envelope hello(Envelope req) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(9100L)
                .setServerProtocolVersion(8).setDefaultLeaseMs(30_000))
                .build();
    }

    /** topic 应答信封（op 回显 + 择用字段）。 */
    private static Envelope topicResp(Envelope req, StatusCode status, long subId, long seq) {
        TopicOpRequest q = req.getTopicOpRequest();
        return req.toBuilder().setType(MessageType.TOPIC_OP)
                .setTopicOpResponse(TopicOpResponse.newBuilder()
                        .setStatus(status).setOp(q.getOp())
                        .setSubscriptionId(subId).setTopicSeq(seq))
                .build();
    }

    /** 构造推送帧（request_id=0，按 subscription_id 路由）。 */
    private static Envelope push(long subId, long seq, String payload) {
        return Envelope.newBuilder()
                .setType(MessageType.TOPIC_MESSAGE).setRequestId(0)
                .setTopicMessage(TopicMessage.newBuilder()
                        .setKey("t").setSubscriptionId(subId).setTopicSeq(seq)
                        .setPublisherSid(4242L).setPublishTsMs(1000L + seq)
                        .setPayloadBytes(com.google.protobuf.ByteString
                                .copyFrom(payload.getBytes(StandardCharsets.UTF_8))))
                .build();
    }

    @Test
    void factoryGuardsRejectBadArguments() {
        try (OpenLatchClient client = OpenLatchClient.builder().address("127.0.0.1:1").build()) {
            assertThatThrownBy(() -> client.newTopic(null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> client.newTopic(""))
                    .isInstanceOf(IllegalArgumentException.class);
            OTopic topic = client.newTopic("t");
            assertThat(topic.key()).isEqualTo("t");
            // 消息体不可为 null（rejectNull 契约）：本地即拒、不发请求。
            assertThatThrownBy(() -> topic.publish((byte[]) null))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> topic.publish((String) null))
                    .isInstanceOf(NullPointerException.class);
        }
    }

    @Test
    void subscribePublishShapesAndDelivery() throws Exception {
        List<Envelope> seen = new java.util.ArrayList<>();
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            seen.add(req);
            TopicOp op = req.getTopicOpRequest().getOp();
            if (op == TopicOp.TOPIC_OP_SUBSCRIBE) {
                return topicResp(req, StatusCode.OK, 7L, 0);
            }
            if (op == TopicOp.TOPIC_OP_PUBLISH) {
                return topicResp(req, StatusCode.OK, 0, seen.size());
            }
            return topicResp(req, StatusCode.OK, 0, 0);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OTopic topic = client.newTopic("t");
                List<OTopicMessage> received = new CopyOnWriteArrayList<>();
                OTopicSubscription sub = topic.subscribe(received::add);

                TopicOpRequest subReq = seen.get(0).getTopicOpRequest();
                assertThat(seen.get(0).getType()).isEqualTo(MessageType.TOPIC_OP);
                assertThat(subReq.getOp()).isEqualTo(TopicOp.TOPIC_OP_SUBSCRIBE);
                assertThat(subReq.getKey()).isEqualTo("t");
                assertThat(subReq.hasPayloadBytes()).isFalse();
                assertThat(subReq.getOpSeq()).isZero();

                long seq = topic.publish("事件A".getBytes(StandardCharsets.UTF_8));
                TopicOpRequest pubReq = seen.get(1).getTopicOpRequest();
                assertThat(pubReq.getOp()).isEqualTo(TopicOp.TOPIC_OP_PUBLISH);
                assertThat(pubReq.hasPayloadBytes()).isTrue();
                assertThat(pubReq.getOpSeq()).isGreaterThanOrEqualTo(1);
                assertThat(seq).isPositive();
                assertThat(topic.publish("事件B")).isGreaterThan(0);

                // 服务端推送经桩写达，客户端按路由键命中句柄。
                server.push(push(7L, 101, "事件A"));
                server.push(push(7L, 103, "事件C")); // seq 102 缺失 = 1 条丢弃
                awaitUntil(() -> received.size() == 2, 5_000);
                assertThat(received.get(0).payload()).isEqualTo("事件A".getBytes(StandardCharsets.UTF_8));
                assertThat(received.get(0).topicSeq()).isEqualTo(101);
                assertThat(received.get(0).publisherSessionId()).isEqualTo(4242L);
                assertThat(received.get(1).topicSeq()).isEqualTo(103);
                // gap 推断：101→103 缺 1 条。
                awaitUntil(() -> sub.droppedCount() == 1, 2_000);
                assertThat(sub.isActive()).isTrue();

                topic.unsubscribe();
                assertThat(sub.isActive()).isFalse();
                // 退订后推送不再命中路由（至多一次：丢弃无害）。
                int before = received.size();
                server.push(push(7L, 104, "迟到"));
                Thread.sleep(300);
                assertThat(received).hasSize(before);
                // 线上退订帧为尽力而为（异步）：静置后到达。
                TopicOpRequest unsub = seen.get(seen.size() - 1).getTopicOpRequest();
                assertThat(unsub.getOp()).isEqualTo(TopicOp.TOPIC_OP_UNSUBSCRIBE);
                // 幂等重复退订不发第二帧。
                topic.unsubscribe();
                assertThat(server.countType(MessageType.TOPIC_OP)).isEqualTo(seen.size());
            }
        }
    }

    @Test
    void notLeaderPublishRetriesSameSeqThenSucceeds() throws Exception {
        List<Envelope> seen = new java.util.ArrayList<>();
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            seen.add(req);
            TopicOp op = req.getTopicOpRequest().getOp();
            if (op == TopicOp.TOPIC_OP_PUBLISH && seen.size() == 1) {
                // 首答同型 NOT_LEADER（W10 修复形态）触发同信封同序号重发。
                return topicResp(req, StatusCode.NOT_LEADER, 0, 0);
            }
            if (op == TopicOp.TOPIC_OP_PUBLISH) {
                return topicResp(req, StatusCode.OK, 0, 5);
            }
            return topicResp(req, StatusCode.OK, 1, 0);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OTopic topic = client.newTopic("t");
                long seq = topic.publish("x".getBytes(StandardCharsets.UTF_8));
                assertThat(seq).isEqualTo(5);
                List<TopicOpRequest> pubs = seen.stream()
                        .filter(e -> e.getTopicOpRequest().getOp() == TopicOp.TOPIC_OP_PUBLISH)
                        .map(Envelope::getTopicOpRequest).toList();
                assertThat(pubs).hasSize(2);
                // 同会话重发必须同 op_seq 同 requestId（服务端去重槽依据）。
                assertThat(pubs.get(0).getOpSeq()).isEqualTo(pubs.get(1).getOpSeq());
                assertThat(seen.get(0).getRequestId()).isEqualTo(seen.get(1).getRequestId());
            }
        }
    }

    @Test
    void capReachedSubscribeRejectsExplicitlyWithoutRetry() throws Exception {
        List<Envelope> seen = new java.util.ArrayList<>();
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            seen.add(req);
            return topicResp(req, StatusCode.REJECT_SUBSCRIBERS, 0, 0);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OTopic topic = client.newTopic("t");
                assertThatThrownBy(() -> topic.subscribe(m -> {
                }))
                        .isInstanceOf(OpenLatchException.class)
                        .extracting(e -> ((OpenLatchException) e).status())
                        .isEqualTo(StatusCode.REJECT_SUBSCRIBERS);
                // 不可重试拒绝：恰一请求帧、零重发。
                assertThat(seen).hasSize(1);
            }
        }
    }

    /** 自旋等待断言条件（不裸 sleep 定长）。 */
    private static void awaitUntil(java.util.function.BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("条件超时未达成");
    }
}
