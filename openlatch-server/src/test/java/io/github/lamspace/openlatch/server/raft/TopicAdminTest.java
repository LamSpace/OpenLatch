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

import com.google.protobuf.ByteString;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse;
import io.github.lamspace.openlatch.protocol.AdminListKeysResponse;
import io.github.lamspace.openlatch.protocol.AdminSummaryResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TopicOp;
import io.github.lamspace.openlatch.protocol.TopicOpRequest;
import io.github.lamspace.openlatch.server.AdminConfig;
import io.github.lamspace.openlatch.server.admin.AdminRequestHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * topic 管理观察夹具（v8）：Leader 视角 SUMMARY {@code topic_entries}、
 * LIST_KEYS {@code family=topic} 行与订阅计数、KEY_DETAIL 订阅者明细三字段；
 * 已交付消息内容零外发；观察零扰动（查询后交付链完好）；Follower 视角
 * 如实缺席（无伪零行、详情明确未命中）。判例 {@code AdminClusterTest}
 * （单 harness 双视角断言，控制端口周转）。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class TopicAdminTest {

    /** 管理令牌。 */
    private static final String TOKEN = "topic-admin-token";

    /** 三节点基座。 */
    private ClusterHarness harness;

    @BeforeEach
    void setUp() throws IOException {
        harness = ClusterHarness.start(3);
    }

    @AfterEach
    void tearDown() {
        harness.close();
    }

    /** 为节点装配管理处理器（集群数据源形态）。 */
    private AdminRequestHandler handlerFor(ClusterHarness.Node node) {
        return new AdminRequestHandler(new AdminConfig(TOKEN), null, node.runtime,
                node.registry, () -> 77L);
    }

    /** 同步发一条 ADMIN 请求并取回应答。 */
    private Envelope admin(ClusterHarness.Node node, AdminRequestHandler handler,
                           ClusterHarness.TestConn conn, Envelope msg) {
        handler.handle(conn.ctx, conn.session, msg);
        return conn.awaitOutbound(10_000);
    }

    /** ADMIN 请求信封（三型共用构造位）。 */
    private static Envelope adminMsg(MessageType type, long rid) {
        Envelope.Builder b = Envelope.newBuilder().setProtocolVersion(3)
                .setType(type).setRequestId(rid);
        switch (type) {
            case ADMIN_SUMMARY -> b.setAdminSummaryRequest(
                    io.github.lamspace.openlatch.protocol.AdminSummaryRequest.newBuilder()
                            .setToken(TOKEN));
            case ADMIN_LIST_KEYS -> b.setAdminListKeysRequest(
                    io.github.lamspace.openlatch.protocol.AdminListKeysRequest.newBuilder()
                            .setToken(TOKEN).setPage(0).setPageSize(100).setPrefix(""));
            default -> b.setAdminKeyDetailRequest(
                    io.github.lamspace.openlatch.protocol.AdminKeyDetailRequest.newBuilder()
                            .setToken(TOKEN).setKey("ta:ad"));
        }
        return b.build();
    }

    /** TOPIC_OP 请求信封。 */
    private static Envelope topic(long rid, String key, TopicOp op, byte[] payload, long opSeq) {
        TopicOpRequest.Builder rb = TopicOpRequest.newBuilder()
                .setKey(key).setOp(op).setOpSeq(opSeq);
        if (payload != null) {
            rb.setPayloadBytes(ByteString.copyFrom(payload));
        }
        return Envelope.newBuilder().setProtocolVersion(8).setType(MessageType.TOPIC_OP)
                .setRequestId(rid).setTopicOpRequest(rb).build();
    }

    /** 发送并取同 rid 应答（途中推送收集入 sink）。 */
    private static Envelope ask(ClusterHarness.TestConn conn, Envelope msg, List<Envelope> sink) {
        Envelope first = conn.request(msg);
        if (first.getType() == MessageType.TOPIC_OP
                && first.getRequestId() == msg.getRequestId()) {
            return first;
        }
        if (first.getType() == MessageType.TOPIC_MESSAGE) {
            sink.add(first);
        }
        for (int i = 0; i < 32; i++) {
            Envelope next = conn.awaitOutbound(10_000);
            if (next.getType() == MessageType.TOPIC_MESSAGE) {
                sink.add(next);
                continue;
            }
            if (next.getType() == MessageType.TOPIC_OP
                    && next.getRequestId() == msg.getRequestId()) {
                return next;
            }
        }
        throw new AssertionError("topic reply timeout");
    }

    @Test
    void leaderViewPresentsTopicAndFollowerOmits() throws Exception {
        ClusterHarness.Node leader = harness.leader();
        ClusterHarness.Node follower = harness.nodes().stream()
                .filter(n -> n != leader && n.alive()).findFirst().orElseThrow();
        var registry = leader.runtime.topicRegistry();
        ClusterHarness.TestConn sub1 = harness.connect(leader);
        sub1.hello(1, 8);
        long sid1 = sub1.session.sessionId();
        ClusterHarness.TestConn sub2 = harness.connect(leader);
        sub2.hello(1, 8);
        long sid2 = sub2.session.sessionId();
        ask(sub1, topic(10L, "ta:ad", TopicOp.TOPIC_OP_SUBSCRIBE, null, 0), new ArrayList<>());
        ask(sub2, topic(11L, "ta:ad", TopicOp.TOPIC_OP_SUBSCRIBE, null, 0), new ArrayList<>());
        // 哨兵载荷：交付一次，一切管理应答不得含其内容。
        ask(sub1, topic(12L, "ta:ad", TopicOp.TOPIC_OP_PUBLISH,
                "admin-payload-secret".getBytes(StandardCharsets.UTF_8), 1L),
                new ArrayList<>());
        assertThat(registry.subscriberCount("ta:ad")).isEqualTo(2);

        AdminRequestHandler handler = handlerFor(leader);
        AdminSummaryResponse sum = admin(leader, handler, sub1,
                adminMsg(MessageType.ADMIN_SUMMARY, 20L)).getAdminSummaryResponse();
        assertThat(sum.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(sum.getTopicEntries()).isEqualTo(1);

        AdminListKeysResponse keys = admin(leader, handler, sub1,
                adminMsg(MessageType.ADMIN_LIST_KEYS, 21L)).getAdminListKeysResponse();
        var topicRow = keys.getItemsList().stream()
                .filter(i -> i.getKey().equals("ta:ad")).findFirst().orElseThrow();
        assertThat(topicRow.getFamily()).isEqualTo("topic");
        assertThat(topicRow.getTopicSubscribers()).isEqualTo(2);
        assertThat(topicRow.getHolders()).isZero();
        assertThat(topicRow.getWaiterCount()).isZero();

        AdminKeyDetailResponse detail = admin(leader, handler, sub1,
                adminMsg(MessageType.ADMIN_KEY_DETAIL, 22L)).getAdminKeyDetailResponse();
        assertThat(detail.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(detail.getFamily()).isEqualTo("topic");
        assertThat(detail.getTopicSubscribers()).isEqualTo(2);
        assertThat(detail.getTopicSubscribersInfoList()).hasSize(2);
        assertThat(detail.getTopicSubscribersInfoList())
                .anyMatch(s -> s.getSessionId() == sid1 && s.getSubscriptionId() > 0
                        && s.getSubscribedAtMs() > 0)
                .anyMatch(s -> s.getSessionId() == sid2);
        // 零外发：全量消息内容不出现在任何管理应答。
        assertThat(sum.toString()).doesNotContain("admin-payload-secret");
        assertThat(keys.toString()).doesNotContain("admin-payload-secret");
        assertThat(detail.toString()).doesNotContain("admin-payload-secret");

        // 观察零扰动：反复查询后交付链完好（再发布即收）。
        for (int i = 0; i < 3; i++) {
            admin(leader, handler, sub1, adminMsg(MessageType.ADMIN_KEY_DETAIL, 30L + i));
        }
        List<Envelope> sink = new ArrayList<>();
        ask(sub1, topic(40L, "ta:ad", TopicOp.TOPIC_OP_PUBLISH,
                "after-observe".getBytes(StandardCharsets.UTF_8), 2L), sink);
        long deadline = System.currentTimeMillis() + 10_000;
        while (sink.isEmpty() && System.currentTimeMillis() < deadline) {
            Envelope e = sub1.pollOutbound();
            if (e != null && e.getType() == MessageType.TOPIC_MESSAGE) {
                sink.add(e);
            }
            Thread.sleep(20);
        }
        assertThat(sink).isNotEmpty();
        assertThat(sink.get(0).getTopicMessage().getTopicSeq()).isEqualTo(2L);

        // Follower 视角：无登记来源，如实缺席（无伪零行、详情明确未命中）。
        AdminRequestHandler followerHandler = handlerFor(follower);
        ClusterHarness.TestConn fconn = harness.connect(follower);
        fconn.hello(1, 8);
        AdminSummaryResponse fSum = admin(follower, followerHandler, fconn,
                adminMsg(MessageType.ADMIN_SUMMARY, 20L)).getAdminSummaryResponse();
        assertThat(fSum.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(fSum.getTopicEntries()).isZero();
        assertThat(fSum.getNodeRole()).isNotEqualTo("LEADER");
        AdminListKeysResponse fKeys = admin(follower, followerHandler, fconn,
                adminMsg(MessageType.ADMIN_LIST_KEYS, 21L)).getAdminListKeysResponse();
        assertThat(fKeys.getItemsList()).noneMatch(i -> i.getFamily().equals("topic"));
        AdminKeyDetailResponse fDetail = admin(follower, followerHandler, fconn,
                adminMsg(MessageType.ADMIN_KEY_DETAIL, 22L)).getAdminKeyDetailResponse();
        assertThat(fDetail.getStatus()).isEqualTo(StatusCode.NOT_HELD);
        assertThat(fDetail.getTopicSubscribers()).isZero();
        assertThat(fDetail.getTopicSubscribersInfoList()).isEmpty();

        fconn.disconnect();
        sub1.disconnect();
        sub2.disconnect();
    }
}
