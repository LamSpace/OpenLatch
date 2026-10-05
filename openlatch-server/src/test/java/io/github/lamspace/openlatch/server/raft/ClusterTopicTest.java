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
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TopicOp;
import io.github.lamspace.openlatch.protocol.TopicOpRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * topic 集群 E2E（v8）：多订阅者 fan-out 完整性与每订阅 seq 升序、并发
 * Publisher 收全不重、Follower 同型 {@code NOT_LEADER} 零副作用、Publisher
 * 同序号重发去重不双扇出、UNSUBSCRIBE/会话死亡回收登记（防泄漏验收点）、
 * 换主后登记清零 + 客户端重挂续收且 term 基线重置。
 *
 * <p><b>夹具纪律</b>：本类刻意压缩为 3 个三节点 harness（连接在判据间复用、
 * 键互不相干），不抬高共享 fork 内 Raft gRPC 的临时端口周转压力
 * （判例 WATCHLIST W2 的 freePort 探针竞态族）。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClusterTopicTest {

    /** 构造 topic 操作信封（v8）。 */
    private static Envelope topic(long rid, String key, TopicOp op, byte[] payload, long opSeq) {
        TopicOpRequest.Builder rb = TopicOpRequest.newBuilder()
                .setKey(key).setOp(op).setOpSeq(opSeq);
        if (payload != null) {
            rb.setPayloadBytes(ByteString.copyFrom(payload));
        }
        return Envelope.newBuilder().setProtocolVersion(8).setType(MessageType.TOPIC_OP)
                .setRequestId(rid).setTopicOpRequest(rb).build();
    }

    /**
     * 发请求并取回同 rid 同型应答；途中混入的 {@code TOPIC_MESSAGE} 推送
     * （request_id=0）收集入给定表（真实 SDK 走回调通道，harness 直驱需自筛）。
     */
    private static Envelope ask(ClusterHarness.TestConn conn, Envelope msg,
            List<Envelope> pushes) {
        Envelope first = conn.request(msg);
        if (first.getType() == MessageType.TOPIC_OP && first.getRequestId() == msg.getRequestId()) {
            return first;
        }
        if (first.getType() == MessageType.TOPIC_MESSAGE) {
            pushes.add(first);
        }
        for (int i = 0; i < 64; i++) {
            Envelope next = conn.awaitOutbound(10_000);
            if (next.getType() == MessageType.TOPIC_MESSAGE) {
                pushes.add(next);
                continue;
            }
            if (next.getType() == MessageType.TOPIC_OP
                    && next.getRequestId() == msg.getRequestId()) {
                return next;
            }
        }
        throw new AssertionError("reply for rid " + msg.getRequestId() + " not found");
    }

    /** 静默订阅登记（断言回执 OK 并返回路由键）。 */
    private static long subscribe(ClusterHarness.TestConn conn, String key, List<Envelope> sink) {
        Envelope resp = ask(conn, topic(100L + sink.size(), key,
                TopicOp.TOPIC_OP_SUBSCRIBE, null, 0), sink);
        assertThat(resp.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        return resp.getTopicOpResponse().getSubscriptionId();
    }

    /**
     * 排空指定连接出站，收集 {@code TOPIC_MESSAGE} 直到 {@code n} 条（推送
     * 写在订阅者自己的连接上——每连接自取是 harness 的对应义务）。
     *
     * @param conn      订阅者连接
     * @param n         期望条数
     * @param timeoutMs 总时限
     * @return 收集到的推送（到达序）
     */
    private static List<Envelope> collectPushes(ClusterHarness.TestConn conn, int n, long timeoutMs)
            throws InterruptedException {
        List<Envelope> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            Envelope e = conn.pollOutbound();
            if (e != null && e.getType() == MessageType.TOPIC_MESSAGE) {
                out.add(e);
                if (out.size() == n) {
                    return out;
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("推送收取不足：期望 " + n + " 实收 " + out.size());
    }

    /** seq 升序断言辅助。 */
    private static List<Long> seqsOf(List<Envelope> pushes) {
        List<Long> out = new ArrayList<>();
        for (Envelope p : pushes) {
            out.add(p.getTopicMessage().getTopicSeq());
        }
        return out;
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void fanOutMetricsFollowerRejectAndDedupOnSharedHarness() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            var registry = leader.runtime.topicRegistry();
            List<ClusterHarness.TestConn> subs = new ArrayList<>();
            List<Long> subIds = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                ClusterHarness.TestConn conn = h.connect(leader);
                conn.hello(1, 8);
                subs.add(conn);
                subIds.add(subscribe(conn, "ct:news", new ArrayList<>()));
            }
            assertThat(registry.subscriberCount("ct:news")).isEqualTo(5);

            ClusterHarness.TestConn pub = h.connect(leader);
            pub.hello(1, 8);
            for (int i = 1; i <= 10; i++) {
                Envelope resp = ask(pub, topic(200L + i, "ct:news",
                        TopicOp.TOPIC_OP_PUBLISH, utf8("号外" + i), i), new ArrayList<>());
                assertThat(resp.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
                assertThat(resp.getTopicOpResponse().getTopicSeq()).isEqualTo(i);
            }

            for (int s = 0; s < 5; s++) {
                final int idx = s;
                List<Envelope> got = collectPushes(subs.get(idx), 10, 30_000);
                assertThat(seqsOf(got)).containsExactlyElementsOf(
                        java.util.stream.LongStream.rangeClosed(1, 10).boxed().toList());
                for (Envelope p : got) {
                    assertThat(p.getTopicMessage().getSubscriptionId()).isEqualTo(subIds.get(idx));
                    assertThat(p.getTopicMessage().getPublisherSid()).isPositive();
                    assertThat(p.getTopicMessage().getPayloadBytes().toStringUtf8())
                            .startsWith("号外");
                    assertThat(p.getRequestId()).isZero();
                }
            }

            // 指标线（在去重段之前 scrape——publish OK 恒为 10）：
            // publish OK ×10 + subscribe OK ×5 归线可观测（收口于受理点）。
            String scrape = leader.metrics().registry().scrape();
            assertThat(scrape)
                    .contains("openlatch_server_topic_total{op=\"publish\",status=\"OK\"} 10")
                    .contains("openlatch_server_topic_total{op=\"subscribe\",status=\"OK\"} 5");

            // Follower 同型拒绝、零副作用、连接保持（拒绝行不计入 Leader 指标——
            // 由 Follower 节点自身作答）。
            ClusterHarness.Node follower = h.nodes().stream()
                    .filter(n -> n != leader && n.alive()).findFirst().orElseThrow();
            var followerRegistry = follower.runtime.topicRegistry();
            ClusterHarness.TestConn fconn = h.connect(follower);
            fconn.hello(1, 8);
            Envelope subResp = ask(fconn, topic(500L, "ct:f", TopicOp.TOPIC_OP_SUBSCRIBE,
                    null, 0), new ArrayList<>());
            assertThat(subResp.getType()).isEqualTo(MessageType.TOPIC_OP);
            assertThat(subResp.hasTopicOpResponse()).isTrue();
            assertThat(subResp.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
            assertThat(subResp.getTopicOpResponse().getOp()).isEqualTo(TopicOp.TOPIC_OP_SUBSCRIBE);
            Envelope pubResp = ask(fconn, topic(501L, "ct:f", TopicOp.TOPIC_OP_PUBLISH,
                    utf8("x"), 1L), new ArrayList<>());
            assertThat(pubResp.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
            assertThat(pubResp.getTopicOpResponse().getOp()).isEqualTo(TopicOp.TOPIC_OP_PUBLISH);
            assertThat(followerRegistry.topicKeyCount()).isZero();
            assertThat(fconn.channel.isActive()).isTrue();

            // 同 (会话, op_seq) 重发：同 seq 回执、单次扇出（静默窗无第二份）。
            ClusterHarness.TestConn dupSub = h.connect(leader);
            dupSub.hello(1, 8);
            long dupId = subscribe(dupSub, "ct:dup", new ArrayList<>());
            ClusterHarness.TestConn dupPub = h.connect(leader);
            dupPub.hello(1, 8);
            Envelope first = ask(dupPub, topic(600L, "ct:dup", TopicOp.TOPIC_OP_PUBLISH,
                    utf8("一条"), 7L), new ArrayList<>());
            long seq1 = first.getTopicOpResponse().getTopicSeq();
            Envelope retry = ask(dupPub, topic(601L, "ct:dup", TopicOp.TOPIC_OP_PUBLISH,
                    utf8("一条"), 7L), new ArrayList<>());
            assertThat(retry.getTopicOpResponse().getTopicSeq()).isEqualTo(seq1);
            assertThat(seqsOf(collectPushes(dupSub, 1, 10_000))).containsExactly(seq1);
            Thread.sleep(300);
            assertThat(dupSub.pollOutbound()).isNull();

            for (ClusterHarness.TestConn conn : subs) {
                conn.disconnect();
            }
            pub.disconnect();
            fconn.disconnect();
            dupSub.disconnect();
            dupPub.disconnect();
        }
    }

    @Test
    void concurrentPublishersAndGcReclaimOnSharedHarness() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            var registry = leader.runtime.topicRegistry();
            ClusterHarness.TestConn subA = h.connect(leader);
            ClusterHarness.TestConn subB = h.connect(leader);
            subA.hello(1, 8);
            subB.hello(1, 8);
            subscribe(subA, "ct:mix", new ArrayList<>());
            subscribe(subB, "ct:mix", new ArrayList<>());

            int producers = 3;
            int perProducer = 15;
            int total = producers * perProducer;
            CountDownLatch go = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            ExecutorService pool = Executors.newFixedThreadPool(producers, r -> {
                Thread t = new Thread(r, "ct-pub");
                t.setDaemon(true);
                return t;
            });
            for (int p = 0; p < producers; p++) {
                final int pid = p;
                pool.submit(() -> {
                    ClusterHarness.TestConn conn = h.connect(leader);
                    try {
                        conn.hello(1, 8);
                        go.await();
                        for (int i = 0; i < perProducer; i++) {
                            Envelope resp = ask(conn, topic(400L + i, "ct:mix",
                                    TopicOp.TOPIC_OP_PUBLISH,
                                    utf8("p" + pid + "#" + i), i + 1L), new ArrayList<>());
                            assertThat(resp.getTopicOpResponse().getStatus())
                                    .isEqualTo(StatusCode.OK);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
            }
            go.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();

            List<Envelope> gotA = collectPushes(subA, total, 60_000);
            List<Envelope> gotB = collectPushes(subB, total, 60_000);
            // 每订阅严格升序且不重（同键全局 seq，Leader 串行分配）。
            assertThat(seqsOf(gotA)).isSorted().doesNotHaveDuplicates();
            assertThat(seqsOf(gotB)).isSorted().doesNotHaveDuplicates();
            assertThat(seqsOf(gotA)).containsExactlyElementsOf(
                    java.util.stream.LongStream.rangeClosed(1, total).boxed().toList());
            // 不丢不重：两订阅收到的载荷多重集一致且等于投递集。
            List<String> payloadsA = gotA.stream()
                    .map(e -> e.getTopicMessage().getPayloadBytes().toStringUtf8())
                    .sorted().toList();
            List<String> payloadsB = gotB.stream()
                    .map(e -> e.getTopicMessage().getPayloadBytes().toStringUtf8())
                    .sorted().toList();
            assertThat(payloadsB).isEqualTo(payloadsA);
            List<String> expected = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                for (int i = 0; i < perProducer; i++) {
                    expected.add("p" + p + "#" + i);
                }
            }
            expected.sort(java.util.Comparator.naturalOrder());
            assertThat(payloadsA).isEqualTo(expected);

            // GC 段（同 harness，键不相干）：退订与会话死亡回收登记。
            ClusterHarness.TestConn gcA = h.connect(leader);
            ClusterHarness.TestConn gcB = h.connect(leader);
            gcA.hello(1, 8);
            gcB.hello(1, 8);
            subscribe(gcA, "ct:gc", new ArrayList<>());
            subscribe(gcB, "ct:gc", new ArrayList<>());
            assertThat(registry.subscriberCount("ct:gc")).isEqualTo(2);
            ask(gcA, topic(300L, "ct:gc", TopicOp.TOPIC_OP_UNSUBSCRIBE, null, 0),
                    new ArrayList<>());
            assertThat(registry.subscriberCount("ct:gc")).isEqualTo(1);
            gcB.disconnect();
            long deadline = System.currentTimeMillis() + 15_000;
            while (registry.subscriberCount("ct:gc") != 0
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertThat(registry.subscriberCount("ct:gc")).isZero();
            // 无订阅者发布照常受理（事件即发即散，键不复活——ct:gc 已消解）。
            ClusterHarness.TestConn gcPub = h.connect(leader);
            gcPub.hello(1, 8);
            Envelope resp = ask(gcPub, topic(310L, "ct:gc", TopicOp.TOPIC_OP_PUBLISH,
                    utf8("无人收件"), 1L), new ArrayList<>());
            assertThat(resp.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(registry.topicKeys()).doesNotContain("ct:gc");

            subA.disconnect();
            subB.disconnect();
            gcA.disconnect();
            gcPub.disconnect();
        }
    }

    @Test
    void leaderKillClearsRegistryResubscribeContinuesWithRebasedSeq() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node oldLeader = h.leader();
            ClusterHarness.TestConn sub = h.connect(oldLeader);
            sub.hello(1, 8);
            subscribe(sub, "ct:ko", new ArrayList<>());
            ClusterHarness.TestConn pub = h.connect(oldLeader);
            pub.hello(1, 8);
            ask(pub, topic(700L, "ct:ko", TopicOp.TOPIC_OP_PUBLISH, utf8("生前"), 1L),
                    new ArrayList<>());
            assertThat(seqsOf(collectPushes(sub, 1, 10_000))).containsExactly(1L);

            h.stopNode(oldLeader.id);
            h.awaitTrue(h::hasLeader, 30_000, "新主选出");
            ClusterHarness.Node newLeader = h.leader();
            assertThat(newLeader).isNotSameAs(oldLeader);
            // 换主清零：新 Leader 登记表为空（判例 WaitQueue 换主清零）。
            assertThat(newLeader.runtime.topicRegistry().topicKeyCount()).isZero();

            // 模拟 SDK 重挂：新连接、新会话、重发 SUBSCRIBE，随后发布。
            ClusterHarness.TestConn sub2 = h.connect(newLeader);
            sub2.hello(1, 8);
            subscribe(sub2, "ct:ko", new ArrayList<>());
            ClusterHarness.TestConn pub2 = h.connect(newLeader);
            pub2.hello(1, 8);
            Envelope resp = ask(pub2, topic(701L, "ct:ko", TopicOp.TOPIC_OP_PUBLISH,
                    utf8("身后"), 1L), new ArrayList<>());
            assertThat(resp.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            // term 基线重置：新 Leader 上该键首条 seq==1。
            assertThat(resp.getTopicOpResponse().getTopicSeq()).isEqualTo(1L);
            assertThat(seqsOf(collectPushes(sub2, 1, 10_000))).containsExactly(1L);
            Thread.sleep(300);
            assertThat(sub2.pollOutbound()).isNull(); // 无换主窗补投
            sub2.disconnect();
            pub2.disconnect();
        }
    }
}
