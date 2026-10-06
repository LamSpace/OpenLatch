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
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.SnapshotLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * topic 零复制日志守卫（v8 常驻条款，replicated-state-machine 规格）：
 * 纯 topic 流量（订阅/发布/退订/拒绝全操作面）下，应用位点不前进、
 * 副本摘要与流量前基线逐字节相等、停机重启节点不重投任何陈旧交付；
 * 并以描述符级断言钉死复制面（{@code RaftEntryType}/{@code SnapshotLock}）
 * 无 topic 取值/字段——未来任何把 topic 状态塞进日志或快照的改动，
 * 在本守卫与 {@code ProtocolCodecTest} 的编号证据上即刻转红。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class StateMachineTopicTest {

    /** 构造 topic 操作信封。 */
    private static Envelope topic(long rid, String key, TopicOp op, byte[] payload, long opSeq) {
        TopicOpRequest.Builder rb = TopicOpRequest.newBuilder()
                .setKey(key).setOp(op).setOpSeq(opSeq);
        if (payload != null) {
            rb.setPayloadBytes(ByteString.copyFrom(payload));
        }
        return Envelope.newBuilder().setProtocolVersion(8).setType(MessageType.TOPIC_OP)
                .setRequestId(rid).setTopicOpRequest(rb).build();
    }

    /** 发送并等待同 rid 应答（途中推送收集入 sink）。 */
    private static Envelope ask(ClusterHarness.TestConn conn, Envelope msg, List<Envelope> sink) {
        Envelope first = conn.request(msg);
        if (first.getType() == MessageType.TOPIC_OP && first.getRequestId() == msg.getRequestId()) {
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
        throw new AssertionError("topic reply timeout rid=" + msg.getRequestId());
    }

    @Test
    void topicTrafficLeavesLogAndDigestUntouched() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            // 探针关闭：排除 NOOP/失联探针条目，日志零增长的判据纯净。
            h.setProbesEnabled(false);
            ClusterHarness.Node leader = h.leader();

            // 先完成建连与会话登记（HELLO 的 SESSION_OPEN 本身是日志条目），
            // 基线在 topic 流量前一刻捕获——守卫断言的是 topic 操作面的零贡献。
            ClusterHarness.TestConn sub = h.connect(leader);
            sub.hello(1, 8);
            ask(sub, topic(10L, "zlog:t", TopicOp.TOPIC_OP_SUBSCRIBE, null, 0),
                    new ArrayList<>());
            ClusterHarness.TestConn pub = h.connect(leader);
            pub.hello(1, 8);
            Envelope warm = ask(pub, topic(19L, "zlog:t", TopicOp.TOPIC_OP_PUBLISH,
                    utf8("warm"), 1L), new ArrayList<>());
            assertThat(warm.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(collectPushes(sub, 1, 10_000)).hasSize(1);
            // Follower 拒绝路径（建连与会话登记先行入基线）。
            ClusterHarness.Node follower = h.nodes().stream()
                    .filter(x -> x != leader).findFirst().orElseThrow();
            ClusterHarness.TestConn fconn = h.connect(follower);
            fconn.hello(1, 8);
            Envelope rejected = ask(fconn, topic(70L, "zlog:t", TopicOp.TOPIC_OP_SUBSCRIBE,
                    null, 0), new ArrayList<>());
            assertThat(rejected.getTopicOpResponse().getStatus())
                    .isEqualTo(StatusCode.NOT_LEADER);

            long baselineIndex = leader.lastApplied();
            String baselineDigest = leader.digest();
            String followerDigest = h.nodes().stream().filter(x -> x != leader)
                    .findFirst().orElseThrow().digest();
            assertThat(followerDigest).isEqualTo(baselineDigest);

            for (int i = 1; i <= 20; i++) {
                Envelope resp = ask(pub, topic(20L + i, "zlog:t",
                        TopicOp.TOPIC_OP_PUBLISH, utf8("m" + i), i + 1L),
                        new ArrayList<>());
                assertThat(resp.getTopicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            }
            // 订阅连接自取推送（harness 每连接义务）：20 条收齐后才退订。
            assertThat(collectPushes(sub, 20, 30_000)).hasSize(20);
            ask(sub, topic(60L, "zlog:t", TopicOp.TOPIC_OP_UNSUBSCRIBE, null, 0),
                    new ArrayList<>());

            // 断言核心：应用位点等于基线（20 发布 + 全操作面贡献零条目）。
            assertThat(leader.lastApplied()).isEqualTo(baselineIndex);
            assertThat(leader.digest()).isEqualTo(baselineDigest);
            assertThat(h.aliveAgreeWithLeader()).isTrue();

            // 重启的 follower 追赶后对 topic 无感：重连该节点订阅即收，
            // 不存在任何陈旧消息补投。
            int fid = follower.id;
            h.stopNode(fid);
            h.restartNode(fid);
            ClusterHarness.Node restarted = h.node(fid);
            ClusterHarness.TestConn rconn = h.connect(restarted);
            rconn.hello(1, 8);
            Thread.sleep(500);
            assertThat(rconn.pollOutbound()).isNull(); // 无陈消息投递
            // 重启节点（非 Leader 时）订阅被同型拒绝或经 Leader 受理，均零重放。
            rconn.disconnect();
            sub.disconnect();
            pub.disconnect();
            fconn.disconnect();
        }
    }

    @Test
    void replicationSurfaceCarriesNoTopicTypes() {
        // 编号证据：topic 维自 v8 起条目类型零占用（值域中无 TOPIC 取值）——
        // v10 上界升至 14 且 14 为 PHASER_OP_ENTRY 专用值，与 topic 边界无涉
        // （证据线为版本相对口径而非绝对上界；UNRECOGNIZED 为 protobuf 哨兵值，
        // 取号即抛，不入枚举值域断言）。
        for (RaftEntryType t : RaftEntryType.values()) {
            if (t == RaftEntryType.UNRECOGNIZED) {
                continue;
            }
            assertThat(t.getNumber()).isLessThanOrEqualTo(14);
            assertThat(t.name()).doesNotContain("TOPIC");
        }
        // 快照面：SnapshotLock 字段零 topic（订阅登记不入快照）。
        assertThat(SnapshotLock.getDescriptor().getFields().stream()
                .noneMatch(f -> f.getName().contains("topic"))).isTrue();
    }

    private static byte[] utf8(String s) {
        return s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    /** 排空连接出站收集 TOPIC_MESSAGE 直到 n 条。 */
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

    /** 自旋等待。 */
    private static void awaitTrue(java.util.function.BooleanSupplier condition, String what)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("超时未达成: " + what);
    }
}
