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

import io.github.lamspace.openlatch.protocol.AtomicOp;
import io.github.lamspace.openlatch.protocol.AtomicOpRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集群路径有值引用端到端（v6）：多连接并发版本 CAS 矩阵（Σ成功 == Δ版本、
 * 终值属候选集、副本 digest 归一）、超限载荷零日志条目（入口钳制的复制面
 * 可执行证明）、Leader 杀停重启后载荷不丢不漂且续写照常（提交经多数派
 * 存续）。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClusterAtomicRefTest {

    /** 构造引用形态原子操作信封（v6，载荷数组 null=不携带）。 */
    private static Envelope ref(long rid, String key, AtomicOp op, byte[] operand,
            byte[] expected, long expectedVersion, byte[] initial, long opSeq) {
        AtomicOpRequest.Builder rb = AtomicOpRequest.newBuilder()
                .setKey(key).setOp(op).setLockType(LockType.LOCK_TYPE_ATOMIC_REFERENCE)
                .setExpectedVersion(expectedVersion).setOpSeq(opSeq);
        if (operand != null) {
            rb.setOperandBytes(com.google.protobuf.ByteString.copyFrom(operand));
        }
        if (expected != null) {
            rb.setExpectedBytes(com.google.protobuf.ByteString.copyFrom(expected));
        }
        if (initial != null) {
            rb.setInitialBytes(com.google.protobuf.ByteString.copyFrom(initial));
        }
        return Envelope.newBuilder().setProtocolVersion(6).setType(MessageType.ATOMIC_OP)
                .setRequestId(rid).setAtomicOpRequest(rb).build();
    }

    /** UTF-8 信封载荷简写。 */
    private static byte[] s(String v) {
        return v.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    @Test
    void concurrentStampedCasMatrixAcrossReplicatedPath() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            int writers = 4;
            int perWriter = 10;
            ExecutorService pool = Executors.newFixedThreadPool(writers);
            CountDownLatch ready = new CountDownLatch(writers);
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger successes = new AtomicInteger();
            Set<String> candidates = java.util.concurrent.ConcurrentHashMap
                    .newKeySet();
            for (int t = 0; t < writers; t++) {
                final int idx = t;
                pool.submit(() -> {
                    ClusterHarness.TestConn conn = h.connect(leader);
                    try {
                        conn.hello(1, 6);
                        ready.countDown();
                        go.await();
                        long rid = 10L * (idx + 1);
                        long seq = 100L + idx;
                        for (int i = 0; i < perWriter; i++) {
                            String cand = "c" + idx + "-" + i;
                            candidates.add(cand);
                            // 版本 CAS 写入：读 (value,version) → 双符才落，未命中重读重试。
                            // 空条目首轮（ver=0）以 expectedVersion=0 的退化形态建立初值
                            // （值判据 expected=null 仍保证恰一次建立）。
                            while (true) {
                                Envelope g = conn.request(ref(rid++, "rcm",
                                        AtomicOp.ATOMIC_GET, null, null, 0, null, 0));
                                byte[] cur = g.getAtomicOpResponse().hasValueBytes()
                                        ? g.getAtomicOpResponse().getValueBytes().toByteArray()
                                        : null;
                                long ver = g.getAtomicOpResponse().getVersion();
                                Envelope c = conn.request(ref(rid++, "rcm",
                                        AtomicOp.ATOMIC_CAS_STAMPED, s(cand), cur,
                                        ver, null, seq++));
                                if (c.getAtomicOpResponse().getApplied()) {
                                    successes.incrementAndGet();
                                    break;
                                }
                            }
                        }
                    } catch (Exception e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        conn.disconnect();
                    }
                });
            }
            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();

            int total = writers * perWriter;
            assertThat(successes.get()).isEqualTo(total);
            ClusterHarness.TestConn check = h.connect(h.leader());
            check.hello(1, 6);
            Envelope g = check.request(ref(900, "rcm", AtomicOp.ATOMIC_GET,
                    null, null, 0, null, 0));
            // Σ成功 CAS == Δversion：版本恰等于落值写次数（未命中不推）。
            assertThat(g.getAtomicOpResponse().getVersion()).isEqualTo(total);
            assertThat(new String(g.getAtomicOpResponse().getValueBytes().toByteArray(),
                    java.nio.charset.StandardCharsets.UTF_8)).isIn(candidates);
            check.disconnect();
            h.awaitTrue(h::aliveAgreeWithLeader, 30_000, "引用 CAS 矩阵后副本一致");
        }
    }

    @Test
    void oversizePayloadRejectedWithoutLogGrowth() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(1, 6);
            assertThat(c.request(ref(2, "cl", AtomicOp.ATOMIC_SET,
                    s("ok"), null, 0, null, 1)).getAtomicOpResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            h.awaitTrue(h::aliveAgreeWithLeader, 15_000, "基线写入收敛");
            var before = h.nodes().stream()
                    .map(ClusterHarness.Node::lastApplied).toList();

            // 5KB 超默认钳制：三节点视角日志位点零推进（超限命令不入复制面）。
            assertThat(c.request(ref(3, "cl", AtomicOp.ATOMIC_SET,
                    new byte[5_000], null, 0, null, 2)).getAtomicOpResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST);
            var after = h.nodes().stream()
                    .map(ClusterHarness.Node::lastApplied).toList();
            assertThat(after).isEqualTo(before);
            // 条目零扰动。
            Envelope g = c.request(ref(4, "cl", AtomicOp.ATOMIC_GET, null, null, 0, null, 0));
            assertThat(new String(g.getAtomicOpResponse().getValueBytes().toByteArray(),
                    java.nio.charset.StandardCharsets.UTF_8)).isEqualTo("ok");
            assertThat(g.getAtomicOpResponse().getVersion()).isEqualTo(1);
            c.disconnect();
        }
    }

    @Test
    void committedPayloadSurvivesLeaderKillAndRestart() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 800)) {
            ClusterHarness.Node leader = h.leader();
            byte[] payload = new byte[1024];
            for (int i = 0; i < payload.length; i++) {
                payload[i] = (byte) (i * 13 + 5);
            }
            ClusterHarness.TestConn c0 = h.connect(leader);
            c0.hello(1, 6);
            assertThat(c0.request(ref(2, "pk", AtomicOp.ATOMIC_SET,
                    payload, null, 0, null, 1)).getAtomicOpResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            c0.disconnect();
            h.awaitTrue(h::aliveAgreeWithLeader, 15_000, "杀前副本一致");

            h.stopNode(leader.id);
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l.id != leader.id;
            }, 30_000, "新主选出");
            ClusterHarness.TestConn c = h.connect(h.leader());
            c.hello(1, 6);
            Envelope g = c.request(ref(2, "pk", AtomicOp.ATOMIC_GET, null, null, 0, null, 0));
            // 载荷字节级不丢不漂（多数派已承载）。
            assertThat(g.getAtomicOpResponse().getValueBytes().toByteArray())
                    .containsExactly(payload);
            assertThat(g.getAtomicOpResponse().getVersion()).isEqualTo(1);
            // 换主后续写照常。
            Envelope w = c.request(ref(3, "pk", AtomicOp.ATOMIC_CAS,
                    s("after"), payload, 0, null, 1));
            assertThat(w.getAtomicOpResponse().getApplied()).isTrue();
            assertThat(w.getAtomicOpResponse().getVersion()).isEqualTo(2);
            c.disconnect();
            h.restartNode(leader.id);
            h.awaitTrue(h::aliveAgreeWithLeader, 30_000, "复活后三副本一致");
        }
    }
}
