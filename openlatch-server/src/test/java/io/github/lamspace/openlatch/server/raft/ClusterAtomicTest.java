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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集群路径原子变量端到端：多连接并发 CAS 矩阵（Σ成功 == Δ值、版本 == 写次数、
 * 副本 digest 归一）、连接断开值不变（不绑定归属）、Leader 杀停重启后
 * 值与版本不丢不漂且续写照常（提交经多数派存续）。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClusterAtomicTest {

    /** 构造原子操作信封（v4）。 */
    private static Envelope atomic(long rid, String key, AtomicOp op, long operand,
            long expected, long expectedVersion, long initial, long opSeq) {
        return Envelope.newBuilder()
                .setProtocolVersion(4)
                .setType(MessageType.ATOMIC_OP)
                .setRequestId(rid)
                .setAtomicOpRequest(AtomicOpRequest.newBuilder()
                        .setKey(key).setOp(op).setLockType(LockType.LOCK_TYPE_ATOMIC_LONG)
                        .setOperand(operand).setExpected(expected)
                        .setExpectedVersion(expectedVersion).setInitialValue(initial)
                        .setOpSeq(opSeq))
                .build();
    }

    /** 同步请求的应答四元组断言辅助：状态码。 */
    private static StatusCode statusOf(Envelope resp) {
        return resp.getAtomicOpResponse().getStatus();
    }

    @Test
    void concurrentCasMatrixAcrossReplicatedPath() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            int writers = 4;
            int perWriter = 12;
            ExecutorService pool = Executors.newFixedThreadPool(writers);
            CountDownLatch ready = new CountDownLatch(writers);
            CountDownLatch go = new CountDownLatch(1);
            AtomicInteger successes = new AtomicInteger();
            long[] seqSeed = new long[writers];
            for (int t = 0; t < writers; t++) {
                final int idx = t;
                seqSeed[idx] = 100L + idx;
                pool.submit(() -> {
                    ClusterHarness.TestConn conn = h.connect(leader);
                    try {
                        conn.hello(1, 4);
                        ready.countDown();
                        go.await();
                        long rid = 10L * (idx + 1);
                        for (int i = 0; i < perWriter; i++) {
                            // 值 CAS 加一：预期=当前则重试；本表内成功计数即线性化写数。
                            while (true) {
                                Envelope g = conn.request(atomic(rid++, "cm",
                                        AtomicOp.ATOMIC_GET, 0, 0, 0, 0, 0));
                                long cur = g.getAtomicOpResponse().getValue();
                                Envelope c = conn.request(atomic(rid++, "cm",
                                        AtomicOp.ATOMIC_CAS, cur + 1, cur, 0, 0,
                                        seqSeed[idx]++));
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
            assertThat(ready.await(20, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(90, TimeUnit.SECONDS)).isTrue();

            int total = writers * perWriter;
            assertThat(successes.get()).isEqualTo(total);
            ClusterHarness.TestConn check = h.connect(h.leader());
            {
                check.hello(1, 4);
                Envelope g = check.request(atomic(900, "cm", AtomicOp.ATOMIC_GET,
                        0, 0, 0, 0, 0));
                assertThat(statusOf(g)).isEqualTo(StatusCode.OK);
                assertThat(g.getAtomicOpResponse().getValue()).isEqualTo(total);
                // 版本恰等于落值写次数（CAS 未命中不推）：无间隙单调。
                assertThat(g.getAtomicOpResponse().getVersion()).isEqualTo(total);
            }
            check.disconnect();
            h.awaitTrue(h::aliveAgreeWithLeader, 20_000, "CAS 矩阵后副本一致");
        }
    }

    @Test
    void disconnectLeavesValueUntouchedForSurvivors() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.TestConn writer = h.connect(h.leader());
            writer.hello(1, 4);
            Envelope w = writer.request(atomic(2, "dv", AtomicOp.ATOMIC_ADD,
                    42, 0, 0, 0, 1));
            assertThat(statusOf(w)).isEqualTo(StatusCode.OK);
            writer.disconnect(); // 会话终结（SESSION_CLOSE 传播 + 引擎清理）

            ClusterHarness.TestConn reader = h.connect(h.leader());
            {
                reader.hello(1, 4);
                Envelope g = reader.request(atomic(3, "dv", AtomicOp.ATOMIC_GET,
                        0, 0, 0, 0, 0));
                assertThat(g.getAtomicOpResponse().getValue()).isEqualTo(42);
                assertThat(g.getAtomicOpResponse().getVersion()).isEqualTo(1);
                Envelope n = reader.request(atomic(4, "dv", AtomicOp.ATOMIC_ADD,
                        1, 0, 0, 0, 9));
                assertThat(n.getAtomicOpResponse().getValue()).isEqualTo(43);
                reader.disconnect();
            }
        }
    }

    @Test
    void committedValueSurvivesLeaderKillAndRestart() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 800)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c0 = h.connect(leader);
            {
                c0.hello(1, 4);
                assertThat(statusOf(c0.request(atomic(2, "sk", AtomicOp.ATOMIC_SET,
                        5, 0, 0, 0, 1)))).isEqualTo(StatusCode.OK);
                c0.disconnect();
                h.awaitTrue(h::aliveAgreeWithLeader, 15_000, "杀前副本一致");
            }
            h.stopNode(leader.id);
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l.id != leader.id;
            }, 30_000, "新主选出");
            // 新主读数：提交值与版本不丢不漂（多数派已承载）。
            ClusterHarness.TestConn c = h.connect(h.leader());
            {
                c.hello(1, 4);
                Envelope g = c.request(atomic(2, "sk", AtomicOp.ATOMIC_GET,
                        0, 0, 0, 0, 0));
                assertThat(g.getAtomicOpResponse().getValue()).isEqualTo(5);
                assertThat(g.getAtomicOpResponse().getVersion()).isEqualTo(1);
                // 换主后续写照常（新会话新序号，去重槽按新槽位推进）。
                Envelope w = c.request(atomic(3, "sk", AtomicOp.ATOMIC_ADD,
                        7, 0, 0, 0, 1));
                assertThat(w.getAtomicOpResponse().getValue()).isEqualTo(12);
                assertThat(w.getAtomicOpResponse().getVersion()).isEqualTo(2);
                c.disconnect();
            }
            h.restartNode(leader.id);
            h.awaitTrue(h::aliveAgreeWithLeader, 30_000, "复活后三副本一致");
        }
    }
}
