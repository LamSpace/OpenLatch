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

import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.PhaserOp;
import io.github.lamspace.openlatch.protocol.PhaserOpRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 相位器集群 E2E（v10）：多方合拢矩阵与返回值保真、中途注册入当相位应到集、
 * {@code ARRIVE_AND_DEREGISTER} 提前合拢、同 request_id 重发单到场、会话散尽
 * 死亡摘除不空转（harness 优雅关会话形态；kill 进程形态由演练负载段承载）、
 * Follower 七操作词同型零副作用、换主账簿存续 + 等待重挂无损耗（有界超时 +
 * eventually 续醒，v9 D12/W10 教训纪律）、并发多相位不丢不重。
 * 判例 {@code ClusterBarrierTest}/{@code ClusterQueueTest}；共享 harness 单类
 * 一个（W2 fork 端口纪律——集群重测试合并压 harness 数）。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClusterPhaserTest {

    /** 构造 PHASER 信封。 */
    private static Envelope phaser(long rid, PhaserOp op, String key, int parties,
            Long expected, long awaitRid) {
        PhaserOpRequest.Builder rb = PhaserOpRequest.newBuilder()
                .setKey(key).setOp(op).setParties(parties).setAwaitRequestId(awaitRid);
        if (expected != null) {
            rb.setExpectedPhase(expected);
        }
        return Envelope.newBuilder().setProtocolVersion(10).setType(MessageType.PHASER_OP)
                .setRequestId(rid).setPhaserOpRequest(rb).build();
    }

    /**
     * 请求并取回同 rid 同型应答——过滤先于应答抵达的 AWAIT_NOTIFY 串扰
     * （判例队列/屏障 harness 自筛纪律）。
     */
    private static Envelope requestReply(ClusterHarness.TestConn conn, Envelope msg) {
        Envelope first = conn.request(msg);
        long wantRid = msg.getRequestId();
        if (first.getType() == msg.getType() && first.getRequestId() == wantRid) {
            return first;
        }
        for (int i = 0; i < 16; i++) {
            Envelope next = conn.awaitOutbound(10_000);
            if (next.getType() == msg.getType() && next.getRequestId() == wantRid) {
                return next;
            }
        }
        throw new AssertionError("reply for rid " + wantRid + " not found");
    }

    private static StatusCode status(Envelope resp) {
        return resp.getPhaserOpResponse().getStatus();
    }

    /** 读到一条 AWAIT_NOTIFY（或读界到点即返——重发环自兜底，判例队列）。 */
    private static void drainUntilNotifyOrTimeout(ClusterHarness.TestConn conn) {
        try {
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                Envelope n = conn.awaitOutbound(1_000);
                if (n != null && n.getType() == MessageType.AWAIT_NOTIFY) {
                    return;
                }
            }
        } catch (AssertionError quiet) {
            // 读界到点：交给重发环。
        }
    }

    @Test
    void tripMatrixDynamicMembershipAndEarlyDeregister() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            ClusterHarness.TestConn b = h.connect(leader);
            ClusterHarness.TestConn c = h.connect(leader);
            a.hello(1, 10);
            b.hello(2, 10);
            c.hello(3, 10);

            // 注册：a 两方 + b 一方 = 3；a、b 各到场挂起（1/3、2/3）。
            assertThat(requestReply(a, phaser(10, PhaserOp.PHASER_OP_REGISTER, "p", 2, null, 0))
                    .getPhaserOpResponse().getPhase()).isZero();
            assertThat(status(requestReply(b, phaser(11, PhaserOp.PHASER_OP_REGISTER,
                    "p", 1, null, 0)))).isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(a, phaser(12,
                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0))))
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(status(requestReply(b, phaser(13,
                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0))))
                    .isEqualTo(StatusCode.QUEUED);
            // 中途注册 c 入当相位应到集合（3→4），其到场亦挂起（3/4）。
            assertThat(status(requestReply(c, phaser(14, PhaserOp.PHASER_OP_REGISTER,
                    "p", 1, null, 0)))).isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(c, phaser(15,
                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0))))
                    .isEqualTo(StatusCode.QUEUED);
            // a 以新请求再到场：4/4 合拢——a 直答 OK（到场相位 0），三方等待收唤醒。
            assertThat(status(requestReply(a, phaser(16, PhaserOp.PHASER_OP_ARRIVE,
                    "p", 0, null, 0)))).isEqualTo(StatusCode.OK);
            // b、c 各收其 rid 的唤醒推送；a 的 notify 可能已被其 rid=16 应答的
            // requestReply 跳环吞并（唤醒先于应答派发，判例屏障）——三方了结重发
            // 以同 rid 命中换代窗口，终态回显 prevPhase=0、不双计。
            Envelope notifyB = b.awaitOutbound(10_000);
            assertThat(notifyB.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(notifyB.getAwaitNotify().getRequestIdRef()).isEqualTo(13);
            Envelope notifyC = c.awaitOutbound(10_000);
            assertThat(notifyC.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(notifyC.getAwaitNotify().getRequestIdRef()).isEqualTo(15);
            assertThat(status(requestReply(a, phaser(12,
                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0))))
                    .isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(b, phaser(13,
                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0))))
                    .isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(c, phaser(15,
                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0))))
                    .isEqualTo(StatusCode.OK);
            Envelope q = requestReply(c, phaser(17, PhaserOp.PHASER_OP_QUERY,
                    "p", 0, null, 0));
            assertThat(q.getPhaserOpResponse().getPhase()).isEqualTo(1);
            assertThat(q.getPhaserOpResponse().getRegistered()).isEqualTo(4);
            assertThat(q.getPhaserOpResponse().getArrived()).isZero();

            // 提前合拢（A_D 缩小应到）：b 到场+离场（4→3、arrived 1）；a、c 到场
            // 计数至 3≥3 合拢 1→2——a 等待被唤醒，全程无人被缺席者饿死。
            assertThat(status(requestReply(b, phaser(20,
                    PhaserOp.PHASER_OP_ARRIVE_AND_DEREGISTER, "p", 0, null, 0))))
                    .isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(a, phaser(21,
                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0))))
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(status(requestReply(c, phaser(22, PhaserOp.PHASER_OP_ARRIVE,
                    "p", 0, null, 0)))).isEqualTo(StatusCode.OK); // 2/3
            assertThat(status(requestReply(c, phaser(23, PhaserOp.PHASER_OP_ARRIVE,
                    "p", 0, null, 0)))).isEqualTo(StatusCode.OK); // 3/3 合拢
            Envelope notify2 = a.awaitOutbound(10_000);
            assertThat(notify2.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(status(requestReply(a, phaser(21,
                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0))))
                    .isEqualTo(StatusCode.OK);
            Envelope q2 = requestReply(b, phaser(24, PhaserOp.PHASER_OP_QUERY,
                    "p", 0, null, 0));
            assertThat(q2.getPhaserOpResponse().getPhase()).isEqualTo(2);
            assertThat(q2.getPhaserOpResponse().getRegistered()).isEqualTo(3);
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "副本一致");
        }
    }

    @Test
    void arriveReplaySingleCountSameRid() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.TestConn a = h.connect(h.leader());
            a.hello(1, 10);
            assertThat(status(requestReply(a, phaser(10, PhaserOp.PHASER_OP_REGISTER,
                    "rp", 2, null, 0)))).isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(a, phaser(11, PhaserOp.PHASER_OP_ARRIVE,
                    "rp", 0, null, 0)))).isEqualTo(StatusCode.OK);
            // 应答丢失模拟：同 rid 重发——不双计（QUERY 恒 arrived=1）。
            assertThat(status(requestReply(a, phaser(11, PhaserOp.PHASER_OP_ARRIVE,
                    "rp", 0, null, 0)))).isEqualTo(StatusCode.OK);
            Envelope q = requestReply(a, phaser(12, PhaserOp.PHASER_OP_QUERY, "rp", 0, null, 0));
            assertThat(q.getPhaserOpResponse().getArrived()).isEqualTo(1);
            // 注册重发同理：同 rid 不双加配额。
            assertThat(status(requestReply(a, phaser(13, PhaserOp.PHASER_OP_REGISTER,
                    "rp", 1, null, 0)))).isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(a, phaser(13, PhaserOp.PHASER_OP_REGISTER,
                    "rp", 1, null, 0)))).isEqualTo(StatusCode.OK);
            Envelope q2 = requestReply(a, phaser(14, PhaserOp.PHASER_OP_QUERY, "rp", 0, null, 0));
            assertThat(q2.getPhaserOpResponse().getRegistered()).isEqualTo(3);
        }
    }

    @Test
    void deathRemovalKeepsPhaseProgressing() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            ClusterHarness.TestConn b = h.connect(leader);
            ClusterHarness.TestConn c = h.connect(leader);
            ClusterHarness.TestConn ghost = h.connect(leader);
            a.hello(1, 10);
            b.hello(2, 10);
            c.hello(3, 10);
            ghost.hello(4, 10);
            // 四方注册；ghost 到场后其会话消亡——配额摘除（4→3）、槽摘但计数
            // 保留（已到场事实不撤销）。
            for (ClusterHarness.TestConn conn : java.util.List.of(a, b, c, ghost)) {
                assertThat(status(requestReply(conn, phaser(10, PhaserOp.PHASER_OP_REGISTER,
                        "d", 1, null, 0)))).isEqualTo(StatusCode.OK);
            }
            assertThat(status(requestReply(ghost, phaser(11, PhaserOp.PHASER_OP_ARRIVE,
                    "d", 0, null, 0)))).isEqualTo(StatusCode.OK);
            ghost.disconnect(); // SESSION_CLOSE 传播（隐式摘除配额，arrived 保持 1）
            // b 挂起（2/3）；c 到场 3/3 合拢——若无摘除需四方、b 永等。
            assertThat(status(requestReply(b, phaser(12,
                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "d", 0, null, 0))))
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(status(requestReply(c, phaser(13, PhaserOp.PHASER_OP_ARRIVE,
                    "d", 0, null, 0)))).isEqualTo(StatusCode.OK);
            Envelope notify = b.awaitOutbound(10_000);
            assertThat(notify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            Envelope reB = requestReply(b, phaser(12, PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT,
                    "d", 0, null, 0));
            assertThat(status(reB)).isEqualTo(StatusCode.OK);
            assertThat(reB.getPhaserOpResponse().getPhase()).isZero(); // 其到场相位
            Envelope q = requestReply(c, phaser(15, PhaserOp.PHASER_OP_QUERY,
                    "d", 0, null, 0));
            assertThat(q.getPhaserOpResponse().getPhase()).isEqualTo(1);
            assertThat(q.getPhaserOpResponse().getRegistered()).isEqualTo(3); // 死者配额已摘
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "摘除收敛一致");
        }
    }

    @Test
    void followerRejectsEveryOpZeroSideEffects() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node follower = null;
            for (ClusterHarness.Node n : h.nodes()) {
                if (n != leader) {
                    follower = n;
                    break;
                }
            }
            assertThat(follower).isNotNull();
            ClusterHarness.TestConn setup = h.connect(leader);
            setup.hello(1, 10);
            assertThat(status(requestReply(setup, phaser(10, PhaserOp.PHASER_OP_REGISTER,
                    "f", 2, null, 0)))).isEqualTo(StatusCode.OK);
            ClusterHarness.TestConn f = h.connect(follower);
            f.hello(2, 10);
            for (PhaserOp op : new PhaserOp[] {PhaserOp.PHASER_OP_REGISTER,
                    PhaserOp.PHASER_OP_ARRIVE, PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT,
                    PhaserOp.PHASER_OP_ARRIVE_AND_DEREGISTER,
                    PhaserOp.PHASER_OP_AWAIT_ADVANCE, PhaserOp.PHASER_OP_CANCEL,
                    PhaserOp.PHASER_OP_QUERY}) {
                Envelope resp = f.request(phaser(20, op, "f", 1, 0L, 5));
                assertThat(status(resp)).as("%s 于 Follower", op)
                        .isEqualTo(StatusCode.NOT_LEADER);
                assertThat(resp.getPhaserOpResponse().getOp()).isEqualTo(op);
            }
            // 零副作用：Leader 账簿纹丝不动。
            Envelope q = requestReply(setup, phaser(30, PhaserOp.PHASER_OP_QUERY,
                    "f", 0, null, 0));
            assertThat(q.getPhaserOpResponse().getRegistered()).isEqualTo(2);
            assertThat(q.getPhaserOpResponse().getArrived()).isZero();
        }
    }

    @Test
    void ledgerSurvivesLeaderFailoverAndAwaitRehangSelfHeals() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 800L)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 10);
            assertThat(status(requestReply(a, phaser(10, PhaserOp.PHASER_OP_REGISTER,
                    "fo", 2, null, 0)))).isEqualTo(StatusCode.OK);
            // 合拢到相位 1（两发到场，均已提交多数派）。
            assertThat(status(requestReply(a, phaser(11, PhaserOp.PHASER_OP_ARRIVE,
                    "fo", 0, null, 0)))).isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(a, phaser(12, PhaserOp.PHASER_OP_ARRIVE,
                    "fo", 0, null, 0)))).isEqualTo(StatusCode.OK);

            h.stopNode(leader.id);
            h.awaitTrue(() -> h.leader() != null && h.leader() != leader, 20_000, "重选主");
            ClusterHarness.TestConn onNew = h.connect(h.leader());
            onNew.hello(20, 10);
            // 账簿为复制状态：相位/配额存续、不回退。
            Envelope q = requestReply(onNew, phaser(21, PhaserOp.PHASER_OP_QUERY,
                    "fo", 0, null, 0));
            assertThat(q.getPhaserOpResponse().getPhase()).isEqualTo(1);
            assertThat(q.getPhaserOpResponse().getRegistered()).isEqualTo(2);
            // 已越相位的迟到等待即刻了结（无损耗——对照条件 signal 丢失窗）。
            Envelope late = requestReply(onNew, phaser(22, PhaserOp.PHASER_OP_AWAIT_ADVANCE,
                    "fo", 0, 0L, 0));
            assertThat(status(late)).isEqualTo(StatusCode.OK);
            assertThat(late.getPhaserOpResponse().getPhase()).isEqualTo(1);
            // 未越相位挂起（专用等待连接）→ 新 Leader 上两发到场合拢唤醒。
            ClusterHarness.TestConn w = h.connect(h.leader());
            w.hello(30, 10);
            assertThat(status(requestReply(w, phaser(23,
                    PhaserOp.PHASER_OP_AWAIT_ADVANCE, "fo", 0, 1L, 0))))
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(status(requestReply(onNew, phaser(24, PhaserOp.PHASER_OP_ARRIVE,
                    "fo", 0, null, 0)))).isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(onNew, phaser(25, PhaserOp.PHASER_OP_ARRIVE,
                    "fo", 0, null, 0)))).isEqualTo(StatusCode.OK); // 2/2 合拢 1→2
            Envelope wn = w.awaitOutbound(10_000);
            assertThat(wn.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(wn.getAwaitNotify().getRequestIdRef()).isEqualTo(23);
            Envelope wOk = requestReply(w, phaser(23, PhaserOp.PHASER_OP_AWAIT_ADVANCE,
                    "fo", 0, 1L, 0));
            assertThat(status(wOk)).isEqualTo(StatusCode.OK);
            assertThat(wOk.getPhaserOpResponse().getPhase()).isEqualTo(2);
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "failover 后副本一致");
        }
    }

    @Test
    void concurrentPartiesAdvancePhasesNoLossNoDup() throws Exception {
        final int parties = 4;
        final int phases = 3;
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn seed = h.connect(leader);
            seed.hello(9, 10);
            assertThat(status(requestReply(seed, phaser(10, PhaserOp.PHASER_OP_REGISTER,
                    "cc", parties, null, 0)))).isEqualTo(StatusCode.OK);
            ExecutorService pool = Executors.newFixedThreadPool(parties);
            CountDownLatch done = new CountDownLatch(parties);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicInteger rids = new AtomicInteger(100);
            for (int p = 0; p < parties; p++) {
                pool.submit(() -> {
                    ClusterHarness.TestConn conn = h.connect(leader);
                    try {
                        conn.hello(1000 + rids.get(), 10);
                        for (int ph = 0; ph < phases; ph++) {
                            long rid = rids.incrementAndGet();
                            Envelope resp = requestReply(conn, phaser(rid,
                                    PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "cc", 0, null, 0));
                            // 合拢恰由本方触达时直答；否则挂起→同 rid 重发了结，
                            // 容忍迟到的他相位唤醒推送（重发环自然收敛，判例队列）。
                            while (status(resp) != StatusCode.OK) {
                                assertThat(status(resp)).isEqualTo(StatusCode.QUEUED);
                                drainUntilNotifyOrTimeout(conn);
                                resp = requestReply(conn, phaser(rid,
                                        PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "cc", 0, null, 0));
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        conn.disconnect();
                        done.countDown();
                    }
                });
            }
            assertThat(done.await(150, TimeUnit.SECONDS)).isTrue();
            pool.shutdownNow();
            assertThat(failure.get()).isNull();
            // parties×phases 恰好：每相位每 party 计一次到场，无丢无重。
            Envelope q = requestReply(seed, phaser(900, PhaserOp.PHASER_OP_QUERY,
                    "cc", 0, null, 0));
            assertThat(q.getPhaserOpResponse().getPhase()).isEqualTo(phases);
            assertThat(q.getPhaserOpResponse().getRegistered()).isEqualTo(parties);
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "并发多相位副本一致");
        }
    }
}
