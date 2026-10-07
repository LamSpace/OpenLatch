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
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TimerOp;
import io.github.lamspace.openlatch.protocol.TimerOpRequest;
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
 * 延时触发集群 E2E（v11）：一到多等待者同刻齐过矩阵（广播共见——队列延时
 * 形态不可表达格的正向证明）、**kill 装载者进程钟照响**（头号验收：对标屏障
 * 破障/phaser 摘除的死亡 E2E 形态）、改期双方向、DISARM 唤醒 {@code DENIED}
 * 了结、同 request_id 重发单换代、换主账簿存续 + 重挂了结 + 新主首扫补偿
 * （有界总超时 + eventually，禁裸时序断言——v9 D12/W10 教训）、Follower 五
 * 操作词同型零副作用、并发装载竞态最后写者赢且等待者终见当代判定。
 * 判例 {@code ClusterPhaserTest}；共享 harness 单类一个（W2 fork 端口纪律）。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClusterTimerTest {

    /** 构造 TIMER 信封（delay=null 即不主张）。 */
    private static Envelope timer(long rid, TimerOp op, String key, Long delay,
            long awaitRid) {
        TimerOpRequest.Builder rb = TimerOpRequest.newBuilder()
                .setKey(key).setOp(op).setAwaitRequestId(awaitRid);
        if (delay != null) {
            rb.setDelayMs(delay);
        }
        return Envelope.newBuilder().setProtocolVersion(11).setType(MessageType.TIMER_OP)
                .setRequestId(rid).setTimerOpRequest(rb).build();
    }

    /** 请求并取回同 rid 同型应答——过滤先到的 AWAIT_NOTIFY 串扰。 */
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
        return resp.getTimerOpResponse().getStatus();
    }

    /** 读到一条 AWAIT_NOTIFY（或读界到点即返——重发环自兜底，判例 phaser）。 */
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

    /** 挂起→唤醒→同 rid 重发的终态了结环（OK/DENIED 皆终态，无回炉）。 */
    private static Envelope settleAwait(ClusterHarness.TestConn conn, long rid, String key) {
        Envelope resp = requestReply(conn, timer(rid, TimerOp.TIMER_OP_AWAIT, key, null, 0));
        while (status(resp) == StatusCode.QUEUED) {
            drainUntilNotifyOrTimeout(conn);
            resp = requestReply(conn, timer(rid, TimerOp.TIMER_OP_AWAIT, key, null, 0));
        }
        return resp;
    }

    @Test
    void manyWaitersFireOnceAllSeeSharedMark() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 11);
            Envelope sch = requestReply(a, timer(10, TimerOp.TIMER_OP_SCHEDULE, "t", 1_500L, 0));
            assertThat(status(sch)).isEqualTo(StatusCode.OK);
            assertThat(sch.getTimerOpResponse().getGeneration()).isEqualTo(1);
            assertThat(sch.getTimerOpResponse().getMarked()).isFalse();
            // 三名旁观者先后挂起（专用连接）。
            ClusterHarness.TestConn w1 = h.connect(leader);
            ClusterHarness.TestConn w2 = h.connect(leader);
            w1.hello(20, 11);
            w2.hello(21, 11);
            assertThat(status(requestReply(w1, timer(30, TimerOp.TIMER_OP_AWAIT,
                    "t", null, 0)))).isEqualTo(StatusCode.QUEUED);
            assertThat(status(requestReply(w2, timer(31, TimerOp.TIMER_OP_AWAIT,
                    "t", null, 0)))).isEqualTo(StatusCode.QUEUED);
            // 到点后 tick 级唤醒：双双经唤醒-重发以 OK{marked} 了结（共见一次到期）。
            Envelope done1 = settleAwait(w1, 30, "t");
            assertThat(status(done1)).isEqualTo(StatusCode.OK);
            assertThat(done1.getTimerOpResponse().getMarked()).isTrue();
            Envelope done2 = settleAwait(w2, 31, "t");
            assertThat(status(done2)).isEqualTo(StatusCode.OK);
            // 粘滞共见：fire 之后新到达者即刻通过、不经挂起（对照队列 take 的消费形）。
            Envelope late = requestReply(a, timer(32, TimerOp.TIMER_OP_AWAIT, "t", null, 0));
            assertThat(status(late)).isEqualTo(StatusCode.OK);
            assertThat(late.getTimerOpResponse().getMarked()).isTrue();
            // 重装载：换代清钟——新等待者落入新一轮阻塞。
            Envelope re = requestReply(a, timer(33, TimerOp.TIMER_OP_SCHEDULE,
                    "t", 60_000L, 0));
            assertThat(re.getTimerOpResponse().getGeneration()).isEqualTo(2);
            assertThat(status(requestReply(a, timer(34, TimerOp.TIMER_OP_QUERY,
                    "t", null, 0)))).isEqualTo(StatusCode.OK);
            Envelope q = requestReply(a, timer(35, TimerOp.TIMER_OP_QUERY, "t", null, 0));
            assertThat(q.getTimerOpResponse().getMarked()).isFalse();
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "副本一致");
        }
    }

    @Test
    void loaderDeathClockStillFires() throws Exception {
        // 头号验收（死亡语义第三形态）：装载者会话消亡后钟照响、旁观者共见——
        // 与 phaser"死亡摘除配额可推进"、barrier"死亡即破障"并列的复制面证据。
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 11);
            ClusterHarness.TestConn w = h.connect(leader);
            w.hello(2, 11);
            assertThat(status(requestReply(a, timer(10, TimerOp.TIMER_OP_SCHEDULE,
                    "d", 2_000L, 0)))).isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(w, timer(11, TimerOp.TIMER_OP_AWAIT,
                    "d", null, 0)))).isEqualTo(StatusCode.QUEUED);
            a.disconnect(); // SESSION_CLOSE 传播：账簿零扰动（与 phaser 摘除反向）
            Envelope done = settleAwait(w, 11, "d");
            assertThat(status(done)).isEqualTo(StatusCode.OK);
            assertThat(done.getTimerOpResponse().getMarked()).isTrue();
            assertThat(done.getTimerOpResponse().getGeneration()).isEqualTo(1);
            assertThat(done.getTimerOpResponse().getArmed()).isTrue(); // 装载未随死亡撤销
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "死亡后副本一致");
        }
    }

    @Test
    void rescheduleBothDirectionsAndDisarmWakesDenied() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 11);
            ClusterHarness.TestConn w = h.connect(leader);
            w.hello(2, 11);
            // 远钟 + 挂起 → 提前改期 → 按新钟先响。
            assertThat(status(requestReply(a, timer(10, TimerOp.TIMER_OP_SCHEDULE,
                    "r", 60_000L, 0)))).isEqualTo(StatusCode.OK);
            assertThat(status(requestReply(w, timer(11, TimerOp.TIMER_OP_AWAIT,
                    "r", null, 0)))).isEqualTo(StatusCode.QUEUED);
            Envelope near = requestReply(a, timer(12, TimerOp.TIMER_OP_SCHEDULE,
                    "r", 1_000L, 0));
            assertThat(near.getTimerOpResponse().getGeneration()).isEqualTo(2);
            Envelope done = settleAwait(w, 11, "r");
            assertThat(status(done)).isEqualTo(StatusCode.OK);
            assertThat(done.getTimerOpResponse().getGeneration()).isEqualTo(2);
            // 推后改期：已共见的钟清零续睡（粘滞归伪、以最新代为准的契约形态）。
            Envelope far = requestReply(a, timer(13, TimerOp.TIMER_OP_SCHEDULE,
                    "r", 60_000L, 0));
            assertThat(far.getTimerOpResponse().getMarked()).isFalse();
            Envelope q = requestReply(a, timer(14, TimerOp.TIMER_OP_QUERY, "r", null, 0));
            assertThat(q.getTimerOpResponse().getMarked()).isFalse();
            // DISARM：新挂起者即刻 DENIED（无"永睡已撤钟"形态）；已挂起者唤醒收束。
            assertThat(status(requestReply(a, timer(15, TimerOp.TIMER_OP_DISARM,
                    "r", null, 0)))).isEqualTo(StatusCode.OK);
            Envelope denied = requestReply(w, timer(16, TimerOp.TIMER_OP_AWAIT,
                    "r", null, 0));
            assertThat(status(denied)).isEqualTo(StatusCode.DENIED);
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "副本一致");
        }
    }

    @Test
    void scheduleReplaySameRidSingleGeneration() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.TestConn a = h.connect(h.leader());
            a.hello(1, 11);
            Envelope first = requestReply(a, timer(10, TimerOp.TIMER_OP_SCHEDULE,
                    "rp", 5_000L, 0));
            assertThat(status(first)).isEqualTo(StatusCode.OK);
            // 应答丢失模拟：同 rid 原信封重发——代次不前进（装载去重槽）。
            Envelope replay = requestReply(a, timer(10, TimerOp.TIMER_OP_SCHEDULE,
                    "rp", 5_000L, 0));
            assertThat(replay.getTimerOpResponse().getGeneration())
                    .isEqualTo(first.getTimerOpResponse().getGeneration());
            Envelope q = requestReply(a, timer(11, TimerOp.TIMER_OP_QUERY, "rp", null, 0));
            assertThat(q.getTimerOpResponse().getGeneration()).isEqualTo(1);
            // DISARM 重发同理：幂等回声、代次保持。
            assertThat(status(requestReply(a, timer(12, TimerOp.TIMER_OP_DISARM,
                    "rp", null, 0)))).isEqualTo(StatusCode.OK);
            Envelope d2 = requestReply(a, timer(12, TimerOp.TIMER_OP_DISARM, "rp", null, 0));
            assertThat(d2.getTimerOpResponse().getGeneration()).isEqualTo(1);
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
            setup.hello(1, 11);
            assertThat(status(requestReply(setup, timer(10, TimerOp.TIMER_OP_SCHEDULE,
                    "f", 10_000L, 0)))).isEqualTo(StatusCode.OK);
            ClusterHarness.TestConn fc = h.connect(follower);
            fc.hello(2, 11);
            for (TimerOp op : new TimerOp[] {TimerOp.TIMER_OP_SCHEDULE, TimerOp.TIMER_OP_DISARM,
                    TimerOp.TIMER_OP_AWAIT, TimerOp.TIMER_OP_CANCEL, TimerOp.TIMER_OP_QUERY}) {
                Envelope resp = fc.request(timer(20, op, "f",
                        op == TimerOp.TIMER_OP_SCHEDULE ? 1_000L : null, 5));
                assertThat(status(resp)).as("%s 于 Follower", op)
                        .isEqualTo(StatusCode.NOT_LEADER);
                assertThat(resp.getTimerOpResponse().getOp()).isEqualTo(op);
            }
            // 零副作用：Leader 账簿纹丝不动（代次仍 1、在装）。
            Envelope q = requestReply(setup, timer(30, TimerOp.TIMER_OP_QUERY,
                    "f", null, 0));
            assertThat(q.getTimerOpResponse().getGeneration()).isEqualTo(1);
            assertThat(q.getTimerOpResponse().getArmed()).isTrue();
        }
    }

    @Test
    void ledgerSurvivesFailoverAwaitRehangAndFirstScanCompensates() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 800L)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 11);
            assertThat(status(requestReply(a, timer(10, TimerOp.TIMER_OP_SCHEDULE,
                    "fo", 3_000L, 0)))).isEqualTo(StatusCode.OK);
            ClusterHarness.TestConn w = h.connect(leader);
            w.hello(2, 11);
            assertThat(status(requestReply(w, timer(11, TimerOp.TIMER_OP_AWAIT,
                    "fo", null, 0)))).isEqualTo(StatusCode.QUEUED);
            h.stopNode(leader.id);
            h.awaitTrue(() -> h.leader() != null && h.leader() != leader, 20_000, "重选主");
            ClusterHarness.TestConn onNew = h.connect(h.leader());
            onNew.hello(20, 11);
            // 账簿为复制状态：三元组存续（绝对到期时刻换主不改判）。
            Envelope q = requestReply(onNew, timer(21, TimerOp.TIMER_OP_QUERY,
                    "fo", null, 0));
            assertThat(q.getTimerOpResponse().getGeneration()).isEqualTo(1);
            assertThat(q.getTimerOpResponse().getArmed()).isTrue();
            // 等待簿记随换主清零——新 Leader 重挂：到期后由新主首扫/tick 补偿
            // 唤醒，重发以原 rid 谓词重评即刻了结（有界超时+eventually，禁裸时序）。
            ClusterHarness.TestConn w2 = h.connect(h.leader());
            w2.hello(30, 11);
            Envelope done = settleAwait(w2, 31, "fo");
            assertThat(status(done)).isEqualTo(StatusCode.OK);
            assertThat(done.getTimerOpResponse().getMarked()).isTrue();
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "failover 后副本一致");
        }
    }

    @Test
    void concurrentLoadsLastWriterWinsAllSeeCurrentVerdict() throws Exception {
        final int loaders = 4;
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn seed = h.connect(leader);
            seed.hello(9, 11);
            ExecutorService pool = Executors.newFixedThreadPool(loaders);
            CountDownLatch done = new CountDownLatch(loaders);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicInteger rids = new AtomicInteger(100);
            for (int i = 0; i < loaders; i++) {
                pool.submit(() -> {
                    ClusterHarness.TestConn conn = h.connect(leader);
                    try {
                        conn.hello(1000 + rids.get(), 11);
                        long rid = rids.incrementAndGet();
                        Envelope resp = requestReply(conn, timer(rid,
                                TimerOp.TIMER_OP_SCHEDULE, "cc", 1_000L, 0));
                        if (status(resp) != StatusCode.OK) {
                            throw new AssertionError("schedule rejected: " + status(resp));
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        conn.disconnect();
                        done.countDown();
                    }
                });
            }
            assertThat(done.await(120, TimeUnit.SECONDS)).isTrue();
            pool.shutdownNow();
            assertThat(failure.get()).isNull();
            // 四发各一换代（每会话单槽互不覆盖）：代次恰 4；最后折算钟由任意
            // 等待者共见（等待者终见当代判定——竞态无丢失）。
            Envelope q = requestReply(seed, timer(900, TimerOp.TIMER_OP_QUERY,
                    "cc", null, 0));
            assertThat(q.getTimerOpResponse().getGeneration()).isEqualTo(loaders);
            ClusterHarness.TestConn w = h.connect(h.leader());
            w.hello(40, 11);
            Envelope done1 = settleAwait(w, 41, "cc");
            assertThat(status(done1)).isEqualTo(StatusCode.OK);
            assertThat(done1.getTimerOpResponse().getGeneration())
                    .isEqualTo(q.getTimerOpResponse().getGeneration());
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "并发装载副本一致");
        }
    }
}
