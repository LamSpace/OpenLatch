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

package io.github.lamspace.openlatch.core;

import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.LatchAwaitCommand;
import io.github.lamspace.openlatch.core.command.LatchCountDownCommand;
import io.github.lamspace.openlatch.core.result.LatchAwaitResult;
import io.github.lamspace.openlatch.core.result.LatchCountDownResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CountDownLatch 语义用例组：total
 * 定型（双通道）、纯初始化、扣减下限、归零全体广播与重发离队、一次性、
 * 等待者无租约（到期零触及）、断连摘除、类型不匹配矩阵。手工时钟，无 sleep。
 */
class CoreEngineLatchTest {

    /** 手工时钟。 */
    private MutableClock clock;
    /** 记录型监听器：广播序断言对象。 */
    private RecordingListener listener;
    /** 被测引擎。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    /** 锁获取命令工厂（不匹配矩阵用）。 */
    private AcquireCommand lockAcq(long s, long r, String key, LockType type, long tid) {
        return new AcquireCommand(s, r, key, type, tid, 30_000, true);
    }

    @Test
    void totalInitializesViaBothChannelsAndPureJoinRejects() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();

        // 无断言的 await / countDown 对不存在屏障均拒。
        assertThat(engine.latchAwait(new LatchAwaitCommand(a, 1, "l", 0)).outcome())
                .isEqualTo(Outcome.REJECT_LATCH_TOTAL);
        assertThat(engine.countDown(new LatchCountDownCommand(a, "l", 1, 0)).outcome())
                .isEqualTo(Outcome.REJECT_LATCH_TOTAL);

        // countDown(0, total=3)：纯初始化。
        LatchCountDownResult init = engine.countDown(new LatchCountDownCommand(a, "l", 0, 3));
        assertThat(init.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(init.remaining()).isEqualTo(3);

        // 断言不符拒绝、相符通过；await 带 total 同样校验。
        assertThat(engine.latchAwait(new LatchAwaitCommand(b, 2, "l", 7)).outcome())
                .isEqualTo(Outcome.REJECT_LATCH_TOTAL);
        LatchAwaitResult q = engine.latchAwait(new LatchAwaitCommand(b, 2, "l", 3));
        assertThat(q.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(q.queuePosition()).isEqualTo(1);
    }

    @Test
    void awaitCreatesWhenFirstWithTotal() {
        long a = engine.sessionOpened();
        LatchAwaitResult r = engine.latchAwait(new LatchAwaitCommand(a, 1, "lc", 2));
        assertThat(r.outcome()).isEqualTo(Outcome.QUEUED);
        long b = engine.sessionOpened();
        LatchCountDownResult cd = engine.countDown(new LatchCountDownCommand(b, "lc", 2, 2));
        assertThat(cd.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(cd.remaining()).isZero();
    }

    @Test
    void countDownFloorAndZeroBroadcastWithResendDeparture() {
        long creator = engine.sessionOpened();
        long w1 = engine.sessionOpened();
        long w2 = engine.sessionOpened();
        long w3 = engine.sessionOpened();

        engine.countDown(new LatchCountDownCommand(creator, "l", 0, 1));
        assertThat(engine.latchAwait(new LatchAwaitCommand(w1, 11, "l", 0)).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(engine.latchAwait(new LatchAwaitCommand(w2, 12, "l", 0)).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(engine.latchAwait(new LatchAwaitCommand(w3, 13, "l", 0)).outcome()).isEqualTo(Outcome.QUEUED);
        listener.clear();

        // 超额扣减下限 0：归零瞬间全体广播。
        LatchCountDownResult r = engine.countDown(new LatchCountDownCommand(creator, "l", 5, 0));
        assertThat(r.remaining()).isZero();
        assertThat(listener.count()).isEqualTo(3);
        assertThat(listener.events()).extracting(RecordingListener.Event::requestId)
                .containsExactly(11L, 12L, 13L);

        // 广播不离队：重发 await 命中"已归零"逐个立即通过并离队。
        LatchAwaitResult a1 = engine.latchAwait(new LatchAwaitCommand(w1, 11, "l", 0));
        assertThat(a1.outcome()).isEqualTo(Outcome.GRANTED);
        // 新到者（无 total）对存续的归零屏障直接通过。
        long w4 = engine.sessionOpened();
        assertThat(engine.latchAwait(new LatchAwaitCommand(w4, 14, "l", 0)).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(engine.latchAwait(new LatchAwaitCommand(w2, 12, "l", 0)).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.latchAwait(new LatchAwaitCommand(w3, 13, "l", 0)).outcome()).isEqualTo(Outcome.GRANTED);

        // 全员离队后一次性护栏持续（条目存续至节点重启）：
        // 参与者散尽也不回收，晚到纯加入始终直接放行。
        assertThat(engine.latchAwait(new LatchAwaitCommand(w4, 15, "l", 0)).outcome())
                .isEqualTo(Outcome.GRANTED);
        engine.sessionClosed(creator);
        engine.sessionClosed(w1);
        engine.sessionClosed(w2);
        engine.sessionClosed(w3);
        engine.sessionClosed(w4);
        long w5 = engine.sessionOpened();
        assertThat(engine.latchAwait(new LatchAwaitCommand(w5, 16, "l", 0)).outcome())
                .isEqualTo(Outcome.GRANTED);
    }

    @Test
    void oneShotNoResetAfterZero() {
        long a = engine.sessionOpened();
        engine.countDown(new LatchCountDownCommand(a, "l", 0, 1));
        engine.countDown(new LatchCountDownCommand(a, "l", 1, 0));
        LatchCountDownResult r = engine.countDown(new LatchCountDownCommand(a, "l", 5, 0));
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r.remaining()).isZero();
    }

    @Test
    void waitersHaveNoLeaseExpireDueNeverTouches() {
        long a = engine.sessionOpened();
        long w = engine.sessionOpened();
        engine.latchAwait(new LatchAwaitCommand(a, 1, "l", 2));
        engine.latchAwait(new LatchAwaitCommand(w, 2, "l", 0));
        listener.clear();

        // 多轮到期扫描 + 大跨度推进：计数与等待者均不受租约机制影响。
        clock.advance(3_600_000);
        assertThat(engine.expireDue()).isZero();
        assertThat(engine.sweepNotifiedHeads()).isZero();
        assertThat(listener.count()).isZero();
        assertThat(engine.countDown(new LatchCountDownCommand(a, "l", 1, 0)).remaining()).isEqualTo(1);
    }

    @Test
    void disconnectRemovesAwaiterOnly() {
        long a = engine.sessionOpened();
        long w1 = engine.sessionOpened();
        long w2 = engine.sessionOpened();
        engine.countDown(new LatchCountDownCommand(a, "l", 0, 2));
        engine.latchAwait(new LatchAwaitCommand(w1, 11, "l", 0));
        engine.latchAwait(new LatchAwaitCommand(w2, 12, "l", 0));

        engine.sessionClosed(w1); // 仅摘等待者：计数不变
        assertThat(engine.countDown(new LatchCountDownCommand(a, "l", 1, 0)).remaining()).isEqualTo(1);
        assertThat(listener.count()).isZero();

        engine.sessionClosed(a); // 创建者关闭不消耗屏障
        LatchCountDownResult r = engine.countDown(new LatchCountDownCommand(w2, "l", 1, 0));
        assertThat(r.remaining()).isZero();
        assertThat(listener.count()).isEqualTo(1); // w2 仍在队，归零广播送达
        assertThat(listener.last().sessionId()).isEqualTo(w2);
    }

    @Test
    void familyMismatchMatrixForLatch() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();

        // 锁 key 上 await / countDown → 家族拒绝，锁态零扰动。
        var lg = engine.acquire(lockAcq(a, 1, "lk", LockType.REENTRANT, 1));
        assertThat(lg.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.latchAwait(new LatchAwaitCommand(b, 2, "lk", 3)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.countDown(new LatchCountDownCommand(b, "lk", 1, 0)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.acquire(lockAcq(a, 3, "lk", LockType.REENTRANT, 1)).leaseToken())
                .isEqualTo(lg.leaseToken());

        // latch key 上锁获取 → 家族拒绝。
        engine.latchAwait(new LatchAwaitCommand(b, 4, "lt", 1));
        assertThat(engine.acquire(lockAcq(a, 5, "lt", LockType.REENTRANT, 2)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // latch 条目上的锁释放/续租：无持有语义 NOT_HELD。
        var rel = engine.release(new io.github.lamspace.openlatch.core.command.ReleaseCommand(a, "lt", 1, 2));
        assertThat(rel.status()).isEqualTo(io.github.lamspace.openlatch.core.result.ReleaseStatus.NOT_HELD);
    }

    @Test
    void awaitIdempotentDedupAndQueueDepth() {
        CoreConfig cfg = new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                CoreConfig.MAX_KEY_LENGTH, 1);
        CoreEngine eng = new CoreEngine(cfg, clock, listener);
        long a = eng.sessionOpened();
        long b = eng.sessionOpened();
        long c = eng.sessionOpened();

        eng.countDown(new LatchCountDownCommand(a, "l", 0, 5));
        assertThat(eng.latchAwait(new LatchAwaitCommand(b, 11, "l", 0)).queuePosition()).isEqualTo(1);
        // 同 (sid, rid) 重发去重：位次不前进。
        assertThat(eng.latchAwait(new LatchAwaitCommand(b, 11, "l", 0)).queuePosition()).isEqualTo(1);
        assertThat(eng.latchAwait(new LatchAwaitCommand(c, 12, "l", 0)).outcome())
                .isEqualTo(Outcome.REJECT_QUEUE_FULL);
    }

    @Test
    void notifiedAwaitAbandonmentSweptAndBarrierStillPassable() {
        long a = engine.sessionOpened();
        long w = engine.sessionOpened();
        long w2 = engine.sessionOpened();
        engine.countDown(new LatchCountDownCommand(a, "l", 0, 1));
        engine.latchAwait(new LatchAwaitCommand(w, 11, "l", 0));
        engine.latchAwait(new LatchAwaitCommand(w2, 12, "l", 0));
        engine.countDown(new LatchCountDownCommand(a, "l", 1, 0)); // 归零：w、w2 全体广播
        assertThat(listener.count()).isEqualTo(2);

        // w 放弃重发：越过响应时限后被清扫；w 重发则按"归零即过"重新入队再放行。
        clock.advance(CoreConfig.HEAD_REPLY_TIMEOUT_MS + 1);
        assertThat(engine.sweepNotifiedHeads()).isEqualTo(1);
        assertThat(engine.latchAwait(new LatchAwaitCommand(w, 11, "l", 0)).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(engine.latchAwait(new LatchAwaitCommand(w2, 12, "l", 0)).outcome())
                .isEqualTo(Outcome.GRANTED);
    }
}
