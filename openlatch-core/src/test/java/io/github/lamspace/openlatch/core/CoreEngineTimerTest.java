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
import io.github.lamspace.openlatch.core.command.QueueOpCommand;
import io.github.lamspace.openlatch.core.command.TimerOpCommand;
import io.github.lamspace.openlatch.core.lock.TimerEntry;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.snapshot.CoreStateRestore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CoreEngine 延时触发门面用例组：SCHEDULE 建条目与无条目拒绝矩阵、跨家族
 * 互拒（TIMER×DELAY_QUEUE×LOCK×PHASER 双向——同题异机的到期语义对照）、
 * ACQUIRE 携 timer 定型入口拒绝、受理通道 horizon 与复制通道不复核的分岔、
 * 会话触及与死亡零扰动（账簿三值不随死亡改写）、单机等待闭环经监听点唤醒
 * 与 {@code wakeTimerReady} 扫描臂、观察面读数（waiterCount 合计/明细视图）
 * 与恢复分支。手工时钟，无 sleep。
 */
class CoreEngineTimerTest {

    /** 手工时钟。 */
    private MutableClock clock;
    /** 记录型监听器。 */
    private RecordingListener listener;
    /** 被测引擎。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    /** 命令工厂。 */
    private static TimerOpCommand op(long s, long r, String key, TimerOpType t,
            Long delay, long awaitReq) {
        return new TimerOpCommand(s, r, key, t, delay, awaitReq);
    }

    /** 装载工厂。 */
    private static TimerOpCommand schedule(long s, long r, String key, long delay) {
        return op(s, r, key, TimerOpType.SCHEDULE, delay, 0);
    }

    /** 等待工厂。 */
    private static TimerOpCommand await(long s, long r, String key) {
        return op(s, r, key, TimerOpType.AWAIT, null, 0);
    }

    /** 撤销装载工厂。 */
    private static TimerOpCommand disarm(long s, long r, String key) {
        return op(s, r, key, TimerOpType.DISARM, null, 0);
    }

    @Test
    void creationAndNoEntryMatrix() {
        long a = engine.sessionOpened();
        // 非 SCHEDULE 无条目：拒绝且不隐式建钟。
        assertThat(engine.timerOp(await(a, 1, "t")).outcome())
                .isEqualTo(Outcome.REJECT_TIMER_NO_ENTRY);
        assertThat(engine.timerOp(disarm(a, 2, "t")).outcome())
                .isEqualTo(Outcome.REJECT_TIMER_NO_ENTRY);
        assertThat(engine.timerOp(op(a, 3, "t", TimerOpType.QUERY, null, 0)).outcome())
                .isEqualTo(Outcome.REJECT_TIMER_NO_ENTRY);
        assertThat(engine.inspectKey("t")).isNull();
        // SCHEDULE 建条目并定型 TIMER 家族。
        assertThat(engine.timerOp(schedule(a, 4, "t", 5_000L)).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(engine.inspectKey("t").family()).isEqualTo(KeyFamily.TIMER);
        // 会话预检。
        assertThat(engine.timerOp(schedule(999, 5, "t2", 1L)).outcome())
                .isEqualTo(Outcome.REJECT_SESSION);
    }

    @Test
    void horizonEnforcedOnAcceptPathOnly() {
        long a = engine.sessionOpened();
        CoreConfig cfg = new CoreConfig(30_000L, 1_000L, 3_600_000L, 5_000L, 512, 4096,
                1024, 60_000L);
        CoreEngine e = new CoreEngine(cfg, clock, listener);
        long s = e.sessionOpened();
        // 受理通道：horizon 越界拒绝、零建条目。
        assertThat(e.timerOp(schedule(s, 1, "t", 60_001L)).outcome())
                .isEqualTo(Outcome.REJECT_TIMER_DELAY_OVER);
        assertThat(e.inspectKey("t")).isNull();
        // 复制通道不复核：已受理的越界条目照常回放（配置漂移不撕裂账簿，
        // 判例 phaser parties 分岔）。
        assertThat(e.timerApply(schedule(s, 2, "t", 60_001L)).outcome())
                .isEqualTo(Outcome.GRANTED);
    }

    @Test
    void crossFamilyMatrixTimerAgainstDelayQueueLockPhaser() {
        long a = engine.sessionOpened();
        // TIMER×DELAY_QUEUE：两到期家族同题异机、条目互拒。
        engine.queueOp(new QueueOpCommand(a, 1, "dq", LockType.DELAY_QUEUE,
                QueueOpType.PUT, false, 2, new byte[] {1}, 1_000L, 0, 0));
        assertThat(engine.timerOp(schedule(a, 2, "dq", 1L)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.inspectKey("dq").family()).isEqualTo(KeyFamily.QUEUE);
        // TIMER×LOCK。
        engine.acquire(new AcquireCommand(a, 3, "lk", LockType.REENTRANT, 1, 30_000, true));
        assertThat(engine.timerOp(schedule(a, 4, "lk", 1L)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // ACQUIRE 携 timer 定型：入口即拒。
        assertThat(engine.acquire(new AcquireCommand(a, 5, "tk", LockType.TIMER,
                1, 30_000, true)).outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // 反向：timer key 上的队列/phaser 请求互拒。
        engine.timerOp(schedule(a, 6, "tk", 5_000L));
        assertThat(engine.queueOp(new QueueOpCommand(a, 7, "tk", LockType.QUEUE,
                QueueOpType.PUT, false, 2, new byte[] {1}, 0L, 0, 0)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
    }

    @Test
    void standaloneAwaitWakesViaListenerAndSweep() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.timerOp(schedule(a, 1, "t", 5_000L));
        assertThat(engine.timerOp(await(b, 2, "t")).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(engine.timerWaiters("t")).hasSize(1);
        // 时钟推至到期点：扫描臂唤醒经监听点投递，出集不滞留。
        clock.advance(5_000L);
        assertThat(engine.wakeTimerReady()).isEqualTo(1);
        assertThat(listener.events()).hasSize(1);
        assertThat(engine.timerWaiters("t")).isEmpty();
        // 共见粘滞：后续等待即刻了结、不经挂起。
        assertThat(engine.timerOp(await(b, 3, "t")).marked()).isTrue();
        // 到期后再装载开新轮（粘滞归伪）。
        engine.timerOp(schedule(a, 4, "t", 1_000L));
        assertThat(engine.timerOp(await(b, 5, "t")).outcome()).isEqualTo(Outcome.QUEUED);
    }

    @Test
    void sessionDeathLeavesLedgerUntouchedAndDropsOwnWaiters() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.timerOp(schedule(a, 1, "t", 5_000L));
        engine.timerOp(await(a, 2, "t")); // 装载者自己也旁观
        engine.timerOp(await(b, 3, "t"));
        // 装载者死亡：账簿三值零扰动（死亡钟照响）、仅其等待位摘除。
        engine.sessionClosed(a);
        TimerEntry.ReplicatedState st = engine.timerReplicatedState("t");
        assertThat(st.generation()).isEqualTo(1);
        assertThat(st.armed()).isTrue();
        assertThat(st.fireAtMs()).isEqualTo(1_000_000L + 5_000L); // 装载时刻+5000（死亡不改判）
        assertThat(engine.timerWaiters("t")).hasSize(1); // 只剩会话 b 的旁观
        clock.advance(5_000L);
        assertThat(engine.wakeTimerReady()).isEqualTo(1); // 钟照响
    }

    @Test
    void statsAndInspectionCountTimerWaits() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.timerOp(schedule(a, 1, "t", 5_000L));
        engine.timerOp(await(b, 2, "t"));
        CoreStats stats = engine.stats();
        assertThat(stats.totalWaiters()).isEqualTo(1); // "等待就是等待"合并口径
        var snap = engine.inspectKey("t");
        assertThat(snap.timerGeneration()).isEqualTo(1);
        assertThat(snap.timerArmed()).isTrue();
        assertThat(snap.timerFireAtMs()).isEqualTo(1_000_000L + 5_000L);
        // timer 家族不入 locks.held 判例：无持有语义。
        assertThat(snap.holders()).isEmpty();
    }

    @Test
    void restoreBranchRebuildsLedgerDeterministically() {
        long a = engine.sessionOpened();
        engine.timerOp(schedule(a, 1, "t", 5_000L));
        TimerEntry.ReplicatedState st = engine.timerReplicatedState("t");
        // 快照导出（含槽表按会话升序）→ restored 直写：三值保真、等待恒空。
        CoreStateRestore.TimerState ts = new CoreStateRestore.TimerState(
                st.generation(), st.armed(), st.fireAtMs(), st.slots());
        CoreStateRestore.Entry en = new CoreStateRestore.Entry("t", LockType.TIMER,
                0, 0, 0, List.of(), 0, 0, 0, null, null, null, null, null, ts);
        CoreEngine fresh = new CoreEngine(new CoreConfig(), new MutableClock(),
                new RecordingListener());
        fresh.restoreFrom(new CoreStateRestore(List.of(en), List.of(), 1L));
        TimerEntry.ReplicatedState rs = fresh.timerReplicatedState("t");
        assertThat(rs.generation()).isEqualTo(1);
        assertThat(rs.fireAtMs()).isEqualTo(1_000_000L + 5_000L);
        assertThat(rs.slots()).isEqualTo(st.slots());
        assertThat(fresh.timerWaiters("t")).isEmpty();
        // 恢复态谓词照常：时钟未到续挂、到了共见。
        long b = fresh.sessionOpened();
        assertThat(fresh.timerOp(await(b, 9, "t")).outcome()).isEqualTo(Outcome.QUEUED);
    }

    @Test
    void entryRestoreRejectsMissingStateGroup() {
        // TimerState 与家族互斥自洽：TIMER 条目缺状态组即拒、他族携带即拒。
        assertThat(List.of(new CoreStateRestore.Entry("t", LockType.TIMER, 0, 0, 0,
                List.of(), 0, 0, 0, null, null, null, null, null,
                new CoreStateRestore.TimerState(1, true, 5_000L, List.of())))).hasSize(1);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new CoreStateRestore.Entry("t", LockType.TIMER, 0, 0, 0,
                        List.of(), 0, 0, 0, null, null, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new CoreStateRestore.Entry("l", LockType.REENTRANT, 1, 30_000L,
                        40_000L,
                        List.of(new CoreStateRestore.Holder(1, 1, 1)), 0, 0, 0,
                        null, null, null, null, null,
                        new CoreStateRestore.TimerState(1, true, 5_000L, List.of())))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
