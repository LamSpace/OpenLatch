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

package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.KeyFamily;
import io.github.lamspace.openlatch.core.TimerOpType;
import io.github.lamspace.openlatch.core.command.TimerOpCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.TimerOpResult;
import io.github.lamspace.openlatch.core.snapshot.CoreStateRestore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * TimerEntry 时钟谓词账簿状态机逐格用例：装载换代清钟、去重槽覆盖式幂等、
 * 到期谓词三时点判定、粘滞共见、DISARM 代终结与事件唤醒、撤销-到期竞态
 * 当值裁决、等待登记幂等与深度护栏、无条目拒绝、死亡零扰动（账簿不改写）、
 * 恢复往返与导出确定性。全部同步直调、时刻显式注入，无网络无 sleep——
 * 派生口径下到期判定完全无壁钟依赖。
 */
class TimerEntryTest {

    /** 测试用键。 */
    private static final String K = "t";
    /** 缺省限额配置。 */
    private static final CoreConfig CFG = new CoreConfig();
    /** 固定测试时刻。 */
    private static final long NOW = 10_000L;

    /** 装载命令工厂。 */
    private static TimerOpCommand schedule(long s, long r, long delay) {
        return new TimerOpCommand(s, r, K, TimerOpType.SCHEDULE, delay, 0);
    }

    /** 撤销装载命令工厂。 */
    private static TimerOpCommand disarm(long s, long r) {
        return new TimerOpCommand(s, r, K, TimerOpType.DISARM, null, 0);
    }

    /** 等待命令工厂。 */
    private static TimerOpCommand await(long s, long r) {
        return new TimerOpCommand(s, r, K, TimerOpType.AWAIT, null, 0);
    }

    /** 撤销等待命令工厂。 */
    private static TimerOpCommand cancel(long s, long r, long target) {
        return new TimerOpCommand(s, r, K, TimerOpType.CANCEL, null, target);
    }

    /** 读数命令工厂。 */
    private static TimerOpCommand query(long s, long r) {
        return new TimerOpCommand(s, r, K, TimerOpType.QUERY, null, 0);
    }

    @Test
    void scheduleCreatesTypedEntryAndEchoesNewGeneration() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        TimerOpResult r = e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r.generation()).isEqualTo(1);
        assertThat(r.armed()).isTrue();
        assertThat(r.fireAtMs()).isEqualTo(NOW + 5_000L);
        assertThat(r.marked()).isFalse();
        assertThat(e.family()).isEqualTo(KeyFamily.TIMER);
        // 二次装载换代清钟：代次 +1、上一代粘滞标记随 armed 重置归伪。
        TimerOpResult r2 = e.timerOp(schedule(1, 12, 1_000L), NOW + 4_000L, CFG, notify);
        assertThat(r2.generation()).isEqualTo(2);
        assertThat(r2.fireAtMs()).isEqualTo(NOW + 5_000L);
        assertThat(notify).isEmpty(); // 装载不采集唤醒（无同步到期承诺，tick 承载）
    }

    @Test
    void scheduleReplayIsIdempotentSingleGeneration() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        // 同 (会话,请求) 重发：回放回声，不双换代、不重写到期时刻。
        TimerOpResult replay = e.timerOp(schedule(1, 11, 5_000L), NOW + 1L, CFG, notify);
        assertThat(replay.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(replay.generation()).isEqualTo(1);
        assertThat(replay.fireAtMs()).isEqualTo(NOW + 5_000L);
        assertThat(e.generation()).isEqualTo(1);
        // 新请求即新代（覆盖式单槽）。
        assertThat(e.timerOp(schedule(1, 12, 1L), NOW, CFG, notify).generation()).isEqualTo(2);
        // 旧 rid 在槽被覆盖后的迟到重发：按新变异执行（声明竞态，判例 v7 队列槽）。
        TimerOpResult late = e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        assertThat(late.generation()).isEqualTo(3);
    }

    @Test
    void zeroDelayScheduleIsImmediatelyShared() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        TimerOpResult r = e.timerOp(schedule(1, 11, 0L), NOW, CFG, notify);
        assertThat(r.marked()).isTrue(); // delay=0：回执即已共见
        assertThat(e.timerOp(await(2, 21), NOW, CFG, notify).outcome())
                .isEqualTo(Outcome.GRANTED); // 后续到达者即刻通过（粘滞共见）
    }

    @Test
    void awaitVerdictThreeTimepoints() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        // 到期前：QUEUED 登记。
        TimerOpResult q = e.timerOp(await(2, 21), NOW + 1_000L, CFG, notify);
        assertThat(q.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(q.marked()).isFalse();
        assertThat(q.generation()).isEqualTo(1); // 受理时刻三元组为观察值
        // 同请求重发：幂等命中在集项，不二次入集。
        assertThat(e.timerOp(await(2, 21), NOW + 2_000L, CFG, notify).outcome())
                .isEqualTo(Outcome.QUEUED);
        assertThat(e.waiterCount()).isEqualTo(1);
        // 恰等与之后：即刻 GRANTED（谓词三时点边界）。
        TimerOpResult at = e.timerOp(await(3, 31), NOW + 5_000L, CFG, notify);
        assertThat(at.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(at.marked()).isTrue();
        assertThat(e.timerOp(query(3, 32), NOW + 6_000L, CFG, notify).marked()).isTrue();
    }

    @Test
    void rescheduleClearsStickyMarkForNewRound() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 1_000L), NOW, CFG, notify);
        assertThat(e.timerOp(await(2, 21), NOW + 2_000L, CFG, notify).marked()).isTrue();
        // 重装载：换代清钟，此后到达者落入新一轮阻塞（round 语义无需"已见"历史）。
        e.timerOp(schedule(1, 12, 9_000L), NOW + 2_000L, CFG, notify);
        TimerOpResult r = e.timerOp(await(2, 22), NOW + 2_000L, CFG, notify);
        assertThat(r.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(r.generation()).isEqualTo(2);
    }

    @Test
    void rescheduleMovesPendingWaitersToNewClock() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 10_000L), NOW, CFG, notify);
        e.timerOp(await(2, 21), NOW, CFG, notify); // 原等 T=now+10000
        // 推后改期：在等者多睡（合法方向，契约句），tick 前不出集。
        e.timerOp(schedule(1, 12, 30_000L), NOW + 1_000L, CFG, notify);
        assertThat(e.wakeReadyHeads(NOW + 10_000L, 0L, notify)).isZero();
        // 提前改期：先响（无害方向）。
        e.timerOp(schedule(1, 13, 500L), NOW + 10_000L, CFG, notify);
        assertThat(e.wakeReadyHeads(NOW + 10_500L, 0L, notify)).isEqualTo(1);
        assertThat(notify).hasSize(1);
        // 唤醒出集后重发即终态了结。
        notify.clear();
        assertThat(e.timerOp(await(2, 21), NOW + 10_600L, CFG, notify).marked()).isTrue();
    }

    @Test
    void tickWakesAllWaitersOnceAndStickyAfter() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        e.timerOp(await(2, 21), NOW, CFG, notify);
        e.timerOp(await(3, 31), NOW + 100L, CFG, notify);
        assertThat(e.wakeReadyHeads(NOW + 2_000L, 0L, notify)).isZero(); // 未到期零动作
        assertThat(e.wakeReadyHeads(NOW + 5_000L, 0L, notify)).isEqualTo(2); // 全员出集
        assertThat(notify).extracting(Waiter::lockType)
                .containsOnly(io.github.lamspace.openlatch.core.LockType.TIMER);
        assertThat(e.waiterCount()).isZero();
        // 无唤醒滞留：到期后新到达者不经等待即刻共见。
        assertThat(e.timerOp(await(4, 41), NOW + 5_000L, CFG, notify).marked()).isTrue();
        // 已到期无等待者：扫描恒零（fire 的"发生"无需通知不在场者）。
        assertThat(e.wakeReadyHeads(NOW + 9_000L, 0L, notify)).isZero();
    }

    @Test
    void disarmsTerminalWithEventDrivenWake() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        e.timerOp(await(2, 21), NOW, CFG, notify);
        e.timerOp(await(3, 31), NOW, CFG, notify);
        TimerOpResult d = e.timerOp(disarm(1, 12), NOW + 1_000L, CFG, notify);
        assertThat(d.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(d.disarmed()).isTrue();
        assertThat(d.armed()).isFalse();
        assertThat(d.fireAtMs()).isEqualTo(NOW + 5_000L); // 历史观察值保持不重写
        assertThat(notify).hasSize(2); // 撤销唤醒靠事件即时，不等到期 tick
        // 唤醒出集后重发 → DENIED 终态（等待不可满足的显式形态）。
        TimerOpResult r = e.timerOp(await(2, 21), NOW + 1_000L, CFG, notify);
        assertThat(r.outcome()).isEqualTo(Outcome.DENIED);
        assertThat(r.marked()).isFalse();
        // DISARM 请求本身恒终结当代：新到达等待者同样 DENIED（DISARMED 优先于
        // 同代到期成立——竞态当值裁决的撤销侧）。
        assertThat(e.timerOp(await(5, 51), NOW + 9_000L, CFG, notify).outcome())
                .isEqualTo(Outcome.DENIED);
    }

    @Test
    void disarmIdempotentEchoAndRescheduleAfterDisarm() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        e.timerOp(disarm(1, 12), NOW + 100L, CFG, notify);
        notify.clear();
        // 同 rid 重发撤销：回声零迁移零唤醒。
        TimerOpResult replay = e.timerOp(disarm(1, 12), NOW + 200L, CFG, notify);
        assertThat(replay.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(replay.disarmed()).isFalse();
        assertThat(notify).isEmpty();
        // 异 rid 再撤销（已 DISARMED）：幂等 OK 零迁移零唤醒。
        TimerOpResult again = e.timerOp(disarm(1, 13), NOW + 300L, CFG, notify);
        assertThat(again.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(again.disarmed()).isFalse();
        assertThat(notify).isEmpty();
        // 撤销后再装载：开新代回到 PENDING（代终结不粘滞跨代）。
        TimerOpResult re = e.timerOp(schedule(1, 14, 1_000L), NOW + 400L, CFG, notify);
        assertThat(re.generation()).isGreaterThan(again.generation());
        assertThat(re.armed()).isTrue();
        assertThat(e.timerOp(await(2, 21), NOW + 400L, CFG, notify).outcome())
                .isEqualTo(Outcome.QUEUED);
    }

    @Test
    void cancelAwaitIdempotentNoLedgerTouch() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        e.timerOp(await(2, 21), NOW, CFG, notify);
        long genBefore = e.generation();
        TimerOpResult c = e.timerOp(cancel(2, 22, 21), NOW, CFG, notify);
        assertThat(c.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(e.waiterCount()).isZero();
        // 幂等：重复撤销/撤销不存在项恒 OK，账簿零扰动。
        assertThat(e.timerOp(cancel(2, 23, 21), NOW, CFG, notify).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.generation()).isEqualTo(genBefore);
        assertThat(e.armed()).isTrue();
    }

    @Test
    void deathRemovesWaitersOnlyLedgerUntouched() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        e.timerOp(await(1, 21), NOW, CFG, notify);
        e.timerOp(await(2, 31), NOW, CFG, notify);
        // 装载者会话死亡：其等待位摘除，账簿三值零扰动——"死亡钟照响"的条目侧。
        e.removeSession(1, NOW + 100L, 5_000L, notify);
        assertThat(e.waiterCount()).isEqualTo(1);
        assertThat(e.generation()).isEqualTo(1);
        assertThat(e.armed()).isTrue();
        assertThat(e.fireAtMs()).isEqualTo(NOW + 5_000L);
        // 死者的装载去重槽行随会话摘除（回声无重放方）；三元组契约面不动。
        assertThat(e.replicatedState().slots()).noneMatch(sl -> sl.sessionId() == 1L);
        assertThat(notify).isEmpty(); // 死亡不代为唤醒
        // 死亡会话的等待位不可再触达（重发按新登记处理，无幽灵槽）。
        assertThat(e.wakeReadyHeads(NOW + 5_000L, 0L, notify)).isEqualTo(1);
        assertThat(notify).extracting(Waiter::sessionId).containsOnly(2L);
    }

    @Test
    void awaitDepthGuardrailSharedWithQueues() {
        // 深度护栏骑 maxQueueDepthPerKey 合并口径的条目侧形态：1 上限。
        CoreConfig cfg = new CoreConfig(30_000L, 1_000L, 3_600_000L, 5_000L, 512, 1, 1024,
                86_400_000L);
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(1, 11, 5_000L), NOW, CFG, notify);
        assertThat(e.timerOp(await(2, 21), NOW, cfg, notify).outcome())
                .isEqualTo(Outcome.QUEUED);
        TimerOpResult over = e.timerOp(await(3, 31), NOW, cfg, notify);
        assertThat(over.outcome()).isEqualTo(Outcome.REJECT_QUEUE_FULL); // 线路 OVERLOADED
        assertThat(e.waiterCount()).isEqualTo(1); // 超限零入集
    }

    @Test
    void clusterModeRejectsLocalOpsAndCollectsNoWake() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        // 集群形态：AWAIT/CANCEL/QUERY 抵达即拒（门面分派缺陷守卫，判例 phaser）。
        assertThat(e.timerOp(await(1, 11), NOW, CFG, notify, true).outcome())
                .isEqualTo(Outcome.REJECT_TIMER_NO_ENTRY);
        assertThat(e.timerOp(cancel(1, 12, 1), NOW, CFG, notify, true).outcome())
                .isEqualTo(Outcome.REJECT_TIMER_NO_ENTRY);
        assertThat(e.timerOp(query(1, 13), NOW, CFG, notify, true).outcome())
                .isEqualTo(Outcome.REJECT_TIMER_NO_ENTRY);
        // DISARM 在集群形态仍迁移账簿，但不采集条目内唤醒（排空在网关簿记侧）。
        e.timerOp(schedule(1, 14, 5_000L), NOW, CFG, notify, true);
        TimerOpResult d = e.timerOp(disarm(1, 15), NOW + 100L, CFG, notify, true);
        assertThat(d.disarmed()).isTrue(); // 标记驱动网关排空
        assertThat(notify).isEmpty();
        assertThat(e.timerOp(await(2, 21), NOW, CFG, notify).outcome())
                .isEqualTo(Outcome.DENIED); // 单机形态谓词一致（代终结跨形态同判）
    }

    @Test
    void lifecycleContractsArePhaserShaped() {
        TimerEntry e = new TimerEntry(K);
        assertThat(e.isEmpty()).isFalse(); // 条目存续不回收（代终结亦保留读数）
        assertThat(e.leaseToken()).isZero();
        assertThat(e.leaseExpiresAtMs()).isZero();
        assertThat(e.sweepNotifiedHead(NOW, 5_000L, new ArrayList<>())).isFalse();
        assertThatThrownBy(() -> e.forceExpire(NOW, 5_000L, new ArrayList<>()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void restoreRoundTripDeterministicExport() {
        TimerEntry e = new TimerEntry(K);
        List<Waiter> notify = new ArrayList<>();
        e.timerOp(schedule(101, 11, 5_000L), NOW, CFG, notify);
        e.timerOp(schedule(102, 12, 8_000L), NOW + 100L, CFG, notify); // 覆盖式：代次 2
        e.timerOp(disarm(100, 13), NOW + 200L, CFG, notify);
        e.timerOp(await(2, 21), NOW + 300L, CFG, notify); // 等待集不入导出
        TimerEntry.ReplicatedState st = e.replicatedState();
        assertThat(st.generation()).isEqualTo(2); // DISARM 不推代次（代终结保持当代）
        assertThat(st.armed()).isFalse();
        assertThat(st.slots()).extracting(TimerEntry.Slot::sessionId)
                .containsExactly(101L, 102L, 100L); // 账簿插入序=首装先后（跨副本确定性）
        // restored 直写：等待集为空、三值保真、代次不回退、命中槽重发单代。
        TimerEntry r = TimerEntry.restored(K, st.generation(), st.armed(), st.fireAtMs(),
                st.slots());
        assertThat(r.replicatedState()).isEqualTo(st);
        assertThat(r.waiterCount()).isZero();
        assertThat(r.timerOp(disarm(100, 13), NOW + 400L, CFG, notify).generation())
                .isEqualTo(2); // 恢复槽命中回声：不双迁移
        // 同账簿终态不同等待集：导出逐字节等（等待集零快照足迹的条目侧对偶）。
        TimerEntry q = new TimerEntry(K);
        q.timerOp(schedule(101, 11, 5_000L), NOW, CFG, notify);
        q.timerOp(schedule(102, 12, 8_000L), NOW + 100L, CFG, notify);
        q.timerOp(disarm(100, 13), NOW + 200L, CFG, notify);
        assertThat(q.replicatedState()).isEqualTo(r.replicatedState());
    }

    @Test
    void timerStateSelfConsistencyGuards() {
        // TimerState 构造钉定：代次 ≥1、槽每会话至多一行。
        assertThatThrownBy(() -> new CoreStateRestore.TimerState(0, true, 1L, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        List<TimerEntry.Slot> dup = List.of(
                new TimerEntry.Slot(1, 1, true, 1, 9),
                new TimerEntry.Slot(1, 2, false, 1, 9));
        assertThatThrownBy(() -> new CoreStateRestore.TimerState(1, true, 9L, dup))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
