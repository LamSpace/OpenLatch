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
import io.github.lamspace.openlatch.core.command.BarrierActionDoneCommand;
import io.github.lamspace.openlatch.core.command.BarrierAwaitCommand;
import io.github.lamspace.openlatch.core.command.BarrierLeaveCommand;
import io.github.lamspace.openlatch.core.result.BarrierActionDoneResult;
import io.github.lamspace.openlatch.core.result.BarrierAwaitResult;
import io.github.lamspace.openlatch.core.result.BarrierFinal;
import io.github.lamspace.openlatch.core.result.Outcome;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * BarrierEntry 世代状态机逐格用例：到场合拢、世代回卷、幂等去重、
 * 了结窗口、队列满护栏、动作两阶段、离场即破障（超时/死亡/纯破障）、
 * 已通知陈旧项的出窗口径与清扫。全部同步直调，无网络无 sleep。
 */
class BarrierEntryTest {

    /** 测试用键。 */
    private static final String K = "b";
    /** 缺省限额配置。 */
    private static final CoreConfig CFG = new CoreConfig();
    /** 已通知等待者响应超时（与默认配置同源）。 */
    private static final long HRT = CoreConfig.HEAD_REPLY_TIMEOUT_MS;
    /** 固定测试时刻。 */
    private static final long NOW = 1_000L;

    /** 到场命令工厂。 */
    private static BarrierAwaitCommand await(long session, long req, long parties) {
        return new BarrierAwaitCommand(session, req, K, parties, false);
    }

    /** 携动作到场命令工厂。 */
    private static BarrierAwaitCommand awaitAction(long session, long req, long parties) {
        return new BarrierAwaitCommand(session, req, K, parties, true);
    }

    /** 离场命令工厂。 */
    private static BarrierLeaveCommand leave(long session, long awaitReq) {
        return new BarrierLeaveCommand(session, K, awaitReq);
    }

    /** 动作了结命令工厂。 */
    private static BarrierActionDoneCommand done(long session, long gen) {
        return new BarrierActionDoneCommand(session, K, gen);
    }

    /**
     * 构造指定队列深度上限的条目测试配置。
     *
     * @param depth 单 key 等待队列深度上限
     * @return 配置
     */
    private static CoreConfig depthConfig(int depth) {
        return new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                CoreConfig.MAX_KEY_LENGTH, depth);
    }

    @Test
    void arrivalsTripAndWrapReuse() {
        BarrierEntry e = new BarrierEntry(K, 3);
        assertThat(e.generation()).isEqualTo(1);
        assertThat(e.await(await(1, 10, 3), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        assertThat(e.await(await(2, 20, 3), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        List<Waiter> notify = new ArrayList<>();
        BarrierAwaitResult third = e.await(await(3, 30, 3), NOW, CFG, HRT, notify);
        // 当回合拢：直答放行，前两位广播（其重发按了结记录出队）。
        assertThat(third.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(third.generation()).isEqualTo(1);
        assertThat(notify).hasSize(2);
        assertThat(e.generation()).isEqualTo(2);
        assertThat(e.arrived()).isZero();
        // 前两位重发命中了结记录，幂等放行。
        assertThat(e.await(await(1, 10, 3), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.await(await(2, 20, 3), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        // 相位复用后重组：新一批到场进世代 2，位次从 1 起。
        BarrierAwaitResult next = e.await(await(4, 40, 0), NOW, CFG, HRT, new ArrayList<>());
        assertThat(next.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(next.generation()).isEqualTo(2);
        assertThat(e.snapshot(NOW).barrierGeneration()).isEqualTo(2);
    }

    @Test
    void resendIdempotentNoDoubleCount() {
        BarrierEntry e = new BarrierEntry(K, 2);
        BarrierAwaitResult first = e.await(await(1, 10, 2), NOW, CFG, HRT, new ArrayList<>());
        assertThat(first.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(first.queuePosition()).isEqualTo(1);
        // 应答丢失重发：去重命中，位次与世代回显一致，不重复计次。
        BarrierAwaitResult again = e.await(await(1, 10, 2), NOW, CFG, HRT, new ArrayList<>());
        assertThat(again.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(again.queuePosition()).isEqualTo(1);
        assertThat(e.arrived()).isEqualTo(1);
        // 若重复计次，下一到场将以 1<parties 被误合拢——此处断言仍需第 2 方。
        assertThat(e.await(await(2, 20, 0), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
    }

    @Test
    void partiesAssertionMismatchZeroPerturbation() {
        BarrierEntry e = new BarrierEntry(K, 3);
        e.await(await(1, 10, 0), NOW, CFG, HRT, new ArrayList<>());
        BarrierAwaitResult bad = e.await(await(2, 20, 4), NOW, CFG, HRT, new ArrayList<>());
        assertThat(bad.outcome()).isEqualTo(Outcome.REJECT_BARRIER_PARTIES);
        assertThat(bad.parties()).isEqualTo(3);
        assertThat(e.arrived()).isEqualTo(1);
        assertThat(e.generation()).isEqualTo(1);
    }

    @Test
    void queueFullRejectedWithoutCountingArrival() {
        BarrierEntry e = new BarrierEntry(K, 9);
        CoreConfig small = depthConfig(2);
        assertThat(e.await(await(1, 10, 0), NOW, small, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        assertThat(e.await(await(2, 20, 0), NOW, small, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        BarrierAwaitResult over = e.await(await(3, 30, 0), NOW, small, HRT, new ArrayList<>());
        assertThat(over.outcome()).isEqualTo(Outcome.REJECT_QUEUE_FULL);
        assertThat(e.arrived()).isEqualTo(2);
    }

    @Test
    void actionTwoPhaseHoldsReleaseUntilDone() {
        BarrierEntry e = new BarrierEntry(K, 2);
        assertThat(e.await(await(1, 10, 2), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        List<Waiter> notify = new ArrayList<>();
        BarrierAwaitResult last = e.await(awaitAction(2, 20, 2), NOW, CFG, HRT, notify);
        // 执行者标记挂起；其余等待者此刻不放行。
        assertThat(last.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(last.executor()).isTrue();
        assertThat(last.generation()).isEqualTo(1);
        assertThat(notify).isEmpty();
        // 非执行者回报被拒；执行者回报生效并广播。
        assertThat(e.actionDone(done(3, 1), NOW, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.REJECT_BARRIER_ACTION);
        List<Waiter> released = new ArrayList<>();
        BarrierActionDoneResult ok = e.actionDone(done(2, 1), NOW, HRT, released);
        assertThat(ok.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(ok.generation()).isEqualTo(1);
        assertThat(released).hasSize(1);
        assertThat(e.generation()).isEqualTo(2);
        // 执行者应答丢失重发与重复回报：幂等。
        assertThat(e.await(awaitAction(2, 20, 2), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.actionDone(done(2, 1), NOW, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
    }

    @Test
    void executorReportAfterBreakGetsBroken() {
        BarrierEntry e = new BarrierEntry(K, 2);
        e.await(await(1, 10, 2), NOW, CFG, HRT, new ArrayList<>());
        e.await(awaitAction(2, 20, 2), NOW, CFG, HRT, new ArrayList<>());
        // 等待方超时离场 → 离场即破障（执行者挂账保留于了结记录）。
        List<Waiter> notify = new ArrayList<>();
        assertThat(e.leave(leave(1, 10), NOW, HRT, notify).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(notify).isEmpty(); // 离场者即唯一在队者，无他方可广播
        // 执行者动作完成回报：世代已破，以 BARRIER_BROKEN 终结本方等待。
        assertThat(e.actionDone(done(2, 1), NOW, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.BARRIER_BROKEN);
        assertThat(e.generation()).isEqualTo(2);
    }

    @Test
    void leaveBreaksCurrentGenerationAndNotifiesPeers() {
        BarrierEntry e = new BarrierEntry(K, 3);
        e.await(await(1, 10, 0), NOW, CFG, HRT, new ArrayList<>());
        e.await(await(2, 20, 0), NOW, CFG, HRT, new ArrayList<>());
        List<Waiter> notify = new ArrayList<>();
        assertThat(e.leave(leave(1, 10), NOW, HRT, notify).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(notify).hasSize(1); // 会话 2 收破障广播
        assertThat(e.generation()).isEqualTo(2);
        assertThat(e.await(await(2, 20, 0), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.BARRIER_BROKEN);
        // 破障作用域限当世代：新到场开新世代正常计次（世代局部自愈）。
        assertThat(e.await(await(3, 30, 0), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        assertThat(e.await(await(3, 30, 0), NOW, CFG, HRT, new ArrayList<>()).queuePosition())
                .isEqualTo(1);
    }

    @Test
    void pureBreakAndUnknownTargetsAreIdempotent() {
        BarrierEntry e = new BarrierEntry(K, 3);
        // 纯破障主张：当前世代无到场记录 → 无操作，世代号不动。
        assertThat(e.leave(leave(1, 0), NOW, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.generation()).isEqualTo(1);
        // 有到场记录后纯破障：破该世代。
        e.await(await(2, 20, 0), NOW, CFG, HRT, new ArrayList<>());
        assertThat(e.leave(leave(1, 0), NOW, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.generation()).isEqualTo(2);
        // 无对应到场记录的目标离场：幂等无操作。
        assertThat(e.leave(leave(9, 99), NOW, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.generation()).isEqualTo(2);
    }

    @Test
    void deathOfArrivedParticipantBreaksOfPeersSilentForNonArrived() {
        BarrierEntry e = new BarrierEntry(K, 3);
        e.await(await(1, 10, 0), NOW, CFG, HRT, new ArrayList<>());
        e.await(await(2, 20, 0), NOW, CFG, HRT, new ArrayList<>());
        // 从未到场会话死亡：零扰动。
        List<Waiter> none = new ArrayList<>();
        e.removeSession(7, NOW, HRT, none);
        assertThat(none).isEmpty();
        assertThat(e.generation()).isEqualTo(1);
        // 到场者死亡：即时破障，同伴收广播并以 BROKEN 了结。
        List<Waiter> notify = new ArrayList<>();
        e.removeSession(1, NOW, HRT, notify);
        assertThat(notify).hasSize(1);
        assertThat(e.generation()).isEqualTo(2);
        assertThat(e.await(await(2, 20, 0), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.BARRIER_BROKEN);
    }

    @Test
    void deathAfterSettledDoesNotBreak() {
        BarrierEntry e = new BarrierEntry(K, 2);
        e.await(await(1, 10, 2), NOW, CFG, HRT, new ArrayList<>());
        e.await(await(2, 20, 2), NOW, CFG, HRT, new ArrayList<>());
        // 会话 1 重发了结后死亡：其到场属已完结世代，MUST NOT 破新世代。
        assertThat(e.await(await(1, 10, 0), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        e.await(await(3, 30, 0), NOW, CFG, HRT, new ArrayList<>()); // 新世代到场 1
        List<Waiter> notify = new ArrayList<>();
        e.removeSession(1, NOW, HRT, notify);
        assertThat(notify).isEmpty();
        assertThat(e.generation()).isEqualTo(2);
        assertThat(e.arrived()).isEqualTo(1);
    }

    @Test
    void staleNotifiedResendOutsideWindowCountsAsNewArrival() {
        BarrierEntry e = new BarrierEntry(K, 2);
        // 世代 1 合拢：会话 1 已通知但未重发。
        e.await(await(1, 10, 2), NOW, CFG, HRT, new ArrayList<>());
        e.await(await(2, 20, 2), NOW, CFG, HRT, new ArrayList<>());
        // 世代 2 合拢替换了结记录 → 会话 1 的旧重发出窗口。
        e.await(await(3, 30, 0), NOW, CFG, HRT, new ArrayList<>());
        e.await(await(4, 40, 0), NOW, CFG, HRT, new ArrayList<>());
        List<Waiter> notify = new ArrayList<>();
        BarrierAwaitResult stale = e.await(await(1, 10, 0), NOW, CFG, HRT, notify);
        // 出窗口径：陈旧在队残留静默摘除，本次按新世代到场计次。
        assertThat(stale.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(stale.generation()).isEqualTo(3);
        assertThat(e.generation()).isEqualTo(3);
        assertThat(e.arrived()).isEqualTo(1);
    }

    @Test
    void sweepOnlyDrainsNotifiedHead() {
        BarrierEntry e = new BarrierEntry(K, 2);
        e.await(await(1, 10, 0), NOW, CFG, HRT, new ArrayList<>());
        // 未通知的当前世代队首不被清扫（离场只能经 leave/removeSession）。
        assertThat(e.sweepNotifiedHead(NOW + HRT * 10, HRT, new ArrayList<>())).isFalse();
        // 合拢广播后：截止时刻前不清扫，超时后摘除遗弃的已通知队首。
        e.await(await(2, 20, 0), NOW, CFG, HRT, new ArrayList<>());
        assertThat(e.sweepNotifiedHead(NOW + 1, HRT, new ArrayList<>())).isFalse();
        assertThat(e.sweepNotifiedHead(NOW + HRT + 1, HRT, new ArrayList<>())).isTrue();
    }

    @Test
    void lifecycleContractReadings() {
        BarrierEntry e = new BarrierEntry(K, 2);
        assertThat(e.family()).isEqualTo(KeyFamily.BARRIER);
        assertThat(e.key()).isEqualTo(K);
        assertThat(e.isEmpty()).isFalse();
        assertThat(e.leaseToken()).isZero();
        assertThat(e.leaseExpiresAtMs()).isZero();
        assertThatThrownBy(() -> e.forceExpire(NOW, HRT, new ArrayList<>()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(e.snapshot(NOW).barrierLastFinal()).isNull();
        // 破障后了结形态读数可见。
        e.await(await(1, 10, 0), NOW, CFG, HRT, new ArrayList<>());
        e.leave(leave(1, 10), NOW, HRT, new ArrayList<>());
        assertThat(e.snapshot(NOW).barrierLastFinal()).isEqualTo(BarrierFinal.BROKEN);
    }

    @Test
    void restoredFactoryCarriesFullReplicatedState() {
        List<BarrierEntry.Arrival> cur = List.of(new BarrierEntry.Arrival(5, 50));
        List<BarrierEntry.Arrival> comp = List.of(new BarrierEntry.Arrival(1, 10));
        BarrierEntry e = BarrierEntry.restored(K, 3, 4, cur, 5, 50, 3,
                BarrierFinal.TRIPPED, comp, 5);
        assertThat(e.parties()).isEqualTo(3);
        assertThat(e.generation()).isEqualTo(4);
        assertThat(e.arrived()).isEqualTo(1);
        assertThat(e.actionSession()).isEqualTo(5);
        assertThat(e.actionRequest()).isEqualTo(50);
        assertThat(e.completedGeneration()).isEqualTo(3);
        assertThat(e.completedResult()).isEqualTo(BarrierFinal.TRIPPED);
        assertThat(e.completedArrivals()).containsExactly(new BarrierEntry.Arrival(1, 10));
        assertThat(e.completedExecutorSession()).isEqualTo(5);
        // 恢复后旧世代重发按了结记录裁决（不重演到场）。
        assertThat(e.await(await(1, 10, 0), NOW, CFG, HRT, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        // 执行者重发回执行者形态挂起（应答丢失兜底）。
        BarrierAwaitResult exec = e.await(awaitAction(5, 50, 0), NOW, CFG, HRT,
                new ArrayList<>());
        assertThat(exec.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(exec.executor()).isTrue();
        assertThat(exec.generation()).isEqualTo(4);
    }
}
