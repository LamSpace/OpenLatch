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
import io.github.lamspace.openlatch.core.PhaserOpType;
import io.github.lamspace.openlatch.core.command.PhaserOpCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.PhaserOpResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PhaserEntry 相位账簿状态机逐格用例：注册配额增减、到场去重与跨推进
 * 重发、合拢单推进、离场扣减与提前合拢、死亡摘除两形态（已到场/未
 * 到场）、归零空转与复活、等待登记幂等与谓词即刻了结、深度护栏分轨
 * （纯等待严、到场族宽容）、复制态导出与恢复往返。全部同步直调，无
 * 网络无 sleep。
 */
class PhaserEntryTest {

    /** 测试用键。 */
    private static final String K = "p";
    /** 缺省限额配置。 */
    private static final CoreConfig CFG = new CoreConfig();
    /** 固定测试时刻。 */
    private static final long NOW = 1_000L;

    /** 注册命令工厂。 */
    private static PhaserOpCommand register(long s, long r, int parties) {
        return new PhaserOpCommand(s, r, K, PhaserOpType.REGISTER, parties, null, 0);
    }

    /** 到场命令工厂。 */
    private static PhaserOpCommand arrive(long s, long r) {
        return new PhaserOpCommand(s, r, K, PhaserOpType.ARRIVE, 0, null, 0);
    }

    /** 到场并等待命令工厂。 */
    private static PhaserOpCommand arriveAwait(long s, long r) {
        return new PhaserOpCommand(s, r, K, PhaserOpType.ARRIVE_AND_AWAIT, 0, null, 0);
    }

    /** 到场并离场命令工厂。 */
    private static PhaserOpCommand arriveDereg(long s, long r) {
        return new PhaserOpCommand(s, r, K, PhaserOpType.ARRIVE_AND_DEREGISTER, 0, null, 0);
    }

    /** 纯等待命令工厂。 */
    private static PhaserOpCommand await(long s, long r, long expected) {
        return new PhaserOpCommand(s, r, K, PhaserOpType.AWAIT_ADVANCE, 0, expected, 0);
    }

    /** 撤销命令工厂。 */
    private static PhaserOpCommand cancel(long s, long r, long target) {
        return new PhaserOpCommand(s, r, K, PhaserOpType.CANCEL, 0, null, target);
    }

    /** 读数命令工厂。 */
    private static PhaserOpCommand query(long s, long r) {
        return new PhaserOpCommand(s, r, K, PhaserOpType.QUERY, 0, null, 0);
    }

    /**
     * 构造指定等待深度上限的测试配置。
     *
     * @param depth 单 key 等待深度上限
     * @return 配置
     */
    private static CoreConfig depthConfig(int depth) {
        return new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                CoreConfig.MAX_KEY_LENGTH, depth);
    }

    @Test
    void registerArriveTripPhaseMonotonic() {
        PhaserEntry e = new PhaserEntry(K);
        assertThat(e.family()).isEqualTo(KeyFamily.PHASER);
        assertThat(e.snapshot(NOW).phaserPhase()).isZero();
        // 注册 3 方（两会话分摊配额）。
        assertThat(e.phaserOp(register(1, 10, 2), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.phaserOp(register(2, 20, 1), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.registered()).isEqualTo(3);
        // 注册不触发推进判定、不视为到场。
        assertThat(e.arrived()).isZero();
        assertThat(e.phase()).isZero();
        // 两到场：回显到场相位 0，未合拢。
        PhaserOpResult a1 = e.phaserOp(arrive(1, 30), NOW, CFG, new ArrayList<>());
        PhaserOpResult a2 = e.phaserOp(arrive(2, 40), NOW, CFG, new ArrayList<>());
        assertThat(a1.phase()).isZero();
        assertThat(a2.arrived()).isEqualTo(2);
        // 第三到场恰合拢：直答回显到场相位 0，相位推进 1、计数清零。
        List<Waiter> notify = new ArrayList<>();
        PhaserOpResult a3 = e.phaserOp(arrive(1, 50), NOW, CFG, notify);
        assertThat(a3.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(a3.phase()).isZero();
        assertThat(a3.arrived()).isEqualTo(0);
        assertThat(e.phase()).isEqualTo(1);
        assertThat(notify).isEmpty();
        // 跨相位注册存续（配额不因推进丢失），新一轮两到场即再推进（3 方应到）。
        e.phaserOp(arrive(1, 60), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(2, 70), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 80), NOW, CFG, new ArrayList<>());
        assertThat(e.phase()).isEqualTo(2);
    }

    @Test
    void registerReplayIsIdempotentPerSession() {
        PhaserEntry e = new PhaserEntry(K);
        PhaserOpResult first = e.phaserOp(register(1, 10, 2), NOW, CFG, new ArrayList<>());
        assertThat(first.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(e.registered()).isEqualTo(2);
        // 同请求重发（应答丢失）：幂等回显首计时到场相位，不双加配额。
        PhaserOpResult replay = e.phaserOp(register(1, 10, 2), NOW, CFG, new ArrayList<>());
        assertThat(replay.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(replay.registered()).isEqualTo(2);
        assertThat(e.registered()).isEqualTo(2);
        // 新请求再注册 = 新配额（JDK register() 可重复调用的加法语义）。
        assertThat(e.phaserOp(register(1, 11, 1), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.registered()).isEqualTo(3);
        // A_D 扣减保留幂等槽（离场后旧 rid 不再误判）。
        assertThat(e.phaserOp(arriveDereg(1, 12), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.replicatedState().parties().get(0).lastRegisterRequestId()).isEqualTo(11L);
    }

    @Test
    void arriveDedupWithinPhase() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 3), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 30), NOW, CFG, new ArrayList<>());
        PhaserOpResult replay = e.phaserOp(arrive(1, 30), NOW, CFG, new ArrayList<>());
        assertThat(replay.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(e.arrived()).isEqualTo(1);
        assertThat(replay.arrived()).isEqualTo(1);
    }

    @Test
    void arriveReissueAcrossTripEchoesPrevWindow() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 2), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 30), NOW, CFG, new ArrayList<>());
        // 另一到场完成合拢（相位 0 → 1）。
        assertThat(e.phaserOp(arrive(1, 40), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.phase()).isEqualTo(1);
        // 旧请求重发：命中换代窗口——终态回显 prevPhase=0，不双计入新相位。
        PhaserOpResult late = e.phaserOp(arrive(1, 30), NOW, CFG, new ArrayList<>());
        assertThat(late.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(late.phase()).isZero();
        assertThat(e.arrived()).isZero();
        assertThat(e.phase()).isEqualTo(1);
        // 再推进一次后窗口滚动：更早的重发按新到场计（声明竞态窗口下界）。
        e.phaserOp(arrive(1, 50), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 60), NOW, CFG, new ArrayList<>());
        assertThat(e.phase()).isEqualTo(2);
        assertThat(e.phaserOp(arrive(1, 30), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.arrived()).isEqualTo(1);
    }

    @Test
    void arriveAndAwaitTrippedDirectlyNotQueued() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 2), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 20), NOW, CFG, new ArrayList<>());
        // 合拢恰由本次到场触达：直答 OK，不发生等待登记。
        PhaserOpResult trip = e.phaserOp(arriveAwait(1, 30), NOW, CFG, new ArrayList<>());
        assertThat(trip.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(trip.phase()).isZero();
        assertThat(e.waiterCount()).isZero();
    }

    @Test
    void arriveAndDeregisterQuotaEarlyTrip() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 2), NOW, CFG, new ArrayList<>());
        e.phaserOp(register(2, 20, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(arriveAwait(1, 30), NOW, CFG, new ArrayList<>()); // arrived=1 queued
        assertThat(e.waiterCount()).isEqualTo(1);
        // s2 到场+离场：arrived=2、registered 3→2 → 2≥2 合拢并唤醒 s1。
        List<Waiter> notify = new ArrayList<>();
        PhaserOpResult ad = e.phaserOp(arriveDereg(2, 40), NOW, CFG, notify);
        assertThat(ad.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(e.registered()).isEqualTo(2); // 3 减 s2 的 1
        assertThat(e.phase()).isEqualTo(1);
        assertThat(notify).hasSize(1);
        assertThat(notify.get(0).requestId()).isEqualTo(30);
        assertThat(e.waiterCount()).isZero();
        // s1 的挂起应答丢失重发（在场槽仍在当前？推进已清槽——命中换代窗）：
        // 重发终态回显、不重新挂起。
        PhaserOpResult replay = e.phaserOp(arriveAwait(1, 30), NOW, CFG, new ArrayList<>());
        assertThat(replay.outcome()).isIn(Outcome.GRANTED, Outcome.QUEUED);
    }

    @Test
    void arriveAndDeregisterZeroQuotaRejected() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 1), NOW, CFG, new ArrayList<>());
        PhaserOpResult bad = e.phaserOp(arriveDereg(2, 20), NOW, CFG, new ArrayList<>());
        assertThat(bad.outcome()).isEqualTo(Outcome.REJECT_PHASER_QUOTA);
        assertThat(e.arrived()).isZero();
        assertThat(e.registered()).isEqualTo(1);
    }

    @Test
    void midRegisterEntersCurrentPhaseObligation() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 20), NOW, CFG, new ArrayList<>());
        assertThat(e.phase()).isEqualTo(1); // 1/1 合拢
        // 新周期：registered 1、arrived 0；中途注册 1 方扩大应到集合。
        e.phaserOp(register(2, 30, 1), NOW, CFG, new ArrayList<>());
        assertThat(e.phase()).isEqualTo(1); // 注册不触发推进
        e.phaserOp(arrive(1, 40), NOW, CFG, new ArrayList<>());
        assertThat(e.phase()).isEqualTo(1); // 1/2 不推进
        e.phaserOp(arrive(2, 50), NOW, CFG, new ArrayList<>());
        assertThat(e.phase()).isEqualTo(2); // 2/2 推进
    }

    @Test
    void deathOfArrivedKeepsArrivedCountAndTrips() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(register(2, 20, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(register(3, 30, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 40), NOW, CFG, new ArrayList<>()); // s1 到场后死亡
        e.phaserOp(arriveAwait(2, 50), NOW, CFG, new ArrayList<>());
        List<Waiter> notify = new ArrayList<>();
        e.removeSession(1, NOW, CoreConfig.HEAD_REPLY_TIMEOUT_MS, notify);
        // registered 3→2、arrived 保持 2 → 合拢、唤醒 s2。
        assertThat(e.registered()).isEqualTo(2);
        assertThat(e.phase()).isEqualTo(1);
        assertThat(notify).hasSize(1);
        assertThat(notify.get(0).requestId()).isEqualTo(50);
        // 换代窗口中的死者槽被摘除（不泄漏内部 sid）。
        PhaserEntry.ReplicatedState rs = e.replicatedState();
        assertThat(rs.prevArrivals()).noneMatch(a -> a.sessionId() == 1);
        assertThat(rs.parties()).noneMatch(p -> p.sessionId() == 1);
    }

    @Test
    void deathOfUnarrivedShrinksObligationNoStarve() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(register(2, 20, 2), NOW, CFG, new ArrayList<>()); // s2 配额 2
        // s2 死亡：registered 3→1、arrived 0 → 不推进（无事实可推进）。
        e.removeSession(2, NOW, CoreConfig.HEAD_REPLY_TIMEOUT_MS, new ArrayList<>());
        assertThat(e.registered()).isEqualTo(1);
        assertThat(e.phase()).isZero();
        // s1 到场即合拢——未被死者饿死。
        e.phaserOp(arrive(1, 30), NOW, CFG, new ArrayList<>());
        assertThat(e.phase()).isEqualTo(1);
    }

    @Test
    void zeroRegisteredIdleSpinThenRevive() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(arriveDereg(1, 20), NOW, CFG, new ArrayList<>());
        assertThat(e.registered()).isZero();
        assertThat(e.phase()).isEqualTo(1); // 该到场事实推进了相位 0
        assertThat(e.isEmpty()).isFalse();
        // 空转：无终止、无粘滞——新注册自当前相位复活运转。
        e.phaserOp(register(2, 30, 1), NOW, CFG, new ArrayList<>());
        assertThat(e.arrived()).isZero();
        e.phaserOp(arrive(2, 40), NOW, CFG, new ArrayList<>());
        assertThat(e.phase()).isEqualTo(2);
    }

    @Test
    void allUnarrivedDeathIdleNoAdvance() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 2), NOW, CFG, new ArrayList<>());
        e.removeSession(1, NOW, CoreConfig.HEAD_REPLY_TIMEOUT_MS, new ArrayList<>());
        // registered 归零、arrived 0 → 空转不推进。
        assertThat(e.registered()).isZero();
        assertThat(e.phase()).isZero();
    }

    @Test
    void awaitAdvancePredicateIdempotentAndTripWake() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 20), NOW, CFG, new ArrayList<>()); // phase→1
        // 已越相位：即刻了结。
        assertThat(e.phaserOp(await(2, 30, 0), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        // 等当前相位：挂起。
        PhaserOpResult q = e.phaserOp(await(2, 40, 1), NOW, CFG, new ArrayList<>());
        assertThat(q.outcome()).isEqualTo(Outcome.QUEUED);
        // 同请求重发幂等续挂（不双登记）。
        assertThat(e.phaserOp(await(2, 40, 1), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        assertThat(e.waiterCount()).isEqualTo(1);
        // 合拢全员唤醒出集。
        List<Waiter> notify = new ArrayList<>();
        e.phaserOp(register(3, 50, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(3, 60), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 70), NOW, CFG, notify);
        assertThat(e.phase()).isEqualTo(2);
        assertThat(notify).extracting(Waiter::requestId).containsExactly(40L);
        assertThat(e.waiterCount()).isZero();
        // 被唤醒者重发取数：谓词即刻了结。
        PhaserOpResult done = e.phaserOp(await(2, 40, 1), NOW, CFG, new ArrayList<>());
        assertThat(done.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(done.phase()).isEqualTo(2);
    }

    @Test
    void awaitDepthGuardStrictForPureLenientForArrive() {
        CoreConfig cfg = depthConfig(2);
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 5), NOW, cfg, new ArrayList<>());
        assertThat(e.phaserOp(await(2, 20, 0), NOW, cfg, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        assertThat(e.phaserOp(await(3, 30, 0), NOW, cfg, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        // 第三纯等待：深度超限拒绝、等待集零扰动。
        assertThat(e.phaserOp(await(4, 40, 0), NOW, cfg, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.REJECT_QUEUE_FULL);
        assertThat(e.waiterCount()).isEqualTo(2);
        // 到场族宽容：第 4 方到场+等待在深度已满时仍登记（到场事实不被护栏绞杀）。
        PhaserOpResult ad = e.phaserOp(arriveAwait(5, 50), NOW, cfg, new ArrayList<>());
        assertThat(ad.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(e.arrived()).isEqualTo(1);
        assertThat(e.waiterCount()).isEqualTo(3);
    }

    @Test
    void cancelIdempotentAndNoWake() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 2), NOW, CFG, new ArrayList<>());
        assertThat(e.phaserOp(await(2, 20, 0), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        assertThat(e.phaserOp(cancel(2, 90, 20), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(e.waiterCount()).isZero();
        // 重复撤销幂等。
        assertThat(e.phaserOp(cancel(2, 91, 20), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
        // 合拢后原等待不再收唤醒（已出集）。
        List<Waiter> notify = new ArrayList<>();
        e.phaserOp(arrive(1, 30), NOW, CFG, notify);
        e.phaserOp(arrive(1, 40), NOW, CFG, notify);
        assertThat(notify).isEmpty();
    }

    @Test
    void queryPureRead() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 2), NOW, CFG, new ArrayList<>());
        e.phaserOp(arrive(1, 20), NOW, CFG, new ArrayList<>());
        PhaserOpResult q = e.phaserOp(query(9, 90), NOW, CFG, new ArrayList<>());
        assertThat(q.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(q.registered()).isEqualTo(2);
        assertThat(q.arrived()).isEqualTo(1);
        assertThat(e.snapshot(NOW).phaserArrived()).isEqualTo(1);
        assertThat(e.snapshot(NOW).phaserRegistered()).isEqualTo(2);
    }

    @Test
    void deathRemovesWaitsWithoutTouchingLedger() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(1, 10, 3), NOW, CFG, new ArrayList<>());
        assertThat(e.phaserOp(await(2, 20, 0), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED); // 旁观者（无配额）挂起
        e.removeSession(2, NOW, CoreConfig.HEAD_REPLY_TIMEOUT_MS, new ArrayList<>());
        assertThat(e.waiterCount()).isZero();
        assertThat(e.registered()).isEqualTo(3); // 无配额会话的死亡不动账簿
    }

    @Test
    void replicatedStateExportAndRestoreRoundTrip() {
        PhaserEntry e = new PhaserEntry(K);
        e.phaserOp(register(2, 10, 1), NOW, CFG, new ArrayList<>());
        e.phaserOp(register(1, 20, 2), NOW, CFG, new ArrayList<>());
        assertThat(e.phaserOp(await(1, 30, 0), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.QUEUED);
        e.phaserOp(arrive(2, 40), NOW, CFG, new ArrayList<>());
        PhaserEntry.ReplicatedState rs = e.replicatedState();
        // 配额与槽按账簿插入序（注册先后）导出——内部 sid 升序跨副本不可比，
        // 确定性由日志序承载。
        assertThat(rs.parties()).extracting(PhaserEntry.PartyView::sessionId)
                .containsExactly(2L, 1L);
        assertThat(rs.arrived()).isEqualTo(1);
        PhaserEntry r = PhaserEntry.restored(K, rs.phase(), rs.registered(), rs.arrived(),
                rs.parties(), rs.arrivals(), rs.prevPhase(), rs.prevArrivals());
        assertThat(r.replicatedState()).isEqualTo(rs);
        assertThat(r.waiterCount()).isZero(); // 等待集不入恢复
        // 续运转：补齐到场即推进。
        List<Waiter> notify = new ArrayList<>();
        r.phaserOp(arrive(1, 50), NOW, CFG, notify);
        r.phaserOp(arrive(1, 60), NOW, CFG, notify);
        assertThat(r.phase()).isEqualTo(1);
        // 重挂旁观等待命中推进即刻了结。
        assertThat(r.phaserOp(await(9, 70, 0), NOW, CFG, new ArrayList<>()).outcome())
                .isEqualTo(Outcome.GRANTED);
    }
}
