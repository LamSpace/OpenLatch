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
import io.github.lamspace.openlatch.core.command.BarrierAwaitCommand;
import io.github.lamspace.openlatch.core.command.PhaserOpCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.snapshot.CoreStateRestore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CoreEngine 相位器门面用例组：REGISTER 建条目与无条目拒绝矩阵、跨家族
 * 互拒（PHASER×BARRIER×LOCK×QUEUE 双向）、ACQUIRE 携 phaser 定型入口
 * 拒绝、受理通道配额上限与复制通道不复核的分岔、会话触及与死亡摘除事件、
 * 观察面读数（waiterCount 合计/明细视图/gauge）与恢复分支。手工时钟，
 * 无 sleep。
 */
class CoreEnginePhaserTest {

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
    private static PhaserOpCommand op(long s, long r, String key, PhaserOpType t,
            int parties, Long expected, long awaitReq) {
        return new PhaserOpCommand(s, r, key, t, parties, expected, awaitReq);
    }

    /** 注册工厂。 */
    private static PhaserOpCommand register(long s, long r, String key, int parties) {
        return op(s, r, key, PhaserOpType.REGISTER, parties, null, 0);
    }

    /** 到场工厂。 */
    private static PhaserOpCommand arrive(long s, long r, String key) {
        return op(s, r, key, PhaserOpType.ARRIVE, 0, null, 0);
    }

    /** 纯等待工厂。 */
    private static PhaserOpCommand await(long s, long r, String key, long expected) {
        return op(s, r, key, PhaserOpType.AWAIT_ADVANCE, 0, expected, 0);
    }

    @Test
    void creationAndNoEntryMatrix() {
        long a = engine.sessionOpened();
        // 非 REGISTER 无条目：拒绝且不隐式建条目。
        assertThat(engine.phaserOp(arrive(a, 1, "p")).outcome())
                .isEqualTo(Outcome.REJECT_PHASER_NO_ENTRY);
        assertThat(engine.phaserOp(await(a, 2, "p", 0)).outcome())
                .isEqualTo(Outcome.REJECT_PHASER_NO_ENTRY);
        assertThat(engine.inspectKey("p")).isNull();
        assertThat(engine.phaserOp(op(a, 3, "p", PhaserOpType.QUERY, 0, null, 0)).outcome())
                .isEqualTo(Outcome.REJECT_PHASER_NO_ENTRY);
        // REGISTER 建条目并定型 PHASER 家族。
        assertThat(engine.phaserOp(register(a, 4, "p", 1)).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.inspectKey("p").family()).isEqualTo(KeyFamily.PHASER);
        // 会话预检。
        assertThat(engine.phaserOp(register(999, 5, "p2", 1)).outcome())
                .isEqualTo(Outcome.REJECT_SESSION);
    }

    @Test
    void crossFamilyMatrixPhaserBarrierLockQueue() {
        long a = engine.sessionOpened();
        engine.barrierAwait(new BarrierAwaitCommand(a, 1, "bk", 2, false));
        assertThat(engine.phaserOp(register(a, 2, "bk", 1)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        engine.acquire(new AcquireCommand(a, 3, "lk", LockType.REENTRANT, 1, 30_000, true));
        assertThat(engine.phaserOp(register(a, 4, "lk", 1)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        engine.phaserOp(register(a, 5, "pk", 1));
        // 反向：phaser key 上屏障/队列/获取请求互拒。
        assertThat(engine.barrierAwait(new BarrierAwaitCommand(a, 6, "pk", 2, false)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.acquire(new AcquireCommand(a, 7, "pk", LockType.REENTRANT, 1,
                30_000, true)).outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.inspectKey("pk").barrierGeneration()).isZero();
    }

    @Test
    void acquireCarryingPhaserTypeRejected() {
        long a = engine.sessionOpened();
        assertThat(engine.acquire(new AcquireCommand(a, 1, "x", LockType.PHASER, 1,
                30_000, true)).outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.inspectKey("x")).isNull();
    }

    @Test
    void acceptChannelEnforcesCapApplyChannelDoesNot() {
        CoreConfig cap2 = new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                CoreConfig.MAX_KEY_LENGTH, CoreConfig.MAX_QUEUE_DEPTH_PER_KEY, 2);
        CoreEngine capped = new CoreEngine(cap2, clock, listener);
        long a = capped.sessionOpened();
        assertThat(capped.phaserOp(register(a, 1, "p", 2)).outcome()).isEqualTo(Outcome.GRANTED);
        // 受理通道：加计超限拒绝、账簿零扰动。
        assertThat(capped.phaserOp(register(a, 2, "p", 1)).outcome())
                .isEqualTo(Outcome.REJECT_PHASER_PARTIES);
        assertThat(capped.inspectKey("p").phaserRegistered()).isEqualTo(2);
        // 复制通道恒宽容（已提交条目照常应用——配置漂移不撕裂账簿）。
        assertThat(capped.phaserApply(register(a, 2, "p", 1)).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(capped.inspectKey("p").phaserRegistered()).isEqualTo(3);
    }

    @Test
    void tripWakesWaitersAndDeathTripsViaSessionClosed() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();
        engine.phaserOp(register(a, 1, "p", 1));
        engine.phaserOp(register(b, 2, "p", 1));
        engine.phaserOp(register(c, 3, "p", 1));
        engine.phaserOp(await(b, 4, "p", 0));
        engine.phaserOp(arrive(a, 5, "p"));
        listener.clear();
        // c 死亡：registered 3→2、arrived 1——未推进；a+b 到场推进，b 收唤醒。
        engine.sessionClosed(c);
        assertThat(engine.inspectKey("p").phaserRegistered()).isEqualTo(2);
        engine.phaserOp(arrive(b, 6, "p"));
        assertThat(listener.events()).anySatisfy(ev -> {
            assertThat(ev.sessionId()).isEqualTo(b);
            assertThat(ev.requestId()).isEqualTo(4);
            assertThat(ev.key()).isEqualTo("p");
        });
        assertThat(engine.inspectKey("p").phaserPhase()).isEqualTo(1);
    }

    @Test
    void observationSurfacesCountPhaserWaiters() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.phaserOp(register(a, 1, "p", 3));
        engine.phaserOp(await(b, 2, "p", 0));
        engine.phaserOp(await(a, 3, "p", 0));
        CoreStats stats = engine.stats();
        assertThat(stats.totalWaiters()).isEqualTo(2); // 等待就是等待（v9 口径延伸）
        assertThat(engine.phaserWaiters("p"))
                .extracting(io.github.lamspace.openlatch.core.lock.PhaserEntry.WaiterView::requestId)
                .containsExactly(2L, 3L);
        assertThat(engine.phaserParties("p"))
                .extracting(io.github.lamspace.openlatch.core.lock.PhaserEntry.PartyView::parties)
                .containsExactly(3);
        assertThat(engine.maxPhaserRegistered()).isEqualTo(3);
        // 读数零扰动：不推进任何状态。
        assertThat(engine.phaserOp(op(b, 9, "p", PhaserOpType.QUERY, 0, null, 0)).arrived())
                .isZero();
    }

    @Test
    void restoreBranchRebuildsLedgerAndKeepsMonotonic() {
        CoreEngine fresh = new CoreEngine(new CoreConfig(), clock, listener);
        fresh.restoreFrom(new CoreStateRestore(List.of(
                new CoreStateRestore.Entry("p", LockType.PHASER, 0, 0, 0, List.of(),
                        0, 0, 0, null, null, null, null,
                        new CoreStateRestore.PhaserState(5, 2, 1,
                                List.of(new io.github.lamspace.openlatch.core.lock.PhaserEntry
                                        .PartyView(101L, 2, 88L, 4L)),
                                List.of(new io.github.lamspace.openlatch.core.lock.PhaserEntry
                                        .Arrival(101L, 77L)),
                                4, List.of()))),
                List.of(101L), 9L));
        assertThat(fresh.inspectKey("p").phaserPhase()).isEqualTo(5);
        assertThat(fresh.inspectKey("p").phaserArrived()).isEqualTo(1);
        // 自恢复账簿续运转（推进不回退）：注册+一次到场 2→3 应到、2<3 不推进。
        assertThat(fresh.phaserOp(register(101, 80, "p", 1)).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(fresh.phaserOp(arrive(101, 81, "p")).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(fresh.inspectKey("p").phaserPhase()).isEqualTo(5);
        assertThat(fresh.inspectKey("p").phaserArrived()).isEqualTo(2);
        // 补齐即推进 6（单调不回退）。
        assertThat(fresh.sessionOpened()).isPositive();
        assertThat(fresh.inspectKey("p")).isNotNull();
    }
}
