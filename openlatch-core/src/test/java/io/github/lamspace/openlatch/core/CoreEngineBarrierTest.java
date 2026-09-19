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
import io.github.lamspace.openlatch.core.command.BarrierActionDoneCommand;
import io.github.lamspace.openlatch.core.command.BarrierAwaitCommand;
import io.github.lamspace.openlatch.core.command.BarrierLeaveCommand;
import io.github.lamspace.openlatch.core.command.LatchAwaitCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * CoreEngine 循环屏障门面用例组：定型创建、纯加入拒绝、跨家族互拒矩阵
 * （LATCH×BARRIER 双向、锁×BARRIER、ACQUIRE 携屏障定型）、会话触及登记与
 * 死亡破障事件、动作了结的放行通知、观察面读数。手工时钟，无 sleep。
 */
class CoreEngineBarrierTest {

    /** 手工时钟。 */
    private MutableClock clock;
    /** 记录型监听器：破障/合拢广播事件断言对象。 */
    private RecordingListener listener;
    /** 被测引擎。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    /** 到场命令工厂。 */
    private BarrierAwaitCommand await(long s, long r, String key, long parties) {
        return new BarrierAwaitCommand(s, r, key, parties, false);
    }

    @Test
    void creationViaAssertedAwaitAndPureJoinRejected() {
        long a = engine.sessionOpened();
        // 不存在的屏障上无主张到场被拒。
        assertThat(engine.barrierAwait(await(a, 1, "b", 0)).outcome())
                .isEqualTo(Outcome.REJECT_BARRIER_PARTIES);
        // 非零主张定型创建。
        assertThat(engine.barrierAwait(await(a, 1, "b", 2)).outcome())
                .isEqualTo(Outcome.QUEUED);
        // 主张不符拒绝且不扰动。
        assertThat(engine.barrierAwait(await(a, 2, "b", 3)).outcome())
                .isEqualTo(Outcome.REJECT_BARRIER_PARTIES);
        // 会话预检。
        assertThat(engine.barrierAwait(await(999, 1, "b", 2)).outcome())
                .isEqualTo(Outcome.REJECT_SESSION);
        assertThat(engine.barrierLeave(new BarrierLeaveCommand(999, "b", 0)).outcome())
                .isEqualTo(Outcome.REJECT_SESSION);
        assertThat(engine.barrierActionDone(new BarrierActionDoneCommand(999, "b", 1)).outcome())
                .isEqualTo(Outcome.REJECT_SESSION);
    }

    @Test
    void crossFamilyMatrixLatchBarrierLockMutuallyReject() {
        long a = engine.sessionOpened();
        // 锁 key 上到场被拒。
        engine.acquire(new AcquireCommand(a, 1, "lk", LockType.REENTRANT, 1, 30_000, true));
        assertThat(engine.barrierAwait(await(a, 2, "lk", 2)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // LATCH key 上到场被拒；BARRIER key 上 latch await 亦被拒（双向）。
        engine.latchAwait(new LatchAwaitCommand(a, 3, "lt", 2));
        assertThat(engine.barrierAwait(await(a, 4, "lt", 2)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.latchAwait(new LatchAwaitCommand(a, 5, "b2", 0)).outcome())
                .isEqualTo(Outcome.REJECT_LATCH_TOTAL);
        engine.barrierAwait(await(a, 6, "b2", 2));
        assertThat(engine.latchAwait(new LatchAwaitCommand(a, 7, "b2", 2)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // 跨家族请求对既有条目零扰动：latch 条目计数仍在。
        assertThat(engine.inspectKey("lt").latchTotal()).isEqualTo(2);
        assertThat(engine.inspectKey("lt").latchRemaining()).isEqualTo(2);
    }

    @Test
    void acquireCarryingBarrierTypeRejected() {
        long a = engine.sessionOpened();
        assertThat(engine.acquire(new AcquireCommand(a, 1, "bk", LockType.BARRIER, 1,
                30_000, true)).outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // 且不留下条目。
        assertThat(engine.inspectKey("bk")).isNull();
    }

    @Test
    void deathOfArrivedSessionBreaksGenerationAndFiresNotify() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.barrierAwait(await(a, 1, "b", 3));
        engine.barrierAwait(await(b, 2, "b", 0));
        listener.clear();
        // 触及登记断言：到场会话的 key 集合随清理遍历。
        engine.sessionClosed(a);
        // 会话 a 死亡 → 世代 1 破障 → b 收放行（破障）广播。
        assertThat(listener.events()).anySatisfy(ev -> {
            assertThat(ev.sessionId()).isEqualTo(b);
            assertThat(ev.requestId()).isEqualTo(2);
            assertThat(ev.key()).isEqualTo("b");
        });
        // b 重发了结为破障。
        assertThat(engine.barrierAwait(await(b, 2, "b", 0)).outcome())
                .isEqualTo(Outcome.BARRIER_BROKEN);
        // 世代局部自愈：新到场进新世代挂起。
        long c = engine.sessionOpened();
        assertThat(engine.barrierAwait(await(c, 3, "b", 0)).outcome())
                .isEqualTo(Outcome.QUEUED);
    }

    @Test
    void tripAndActionDoneBroadcastThroughListener() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long x = engine.sessionOpened();
        // 动作两阶段：最后到场者（x 持动作）应答不触发 a 的通知。
        engine.barrierAwait(await(a, 1, "b", 3));
        engine.barrierAwait(await(b, 2, "b", 0));
        listener.clear();
        engine.barrierAwait(new BarrierAwaitCommand(x, 3, "b", 0, true));
        assertThat(listener.events()).isEmpty();
        // 回报生效后 a、b 收广播。
        assertThat(engine.barrierActionDone(new BarrierActionDoneCommand(x, "b", 1)).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(listener.events()).hasSize(2);
        assertThat(engine.inspectKey("b").barrierGeneration()).isEqualTo(2);
    }

    @Test
    void pureBreakOnMissingKeyIsNoOpOk() {
        long a = engine.sessionOpened();
        assertThat(engine.barrierLeave(new BarrierLeaveCommand(a, "ghost", 0)).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(engine.inspectKey("ghost")).isNull();
    }

    @Test
    void inspectExposesBarrierReadings() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.barrierAwait(await(a, 1, "b", 2));
        engine.barrierAwait(new BarrierAwaitCommand(b, 2, "b", 0, true));
        CoreInspection.KeySnapshot snap = engine.inspectKey("b");
        assertThat(snap.family()).isEqualTo(KeyFamily.BARRIER);
        assertThat(snap.barrierParties()).isEqualTo(2);
        assertThat(snap.barrierGeneration()).isEqualTo(1);
        assertThat(snap.barrierArrived()).isEqualTo(2);
        assertThat(snap.barrierActionPending()).isTrue();
        assertThat(snap.holders()).isEmpty();
        assertThat(snap.leaseToken()).isZero();
        assertThat(snap.waiters()).hasSize(1); // 仅普通到场者在队（执行者已摘队）
        assertThat(snap.waiters().get(0).sessionId()).isEqualTo(a);
        assertThat(snap.waiters().get(0).threadId()).isZero();
        // 统计观察面：无 held 语义、等待者计入总量。
        CoreStats stats = engine.stats();
        assertThat(stats.heldLocks()).isZero();
        assertThat(stats.totalWaiters()).isEqualTo(1);
    }
}
