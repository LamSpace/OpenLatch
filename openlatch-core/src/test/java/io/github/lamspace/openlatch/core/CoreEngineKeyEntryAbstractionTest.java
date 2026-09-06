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
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P3-01 KeyEntry 抽象层用例组：家族判定的状态零扰动语义，与条目跨家族
 * 操作（释放/续租/到期/会话关闭/队首清扫）经 {@code KeyEntry} 抽象收口的
 * 行为等价性。类型不匹配拒绝（{@link Outcome#REJECT_TYPE_MISMATCH}）的
 * 可达面自 P3-03 起由 {@code CoreEngineSemaphoreTest} 类型矩阵覆盖，
 * 本组锁侧语义钉在"抽象化不改变既有行为"的契约上。
 */
class CoreEngineKeyEntryAbstractionTest {

    /** 手工时钟。 */
    private MutableClock clock;
    /** 记录型监听器。 */
    private RecordingListener listener;
    /** 被测引擎（默认配置）。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    /**
     * 构造指定队列深度上限的引擎（队列满拒绝用例用）。
     *
     * @param depth 单 key 等待队列深度上限
     * @return 配置了深度上限的引擎
     */
    private CoreEngine engineWithDepth(int depth) {
        CoreConfig cfg = new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                CoreConfig.MAX_KEY_LENGTH, depth);
        return new CoreEngine(cfg, clock, listener);
    }

    /**
     * AcquireCommand 工厂：固定前提 {@code leaseMs=30_000}，queueIfBusy 由调用方传入。
     */
    private AcquireCommand acquire(long s, long r, String key, LockType type, long tid, boolean queue) {
        return new AcquireCommand(s, r, key, type, tid, 30_000, queue);
    }

    @Test
    void rejectedAcquireLeavesNoSessionTouchResidue() {
        // 会话级校验先于 key 校验（Outcome 契约"首个不满足者"）：不存在会话的
        // 非法 key 请求回 REJECT_SESSION，且会话登记表 MUST NOT 残留该 key——
        // sessionClosed 不得为从未登记的请求执行条目清理。
        AcquireResult ghost = engine.acquire(acquire(999_999L, 1, "", LockType.REENTRANT, 1, true));
        assertThat(ghost.outcome()).isEqualTo(Outcome.REJECT_SESSION);

        long a = engine.sessionOpened();
        AcquireResult g1 = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, true));
        assertThat(g1.outcome()).isEqualTo(Outcome.GRANTED);
        engine.sessionClosed(999_999L);
        // 会话 999_999 从未触及 "k"：其关闭不影响 "k" 的持有。
        AcquireResult g2 = engine.acquire(acquire(a, 2, "k", LockType.REENTRANT, 1, true));
        assertThat(g2.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(g2.leaseToken()).isEqualTo(g1.leaseToken());
        engine.sessionClosed(a);
        assertThat(listener.count()).isZero();
    }

    @Test
    void sessionClosedReapsWaitQueueThroughAbstraction() {
        // 会话关闭经 KeyEntry 抽象摘除持有与等待：队首为持有者本人（立即式
        // 获取自锁排队不可能，等待者以 queueIfBusy=true 入队），关闭其会话后
        // 新队首获得通知、重发被授予。
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, true));
        assertThat(ga.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2, true)).outcome())
                .isEqualTo(Outcome.QUEUED);
        assertThat(engine.acquire(acquire(c, 3, "k", LockType.REENTRANT, 3, true)).outcome())
                .isEqualTo(Outcome.QUEUED);

        engine.sessionClosed(b);
        assertThat(listener.count()).isZero(); // 非队首出队不产生通知

        engine.sessionClosed(a); // 持有者关闭：归还 + 通知新队首 C
        assertThat(listener.count()).isEqualTo(1);
        assertThat(listener.last().sessionId()).isEqualTo(c);
        assertThat(listener.last().requestId()).isEqualTo(3);

        AcquireResult rc = engine.acquire(acquire(c, 3, "k", LockType.REENTRANT, 3, true));
        assertThat(rc.outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void forceExpireReapsHoldersThroughAbstraction() {
        // 到期回收经 KeyEntry 抽象（forceExpire）：多读者与写侧同扫——读者
        // 会话关闭时其读者痕迹一并消失（重发被授予说明队列与持有均被清空）。
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long w = engine.sessionOpened();

        assertThat(engine.acquire(acquire(a, 1, "k", LockType.READ, 1, true)).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(acquire(b, 2, "k", LockType.READ, 2, true)).outcome())
                .isEqualTo(Outcome.GRANTED);
        AcquireResult rw = engine.acquire(acquire(w, 3, "k", LockType.WRITE, 3, true));
        assertThat(rw.outcome()).isEqualTo(Outcome.QUEUED);

        clock.advance(30_001);
        assertThat(engine.expireDue()).isEqualTo(1);
        assertThat(listener.count()).isEqualTo(1); // 仅通知队首 W
        // 读者痕迹已被强制回收：条目无持有（仅 W 在队），旧凭证释放回 NOT_HELD。
        ReleaseResult ra = engine.release(new ReleaseCommand(a, "k", 1, 1));
        assertThat(ra.status()).isEqualTo(ReleaseStatus.NOT_HELD);

        AcquireResult rw2 = engine.acquire(acquire(w, 3, "k", LockType.WRITE, 3, true));
        assertThat(rw2.outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void sweepAndRenewKeepNotifiedHeadInvariant() {
        // 队首"已通知、待重发"不变量（KeyEntry 抽象收口 sweepNotifiedHead 后
        // 依旧成立）：清扫窗口内续租不改变通知判定；超时后新队首被清扫
        // 补发通知；原队首超时出队后重发按规则 6 重新排队。
        CoreEngine eng = engineWithDepth(1);
        long a = eng.sessionOpened();
        long b = eng.sessionOpened();
        long c = eng.sessionOpened();

        AcquireResult ga = eng.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, true));
        assertThat(ga.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(eng.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2, true)).outcome())
                .isEqualTo(Outcome.QUEUED);
        // 深度上限 1：C 被拒。
        assertThat(eng.acquire(acquire(c, 3, "k", LockType.REENTRANT, 3, true)).outcome())
                .isEqualTo(Outcome.REJECT_QUEUE_FULL);

        // 通知 B：释放后队首标记待重发。
        eng.release(new ReleaseCommand(a, "k", ga.leaseToken(), 1));
        assertThat(listener.count()).isEqualTo(1);

        // 清扫窗口内：未超时不动队首（续租无持有者 → NOT_HELD，说明队首
        // 未被误摘除、条目因等待者仍存活）。
        clock.advance(CoreConfig.HEAD_REPLY_TIMEOUT_MS - 1);
        RenewResult rn = eng.renew(new RenewCommand(b, "k", ga.leaseToken(), 30_000));
        assertThat(rn.status()).isEqualTo(ReleaseStatus.NOT_HELD);
        assertThat(eng.sweepNotifiedHeads()).isZero();

        // 超时：B 出队、条目空被回收——清扫生效的可观测面：C 此刻经快路径被授予
        // （若 B 仍占队列则必为 QUEUED/QUEUE_FULL），B 重发降格为普通请求入队。
        clock.advance(2);
        assertThat(eng.sweepNotifiedHeads()).isEqualTo(1);
        assertThat(eng.acquire(acquire(c, 4, "k", LockType.REENTRANT, 4, true)).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(eng.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2, true)).outcome())
                .isEqualTo(Outcome.QUEUED);
    }
}
