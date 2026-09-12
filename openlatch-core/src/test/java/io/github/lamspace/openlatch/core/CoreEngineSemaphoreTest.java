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
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Semaphore 语义用例组：严格 FIFO
 * 仅队首授予（防大请求饥饿）、重入按次累加、租约到期整体归还、
 * 超额释放拒绝、会话清理不泄漏、许可总量定型与断言、类型不匹配矩阵。
 * 手工时钟驱动，无 sleep。
 */
class CoreEngineSemaphoreTest {

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

    /** 锁获取命令工厂（矩阵内 30s 租约不过期）。 */
    private AcquireCommand lockAcq(long s, long r, String key, LockType type, long tid) {
        return new AcquireCommand(s, r, key, type, tid, 30_000, true);
    }

    /** 许可请求工厂（不主张总量）。 */
    private AcquireCommand sem(long s, long r, String key, long tid, int permits, int total, boolean queue) {
        return new AcquireCommand(s, r, key, LockType.SEMAPHORE, tid, 30_000, queue, permits, total);
    }

    /** 许可释放工厂。 */
    private ReleaseCommand semRel(long s, String key, long tid, long token, int permits) {
        return new ReleaseCommand(s, key, token, tid, permits);
    }

    @Test
    void createRequiresTotalAndAssertsOnExisting() {
        long a = engine.sessionOpened();

        // 建条目必须主张总量。
        assertThat(engine.acquire(sem(a, 1, "s", 1, 1, 0, true)).outcome())
                .isEqualTo(Outcome.REJECT_SEMAPHORE_TOTAL);

        assertThat(engine.acquire(sem(a, 2, "s", 1, 1, 5, true)).outcome()).isEqualTo(Outcome.GRANTED);

        // 既有条目：非零主张须匹配，零为不主张。
        assertThat(engine.acquire(sem(a, 3, "s", 1, 1, 7, true)).outcome())
                .isEqualTo(Outcome.REJECT_SEMAPHORE_TOTAL);
        assertThat(engine.acquire(sem(a, 4, "s", 2, 1, 5, true)).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(sem(a, 5, "s", 3, 1, 0, true)).outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void countingAndDefaultSinglePermit() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        // 建条目 permits=1（默认单许可请求）。
        AcquireResult g1 = engine.acquire(new AcquireCommand(a, 1, "s", LockType.SEMAPHORE, 1, 30_000,
                true, 1, 2));
        assertThat(g1.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(sem(b, 2, "s", 2, 1, 0, true)).outcome()).isEqualTo(Outcome.GRANTED);
        // 池尽：立即式被拒。
        assertThat(engine.acquire(sem(b, 3, "s", 2, 1, 0, false)).outcome()).isEqualTo(Outcome.DENIED);
        // 归还 1 后可再授。
        assertThat(engine.release(semRel(a, "s", 1, g1.leaseToken(), 1)).status()).isEqualTo(ReleaseStatus.OK);
        assertThat(engine.acquire(sem(b, 4, "s", 2, 1, 0, true)).outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void strictFifoHeadOnlyPreventsStarvation() {
        // total=2，A 持 2；队列 [B:2, C:1]；归还 1 仅满足 C——C 不得越过队首 B。
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        AcquireResult ga = engine.acquire(sem(a, 1, "s", 1, 2, 2, true));
        assertThat(ga.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(sem(b, 2, "s", 2, 2, 0, true)).queuePosition()).isEqualTo(1);
        assertThat(engine.acquire(sem(c, 3, "s", 3, 1, 0, true)).queuePosition()).isEqualTo(2);

        // 归还 1（A 仍持 1）：available=1 < 队首 2 → 无通知；C 新请求也只能排到队尾。
        assertThat(engine.release(semRel(a, "s", 1, ga.leaseToken(), 1)).fullyReleased()).isFalse();
        assertThat(listener.count()).isZero();
        assertThat(engine.acquire(sem(c, 4, "s", 3, 1, 0, true)).outcome()).isEqualTo(Outcome.QUEUED);

        // 再归还 1：available=2 满足队首 B → 通知队首，B 重发被授予。
        assertThat(engine.release(semRel(a, "s", 1, ga.leaseToken(), 1)).fullyReleased()).isTrue();
        assertThat(listener.count()).isEqualTo(1);
        assertThat(listener.last().sessionId()).isEqualTo(b);
        AcquireResult gb = engine.acquire(sem(b, 2, "s", 2, 2, 0, true));
        assertThat(gb.outcome()).isEqualTo(Outcome.GRANTED);
        // C 的两个等待项按幂等去重合并显示同一请求位次。
        assertThat(gb.queuePosition()).isZero();
    }

    @Test
    void reentrantAccumulatesNotBlockedByHead() {
        // A 持 2（total=4），B 排队；A 重入 +1 不受队首约束、凭证复用。
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();

        AcquireResult ga = engine.acquire(sem(a, 1, "s", 1, 2, 4, true));
        assertThat(ga.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(sem(b, 2, "s", 2, 3, 0, true)).outcome()).isEqualTo(Outcome.QUEUED);

        AcquireResult g2 = engine.acquire(sem(a, 3, "s", 1, 1, 0, true));
        assertThat(g2.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(g2.leaseToken()).isEqualTo(ga.leaseToken());

        // 重入累加到 3：释放 1 还剩 2（未完全释放）。
        assertThat(engine.release(semRel(a, "s", 1, ga.leaseToken(), 1)).fullyReleased()).isFalse();
        // A 再释放 2 → 持有归零，available=4 ≥ 队首 B(3) → 通知队首。
        assertThat(engine.release(semRel(a, "s", 1, ga.leaseToken(), 2)).fullyReleased()).isTrue();
        assertThat(listener.count()).isEqualTo(1);
        assertThat(listener.last().sessionId()).isEqualTo(b);
        // 队首 B 待重发窗口内：新到者 C（纯加入，不主张总量）只能排队尾。
        long c = engine.sessionOpened();
        assertThat(engine.acquire(sem(c, 4, "s", 3, 1, 0, true)).queuePosition()).isEqualTo(2);
    }

    @Test
    void overReleaseRejectedAndNotHeldAfterFullRelease() {
        long a = engine.sessionOpened();
        AcquireResult ga = engine.acquire(sem(a, 1, "s", 1, 2, 3, true));
        assertThat(ga.outcome()).isEqualTo(Outcome.GRANTED);

        // 归还 3 > 持有 2 → OVER_RELEASE，池零扰动。
        assertThat(engine.release(semRel(a, "s", 1, ga.leaseToken(), 3)).status())
                .isEqualTo(ReleaseStatus.OVER_RELEASE);
        // 正确归还 2 → 完全释放；再释放 → NOT_HELD。
        assertThat(engine.release(semRel(a, "s", 1, ga.leaseToken(), 2)).fullyReleased()).isTrue();
        assertThat(engine.release(semRel(a, "s", 1, ga.leaseToken(), 1)).status())
                .isEqualTo(ReleaseStatus.NOT_HELD);

        // 全体释放后条目已空被回收——池完整需重新定型：以 total=3 再建再取 3。
        long b = engine.sessionOpened();
        assertThat(engine.acquire(sem(b, 2, "s", 2, 3, 3, true)).outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void wrongTokenRejectedWhileHeld() {
        long a = engine.sessionOpened();
        AcquireResult ga = engine.acquire(sem(a, 1, "s", 1, 1, 2, true));
        assertThat(engine.release(semRel(a, "s", 1, ga.leaseToken() + 999, 1)).status())
                .isEqualTo(ReleaseStatus.INVALID_TOKEN);
        // 无效凭证不归还许可：请求 2 个排队（池内仅 1），后续 1 个立即式因
        // 队列非空也被拒（越位禁止）。
        long b = engine.sessionOpened();
        assertThat(engine.acquire(sem(b, 2, "s", 2, 2, 0, true)).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(engine.acquire(sem(b, 3, "s", 2, 1, 0, false)).outcome()).isEqualTo(Outcome.DENIED);
    }

    @Test
    void leaseExpiryReturnsAllHoldersPermits() {
        // 共享租约：任一持有者到期被强制回收时全体归还（与锁读者群口径一致），
        // 归还后队首获得通知。
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        AcquireResult ga = engine.acquire(sem(a, 1, "s", 1, 2, 4, true));
        assertThat(ga.outcome()).isEqualTo(Outcome.GRANTED);
        AcquireResult gb = engine.acquire(sem(b, 2, "s", 2, 1, 0, true));
        assertThat(gb.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(gb.leaseToken()).isEqualTo(ga.leaseToken()); // 共享凭证
        assertThat(engine.acquire(sem(c, 3, "s", 3, 4, 0, true)).outcome()).isEqualTo(Outcome.QUEUED);

        clock.advance(30_001);
        assertThat(engine.expireDue()).isEqualTo(1);
        assertThat(listener.count()).isEqualTo(1);
        assertThat(listener.last().sessionId()).isEqualTo(c);

        AcquireResult gc = engine.acquire(sem(c, 3, "s", 3, 4, 0, true));
        assertThat(gc.outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void sessionClosedReturnsPermitsWithoutLeak() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        engine.acquire(sem(a, 1, "s", 1, 3, 5, true));   // A 持 3，余 2
        engine.acquire(sem(b, 2, "s", 2, 2, 0, true));   // 恰好取尽（B 持 2）
        engine.acquire(sem(c, 3, "s", 3, 2, 0, true));   // C 排队

        engine.sessionClosed(b); // B 归还 2 → available=2 ≥ 队首 2 → 通知 C
        assertThat(listener.count()).isEqualTo(1);
        assertThat(listener.last().sessionId()).isEqualTo(c);

        engine.sessionClosed(a); // A 持有 3 归还；C 尚未重发，在队不受影响
        AcquireResult gc = engine.acquire(sem(c, 3, "s", 3, 2, 0, true));
        assertThat(gc.outcome()).isEqualTo(Outcome.GRANTED);
        // 剩余池 = 5 − 2（C 持有）= 3。
        long d = engine.sessionOpened();
        assertThat(engine.acquire(sem(d, 4, "s", 4, 3, 0, true)).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(sem(d, 5, "s", 4, 1, 0, false)).outcome()).isEqualTo(Outcome.DENIED);
    }

    @Test
    void familyMismatchMatrix() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        // 锁条目 → Semaphore 请求：拒绝且锁态零扰动。
        AcquireResult lg = engine.acquire(lockAcq(a, 1, "lk", LockType.REENTRANT, 1));
        assertThat(engine.acquire(sem(b, 2, "lk", 2, 1, 3, true)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        AcquireResult re = engine.acquire(lockAcq(a, 3, "lk", LockType.REENTRANT, 1));
        assertThat(re.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(re.leaseToken()).isEqualTo(lg.leaseToken());

        // Semaphore 条目 → 锁请求：拒绝。
        AcquireResult sg = engine.acquire(sem(c, 4, "sk", 3, 1, 2, true));
        assertThat(sg.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(lockAcq(b, 5, "sk", LockType.REENTRANT, 4)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // Semaphore 条目上的读锁请求同样拒绝（家族边界覆盖读写型）。
        assertThat(engine.acquire(lockAcq(b, 6, "sk", LockType.READ, 4)).outcome())
                .isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
    }

    @Test
    void queueDepthLimitAppliesToSemaphoreWaiters() {
        CoreConfig cfg = new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                CoreConfig.MAX_KEY_LENGTH, 1);
        CoreEngine eng = new CoreEngine(cfg, clock, listener);
        long a = eng.sessionOpened();
        long b = eng.sessionOpened();
        long c = eng.sessionOpened();

        eng.acquire(sem(a, 1, "s", 1, 1, 1, true));
        assertThat(eng.acquire(sem(b, 2, "s", 2, 1, 0, true)).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(eng.acquire(sem(c, 3, "s", 3, 1, 0, true)).outcome())
                .isEqualTo(Outcome.REJECT_QUEUE_FULL);
    }
}
