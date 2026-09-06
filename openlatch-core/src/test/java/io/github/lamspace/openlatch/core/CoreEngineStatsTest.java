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
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 T2 只读统计观察面用例（spec"只读统计观察面"）：稳态精确断言
 * （held 按家族、LATCH 排除、等待者含 awaiter、队深、会话数）与
 * 并发读数安全（无异常、弱一致合理区间）。手工时钟，无 sleep。
 */
class CoreEngineStatsTest {

    /** 被测引擎（手工时钟）。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        engine = new CoreEngine(new CoreConfig(), new MutableClock(), (s, r, k) -> {
        });
    }

    private AcquireCommand lockAcq(long s, long r, String key, LockType type, boolean queue) {
        return new AcquireCommand(s, r, key, type, 1, 30_000, queue);
    }

    @Test
    void emptyEngineAllZero() {
        CoreStats stats = engine.stats();
        assertThat(stats.heldLocks()).isZero();
        assertThat(stats.heldSemaphores()).isZero();
        assertThat(stats.totalWaiters()).isZero();
        assertThat(stats.maxQueueDepth()).isZero();
        assertThat(stats.sessionCount()).isZero();
    }

    @Test
    void steadyStateAggregatesByFamily() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();
        long d = engine.sessionOpened();
        long e = engine.sessionOpened();

        // lock-a：a 持有，b/c 排队（队深 2）。
        assertThat(engine.acquire(lockAcq(a, 1, "lock-a", LockType.REENTRANT, true)).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(lockAcq(b, 2, "lock-a", LockType.REENTRANT, true)).outcome())
                .isEqualTo(Outcome.QUEUED);
        assertThat(engine.acquire(lockAcq(c, 3, "lock-a", LockType.REENTRANT, true)).outcome())
                .isEqualTo(Outcome.QUEUED);
        // sem：d 持有全部许可，e 排队。
        assertThat(engine.acquire(new AcquireCommand(d, 4, "sem", LockType.SEMAPHORE,
                1, 30_000, true, 1, 1)).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(new AcquireCommand(e, 5, "sem", LockType.SEMAPHORE,
                1, 30_000, true, 1, 0)).outcome()).isEqualTo(Outcome.QUEUED);
        // latch：b 的 awaiter 挂起（不计 held、计入 waiters）。
        assertThat(engine.latchAwait(new LatchAwaitCommand(b, 6, "latch", 2)).outcome())
                .isEqualTo(Outcome.QUEUED);

        CoreStats stats = engine.stats();
        assertThat(stats.heldLocks()).isEqualTo(1);
        assertThat(stats.heldSemaphores()).isEqualTo(1);
        assertThat(stats.totalWaiters()).isEqualTo(4); // 2 锁等待 + 1 许可等待 + 1 awaiter
        assertThat(stats.maxQueueDepth()).isEqualTo(2); // lock-a 队深
        assertThat(stats.sessionCount()).isEqualTo(5);
    }

    @Test
    void waitingOnlyEntryNotHeld() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        // b 先建条目并持有，a 释放后条目仅剩等待——held 读数须归零。
        var r1 = engine.acquire(lockAcq(a, 1, "k", LockType.REENTRANT, true));
        engine.acquire(lockAcq(b, 2, "k", LockType.REENTRANT, true));
        engine.release(new ReleaseCommand(a, "k", r1.leaseToken(), 1));
        CoreStats stats = engine.stats();
        assertThat(stats.heldLocks()).isZero();
        assertThat(stats.totalWaiters()).isEqualTo(1);
    }

    @Test
    void sessionClosedDropsFromSessionCount() {
        long a = engine.sessionOpened();
        engine.sessionOpened();
        assertThat(engine.stats().sessionCount()).isEqualTo(2);
        engine.sessionClosed(a);
        assertThat(engine.stats().sessionCount()).isEqualTo(1);
    }

    @Test
    void concurrentStatsNeverThrows() throws Exception {
        long s = engine.sessionOpened();
        int writers = 4;
        int rounds = 200;
        ExecutorService pool = Executors.newFixedThreadPool(writers + 2);
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            for (int w = 0; w < writers; w++) {
                final long sid = s;
                final int wid = w;
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        for (int i = 0; i < rounds; i++) {
                            String key = "k-" + wid + "-" + (i % 5);
                            var r = engine.acquire(new AcquireCommand(sid, 1000L + wid * 10000 + i,
                                    key, LockType.REENTRANT, wid, 30_000, false));
                            if (r.outcome() == Outcome.GRANTED) {
                                engine.release(new ReleaseCommand(sid, key, r.leaseToken(), wid));
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    }
                });
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            pool.submit(() -> {
                try {
                    go.await();
                    for (int i = 0; i < rounds * 4; i++) {
                        CoreStats stats = engine.stats();
                        if (stats.totalWaiters() < 0 || stats.maxQueueDepth() < 0
                                || stats.heldLocks() < 0) {
                            failure.compareAndSet(null, new AssertionError("negative stats: " + stats));
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            pool.submit(() -> {
                try {
                    go.await();
                    engine.expireDue();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            go.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        } finally {
            pool.shutdownNow();
        }
        assertThat(failure.get()).isNull();
    }
}
