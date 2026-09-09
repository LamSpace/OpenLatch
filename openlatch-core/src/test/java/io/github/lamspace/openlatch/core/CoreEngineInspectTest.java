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
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 T3 明细只读观察面用例（spec"明细只读观察面"）：预置多家族多持有者状态
 * 后逐字段断言快照（角色/计数/位次/时长折算），并钉住"纯读零扰动 + 并发观察
 * 零异常 + 条目内自洽"三条契约。手工时钟，无 sleep。
 */
class CoreEngineInspectTest {

    /** 被测引擎（手工时钟）。 */
    private CoreEngine engine;
    /** 与引擎共用的手工时钟（推进租约/等待时长判定的时间基准）。 */
    private MutableClock clock;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        engine = new CoreEngine(new CoreConfig(), clock, (s, r, k) -> {
        });
    }

    /** key → 快照索引（用例逐 key 取用）。 */
    private Map<String, CoreInspection.KeySnapshot> inspectByKey() {
        return engine.inspect().keys().stream()
                .collect(Collectors.toMap(CoreInspection.KeySnapshot::key, Function.identity()));
    }

    @Test
    void emptyEngineYieldsNoKeys() {
        CoreInspection inspection = engine.inspect();
        assertThat(inspection.keys()).isEmpty();
        assertThat(inspection.sessionCount()).isZero();
        assertThat(inspection.sampledAtMs()).isEqualTo(clock.nowMs());
    }

    @Test
    void reentrantLockHoldersAndWaitersVisible() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();
        long token = engine.acquire(new AcquireCommand(a, 1, "lk", LockType.REENTRANT, 10, 30_000, true))
                .leaseToken();
        engine.acquire(new AcquireCommand(a, 2, "lk", LockType.REENTRANT, 10, 30_000, true));
        engine.acquire(new AcquireCommand(b, 3, "lk", LockType.REENTRANT, 20, 30_000, true));
        clock.advance(5_000);
        engine.acquire(new AcquireCommand(c, 4, "lk", LockType.REENTRANT, 30, 30_000, true));
        clock.advance(2_000);

        CoreInspection.KeySnapshot snap = inspectByKey().get("lk");
        assertThat(snap.family()).isEqualTo(KeyFamily.LOCK);
        assertThat(snap.reentrant()).isTrue();
        assertThat(snap.leaseToken()).isEqualTo(token);
        // 剩余租约 = 30_000 − 已流逝 7_000（两次推进），采样时刻折算。
        assertThat(snap.remainingLeaseMs()).isEqualTo(23_000);
        // 持有者：写侧单归属、重入计数 2（b 只是等待者）。
        assertThat(snap.holders()).singleElement().satisfies(h -> {
            assertThat(h.sessionId()).isEqualTo(a);
            assertThat(h.threadId()).isEqualTo(10);
            assertThat(h.count()).isEqualTo(2);
            assertThat(h.role()).isEqualTo(CoreInspection.HolderRole.WRITER);
        });
        // 等待队列 FIFO：b 先入队（已等 7_000），c 后入队（已等 2_000）。
        assertThat(snap.waiters()).element(0).satisfies(w -> {
            assertThat(w.sessionId()).isEqualTo(b);
            assertThat(w.waitedMs()).isEqualTo(7_000);
        });
        assertThat(snap.waiters()).element(1).satisfies(w -> {
            assertThat(w.sessionId()).isEqualTo(c);
            assertThat(w.waitedMs()).isEqualTo(2_000);
        });
        // 与聚合口径互相自洽。
        CoreStats stats = engine.stats();
        assertThat(stats.heldLocks()).isEqualTo(1);
        assertThat(stats.totalWaiters()).isEqualTo(snap.waiters().size());
        assertThat(stats.sessionCount()).isEqualTo(engine.inspect().sessionCount());

        // 零扰动：观察之后凭证仍有效、释放路径如常。
        assertThat(engine.release(new ReleaseCommand(a, "lk", token, 10)).status().name()).isEqualTo("OK");
    }

    @Test
    void readSideHoldersShareLease() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.acquire(new AcquireCommand(a, 1, "rk", LockType.READ, 10, 30_000, true));
        engine.acquire(new AcquireCommand(b, 2, "rk", LockType.READ, 20, 30_000, true));

        CoreInspection.KeySnapshot snap = inspectByKey().get("rk");
        assertThat(snap.holders()).hasSize(2)
                .allSatisfy(h -> assertThat(h.role()).isEqualTo(CoreInspection.HolderRole.READER));
        assertThat(snap.leaseToken()).isNotZero();
        assertThat(snap.waiters()).isEmpty();
    }

    @Test
    void semaphorePoolHoldersAndPermitsVisible() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.acquire(new AcquireCommand(a, 1, "sem", LockType.SEMAPHORE, 10, 30_000, true, 2, 3));
        engine.acquire(new AcquireCommand(b, 2, "sem", LockType.SEMAPHORE, 20, 30_000, true, 3, 0));

        CoreInspection.KeySnapshot snap = inspectByKey().get("sem");
        assertThat(snap.family()).isEqualTo(KeyFamily.SEMAPHORE);
        assertThat(snap.permitsTotal()).isEqualTo(3);
        assertThat(snap.permitsAvailable()).isEqualTo(1);
        assertThat(snap.holders()).singleElement().satisfies(h -> {
            assertThat(h.count()).isEqualTo(2);
            assertThat(h.role()).isEqualTo(CoreInspection.HolderRole.HOLDER);
        });
        // 队首 b 请求 3 许可（池内仅 1，等待项携带请求量）。
        assertThat(snap.waiters()).singleElement().satisfies(w -> {
            assertThat(w.permits()).isEqualTo(3);
            assertThat(w.sessionId()).isEqualTo(b);
        });
    }

    @Test
    void latchCountAndAwaitersVisible() {
        long creator = engine.sessionOpened();
        long w1 = engine.sessionOpened();
        long w2 = engine.sessionOpened();
        engine.latchAwait(new LatchAwaitCommand(creator, 1, "gate", 3));
        engine.latchAwait(new LatchAwaitCommand(w1, 2, "gate", 0));
        engine.latchAwait(new LatchAwaitCommand(w2, 3, "gate", 0));
        clock.advance(1_500);
        engine.countDown(new LatchCountDownCommand(creator, "gate", 1, 0));

        CoreInspection.KeySnapshot snap = inspectByKey().get("gate");
        assertThat(snap.family()).isEqualTo(KeyFamily.LATCH);
        assertThat(snap.latchTotal()).isEqualTo(3);
        assertThat(snap.latchRemaining()).isEqualTo(2);
        assertThat(snap.leaseToken()).isZero();
        assertThat(snap.holders()).isEmpty();
        assertThat(snap.latchParticipants()).containsExactlyInAnyOrder(creator, w1, w2);
        // 三个 await 均挂起（创建者的 await 也是等待者），入队后推进 1_500。
        assertThat(snap.waiters()).hasSize(3).allSatisfy(w -> {
            assertThat(w.waitedMs()).isEqualTo(1_500);
            assertThat(w.threadId()).isZero();
        });
    }

    @Test
    void concurrentInspectionSelfConsistentAndNonMutating() throws Exception {
        long s = engine.sessionOpened();
        int writers = 4;
        int rounds = 200;
        ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch go = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            for (int w = 0; w < writers; w++) {
                final long wid = w;
                pool.submit(() -> {
                    ready.countDown();
                    try {
                        go.await();
                        for (int i = 0; i < rounds; i++) {
                            String key = "k-" + wid + "-" + (i % 5);
                            var r = engine.acquire(new AcquireCommand(s, 1000L + wid * 10000 + i,
                                    key, LockType.REENTRANT, wid, 30_000, false));
                            if (r.outcome() == Outcome.GRANTED) {
                                engine.release(new ReleaseCommand(s, key, r.leaseToken(), wid));
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
                        CoreInspection insp = engine.inspect();
                        if (insp.sessionCount() < 1) {
                            failure.compareAndSet(null, new AssertionError("session lost: " + insp));
                        }
                        for (CoreInspection.KeySnapshot k : insp.keys()) {
                            // 条目内自洽：等待者非负、持有列表与家族一致。
                            if (k.waiters().size() < 0
                                    || (k.family() == KeyFamily.LATCH && !k.holders().isEmpty())
                                    || (k.family() == KeyFamily.LOCK && k.holders().isEmpty()
                                        && k.leaseToken() != 0)) {
                                failure.compareAndSet(null, new AssertionError("inconsistent: " + k));
                            }
                        }
                    }
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
        // 观察结束后引擎回到净空（全部写入线程已释放）。
        assertThat(engine.stats().heldLocks()).isZero();
    }

    @Test
    void inspectionKeysCoverAllFamiliesTogether() {
        long a = engine.sessionOpened();
        engine.acquire(new AcquireCommand(a, 1, "lk", LockType.REENTRANT, 10, 30_000, true));
        engine.acquire(new AcquireCommand(a, 2, "sem", LockType.SEMAPHORE, 10, 30_000, true, 1, 2));
        engine.latchAwait(new LatchAwaitCommand(a, 3, "gate", 1));
        List<String> keys = engine.inspect().keys().stream()
                .map(CoreInspection.KeySnapshot::key).sorted().toList();
        assertThat(keys).containsExactly("gate", "lk", "sem");
    }
}
