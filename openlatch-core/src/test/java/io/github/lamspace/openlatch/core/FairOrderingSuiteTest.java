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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FairLock 公平性回归套件（Phase 3 详设 §2.2/P3-02，验收标准 §8-1 常开档）：
 * "并发竞争下授予顺序 == 排队顺序"的 core 直驱档。{@code FAIR} 与
 * {@code REENTRANT} 参数化跑同一矩阵——矩阵对 REENTRANT 同样成立即是
 * "语义等价"的机器验证（别名不是特权）；任何未来优化（如读者批量授予）
 * 破坏公平性时本套件立即报警。
 *
 * <p>手工时钟驱动，无 sleep；服务端端到端档见
 * {@code FairOrderingSuiteE2ETest}，集群档见 {@code FairOrderingSuiteClusterTest}。
 */
class FairOrderingSuiteTest {

    /** 手工时钟。 */
    private MutableClock clock;
    /** 记录型监听器：通知序列即公平性断言对象。 */
    private RecordingListener listener;
    /** 被测引擎。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    /**
     * 排队式获取命令工厂（固定 30s 租约，矩阵内无到期干扰）。
     */
    private AcquireCommand acquire(long s, long r, String key, LockType type, long tid) {
        return new AcquireCommand(s, r, key, type, tid, 30_000, true);
    }

    @ParameterizedTest(name = "type={0}")
    @EnumSource(value = LockType.class, names = {"FAIR", "REENTRANT"})
    void grantOrderEqualsEnqueueOrder(LockType type) {
        // 持有者 + 5 个排队者按 A..E 顺序入队，逐轮释放：通知序与授予序
        // 必须逐项等于入队序（位次 1..5），任何插队即断言失败。
        long holder = engine.sessionOpened();
        String key = "fair-seq";
        long[] waiters = new long[5];
        for (int i = 0; i < 5; i++) {
            waiters[i] = engine.sessionOpened();
        }

        AcquireResult g = engine.acquire(acquire(holder, 100, key, type, 10));
        assertThat(g.outcome()).isEqualTo(Outcome.GRANTED);
        long token = g.leaseToken();
        for (int i = 0; i < 5; i++) {
            AcquireResult q = engine.acquire(acquire(waiters[i], 200 + i, key, type, 10 + i));
            assertThat(q.outcome()).isEqualTo(Outcome.QUEUED);
            assertThat(q.queuePosition()).isEqualTo(i + 1);
        }

        long currentHolder = holder;
        long currentToken = token;
        for (int i = 0; i < 5; i++) {
            listener.clear();
            engine.release(new ReleaseCommand(currentHolder, key, currentToken, 10 + (currentHolder == holder ? 0 : i - 1)));
            // 每轮仅通知队首，且必须是下一个排队者。
            assertThat(listener.count()).isEqualTo(1);
            assertThat(listener.last().sessionId()).isEqualTo(waiters[i]);
            assertThat(listener.last().requestId()).isEqualTo(200 + i);

            AcquireResult g2 = engine.acquire(acquire(waiters[i], 200 + i, key, type, 10 + i));
            assertThat(g2.outcome()).isEqualTo(Outcome.GRANTED);
            currentHolder = waiters[i];
            currentToken = g2.leaseToken();
        }
    }

    @ParameterizedTest(name = "type={0}")
    @EnumSource(value = LockType.class, names = {"FAIR", "REENTRANT"})
    void lateArrivalCannotOvertakeNotifiedHead(LockType type) {
        // 已通知待重发窗口内：无持有者也不得放行新到者（规则 3），
        // 其位次必须落在队尾。
        String key = "fair-window";
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        AcquireResult ga = engine.acquire(acquire(a, 1, key, type, 1));
        engine.acquire(acquire(b, 2, key, type, 2));
        engine.release(new ReleaseCommand(a, key, ga.leaseToken(), 1));
        assertThat(listener.count()).isEqualTo(1);

        AcquireResult qc = engine.acquire(acquire(c, 3, key, type, 3));
        assertThat(qc.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(qc.queuePosition()).isEqualTo(2);
    }

    @ParameterizedTest(name = "type={0}")
    @EnumSource(value = LockType.class, names = {"FAIR", "REENTRANT"})
    void mixedTypeSequencePreservesOrder(LockType type) {
        // 与 SIMPLE 混排同样守序：不同互斥型交错入队，授予序等于入队序。
        String key = "fair-mixed";
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();
        long d = engine.sessionOpened();

        AcquireResult ga = engine.acquire(acquire(a, 1, key, type, 1));
        assertThat(ga.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.acquire(acquire(b, 2, key, LockType.SIMPLE, 2)).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(engine.acquire(acquire(c, 3, key, type, 3)).outcome()).isEqualTo(Outcome.QUEUED);

        engine.release(new ReleaseCommand(a, key, ga.leaseToken(), 1));
        assertThat(listener.last().requestId()).isEqualTo(2);
        AcquireResult gb = engine.acquire(acquire(b, 2, key, LockType.SIMPLE, 2));
        assertThat(gb.outcome()).isEqualTo(Outcome.GRANTED);

        engine.release(new ReleaseCommand(b, key, gb.leaseToken(), 2));
        assertThat(listener.last().requestId()).isEqualTo(3);
        assertThat(engine.acquire(acquire(c, 3, key, type, 3)).outcome()).isEqualTo(Outcome.GRANTED);
        // d 最后到，只可能在队尾。
        assertThat(engine.acquire(acquire(d, 4, key, type, 4)).queuePosition()).isEqualTo(1);
    }

    @ParameterizedTest(name = "fair-then-reentrant: {0}")
    @EnumSource(value = LockType.class, names = {"FAIR", "REENTRANT"})
    void fairAndReentrantAreInterchangeableAliases(LockType first) {
        // FAIR ≡ REENTRANT：先手类型定型条目，后手别名类型必须按同族
        // 重入互认（同 token、计数累加），逐层释放对称。
        LockType second = first == LockType.FAIR ? LockType.REENTRANT : LockType.FAIR;
        String key = "alias";
        long s = engine.sessionOpened();

        AcquireResult g1 = engine.acquire(acquire(s, 1, key, first, 7));
        assertThat(g1.outcome()).isEqualTo(Outcome.GRANTED);
        AcquireResult g2 = engine.acquire(acquire(s, 2, key, second, 7));
        assertThat(g2.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(g2.leaseToken()).isEqualTo(g1.leaseToken());

        // 两层持有：单层释放不归还锁（重入计数逐层）。
        assertThat(engine.release(new ReleaseCommand(s, key, g1.leaseToken(), 7)).fullyReleased())
                .isFalse();
        assertThat(engine.release(new ReleaseCommand(s, key, g1.leaseToken(), 7)).fullyReleased())
                .isTrue();
    }
}
