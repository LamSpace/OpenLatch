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
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §10.1/10.3 并发不变量校验。本文件仅断言两条不变量：互斥成立（临界区同时至多一个持有者）
 * 与授予无丢失（排队请求者最终全部被授予）；无重复入队与无孤儿等待者两条不变量的真实覆盖
 * 在 {@link CoreEngineSessionIdempotencyLimitTest#duplicateAcquireDoesNotEnqueueTwice()}。
 *
 * <p>两场景的交错维度：立即式 tryLock 快路径竞争（8 线程 × 500 轮，{@code queueIfBusy=false}，
 * 未授予轮次直接跳过）；排队 + 通知重发路径竞争（8 线程 × 200 轮，{@code queueIfBusy=true}，
 * QUEUED 后经监听器阻塞等待队首通知，再以同 requestId 重发直至 GRANTED）。
 */
class CoreEngineConcurrencyTest {

    /** 本套件全体线程竞争的同一把锁的键。 */
    private static final String KEY = "k";

    @Test
    void immediateTryLockIsMutuallyExclusiveUnderConcurrency() throws Exception {
        CoreEngine engine = new CoreEngine(new CoreConfig(), new MutableClock(), new RecordingListener());
        int threads = 8;
        int iterations = 500;
        AtomicInteger inCritical = new AtomicInteger();
        AtomicInteger grants = new AtomicInteger();

        runConcurrent(threads, () -> {
            long session = engine.sessionOpened();
            long tid = Thread.currentThread().threadId();
            for (int i = 0; i < iterations; i++) {
                AcquireResult r = engine.acquire(
                        new AcquireCommand(session, i, KEY, LockType.REENTRANT, tid, 30_000, false));
                if (r.outcome() == Outcome.GRANTED) {
                    assertThat(inCritical.incrementAndGet()).isEqualTo(1); // 互斥
                    grants.incrementAndGet();
                    inCritical.decrementAndGet();
                    assertThat(engine.release(new ReleaseCommand(session, KEY, r.leaseToken(), tid)).status())
                            .isEqualTo(ReleaseStatus.OK);
                }
            }
        });

        assertThat(grants.get()).isGreaterThan(0);
    }

    @Test
    void queuedAcquirersAllGrantedWithMutualExclusion() throws Exception {
        MutableClock clock = new MutableClock();
        QueueingListener listener = new QueueingListener();
        CoreEngine engine = new CoreEngine(new CoreConfig(), clock, listener);

        int threads = 8;
        int iterations = 200;
        AtomicInteger inCritical = new AtomicInteger();
        AtomicInteger grants = new AtomicInteger();

        runConcurrent(threads, () -> {
            long session = engine.sessionOpened();
            long tid = Thread.currentThread().threadId();
            BlockingQueue<QueueingListener.Event> queue = listener.register(session);
            for (int i = 0; i < iterations; i++) {
                AcquireCommand cmd = new AcquireCommand(session, i, KEY, LockType.REENTRANT, tid, 60_000, true);
                AcquireResult r = engine.acquire(cmd);
                if (r.outcome() == Outcome.QUEUED) {
                    QueueingListener.Event e = queue.poll(10, TimeUnit.SECONDS);
                    if (e == null) {
                        throw new AssertionError("notify timeout for " + session + "/" + i);
                    }
                    assertThat(e.requestId()).isEqualTo(i);
                    r = engine.acquire(cmd); // 队首重发 → 授予
                }
                assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
                assertThat(inCritical.incrementAndGet()).isEqualTo(1);
                grants.incrementAndGet();
                inCritical.decrementAndGet();
                assertThat(engine.release(new ReleaseCommand(session, KEY, r.leaseToken(), tid)).status())
                        .isEqualTo(ReleaseStatus.OK);
            }
        });

        // 无丢失：每个会话每次迭代最终都被授予一次。
        assertThat(grants.get()).isEqualTo(threads * iterations);
    }

    /** 并发测试体：由每个工作线程执行一次，抛出的异常交由 {@link #runConcurrent} 统一收集断言。 */
    private interface Body {
        void run() throws Exception;
    }

    /**
     * 以 {@code threads} 个线程并发执行 {@code body}。起跑协议：各线程先阻塞在单发
     * {@link CountDownLatch} 起跑门上，全部线程启动后统一放行，以最大化交错竞争窗口。
     * 失败处理：工作线程体内抛出的任何 {@link Throwable}（含断言失败的 {@link AssertionError}）
     * 被收集到 synchronized 列表，待全部线程汇合后在调用方测试线程末尾统一断言为空——
     * 错误以 AssertionError 在测试线程浮现，而非使工作线程静默崩溃。每线程 join 期限
     * 60 秒，超时未结束的线程先行断言失败。
     *
     * @param threads 并发线程数
     * @param body    每线程执行体
     * @throws InterruptedException 等待线程汇合时被中断
     */
    private static void runConcurrent(int threads, Body body) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        List<Throwable> errors = java.util.Collections.synchronizedList(new ArrayList<>());
        for (int t = 0; t < threads; t++) {
            Thread th = new Thread(() -> {
                try {
                    start.await();
                    body.run();
                } catch (Throwable e) {
                    errors.add(e);
                }
            });
            th.start();
            ts.add(th);
        }
        start.countDown();
        for (Thread th : ts) {
            th.join(TimeUnit.SECONDS.toMillis(60));
            assertThat(th.isAlive()).as("thread did not finish in time").isFalse();
        }
        assertThat(errors).isEmpty();
    }
}
