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

/** §10.1/10.3 并发不变量：互斥成立、授予无丢失、无重复入队、无孤儿等待者。 */
class CoreEngineConcurrencyTest {

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

    private interface Body {
        void run() throws Exception;
    }

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
