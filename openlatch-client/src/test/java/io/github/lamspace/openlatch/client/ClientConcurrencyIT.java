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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.server.OpenLatchServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集成测试套件（§10.3，tasks 8.1/8.5/8.6）：多线程并发互斥、授予公平性、
 * 放弃等待后的队列恢复（超时与通知竞争端到端）。
 */
class ClientConcurrencyIT {

    /** 并发线程数（§10.3 要求 ≥16）。 */
    private static final int THREADS = 16;
    /** 竞争轮数。 */
    private static final int ROUNDS = 5;

    /** 被测服务器。 */
    private OpenLatchServer server;
    /** 共享客户端。 */
    private OpenLatchClient client;

    /**
     * 启动服务器与客户端。
     *
     * @throws Exception 建连失败
     */
    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        client = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        client.connectAsync().get(5, TimeUnit.SECONDS);
    }

    /**
     * 关停资源。
     */
    @AfterEach
    void tearDown() {
        client.shutdown();
        server.stop();
    }

    /** 8.1 并发互斥：16 线程多轮竞争，临界区计数断言互斥成立、无丢失。 */
    @Test
    void sixteenThreadsContendWithoutLossOrOverlap() throws Exception {
        OLock lock = client.newReentrantLock("stress");
        AtomicInteger inSection = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        AtomicInteger violations = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(THREADS);

        for (int i = 0; i < THREADS; i++) {
            new Thread(() -> {
                try {
                    for (int round = 0; round < ROUNDS; round++) {
                        lock.lock();
                        try {
                            if (inSection.incrementAndGet() != 1) {
                                violations.incrementAndGet();
                            }
                            inSection.decrementAndGet();
                        } finally {
                            lock.unlock();
                        }
                        completed.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            }).start();
        }

        assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(violations.get()).isZero();
        assertThat(completed.get()).isEqualTo(THREADS * ROUNDS);
    }

    /** 8.5 公平性端到端：授予顺序 == 发起顺序。 */
    @Test
    void grantsFollowFifoOrder() throws Exception {
        String key = "fairness";
        OLock holder = client.newReentrantLock(key);
        holder.lock();

        List<Integer> grantOrder = new CopyOnWriteArrayList<>();
        List<Throwable> threadFailures = new CopyOnWriteArrayList<>();
        CountDownLatch allStarted = new CountDownLatch(3);
        CountDownLatch allDone = new CountDownLatch(3);

        for (int i = 0; i < 3; i++) {
            final int ordinal = i;
            OpenLatchClient waiter = OpenLatchClient.builder()
                    .address("127.0.0.1:" + server.port()).build();
            waiter.connectAsync().get(5, TimeUnit.SECONDS);
            new Thread(() -> {
                try {
                    OLock lock = waiter.newReentrantLock(key);
                    allStarted.countDown();
                    lock.lock();
                    try {
                        grantOrder.add(ordinal);
                    } finally {
                        lock.unlock();
                    }
                } catch (Throwable t) {
                    threadFailures.add(t);
                    if (t instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                } finally {
                    allDone.countDown();
                    waiter.shutdown();
                }
            }).start();
            Thread.sleep(250); // 保证发起顺序明确
        }

        allStarted.await(5, TimeUnit.SECONDS);
        Thread.sleep(300); // 三个等待者全部排队后释放
        holder.unlock();

        assertThat(allDone.await(20, TimeUnit.SECONDS)).isTrue();
        assertThat(threadFailures).as("waiter thread failures").isEmpty();
        assertThat(grantOrder).containsExactly(0, 1, 2);
    }

    /** 8.6 放弃等待与通知竞争：静默放弃不造成队列停摆，后继等待者正常授予。 */
    @Test
    void abandonedWaiterDoesNotStallQueue() throws Exception {
        String key = "abandon";
        OpenLatchClient waiterClient = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).build();
        waiterClient.connectAsync().get(5, TimeUnit.SECONDS);
        try {
            OLock holder = client.newReentrantLock(key);
            holder.lock();

            // 等待者限时 600ms：到时放弃（服务端队列条目惰性回收，§6.3）
            assertThat(waiterClient.newReentrantLock(key)
                    .tryLock(600, TimeUnit.MILLISECONDS)).isFalse();

            // 持有者在放弃之后释放：立即式获取仍被队列中的陈旧等待项挡住
            holder.unlock();
            assertThat(client.newReentrantLock(key).tryLock()).isFalse();

            // 后继限时等待者：等队首响应超时清扫后正常授予，队列不停摆
            OLock successor = client.newReentrantLock(key);
            long start = System.currentTimeMillis();
            assertThat(successor.tryLock(10, TimeUnit.SECONDS)).isTrue();
            successor.unlock();
            assertThat(System.currentTimeMillis() - start).isGreaterThan(1000);
        } finally {
            waiterClient.shutdown();
        }
    }
}
