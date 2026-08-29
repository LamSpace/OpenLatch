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

package io.github.lamspace.openlatch.examples;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;

/**
 * 示例 2：多线程竞争同一互斥锁，打印入队与授予顺序（详设 §9，
 * FIFO 公平性可观察）。授予顺序 == 入队顺序在集成测试中严格断言，
 * 本示例仅演示观察方法。
 *
 * <p>运行：{@code mvn -pl openlatch-examples exec:java
 * -Dexec.mainClass=io.github.lamspace.openlatch.examples.ConcurrencyExample}
 */
public final class ConcurrencyExample {

    /**
     * 私有构造：入口类。
     */
    private ConcurrencyExample() {
    }

    /**
     * 记录一次"排队→授予→持有→释放"的时序。
     *
     * @param threadName  线程名（排队顺序的发起序标签）
     * @param enqueuedAt  发起获取时刻（纳秒）
     * @param acquiredAt  授予时刻（纳秒）
     * @param releasedAt  释放时刻（纳秒）
     * @param concurrentHeld 授予瞬间并发持有者数的观测值（恒应为 1）
     */
    record Timeline(String threadName, long enqueuedAt, long acquiredAt,
                    long releasedAt, int concurrentHeld) {
    }

    /**
     * 入口：16 个线程以 30ms 间隔先后发起 {@code lock()}，
     * 输出入队顺序与授予顺序的对照表。
     *
     * @param args 未使用
     * @throws Exception 线程或连接异常
     */
    public static void main(String[] args) throws Exception {
        final int threads = 16;
        OpenLatchServer server = ExampleServers.startDefault();
        List<Timeline> timelines = new ArrayList<>();
        java.util.concurrent.atomic.AtomicInteger inside =
                new java.util.concurrent.atomic.AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port())
                .defaultWaitTimeout(java.time.Duration.ofSeconds(60))
                .build()) {
            client.connectAsync().get(5, TimeUnit.SECONDS);
            OLock lock = client.newReentrantLock("concurrency:hot-key");

            for (int i = 0; i < threads; i++) {
                final int seq = i;
                pool.submit(() -> {
                    String name = "T" + seq;
                    long enqueued = System.nanoTime();
                    try {
                        lock.lock();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    long acquired = System.nanoTime();
                    int concurrent = inside.incrementAndGet();
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    inside.decrementAndGet();
                    long released = System.nanoTime();
                    synchronized (timelines) {
                        timelines.add(new Timeline(name, enqueued, acquired,
                                released, concurrent));
                    }
                    lock.unlock();
                });
                Thread.sleep(30);
            }
            pool.shutdown();
            pool.awaitTermination(60, TimeUnit.SECONDS);
        } finally {
            server.stop();
        }

        List<Timeline> byEnqueue = new ArrayList<>(timelines);
        byEnqueue.sort(Comparator.comparingLong(Timeline::enqueuedAt));
        List<Timeline> byGrant = new ArrayList<>(timelines);
        byGrant.sort(Comparator.comparingLong(Timeline::acquiredAt));

        System.out.println("enqueued order : " + byEnqueue.stream()
                .map(Timeline::threadName).toList());
        System.out.println("granted  order : " + byGrant.stream()
                .map(Timeline::threadName).toList());
        boolean fifo = byEnqueue.stream().map(Timeline::threadName).toList()
                .equals(byGrant.stream().map(Timeline::threadName).toList());
        int maxConcurrent = timelines.stream().mapToInt(Timeline::concurrentHeld).max()
                .orElse(0);
        System.out.printf("[done ] FIFO order observed = %b, max concurrent holders = %d%n",
                fifo, maxConcurrent);
        System.exit(0);
    }
}
