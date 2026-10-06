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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.github.lamspace.openlatch.client.OAtomicLong;
import io.github.lamspace.openlatch.client.OAtomicReference;
import io.github.lamspace.openlatch.client.OBarrier;
import io.github.lamspace.openlatch.client.OBlockingQueue;
import io.github.lamspace.openlatch.client.ODelayQueue;
import io.github.lamspace.openlatch.client.OCondition;
import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OTopic;
import io.github.lamspace.openlatch.client.OTopicSubscription;
import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;

/**
 * 基准 harness（手写方案）：
 * 产出三项基线指标——无竞争 {@code tryLock} 往返吞吐、竞争（16/64 线程）
 * 吞吐、竞争授予延迟分位数（蓄水池采样，P50/P99）。
 *
 * <p><b>定位</b>：记录为基线防退化参考，<b>不作发布门槛</b>；
 * 结果写入仓库根 {@code target/benchmark/benchmark-baseline-<date>.md}（运行时工件
 * 不入库，常态经 CI artifacts 分布；可用系统属性
 * {@code -Dbenchmark.output=<path>} 覆盖），报告注明机器/JDK/服务器档位。
 *
 * <p>运行：{@code mvn -pl openlatch-examples exec:java
 * -Dexec.mainClass=io.github.lamspace.openlatch.examples.BenchmarkMain}
 * （约 60s）。
 */
public final class BenchmarkMain {

    /** 热身时长（毫秒）。 */
    private static final long WARMUP_MS = 4_000;
    /** 单批采样时长（毫秒）。 */
    private static final long SAMPLE_MS = 5_000;
    /** 采样批数（报告取中位）。 */
    private static final int BATCHES = 3;
    /** 蓄水池容量（每线程）。 */
    private static final int RESERVOIR = 32_768;
    /** 竞争档位（线程数）。 */
    private static final int[] CONTENDED_LEVELS = {16, 64};
    /** 原子 CAS 争用档位（线程数）。 */
    private static final int[] ATOMIC_CAS_LEVELS = {16};
    /** 循环屏障合拢档位（parties = 会合线程数）。 */
    private static final int[] BARRIER_LEVELS = {2, 4};

    /** 队列扇出相的消费者线程数（v7）。 */
    private static final int QUEUE_FANOUT_CONSUMERS = 8;
    /** 队列扇出相每轮流水元素数（v7）。 */
    private static final int QUEUE_FANOUT_ITEMS = 256;
    /** 队列 drain 相的每轮批量（v7）。 */
    private static final int QUEUE_DRAIN_BATCH = 32;
    /** topic 扇出相订阅者连接数（v8，独立会话订阅同键）。 */
    private static final int TOPIC_FANOUT_SUBSCRIBERS = 8;

    /**
     * 私有构造：入口类。
     */
    private BenchmarkMain() {
    }

    /**
     * 入口。
     *
     * @param args 未使用
     * @throws Exception 连接/IO/线程异常
     */
    public static void main(String[] args) throws Exception {
        OpenLatchServer server = ExampleServers.startDefault();
        OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port())
                .defaultWaitTimeout(Duration.ofSeconds(60))
                .build();
        try {
            client.connectAsync().get(10, TimeUnit.SECONDS);
            System.out.printf("[bench] warmup %ds%n", WARMUP_MS / 1000);
            runUncontended(client, WARMUP_MS);
            runContended(client, CONTENDED_LEVELS[0], WARMUP_MS);

            List<long[]> uncThroughput = new ArrayList<>();
            List<double[]> uncLatencyBatches = new ArrayList<>();
            List<List<long[]>> contThroughput = new ArrayList<>();
            List<List<double[]>> latencies = new ArrayList<>();

            for (int level : CONTENDED_LEVELS) {
                contThroughput.add(new ArrayList<>());
                latencies.add(new ArrayList<>());
            }
            for (int b = 0; b < BATCHES; b++) {
                System.out.printf("[bench] batch %d/%d%n", b + 1, BATCHES);
                Result unc = runUncontended(client, SAMPLE_MS);
                uncThroughput.add(new long[]{unc.opsPerSec});
                uncLatencyBatches.add(unc.latencies);
                for (int i = 0; i < CONTENDED_LEVELS.length; i++) {
                    Result r = runContended(client, CONTENDED_LEVELS[i], SAMPLE_MS);
                    contThroughput.get(i).add(new long[]{r.opsPerSec});
                    latencies.get(i).add(r.latencies);
                }
            }
            // 原子相：热身 + 采样（写/读单线程 RTT 与吞吐；CAS 争用放大）。
            runAtomicAdd(client, WARMUP_MS);
            runAtomicGet(client, WARMUP_MS);
            for (int level : ATOMIC_CAS_LEVELS) {
                runAtomicCasContended(client, level, WARMUP_MS);
            }
            // 引用相（v6）：小载荷/恰限 4KB 写、读 RTT、版本 CAS 争用——热身。
            byte[] refSmall = "bench-ref-16B-payload".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            byte[] refExact = new byte[4096];
            for (int i = 0; i < refExact.length; i++) {
                refExact[i] = (byte) (i * 7 + 1);
            }
            runRefSet(client, refSmall, WARMUP_MS);
            runRefSet(client, refExact, WARMUP_MS);
            runRefGet(client, WARMUP_MS);
            for (int level : ATOMIC_CAS_LEVELS) {
                runRefCasContended(client, level, WARMUP_MS);
            }
            // 队列相（v7）热身：配对交接、8 消费扇出、批量 drain、延时到期。
            runQueueHandoff(client, WARMUP_MS / 2);
            runQueueFanout(client, QUEUE_FANOUT_CONSUMERS, QUEUE_FANOUT_ITEMS,
                    WARMUP_MS / 2);
            runQueueDrain(client, WARMUP_MS / 2);
            runQueueDelay(client, WARMUP_MS / 2);
            // topic 相（v8）热身：发布受理 RTT（零订阅）与 1×8 扇出交付。
            runTopicPublish(client, WARMUP_MS / 2);
            runTopicFanout(client, server.port(), TOPIC_FANOUT_SUBSCRIBERS, WARMUP_MS / 2);
            // 条件相（v9）热身：signal 受理 RTT（空集）与 await/signal 乒乓交接。
            runConditionSignal(client, WARMUP_MS / 2);
            runConditionHandoff(client, server.port(), WARMUP_MS / 2);
            List<long[]> queueHandoffThroughput = new ArrayList<>();
            List<double[]> queueHandoffLatencies = new ArrayList<>();
            List<long[]> queueFanoutThroughput = new ArrayList<>();
            List<double[]> queueFanoutLatencies = new ArrayList<>();
            List<long[]> queueDrainThroughput = new ArrayList<>();
            List<double[]> queueDrainLatencies = new ArrayList<>();
            List<double[]> queueDelayOvershoot = new ArrayList<>();
            List<long[]> topicPublishThroughput = new ArrayList<>();
            List<double[]> topicPublishLatencies = new ArrayList<>();
            List<long[]> topicFanoutThroughput = new ArrayList<>();
            List<double[]> topicFanoutLatencies = new ArrayList<>();
            List<long[]> conditionSignalThroughput = new ArrayList<>();
            List<double[]> conditionSignalLatencies = new ArrayList<>();
            List<long[]> conditionHandoffThroughput = new ArrayList<>();
            List<double[]> conditionHandoffLatencies = new ArrayList<>();
            List<long[]> refSmallThroughput = new ArrayList<>();
            List<double[]> refSmallLatencies = new ArrayList<>();
            List<long[]> refBigThroughput = new ArrayList<>();
            List<double[]> refBigLatencies = new ArrayList<>();
            List<long[]> refGetThroughput = new ArrayList<>();
            List<double[]> refGetLatencies = new ArrayList<>();
            List<List<long[]>> refCasThroughput = new ArrayList<>();
            List<List<double[]>> refCasLatencies = new ArrayList<>();
            for (int level : ATOMIC_CAS_LEVELS) {
                refCasThroughput.add(new ArrayList<>());
                refCasLatencies.add(new ArrayList<>());
            }
            List<long[]> addThroughput = new ArrayList<>();
            List<double[]> addLatencies = new ArrayList<>();
            List<long[]> getThroughput = new ArrayList<>();
            List<double[]> getLatencies = new ArrayList<>();
            List<List<long[]>> casThroughput = new ArrayList<>();
            List<List<double[]>> casLatencies = new ArrayList<>();
            for (int level : ATOMIC_CAS_LEVELS) {
                casThroughput.add(new ArrayList<>());
                casLatencies.add(new ArrayList<>());
            }
            // 屏障相：热身（无动作两档 + 动作档）。
            for (int level : BARRIER_LEVELS) {
                runBarrierTrips(client, level, false, "bench:barrier:warm:" + level, WARMUP_MS);
            }
            runBarrierTrips(client, 2, true, "bench:barrier:warm:act", WARMUP_MS);
            List<List<long[]>> barrierThroughput = new ArrayList<>();
            List<List<double[]>> barrierLatencies = new ArrayList<>();
            List<long[]> actionThroughput = new ArrayList<>();
            List<double[]> actionLatencies = new ArrayList<>();
            for (int level : BARRIER_LEVELS) {
                barrierThroughput.add(new ArrayList<>());
                barrierLatencies.add(new ArrayList<>());
            }
            for (int b = 0; b < BATCHES; b++) {
                Result add = runAtomicAdd(client, SAMPLE_MS);
                addThroughput.add(new long[] {add.opsPerSec});
                addLatencies.add(add.latencies);
                Result get = runAtomicGet(client, SAMPLE_MS);
                getThroughput.add(new long[] {get.opsPerSec});
                getLatencies.add(get.latencies);
                for (int i = 0; i < ATOMIC_CAS_LEVELS.length; i++) {
                    Result r = runAtomicCasContended(client, ATOMIC_CAS_LEVELS[i], SAMPLE_MS);
                    casThroughput.get(i).add(new long[] {r.opsPerSec});
                    casLatencies.get(i).add(r.latencies);
                }
                for (int i = 0; i < BARRIER_LEVELS.length; i++) {
                    // 每批用新 key：采样窗尾部未集齐世代的破障收场不污染后续批次。
                    Result br = runBarrierTrips(client, BARRIER_LEVELS[i], false,
                            "bench:barrier:" + BARRIER_LEVELS[i] + ":" + b, SAMPLE_MS);
                    barrierThroughput.get(i).add(new long[] {br.opsPerSec});
                    barrierLatencies.get(i).add(br.latencies);
                }
                Result ba = runBarrierTrips(client, 2, true,
                        "bench:barrier:act:" + b, SAMPLE_MS);
                actionThroughput.add(new long[] {ba.opsPerSec});
                actionLatencies.add(ba.latencies);
                // 引用相采样：小/恰限写、读、版本 CAS 争用。
                Result rs = runRefSet(client, refSmall, SAMPLE_MS);
                refSmallThroughput.add(new long[] {rs.opsPerSec});
                refSmallLatencies.add(rs.latencies);
                Result rb = runRefSet(client, refExact, SAMPLE_MS);
                refBigThroughput.add(new long[] {rb.opsPerSec});
                refBigLatencies.add(rb.latencies);
                Result rg = runRefGet(client, SAMPLE_MS);
                refGetThroughput.add(new long[] {rg.opsPerSec});
                refGetLatencies.add(rg.latencies);
                for (int i = 0; i < ATOMIC_CAS_LEVELS.length; i++) {
                    Result rr = runRefCasContended(client, ATOMIC_CAS_LEVELS[i], SAMPLE_MS);
                    refCasThroughput.get(i).add(new long[] {rr.opsPerSec});
                    refCasLatencies.get(i).add(rr.latencies);
                }
                // 队列相采样（v7）。
                Result qh = runQueueHandoff(client, SAMPLE_MS);
                queueHandoffThroughput.add(new long[] {qh.opsPerSec});
                queueHandoffLatencies.add(qh.latencies);
                Result qf = runQueueFanout(client, QUEUE_FANOUT_CONSUMERS,
                        QUEUE_FANOUT_ITEMS, SAMPLE_MS);
                queueFanoutThroughput.add(new long[] {qf.opsPerSec});
                queueFanoutLatencies.add(qf.latencies);
                Result qd = runQueueDrain(client, SAMPLE_MS);
                queueDrainThroughput.add(new long[] {qd.opsPerSec});
                queueDrainLatencies.add(qd.latencies);
                Result qy = runQueueDelay(client, SAMPLE_MS);
                queueDelayOvershoot.add(qy.latencies);
                Result tp = runTopicPublish(client, SAMPLE_MS);
                topicPublishThroughput.add(new long[] {tp.opsPerSec});
                topicPublishLatencies.add(tp.latencies);
                Result tf = runTopicFanout(client, server.port(),
                        TOPIC_FANOUT_SUBSCRIBERS, SAMPLE_MS);
                topicFanoutThroughput.add(new long[] {tf.opsPerSec});
                topicFanoutLatencies.add(tf.latencies);
                Result cs = runConditionSignal(client, SAMPLE_MS);
                conditionSignalThroughput.add(new long[] {cs.opsPerSec});
                conditionSignalLatencies.add(cs.latencies);
                Result ch = runConditionHandoff(client, server.port(), SAMPLE_MS);
                conditionHandoffThroughput.add(new long[] {ch.opsPerSec});
                conditionHandoffLatencies.add(ch.latencies);
            }
            String report = renderReport(uncThroughput, uncLatencyBatches,
                    contThroughput, latencies, addThroughput, addLatencies,
                    getThroughput, getLatencies, casThroughput, casLatencies,
                    barrierThroughput, barrierLatencies, actionThroughput, actionLatencies,
                    refSmallThroughput, refSmallLatencies, refBigThroughput, refBigLatencies,
                    refGetThroughput, refGetLatencies, refCasThroughput, refCasLatencies);
            report = report + renderQueueSection(queueHandoffThroughput, queueHandoffLatencies,
                    queueFanoutThroughput, queueFanoutLatencies, queueDrainThroughput,
                    queueDrainLatencies, queueDelayOvershoot);
            report = report + renderTopicSection(topicPublishThroughput, topicPublishLatencies,
                    topicFanoutThroughput, topicFanoutLatencies);
            report = report + renderConditionSection(conditionSignalThroughput,
                    conditionSignalLatencies, conditionHandoffThroughput,
                    conditionHandoffLatencies);
            System.out.println(report);
            Path out = resolveOutputPath();
            Files.createDirectories(out.getParent());
            Files.writeString(out, report);
            System.out.printf("[bench] baseline written to %s%n", out.toAbsolutePath());
        } finally {
            client.shutdown();
            server.stop();
        }
        System.exit(0);
    }

    /**
     * 一轮结果：吞吐与延迟样本数组。
     *
     * @param opsPerSec 每秒完成操作数
     * @param latencies 排序后的延迟样本（毫秒，蓄水池）
     */
    record Result(long opsPerSec, double[] latencies) {
    }

    /**
     * 无竞争吞吐：单线程 tryLock+unlock 循环。
     *
     * @param client 客户端
     * @param millis 采样时长
     * @return 结果
     */
    private static Result runUncontended(OpenLatchClient client, long millis) {
        OLock lock = client.newSimpleLock("bench:uncontended");
        AtomicLong ops = new AtomicLong();
        Reservoir reservoir = new Reservoir();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            long start = System.nanoTime();
            if (!lock.tryLock()) {
                throw new IllegalStateException("unexpected denial in uncontended bench");
            }
            reservoir.record(System.nanoTime() - start);
            lock.unlock();
            ops.incrementAndGet();
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                reservoir.sortedSamples());
    }

    /**
     * 竞争吞吐与授予延迟：N 线程对同一 key 执行 {@code lock()}
     * （授予延迟 = 发起到授予的排队时长）+ 立即 {@code unlock()}。
     *
     * @param client 客户端
     * @param threads 并发线程数
     * @param millis 采样时长
     * @return 结果（延迟为全部线程合并样本）
     * @throws InterruptedException 等待被打断
     */
    private static Result runContended(OpenLatchClient client, int threads, long millis)
            throws InterruptedException {
        OLock lock = client.newReentrantLock("bench:contended:" + threads);
        AtomicLong ops = new AtomicLong();
        Reservoir[] reservoirs = new Reservoir[threads];
        for (int i = 0; i < threads; i++) {
            reservoirs[i] = new Reservoir();
        }
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                ready.countDown();
                try {
                    go.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
                long localOps = 0;
                while (System.nanoTime() < deadline) {
                    long start = System.nanoTime();
                    try {
                        lock.lock();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    reservoirs[idx].record(System.nanoTime() - start);
                    lock.unlock();
                    localOps++;
                }
                ops.addAndGet(localOps);
            }));
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        pool.shutdown();
        if (!pool.awaitTermination(120, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }
        for (java.util.concurrent.Future<?> f : futures) {
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("bench worker failed", e);
            }
        }
        double[] merged = mergeSorted(reservoirs);
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis), merged);
    }

    /**
     * 原子写往返：单线程对同 key {@code addAndGet(1)} 循环——度量写路径
     * （含 Raft 提交与去重槽推进）的 RTT 与吞吐。
     *
     * @param client 客户端
     * @param millis 采样时长
     * @return 结果
     * @throws InterruptedException 采样被打断
     */
    private static Result runAtomicAdd(OpenLatchClient client, long millis)
            throws InterruptedException {
        OAtomicLong counter = client.newAtomicLong("bench:atomic:add");
        AtomicLong ops = new AtomicLong();
        Reservoir reservoir = new Reservoir();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            long start = System.nanoTime();
            counter.addAndGet(1);
            reservoir.record(System.nanoTime() - start);
            ops.incrementAndGet();
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                reservoir.sortedSamples());
    }

    /**
     * 原子读往返：单线程 {@code get()} 循环——度量 GET 经 Raft 的读数路径
     * （v4 裁决：读亦线性一致，代价是每读一次提交）。
     *
     * @param client 客户端
     * @param millis 采样时长
     * @return 结果
     * @throws InterruptedException 采样被打断
     */
    private static Result runAtomicGet(OpenLatchClient client, long millis)
            throws InterruptedException {
        OAtomicLong counter = client.newAtomicLong("bench:atomic:get");
        AtomicLong ops = new AtomicLong();
        Reservoir reservoir = new Reservoir();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            long start = System.nanoTime();
            counter.get();
            reservoir.record(System.nanoTime() - start);
            ops.incrementAndGet();
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                reservoir.sortedSamples());
    }

    /**
     * 原子 CAS 争用：N 线程对同 key 循环"读 stamped → 带版本 CAS 加一"，
     * 直至落值成功——延迟列为<b>单次成功的完整耗时</b>（含重试轮次），
     * 吞吐为跨线程合并的成功计数，度量争用放大。
     *
     * @param client  客户端
     * @param threads 并发线程数
     * @param millis  采样时长
     * @return 结果（合并样本）
     * @throws InterruptedException 等待被打断
     */
    private static Result runAtomicCasContended(OpenLatchClient client, int threads, long millis)
            throws InterruptedException {
        OAtomicLong counter = client.newAtomicLong("bench:atomic:cas:" + threads);
        AtomicLong ops = new AtomicLong();
        Reservoir[] reservoirs = new Reservoir[threads];
        for (int i = 0; i < threads; i++) {
            reservoirs[i] = new Reservoir();
        }
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                ready.countDown();
                try {
                    go.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                long deadline2 = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
                long localOps = 0;
                while (System.nanoTime() < deadline2) {
                    long start = System.nanoTime();
                    try {
                        while (!counter.compareAndSetStamped(
                                counter.getStamped().value(),
                                counter.getVersion(),
                                // 读后加一：版本不符即重试（服务端裁决）。
                                counter.get() + 1)) {
                            if (System.nanoTime() >= deadline2) {
                                break;
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    reservoirs[idx].record(System.nanoTime() - start);
                    localOps++;
                }
                ops.addAndGet(localOps);
            }));
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        pool.shutdown();
        if (!pool.awaitTermination(120, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }
        for (java.util.concurrent.Future<?> f : futures) {
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("atomic bench worker failed", e);
            }
        }
        double[] merged = mergeSorted(reservoirs);
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis), merged);
    }

    /**
     * 有值引用写往返（v6）：单线程 {@code getAndSet} 固定载荷循环——度量
     * 载荷写入通道的提交+应答全成本；载荷长度入 key（16B 小载荷与 4KB
     * 恰限对照，写同一载荷内容，条目常驻不回收由 key 隔离批次）。
     *
     * @param client  客户端
     * @param payload 固定载荷字节
     * @param millis  采样时长
     * @return 结果
     * @throws InterruptedException 采样被打断
     */
    private static Result runRefSet(OpenLatchClient client, byte[] payload, long millis)
            throws InterruptedException {
        OAtomicReference ref = client.newAtomicReference("bench:ref:set:" + payload.length);
        AtomicLong ops = new AtomicLong();
        Reservoir reservoir = new Reservoir();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            long start = System.nanoTime();
            ref.getAndSet(payload);
            reservoir.record(System.nanoTime() - start);
            ops.incrementAndGet();
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                reservoir.sortedSamples());
    }

    /**
     * 有值引用读往返：单线程 {@code get()} 循环（经 Raft 的线性一致读数，
     * 与 {@link #runAtomicGet} 同判例）。
     *
     * @param client 客户端
     * @param millis 采样时长
     * @return 结果
     * @throws InterruptedException 采样被打断
     */
    private static Result runRefGet(OpenLatchClient client, long millis)
            throws InterruptedException {
        OAtomicReference ref = client.newAtomicReference("bench:ref:get");
        AtomicLong ops = new AtomicLong();
        Reservoir reservoir = new Reservoir();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            long start = System.nanoTime();
            ref.get();
            reservoir.record(System.nanoTime() - start);
            ops.incrementAndGet();
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                reservoir.sortedSamples());
    }

    /**
     * 有值引用版本 CAS 争用：N 线程对同 key（64B 载荷）循环
     * "读 stamped → 值+版本双符 CAS 递增计数"，直至落值成功——延迟列为
     * 单次成功的完整耗时（含重试轮次），吞吐为跨线程合并的成功计数，
     * 度量载荷通道上的争用放大（对照标量 CAS 相）。
     *
     * @param client  客户端
     * @param threads 并发线程数
     * @param millis  采样时长
     * @return 结果（合并样本）
     * @throws InterruptedException 等待被打断
     */
    private static Result runRefCasContended(OpenLatchClient client, int threads, long millis)
            throws InterruptedException {
        OAtomicReference ref = client.newAtomicReference("bench:ref:cas:" + threads);
        AtomicLong ops = new AtomicLong();
        Reservoir[] reservoirs = new Reservoir[threads];
        for (int i = 0; i < threads; i++) {
            reservoirs[i] = new Reservoir();
        }
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                ready.countDown();
                try {
                    go.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                long deadline2 = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
                long localOps = 0;
                while (System.nanoTime() < deadline2) {
                    long start = System.nanoTime();
                    try {
                        while (true) {
                            OAtomicReference.Stamped cur = ref.getStamped();
                            long next = cur.value() == null ? 1
                                    : Long.parseLong(new String(cur.value(),
                                    java.nio.charset.StandardCharsets.UTF_8)) + 1;
                            // 读后加一：值与版本双符才落（服务端裁决）。
                            if (ref.compareAndSetStamped(cur.value(), cur.version(),
                                    Long.toString(next).getBytes(
                                            java.nio.charset.StandardCharsets.UTF_8))) {
                                break;
                            }
                            if (System.nanoTime() >= deadline2) {
                                break;
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    reservoirs[idx].record(System.nanoTime() - start);
                    localOps++;
                }
                ops.addAndGet(localOps);
            }));
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        pool.shutdown();
        if (!pool.awaitTermination(120, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }
        for (java.util.concurrent.Future<?> f : futures) {
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("atomic reference bench worker failed", e);
            }
        }
        double[] merged = mergeSorted(reservoirs);
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis), merged);
    }

    /**
     * 循环屏障合拢往返：parties 个线程对同一 barrier key 反复会合（世代
     * 回卷复用直至采样窗耗尽）——吞吐为<b>完成世代数/秒</b>（跨线程合并
     * 计数除以 parties 向下取整），延迟列为单次 {@code await} 耗时（含等待
     * 其余到场方，量级即"首到场至全体放行"的合拢延迟）。
     *
     * <p>采样窗截止时未集齐的世代按破障收场（离场即破障的正常裁决路径，
     * 计作失败静默丢弃）；每批使用独立 key 以免跨批污染。
     *
     * @param client    客户端
     * @param parties   会合方数（=线程数）
     * @param withAction 是否给 0 号线程句柄挂 barrierAction（度量两阶段放行开销）
     * @param key       屏障 key（调用方保证批次内唯一）
     * @param millis    采样时长
     * @return 结果（吞吐=世代/秒，延迟样本为到场等待耗时）
     * @throws InterruptedException 等待被打断
     */
    private static Result runBarrierTrips(OpenLatchClient client, int parties, boolean withAction,
            String key, long millis) throws InterruptedException {
        AtomicLong arrivals = new AtomicLong();
        Reservoir[] reservoirs = new Reservoir[parties];
        for (int i = 0; i < parties; i++) {
            reservoirs[i] = new Reservoir();
        }
        CountDownLatch ready = new CountDownLatch(parties);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(parties);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < parties; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                OBarrier barrier = withAction && idx == 0
                        ? client.newBarrier(key, parties, () -> { })
                        : client.newBarrier(key, parties);
                ready.countDown();
                try {
                    go.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                long deadline2 = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
                while (System.nanoTime() < deadline2) {
                    long start = System.nanoTime();
                    try {
                        if (!barrier.await(20, TimeUnit.SECONDS)) {
                            break; // 本方超时：世代已破，退出采样
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (RuntimeException e) {
                        break; // 采样窗尾破障/拒绝收场：不计世代
                    }
                    reservoirs[idx].record(System.nanoTime() - start);
                    arrivals.incrementAndGet();
                }
            }));
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        pool.shutdown();
        if (!pool.awaitTermination(120, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }
        for (java.util.concurrent.Future<?> f : futures) {
            try {
                f.get(1, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IllegalStateException("barrier bench worker failed", e);
            }
        }
        long generations = arrivals.get() / parties;
        return new Result(Math.round(generations * 1_000.0 / millis), mergeSorted(reservoirs));
    }

    /**
     * 合并各线程蓄水池样本并排序（直接拼接各池样本，分位数为近似值，
     * 报告已注明）。
     *
     * @param reservoirs 每线程蓄水池
     * @return 排序后的合并样本（毫秒）
     */
    private static double[] mergeSorted(Reservoir[] reservoirs) {
        int capacity = 0;
        for (Reservoir r : reservoirs) {
            capacity += r.size();
        }
        double[] merged = new double[capacity];
        int pos = 0;
        for (Reservoir r : reservoirs) {
            for (long s : r.samples()) {
                merged[pos++] = s / 1_000_000.0;
            }
        }
        Arrays.sort(merged);
        return merged;
    }

    /**
     * 队列配对交接相（v7）：单线程 {@code put}+{@code take} 交替（队列恒空，
     * 无等待路径），延迟为一次配对往返、ops 计两操作。
     *
     * @param client 客户端
     * @param millis 采样时长
     * @return 结果
     * @throws InterruptedException 采样被打断
     */
    private static Result runQueueHandoff(OpenLatchClient client, long millis)
            throws InterruptedException {
        OBlockingQueue queue = client.newBlockingQueue("bench:queue:handoff", 16);
        byte[] element = "bench-queue-element".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        AtomicLong ops = new AtomicLong();
        Reservoir reservoir = new Reservoir();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            long start = System.nanoTime();
            queue.put(element);
            queue.take();
            reservoir.record(System.nanoTime() - start);
            ops.addAndGet(2);
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                reservoir.sortedSamples());
    }

    /**
     * 队列扇出争用相（v7）：每轮单生产者 {@value #QUEUE_FANOUT_ITEMS} 条阻塞
     * put 与 {@value #QUEUE_FANOUT_CONSUMERS} 线程阻塞 take 竞速清空管道；take
     * 延迟含生产节奏等待（争用面基线，非纯 RTT），ops 计每次 put/take。
     *
     * @param client    客户端
     * @param consumers 消费者线程数
     * @param items     每轮流水元素数
     * @param millis    采样时长
     * @return 结果
     * @throws InterruptedException 采样被打断
     */
    private static Result runQueueFanout(OpenLatchClient client, int consumers, int items,
            long millis) throws InterruptedException {
        OBlockingQueue queue = client.newBlockingQueue("bench:queue:fanout",
                QUEUE_FANOUT_ITEMS * 4);
        AtomicLong ops = new AtomicLong();
        Reservoir[] reservoirs = new Reservoir[consumers];
        for (int i = 0; i < consumers; i++) {
            reservoirs[i] = new Reservoir();
        }
        ExecutorService pool = Executors.newFixedThreadPool(consumers);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        int round = 0;
        while (System.nanoTime() < deadline) {
            java.util.concurrent.atomic.AtomicInteger remaining =
                    new java.util.concurrent.atomic.AtomicInteger(items);
            CountDownLatch done = new CountDownLatch(consumers);
            final int r = round++;
            for (int c = 0; c < consumers; c++) {
                final int idx = c;
                pool.submit(() -> {
                    try {
                        while (remaining.getAndDecrement() > 0) {
                            long start = System.nanoTime();
                            queue.take();
                            reservoirs[idx].record(System.nanoTime() - start);
                            ops.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            byte[] element = ("bench-queue-fanout-" + (r & 0xff))
                    .getBytes(java.nio.charset.StandardCharsets.UTF_8);
            try {
                for (int i = 0; i < items; i++) {
                    queue.put(element);
                    ops.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            if (!done.await(60, TimeUnit.SECONDS)) {
                throw new IllegalStateException("queue fanout round stuck");
            }
        }
        pool.shutdown();
        if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
            pool.shutdownNow();
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                mergeSorted(reservoirs));
    }

    /**
     * 队列 drain 摊薄相（v7）：每轮 {@value #QUEUE_DRAIN_BATCH} 次 put 后一次
     * {@code drainTo} 摘回——一次提交摊薄批量搬运，ops 计每次元素搬运
     * （put 与 drain 交付各一）。
     *
     * @param client 客户端
     * @param millis 采样时长
     * @return 结果
     * @throws InterruptedException 采样被打断
     */
    private static Result runQueueDrain(OpenLatchClient client, long millis)
            throws InterruptedException {
        OBlockingQueue queue = client.newBlockingQueue("bench:queue:drain", 512);
        java.util.List<byte[]> sink = new ArrayList<>();
        byte[] element = "bench-queue-drain".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        AtomicLong moved = new AtomicLong();
        Reservoir reservoir = new Reservoir();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            long start = System.nanoTime();
            for (int i = 0; i < QUEUE_DRAIN_BATCH; i++) {
                queue.put(element);
            }
            moved.addAndGet(QUEUE_DRAIN_BATCH);
            sink.clear();
            int drained = queue.drainTo(sink, QUEUE_DRAIN_BATCH);
            moved.addAndGet(drained);
            reservoir.record(System.nanoTime() - start);
        }
        return new Result(Math.round(moved.doubleValue() * 1_000.0 / millis),
                reservoir.sortedSamples());
    }

    /**
     * 队列延时到期相（v7）：单线程 {@code offerDelayed(100ms)} 后立即 {@code take}
     * ——吞吐为流水化双操作，延迟样本为<b>超出 100ms 基线的唤醒尾延</b>
     * （tick 精度基线：P50≈tick/2、P99≈tick 量级，判例就绪扫描语义）。
     *
     * @param client 客户端
     * @param millis 采样时长
     * @return 结果（{@code latencies} 为尾延毫秒样本）
     * @throws InterruptedException 采样被打断
     */
    private static Result runQueueDelay(OpenLatchClient client, long millis)
            throws InterruptedException {
        ODelayQueue queue = client.newDelayQueue("bench:queue:delay", 512);
        byte[] element = "bench-queue-delay".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long baseNs = TimeUnit.MILLISECONDS.toNanos(100);
        AtomicLong ops = new AtomicLong();
        Reservoir overshoot = new Reservoir();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            queue.offerDelayed(element, 100, TimeUnit.MILLISECONDS);
            long start = System.nanoTime();
            queue.take();
            overshoot.record(Math.max(0, System.nanoTime() - start - baseNs));
            ops.addAndGet(2);
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                overshoot.sortedSamples());
    }

    /**
     * topic 发布受理相（v8）：零订阅 hot loop 下 publish 的受理路径 RTT 与
     * 吞吐（fan-out 面为空的纯提交开销基线，含去重槽写与 seq 分配）。
     *
     * @param client 客户端
     * @param millis 采样时长
     * @return 结果（ops=publish 次数，延迟为单发布 RTT）
     * @throws InterruptedException 采样被打断
     */
    private static Result runTopicPublish(OpenLatchClient client, long millis)
            throws InterruptedException {
        OTopic topic = client.newTopic("bench:topic:pub");
        byte[] message = "bench-topic-message".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        AtomicLong ops = new AtomicLong();
        Reservoir reservoir = new Reservoir();
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
        while (System.nanoTime() < deadline) {
            long start = System.nanoTime();
            topic.publish(message);
            reservoir.record(System.nanoTime() - start);
            ops.incrementAndGet();
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                reservoir.sortedSamples());
    }

    /**
     * topic 扇出交付相（v8）：主连接持续 publish，{@code subscribers} 条独立
     * 连接订阅同键；吞吐计受理 publish 数（广播 ops 放大为 ops×subscribers），
     * 延迟取发布→订阅 0 收到首份交付的跨线程时延（订阅侧本地缓冲丢弃不影响
     * 本相断言——至多一次基线本就允许丢）。
     *
     * @param client      发布端客户端
     * @param port        服务端端口（订阅端另建连接）
     * @param subscribers 订阅端连接数
     * @param millis      采样时长
     * @return 结果
     * @throws InterruptedException 建连/采样被打断
     * @throws java.util.concurrent.ExecutionException 订阅端建连失败
     * @throws java.util.concurrent.TimeoutException 订阅端建连超时
     */
    private static Result runTopicFanout(OpenLatchClient client, int port, int subscribers,
            long millis) throws InterruptedException, java.util.concurrent.ExecutionException,
            java.util.concurrent.TimeoutException {
        OTopic publisher = client.newTopic("bench:topic:fanout");
        byte[] message = "bench-topic-fanout".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.util.concurrent.ConcurrentHashMap<Long, Long> inflight =
                new java.util.concurrent.ConcurrentHashMap<>();
        AtomicLong ops = new AtomicLong();
        Reservoir reservoir = new Reservoir();
        java.util.List<OpenLatchClient> subClients = new ArrayList<>();
        java.util.List<OTopicSubscription> subs = new ArrayList<>();
        try {
            for (int i = 0; i < subscribers; i++) {
                OpenLatchClient subClient = OpenLatchClient.builder()
                        .address("127.0.0.1:" + port)
                        .defaultWaitTimeout(Duration.ofSeconds(60))
                        .build();
                subClient.connectAsync().get(10, TimeUnit.SECONDS);
                subClients.add(subClient);
                final boolean first = i == 0;
                subs.add(subClient.newTopic("bench:topic:fanout").subscribe(m -> {
                    if (first) {
                        Long start = inflight.remove(m.topicSeq());
                        if (start != null) {
                            reservoir.record(System.nanoTime() - start);
                        }
                    }
                }));
            }
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
            while (System.nanoTime() < deadline) {
                long start = System.nanoTime();
                long seq = publisher.publish(message);
                ops.incrementAndGet();
                if (inflight.size() > 8_192) {
                    inflight.clear(); // 慢订阅滞后窗：丢弃最旧基线（本相只测交付能力）
                }
                inflight.put(seq, start);
            }
        } finally {
            for (OTopicSubscription sub : subs) {
                sub.close();
            }
            for (OpenLatchClient subClient : subClients) {
                subClient.shutdown();
            }
        }
        return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                reservoir.sortedSamples());
    }

    /**
     * 渲染 topic 相小节（v8），追加至统一基线报告。
     *
     * @param publishThroughput 发布受理吞吐批
     * @param publishLatencies  发布 RTT 延迟批
     * @param fanoutThroughput  扇出受理吞吐批
     * @param fanoutLatencies   扇出交付时延批（订阅 0 视角）
     * @return Markdown 小节
     */
    private static String renderTopicSection(List<long[]> publishThroughput,
            List<double[]> publishLatencies, List<long[]> fanoutThroughput,
            List<double[]> fanoutLatencies) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n## topic 相（v8）\n\n");
        sb.append("| 场景 | ops/s（中位） | 延迟 P50 (ms) | P99 (ms) |\n");
        sb.append("|---|---|---|---|\n");
        sb.append("| 零订阅发布受理（RTT 基线） | ").append(medianOps(publishThroughput))
                .append(" | ").append(fmt(medianQuantile(publishLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(publishLatencies, 0.99))).append(" |\n");
        sb.append("| 1×8 扇出交付（发布→首订阅时延） | ")
                .append(medianOps(fanoutThroughput))
                .append(" | ").append(fmt(medianQuantile(fanoutLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(fanoutLatencies, 0.99))).append(" |\n");
        return sb.toString();
    }

    /**
     * signal 受理相（v9）：持有者对空集条件的 signal hot loop——纯 Leader 本地
     * 无操作裁决的 CONDITION_OP 往返 RTT 基线（零搬运、零日志贡献的入口面）。
     *
     * @param client 客户端（单连接持锁）
     * @param millis 采样时长
     * @return 结果（ops=signal 次数，延迟为单帧 RTT）
     * @throws InterruptedException 采样被打断
     */
    private static Result runConditionSignal(OpenLatchClient client, long millis)
            throws InterruptedException {
        OLock lock = client.newReentrantLock("bench:cond:signal");
        OCondition cond = lock.newCondition("tick");
        lock.lock();
        try {
            AtomicLong ops = new AtomicLong();
            Reservoir reservoir = new Reservoir();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
            while (System.nanoTime() < deadline) {
                long start = System.nanoTime();
                cond.signal();
                reservoir.record(System.nanoTime() - start);
                ops.incrementAndGet();
            }
            return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                    reservoir.sortedSamples());
        } finally {
            lock.unlock();
        }
    }

    /**
     * await/signal 乒乓交接相（v9）：主连接（生产侧）与独立连接（消费侧）经
     * 同 key 同条件 {@code "turn"} 交替持锁交接——每轮测"signal 发出→对侧从
     * await 携锁返回"的唤醒全链路时延（折叠登记、搬运、释放接力通知、原 id
     * 重发授予四段合计），吞吐为完成轮数。守卫循环用带预算 await 形态
     * （guard loop 标准惯用法即用户面真实路径）。
     *
     * @param client 生产侧客户端
     * @param port   服务端端口（消费侧另建连接）
     * @param millis 采样时长
     * @return 结果
     * @throws InterruptedException 建连/采样被打断
     * @throws java.util.concurrent.ExecutionException 消费端建连失败
     * @throws java.util.concurrent.TimeoutException 消费端建连超时
     */
    private static Result runConditionHandoff(OpenLatchClient client, int port, long millis)
            throws InterruptedException, java.util.concurrent.ExecutionException,
            java.util.concurrent.TimeoutException {
        OLock pLock = client.newReentrantLock("bench:cond:ping");
        OCondition pCond = pLock.newCondition("turn");
        OpenLatchClient subClient = OpenLatchClient.builder()
                .address("127.0.0.1:" + port)
                .defaultWaitTimeout(Duration.ofSeconds(60))
                .build();
        subClient.connectAsync().get(10, TimeUnit.SECONDS);
        try {
            OLock cLock = subClient.newReentrantLock("bench:cond:ping");
            OCondition cCond = cLock.newCondition("turn");
            java.util.concurrent.atomic.AtomicInteger state =
                    new java.util.concurrent.atomic.AtomicInteger();   // 0=生产轮 1=消费轮
            java.util.concurrent.atomic.AtomicBoolean ended =
                    new java.util.concurrent.atomic.AtomicBoolean();
            java.util.concurrent.atomic.AtomicLong signalTs =
                    new java.util.concurrent.atomic.AtomicLong();
            AtomicLong ops = new AtomicLong();
            Reservoir reservoir = new Reservoir();
            Thread consumer = new Thread(() -> {
                try {
                    while (!ended.get()) {
                        cLock.lock();
                        try {
                            while (state.get() != 1 && !ended.get()) {
                                cCond.await(1, TimeUnit.SECONDS);
                            }
                            if (ended.get()) {
                                return;
                            }
                            reservoir.record(System.nanoTime() - signalTs.get());
                            state.set(0);
                            cCond.signal();
                        } finally {
                            cLock.unlock();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, "bench-cond-consumer");
            consumer.setDaemon(true);
            consumer.start();
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
            while (System.nanoTime() < deadline) {
                pLock.lock();
                try {
                    while (state.get() != 0) {
                        pCond.await(1, TimeUnit.SECONDS);
                    }
                    state.set(1);
                    signalTs.set(System.nanoTime());
                    pCond.signal();
                } finally {
                    pLock.unlock();
                }
                ops.incrementAndGet();
            }
            // 收工唤醒：终结哨兵经消费侧守卫循环的 ended 分支生效，不留悬挂等待。
            ended.set(true);
            cLock.lock();
            try {
                state.set(1);
                cCond.signal();
            } finally {
                cLock.unlock();
            }
            consumer.join(10_000);
            return new Result(Math.round(ops.doubleValue() * 1_000.0 / millis),
                    reservoir.sortedSamples());
        } finally {
            subClient.shutdown();
        }
    }

    /**
     * 渲染条件相小节（v9），追加至统一基线报告。
     *
     * @param signalThroughput  signal 受理吞吐批
     * @param signalLatencies   signal 受理 RTT 批
     * @param handoffThroughput 乒乓交接吞吐批
     * @param handoffLatencies  唤醒全链路时延批
     * @return Markdown 小节
     */
    private static String renderConditionSection(List<long[]> signalThroughput,
            List<double[]> signalLatencies, List<long[]> handoffThroughput,
            List<double[]> handoffLatencies) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n## 条件相（v9）\n\n");
        sb.append("| 场景 | ops/s（中位） | 延迟 P50 (ms) | P99 (ms) |\n");
        sb.append("|---|---|---|---|\n");
        sb.append("| signal 受理（空集 RTT 基线） | ").append(medianOps(signalThroughput))
                .append(" | ").append(fmt(medianQuantile(signalLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(signalLatencies, 0.99))).append(" |\n");
        sb.append("| await/signal 乒乓交接（唤醒全链路） | ")
                .append(medianOps(handoffThroughput))
                .append(" | ").append(fmt(medianQuantile(handoffLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(handoffLatencies, 0.99))).append(" |\n");
        return sb.toString();
    }

    /**
     * 渲染队列相小节（v7），追加至统一基线报告。
     *
     * @param handoffThroughput 配对交接吞吐批
     * @param handoffLatencies  配对交接延迟批
     * @param fanoutThroughput  扇出吞吐批
     * @param fanoutLatencies   扇出 take 延迟批（含等待）
     * @param drainThroughput   drain 搬运吞吐批
     * @param drainLatencies    drain 整轮（32 put + 1 drain）耗时批
     * @param delayOvershoot    延时尾延样本批
     * @return Markdown 小节
     */
    private static String renderQueueSection(List<long[]> handoffThroughput,
            List<double[]> handoffLatencies, List<long[]> fanoutThroughput,
            List<double[]> fanoutLatencies, List<long[]> drainThroughput,
            List<double[]> drainLatencies, List<double[]> delayOvershoot) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n## 队列相（v7）\n\n");
        sb.append("| 场景 | ops/s（中位） | 延迟 P50 (ms) | P99 (ms) |\n");
        sb.append("|---|---|---|---|\n");
        sb.append("| put+take 配对交接（2 ops/轮） | ").append(medianOps(handoffThroughput))
                .append(" | ").append(fmt(medianQuantile(handoffLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(handoffLatencies, 0.99))).append(" |\n");
        sb.append("| 1×8 扇出争用（take 含等待） | ").append(medianOps(fanoutThroughput))
                .append(" | ").append(fmt(medianQuantile(fanoutLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(fanoutLatencies, 0.99))).append(" |\n");
        sb.append("| 32-put + 1-drain 摊薄（元素搬运计；延迟为整轮） | ")
                .append(medianOps(drainThroughput))
                .append(" | ").append(fmt(medianQuantile(drainLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(drainLatencies, 0.99))).append(" |\n");
        sb.append("| 延时到期尾延（100ms 基线之外的 tick 超调，ms） | —")
                .append(" | ").append(fmt(medianQuantile(delayOvershoot, 0.5)))
                .append(" | ").append(fmt(medianQuantile(delayOvershoot, 0.99))).append(" |\n");
        return sb.toString();
    }

    /**
     * 每线程蓄水池采样（Vitter 算法 R）。
     */
    static final class Reservoir {

        /**
         * 构造空蓄水池。
         */
        Reservoir() {
        }

        /** 样本存储。 */
        private final long[] store = new long[RESERVOIR];
        /** 已观测样本数。 */
        private long seen;
        /** 伪随机源（线程内使用，无共享）。 */
        private final java.util.Random random = new java.util.Random();

        /**
         * 记录一个样本。
         *
         * @param nanos 样本值
         */
        void record(long nanos) {
            seen++;
            if (seen <= store.length) {
                store[(int) seen - 1] = nanos;
            } else {
                long j = (long) (random.nextDouble() * seen);
                if (j < store.length) {
                    store[(int) j] = nanos;
                }
            }
        }

        /**
         * 观测总数。
         *
         * @return seen
         */
        long seen() {
            return seen;
        }

        /**
         * 当前样本数。
         *
         * @return 样本数
         */
        int size() {
            return (int) Math.min(seen, store.length);
        }

        /**
         * 原始样本视图。
         *
         * @return 样本数组（未排序）
         */
        long[] samples() {
            return Arrays.copyOf(store, size());
        }

        /**
         * 排序样本（毫秒；分位数按最近邻法取值）。
         *
         * @return 排序后毫秒样本
         */
        double[] sortedSamples() {
            double[] out = new double[size()];
            long[] raw = Arrays.copyOf(store, size());
            Arrays.sort(raw);
            for (int i = 0; i < out.length; i++) {
                out[i] = raw[i] / 1_000_000.0;
            }
            return out;
        }
    }

    /**
     * 分位数取值（最近邻法）。
     *
     * @param sorted 排序样本
     * @param q      分位（0~1）
     * @return 毫秒值
     */
    private static double quantile(double[] sorted, double q) {
        if (sorted.length == 0) {
            return 0;
        }
        int idx = (int) Math.min(sorted.length - 1, Math.ceil(q * sorted.length) - 1);
        return sorted[idx];
    }

    /**
     * 多批吞吐取中位。
     *
     * @param batchResults 每批结果
     * @return 中位吞吐
     */
    private static long medianOps(List<long[]> batchResults) {
        long[] values = batchResults.stream().mapToLong(a -> a[0]).sorted().toArray();
        return values[values.length / 2];
    }

    /**
     * 多批延迟样本合并后取中位分位（对每批分别算分位再取中位）。
     *
     * @param batches 每批排序样本
     * @param q       分位
     * @return 毫秒值中位
     */
    private static double medianQuantile(List<double[]> batches, double q) {
        double[] qs = batches.stream().mapToDouble(s -> quantile(s, q)).sorted().toArray();
        return qs[qs.length / 2];
    }

    /**
     * 渲染 Markdown 报告。
     *
     * @param uncThroughput  无竞争吞吐各批
     * @param uncLatencyBatches 无竞争往返延迟各批样本
     * @param contThroughput 各竞争档位吞吐
     * @param latencies      各竞争档位延迟样本批次
     * @param addThroughput  原子写吞吐各批
     * @param addLatencies   原子写延迟各批样本
     * @param getThroughput  原子读吞吐各批
     * @param getLatencies   原子读延迟各批样本
     * @param casThroughput  原子 CAS 争用各档位吞吐
     * @param casLatencies   原子 CAS 争用各档位延迟样本批次
     * @param barrierThroughput 循环屏障各 parties 档位世代吞吐（世代/秒）
     * @param barrierLatencies  循环屏障各档位到场等待延迟样本批次
     * @param actionThroughput  携 barrierAction 世代的吞吐各批
     * @param actionLatencies   携 barrierAction 世代的延迟各批样本
     * @param refSmallThroughput 有值引用 16B 载荷写各批吞吐
     * @param refSmallLatencies  有值引用 16B 载荷写各批延迟样本
     * @param refBigThroughput   有值引用 4KB 恰限载荷写各批吞吐
     * @param refBigLatencies    有值引用 4KB 恰限载荷写各批延迟样本
     * @param refGetThroughput   有值引用读各批吞吐
     * @param refGetLatencies    有值引用读各批延迟样本
     * @param refCasThroughput   引用版本 CAS 争用各档各批吞吐
     * @param refCasLatencies    引用版本 CAS 争用各档各批延迟样本
     * @return Markdown 文本
     */
    private static String renderReport(List<long[]> uncThroughput,
                                       List<double[]> uncLatencyBatches,
                                       List<List<long[]>> contThroughput,
                                       List<List<double[]>> latencies,
                                       List<long[]> addThroughput,
                                       List<double[]> addLatencies,
                                       List<long[]> getThroughput,
                                       List<double[]> getLatencies,
                                       List<List<long[]>> casThroughput,
                                       List<List<double[]>> casLatencies,
                                       List<List<long[]>> barrierThroughput,
                                       List<List<double[]>> barrierLatencies,
                                       List<long[]> actionThroughput,
                                       List<double[]> actionLatencies,
                                       List<long[]> refSmallThroughput,
                                       List<double[]> refSmallLatencies,
                                       List<long[]> refBigThroughput,
                                       List<double[]> refBigLatencies,
                                       List<long[]> refGetThroughput,
                                       List<double[]> refGetLatencies,
                                       List<List<long[]>> refCasThroughput,
                                       List<List<double[]>> refCasLatencies) {
        StringBuilder sb = new StringBuilder();
        sb.append("# OpenLatch 基准基线\n\n");
        sb.append("生成：").append(java.time.LocalDate.now())
                .append("　来源：`BenchmarkMain`（手写 harness）\n\n");
        sb.append("## 环境\n\n");
        sb.append("| 项 | 值 |\n|---|---|\n");
        sb.append("| OS | ").append(System.getProperty("os.name"))
                .append(" ").append(System.getProperty("os.version")).append(" |\n");
        sb.append("| CPU | ").append(Runtime.getRuntime().availableProcessors())
                .append(" 核 |\n");
        sb.append("| JDK | ").append(System.getProperty("java.version")).append(" |\n");
        sb.append("| 服务器 | 进程内内嵌，默认配置（lease 30s / tick 500ms / 临时端口） |\n");
        sb.append("| 客户端 | 单实例（连接多路复用），1 EventLoop 线程 |\n");
        sb.append("| 采样 | 热身 4s；").append(BATCHES).append(" 批 × ")
                .append(SAMPLE_MS / 1000).append("s，吞吐取批中位；")
                .append("延迟为蓄水池样本（每线程 ").append(RESERVOIR)
                .append("），分位数按批计算后取中位 |\n\n");
        sb.append("## 指标\n\n");
        sb.append("| 场景 | ops/s（中位） | 往返/授予延迟 P50 (ms) | P99 (ms) |\n");
        sb.append("|---|---|---|---|\n");
        sb.append("| 无竞争 tryLock 往返 | ").append(medianOps(uncThroughput))
                .append(" | ").append(fmt(medianQuantile(uncLatencyBatches, 0.5)))
                .append(" | ").append(fmt(medianQuantile(uncLatencyBatches, 0.99))).append(" |\n");
        for (int i = 0; i < CONTENDED_LEVELS.length; i++) {
            sb.append("| ").append(CONTENDED_LEVELS[i]).append(" 线程竞争 lock() | ")
                    .append(medianOps(contThroughput.get(i)))
                    .append(" | ").append(fmt(medianQuantile(latencies.get(i), 0.5)))
                    .append(" | ").append(fmt(medianQuantile(latencies.get(i), 0.99)))
                    .append(" |\n");
        }
        sb.append("| 原子 addAndGet（写 RTT，含提交） | ").append(medianOps(addThroughput))
                .append(" | ").append(fmt(medianQuantile(addLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(addLatencies, 0.99))).append(" |\n");
        sb.append("| 原子 get（读 RTT，经 Raft） | ").append(medianOps(getThroughput))
                .append(" | ").append(fmt(medianQuantile(getLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(getLatencies, 0.99))).append(" |\n");
        for (int i = 0; i < ATOMIC_CAS_LEVELS.length; i++) {
            sb.append("| ").append(ATOMIC_CAS_LEVELS[i])
                    .append(" 线程争用 CAS 加一 | ").append(medianOps(casThroughput.get(i)))
                    .append(" | ").append(fmt(medianQuantile(casLatencies.get(i), 0.5)))
                    .append(" | ").append(fmt(medianQuantile(casLatencies.get(i), 0.99)))
                    .append(" |\n");
        }
        sb.append("| 有值引用 getAndSet（16B 载荷写 RTT） | ").append(medianOps(refSmallThroughput))
                .append(" | ").append(fmt(medianQuantile(refSmallLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(refSmallLatencies, 0.99))).append(" |\n");
        sb.append("| 有值引用 getAndSet（4KB 恰限载荷写 RTT） | ")
                .append(medianOps(refBigThroughput))
                .append(" | ").append(fmt(medianQuantile(refBigLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(refBigLatencies, 0.99))).append(" |\n");
        sb.append("| 有值引用 get（读 RTT，经 Raft） | ").append(medianOps(refGetThroughput))
                .append(" | ").append(fmt(medianQuantile(refGetLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(refGetLatencies, 0.99))).append(" |\n");
        for (int i = 0; i < ATOMIC_CAS_LEVELS.length; i++) {
            sb.append("| ").append(ATOMIC_CAS_LEVELS[i])
                    .append(" 线程争用引用版本 CAS | ").append(medianOps(refCasThroughput.get(i)))
                    .append(" | ").append(fmt(medianQuantile(refCasLatencies.get(i), 0.5)))
                    .append(" | ").append(fmt(medianQuantile(refCasLatencies.get(i), 0.99)))
                    .append(" |\n");
        }
        for (int i = 0; i < BARRIER_LEVELS.length; i++) {
            sb.append("| ").append(BARRIER_LEVELS[i])
                    .append(" 方 barrier 会合（世代回卷） | ").append(medianOps(barrierThroughput.get(i)))
                    .append(" | ").append(fmt(medianQuantile(barrierLatencies.get(i), 0.5)))
                    .append(" | ").append(fmt(medianQuantile(barrierLatencies.get(i), 0.99)))
                    .append(" |\n");
        }
        sb.append("| 2 方 barrier 会合携 action（两阶段） | ").append(medianOps(actionThroughput))
                .append(" | ").append(fmt(medianQuantile(actionLatencies, 0.5)))
                .append(" | ").append(fmt(medianQuantile(actionLatencies, 0.99))).append(" |\n");
        sb.append("\n> 竞争场景延迟列为**授予延迟**（发起到授予，含排队）；")
                .append("CAS 争用场景为**单次成功的完整耗时**（含重试轮次）；")
                .append("barrier 吞吐为**完成世代数/秒**，延迟为单次到场等待耗时")
                .append("（量级即合拢端到端延迟，携 action 行对照两阶段放行开销）。")
                .append("本基线仅作防退化参考，不作发布门槛。\n");
        return sb.toString();
    }

    /**
     * 毫秒浮点格式化。
     *
     * @param v 值
     * @return 字符串
     */
    private static String fmt(double v) {
        return String.format("%.2f", v);
    }

    /**
     * 报告输出路径：{@code -Dbenchmark.output} 优先，否则仓库根
     * {@code target/benchmark/benchmark-baseline-<date>.md}——仓库根自当前目录
     * 向上寻找含 {@code docs} 目录的祖先（exec:java 与直接 {@code java}
     * 启动的工作目录不一致，故不依赖相对路径）。
     *
     * @return 输出路径
     */
    private static Path resolveOutputPath() {
        String override = System.getProperty("benchmark.output");
        if (override != null) {
            return Path.of(override);
        }
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null && !Files.isDirectory(dir.resolve("docs"))) {
            dir = dir.getParent();
        }
        Path root = dir != null ? dir : Path.of("").toAbsolutePath();
        return root.resolve("target").resolve("benchmark")
                .resolve("benchmark-baseline-" + java.time.LocalDate.now() + ".md");
    }
}
