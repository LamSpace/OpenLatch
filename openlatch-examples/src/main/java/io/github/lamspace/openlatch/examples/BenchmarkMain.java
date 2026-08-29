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

import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;

/**
 * 基准 harness（详设 §9/§10.5，design D5 手写方案）：
 * 产出三项基线指标——无竞争 {@code tryLock} 往返吞吐、竞争（16/64 线程）
 * 吞吐、竞争授予延迟分位数（蓄水池采样，P50/P99）。
 *
 * <p><b>定位</b>：记录为基线防退化参考，<b>不作发布门槛</b>（§10.5）；
 * 结果写入 {@code docs/benchmark-baseline-<date>.md}（可用系统属性
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
            String report = renderReport(uncThroughput, uncLatencyBatches,
                    contThroughput, latencies);
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
     * @return Markdown 文本
     */
    private static String renderReport(List<long[]> uncThroughput,
                                       List<double[]> uncLatencyBatches,
                                       List<List<long[]>> contThroughput,
                                       List<List<double[]>> latencies) {
        StringBuilder sb = new StringBuilder();
        sb.append("# OpenLatch Phase 1 基准基线\n\n");
        sb.append("生成：").append(java.time.LocalDate.now())
                .append("　来源：`BenchmarkMain`（design D5 手写 harness）\n\n");
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
        sb.append("\n> 竞争场景延迟列为**授予延迟**（发起到授予，含排队）。")
                .append("本基线仅作防退化参考，不作发布门槛（详设 §10.5）。\n");
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
     * {@code docs/benchmark-baseline-<date>.md}——仓库根自当前目录向上
     * 寻找含 {@code docs} 目录的祖先（exec:java 与直接 {@code java}
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
        return root.resolve("docs")
                .resolve("benchmark-baseline-" + java.time.LocalDate.now() + ".md");
    }
}
