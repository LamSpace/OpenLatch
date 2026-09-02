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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 滚动重启演练（S4/P2-18；详设 §8"滚动重启"行、§11 验收 5）：三节点集群在
 * 持续混合负载下逐台重启（任意时刻 ≥ 多数派存活），两种顺序（先主后从 /
 * 先从后主），统计<b>应用可见</b>客户端错误率——验收口径 &lt; 1%（切换窗口
 * 瞬断由 SDK 内建重连重放与 NOT_LEADER 改道吸收，详设 §6.3；等待式获取的
 * waitMs 取 2s 以覆盖选举窗口）。
 *
 * <p>产物：报告追加 {@code docs/rolling-restart-drill-<日期>.md}（两顺序的
 * 总请求/错误/错误率/单台恢复时长，失败轮次如实记录）；门控与 shaded jar
 * 纪律同 {@link LeaderKillDrillIT}（{@code @Tag("drill")}，缺失显式告警跳过）。
 */
@Tag("drill")
@Timeout(value = 340, unit = TimeUnit.SECONDS)
class RollingRestartDrillIT {

    /** 单节点重启后端口就绪等待上限（秒）。 */
    private static final long WAIT_SECONDS = 30;
    /** 负载线程数与节奏（毫秒/轮：一轮 = 获取+释放）。 */
    private static final int DRIVERS = 2;
    private static final long DRIVE_INTERVAL_MS = 150;
    /** 负载总时长（秒）：重启窗口 ~15s，其余时间为摊薄窗内瞬错的稳态流量。 */
    private static final long DRIVE_SECONDS = 200;

    /** 可重启节点句柄（配置与数据目录跨重启复用；process 随重启替换）。 */
    private static final class Node {
        /** 节点 id。 */
        final int id;
        /** 配置文件（重启复用）。 */
        final Path cfg;
        /** 数据目录（重启复用）。 */
        final Path dataDir;
        /** 接入端口（重启复用）。 */
        final int accessPort;
        /** Raft 端口（重启复用）。 */
        final int raftPort;
        /** 当前子进程（{@code null}=已停未启）。 */
        Process process;

        Node(int id, Path cfg, Path dataDir, int accessPort, int raftPort) {
            this.id = id;
            this.cfg = cfg;
            this.dataDir = dataDir;
            this.accessPort = accessPort;
            this.raftPort = raftPort;
        }

        /** @return 节点 id */ int id() { return id; }
        /** @return 配置文件 */ Path cfg() { return cfg; }
        /** @return 数据目录 */ Path dataDir() { return dataDir; }
        /** @return 接入端口 */ int accessPort() { return accessPort; }
        /** @return Raft 端口 */ int raftPort() { return raftPort; }
    }

    @Test
    void rollingRestartLeaderFirstKeepsErrorRateBelowOnePercent() throws Exception {
        runRoll(true);
    }

    @Test
    void rollingRestartFollowersFirstKeepsErrorRateBelowOnePercent() throws Exception {
        runRoll(false);
    }

    /** 一轮滚动重启演练：起集群→双线程负载→按序重启三节点→错误率判定→报告。 */
    private void runRoll(boolean leaderFirst) throws Exception {
        // 调试可用 -Ddrill.driveSeconds=<s> 缩短稳态时长快速迭代（默认 DRIVE_SECONDS）。
        long driveSeconds = Long.getLong("drill.driveSeconds", DRIVE_SECONDS);
        Path jar = requireServerJar();
        List<Node> nodes = startCluster(jar);
        OpenLatchClient client = null;
        AtomicBoolean stop = new AtomicBoolean();
        AtomicLong total = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        List<Thread> drivers = new ArrayList<>();
        List<Long> restartMs = new ArrayList<>();
        List<String> errorSamples = java.util.Collections.synchronizedList(new ArrayList<>());
        // 错误与重启窗口的时间线（相对 t0，毫秒）：区分"切换窗口内突发"与
        // "降级后持续失败"两类形态——后者即客户端/服务端真实缺陷（P2-18 目的）。
        long drillT0 = System.currentTimeMillis();
        List<Long> errorTimesMs = java.util.Collections.synchronizedList(new ArrayList<>());
        List<long[]> restartWindows = java.util.Collections.synchronizedList(new ArrayList<>());
        try {
            Node leader = waitLeader(nodes);
            client = OpenLatchClient.builder()
                    .seeds(nodes.stream().map(n -> "127.0.0.1:" + n.accessPort()).toList())
                    .requestTimeout(Duration.ofSeconds(5))
                    .reconnectInitialBackoff(Duration.ofMillis(100))
                    .reconnectMaxBackoff(Duration.ofSeconds(1))
                    .defaultWaitTimeout(Duration.ofSeconds(20))
                    .build();
            client.connectAsync().get(WAIT_SECONDS, TimeUnit.SECONDS);

            final OpenLatchClient c = client;
            for (int t = 0; t < DRIVERS; t++) {
                int tid = t;
                Thread th = Thread.ofPlatform().name("roll-driver-" + t).start(() -> {
                    int i = 0;
                    while (!stop.get()) {
                        String key = "roll-" + tid + "-" + (i++ % 4);
                        try {
                            LockGrant g = c.acquireAsync(new AcquireSpec(key, LockType.REENTRANT,
                                    Thread.currentThread().threadId(), 30_000, 2_000))
                                    .get(8, TimeUnit.SECONDS);
                            c.releaseAsync(key, g.leaseToken(), Thread.currentThread().threadId())
                                    .get(8, TimeUnit.SECONDS);
                            total.incrementAndGet();
                        } catch (Exception e) {
                            total.incrementAndGet();
                            errors.incrementAndGet();
                            errorTimesMs.add(System.currentTimeMillis() - drillT0);
                            if (errorSamples.size() < 8) {
                                errorSamples.add(e.getClass().getSimpleName() + ": " + e.getMessage());
                            }
                        }
                        try {
                            Thread.sleep(DRIVE_INTERVAL_MS);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                });
                drivers.add(th);
            }

            // 重启序列：按选定顺序逐台 destroy(优雅)→等待退出→原目录原端口重启。
            List<Node> order = new ArrayList<>();
            if (leaderFirst) {
                order.add(leader);
                nodes.stream().filter(n -> n != leader).forEach(order::add);
            } else {
                nodes.stream().filter(n -> n != leader).forEach(order::add);
                order.add(leader);
            }
            for (Node n : order) {
                long t0 = System.currentTimeMillis();
                n.process.destroy();
                assertThat(n.process.waitFor(15, TimeUnit.SECONDS))
                        .as("节点%d 优雅退出", n.id()).isTrue();
                n.process = relaunch(n, nodes);
                waitForAccess(n);
                restartMs.add(System.currentTimeMillis() - t0);
                restartWindows.add(new long[]{t0 - drillT0, System.currentTimeMillis() - drillT0});
                Thread.sleep(1_000); // 让集群稳定再动下一台
            }

            // 稳态续流：重启序列后继续驱动至总时长，摊薄切换窗瞬错（§11-5
            // 口径"仅切换窗口瞬时错误"，比率判定需要足够分母）。
            long driveDeadline = System.currentTimeMillis()
                    + TimeUnit.SECONDS.toMillis(driveSeconds);
            while (System.currentTimeMillis() < driveDeadline && !stop.get()) {
                Thread.sleep(500);
            }
            stop.set(true);
            for (Thread th : drivers) {
                th.join(20_000);
            }
            // 终态：集群健康，业务可完成。
            LockGrant finalGrant = client.acquireAsync(new AcquireSpec("roll-final",
                    LockType.REENTRANT, Thread.currentThread().threadId(), 5_000, 5_000))
                    .get(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(finalGrant.leaseToken()).isPositive();

            long tot = total.get();
            long err = errors.get();
            double ratePct = tot == 0 ? 100 : 100.0 * err / tot;
            String tag = leaderFirst ? "先主后从" : "先从后主";
            // 形态分类（stdout 即见）：重启窗口区间 vs 错误时刻。
            System.out.println("[DRILL] " + tag + " errors=" + err + "/" + tot
                    + " (" + String.format("%.2f", ratePct) + "%)"
                    + " restartWindows=" + windowsToString(restartWindows)
                    + " errorTimes=" + timesToString(errorTimesMs));
            appendReport(tag, tot, err, ratePct, restartMs, errorSamples,
                    errorTimesMs, restartWindows);
            assertThat(ratePct).as("滚动重启（%s）客户端错误率 < 1%%（§11-5；总 %d 错 %d）",
                    tag, tot, err).isLessThan(1.0);
        } finally {
            stop.set(true);
            if (client != null) {
                client.shutdown();
            }
            for (Node n : nodes) {
                Process p = n.process;
                if (p != null && p.isAlive()) {
                    p.destroyForcibly();
                }
            }
        }
    }

    // ---------- 集群装配 / 重启 / 探测（纪律同 LeaderKillDrillIT） ----------

    private static List<Node> startCluster(Path jar) throws IOException, InterruptedException {
        int n = 3;
        int[] access = new int[n];
        int[] raft = new int[n];
        for (int i = 0; i < n; i++) {
            access[i] = freePort();
            raft[i] = freePort();
        }
        StringBuilder peers = new StringBuilder();
        StringBuilder addrs = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                peers.append(',');
                addrs.append(',');
            }
            peers.append((i + 1)).append("@127.0.0.1:").append(raft[i]);
            addrs.append((i + 1)).append("@127.0.0.1:").append(access[i]);
        }
        List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Path dir = Files.createTempDirectory("openlatch-roll-node-");
            Path cfg = Files.createTempFile("openlatch-roll", ".properties");
            Files.writeString(cfg, """
                    openlatch.server.port=%d
                    openlatch.cluster.enabled=true
                    openlatch.cluster.node-id=%d
                    openlatch.cluster.peers=%s
                    openlatch.cluster.client-addresses=%s
                    openlatch.cluster.raft-port=%d
                    openlatch.cluster.data-dir=%s
                    openlatch.cluster.election-timeout-ms=800
                    """.formatted(access[i], i + 1, peers, addrs, raft[i], dir));
            Node node = new Node(i + 1, cfg, dir, access[i], raft[i]);
            node.process = launch(jar, node);
            waitForAccess(node);
            nodes.add(node);
        }
        return nodes;
    }

    private static Process relaunch(Node n, List<Node> all) throws IOException {
        return launch(locateServerJar(), n);
    }

    private static Process launch(Path jar, Node n) throws IOException {
        Path nodeLog = Path.of("target", "drill-logs",
                "roll-node" + n.id() + "-" + n.accessPort() + ".log");
        Files.createDirectories(nodeLog.getParent());
        return new ProcessBuilder("java", "-Dopenlatch.config=" + n.cfg(),
                "-jar", jar.toAbsolutePath().toString())
                .redirectErrorStream(true)
                .redirectOutput(nodeLog.toFile())
                .start();
    }

    private static void waitForAccess(Node n) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(WAIT_SECONDS);
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket()) {
                ignored.connect(new InetSocketAddress("127.0.0.1", n.accessPort()), 200);
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new AssertionError("节点 " + n.id() + " 接入端口 " + n.accessPort() + " 未在时限内就绪");
    }

    private static Node waitLeader(List<Node> nodes) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (Node n : nodes) {
                Process p = n.process;
                if (p != null && p.isAlive() && isLeaderNode(n)) {
                    return n;
                }
            }
            Thread.sleep(300);
        }
        throw new AssertionError("30s 内未探测到 Leader");
    }

    /** 与 LeaderKillDrillIT 同法：原始 socket HELLO 自报 hint 判角色。 */
    private static boolean isLeaderNode(Node n) {
        try (Socket s = new Socket()) {
            s.setSoTimeout(3_000);
            s.connect(new InetSocketAddress("127.0.0.1", n.accessPort()), 1_000);
            io.github.lamspace.openlatch.protocol.Envelope hello =
                    io.github.lamspace.openlatch.protocol.Envelope.newBuilder()
                            .setProtocolVersion(2)
                            .setType(io.github.lamspace.openlatch.protocol.MessageType.HELLO)
                            .setRequestId(1L)
                            .setHelloRequest(io.github.lamspace.openlatch.protocol.HelloRequest
                                    .newBuilder().setClientProtocolVersion(2)
                                    .setClientName("roll-role-probe"))
                            .build();
            byte[] body = hello.toByteArray();
            java.io.DataOutputStream out = new java.io.DataOutputStream(s.getOutputStream());
            out.writeInt(body.length);
            out.write(body);
            out.flush();
            java.io.DataInputStream in = new java.io.DataInputStream(s.getInputStream());
            int len = in.readInt();
            byte[] respBytes = in.readNBytes(len);
            io.github.lamspace.openlatch.protocol.Envelope resp =
                    io.github.lamspace.openlatch.protocol.Envelope.parseFrom(respBytes);
            return resp.hasHelloResponse()
                    && resp.getHelloResponse().getStatus()
                    == io.github.lamspace.openlatch.protocol.StatusCode.OK
                    && resp.getHelloResponse().getLeaderHint() == n.id();
        } catch (Exception e) {
            return false;
        }
    }

    private static Path requireServerJar() {
        Path jar = locateServerJar();
        if (jar == null) {
            System.err.println("[WARN] RollingRestartDrillIT SKIPPED: openlatch-server executable "
                    + "shade jar not found; run 'mvn -s <settings> -pl openlatch-server -am package' "
                    + "before '-Pdrill' to make this P2-18 fault-injection case effective.");
        }
        assumeTrue(jar != null, "openlatch-server shaded jar not built; run package first");
        return jar;
    }

    private static Path locateServerJar() {
        List<Path> candidates = List.of(
                Path.of("..", "openlatch-server", "target",
                        "openlatch-server-1.0-SNAPSHOT-executable.jar"),
                Path.of("openlatch-server", "target",
                        "openlatch-server-1.0-SNAPSHOT-executable.jar"));
        for (Path c : candidates) {
            Path abs = c.toAbsolutePath().normalize();
            if (Files.exists(abs)) {
                return abs;
            }
        }
        return null;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** 报告追加（同日单文件，两顺序各写一节；入库仓库根 docs/）。 */
    private static void appendReport(String tag, long tot, long err, double ratePct,
                                     List<Long> restartMs, List<String> errorSamples,
                                     List<Long> errorTimesMs, List<long[]> restartWindows)
            throws IOException {
        Path out = Path.of("..", "docs", "rolling-restart-drill-"
                + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE) + ".md");
        Files.createDirectories(out.getParent());
        if (!Files.exists(out)) {
            Files.writeString(out, "# 滚动重启演练报告（s4 P2-18）\n\n"
                    + "- 生成：3 节点本机 shaded jar，election-timeout 800ms，"
                    + DRIVERS + " 驱动线程 × " + DRIVE_INTERVAL_MS + "ms 节奏\n\n");
        }
        Files.writeString(out, "## 顺序：" + tag + "\n\n"
                + "| 指标 | 值 | 判定 |\n|---|---|---|\n"
                + "| 应用可见请求总数 | " + tot + " | — |\n"
                + "| 应用可见错误数 | " + err + " | — |\n"
                + "| 客户端错误率 | " + String.format("%.2f", ratePct) + " % | < 1 % "
                + (ratePct < 1.0 ? "✅" : "❌（如实记录）") + " |\n"
                + "| 逐台重启耗时（ms） | " + restartMs + " | 任意时刻 ≥2/3 存活 |\n"
                + "| 重启窗口（ms 相对 t0） | " + windowsToString(restartWindows) + " | — |\n"
                + "| 错误时刻（ms 相对 t0） | " + timesToString(errorTimesMs)
                + " | 判定突发/持续 |\n"
                + "| 错误归因样本（≤8） | " + String.join("; ", errorSamples) + " | — |\n\n",
                java.nio.file.StandardOpenOption.APPEND);
        System.out.println("[DRILL] report appended: " + out.toAbsolutePath());
    }

    /** 重启窗口列表格式化（{@code [start,end]} 各一）。 */
    private static String windowsToString(List<long[]> ws) {
        StringBuilder sb = new StringBuilder();
        for (long[] w : ws) {
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append('[').append(w[0]).append(',').append(w[1]).append(']');
        }
        return sb.toString();
    }

    /** 错误时刻分布：≤24 个逐列，超出则以区间摘要防报告爆炸。 */
    private static String timesToString(List<Long> ts) {
        if (ts.isEmpty()) {
            return "（无）";
        }
        if (ts.size() <= 24) {
            return ts.toString();
        }
        return ts.size() + " 个，首 " + ts.get(0) + " → 末 " + ts.get(ts.size() - 1)
                + " ms（相邻采样中位间隔 "
                + medianGapMs(ts) + " ms）";
    }

    /** 相邻错误时刻间隔的中位数（持续失败=小且稳定；突发=大间断）。 */
    private static long medianGapMs(List<Long> ts) {
        List<Long> sorted = new ArrayList<>(ts);
        java.util.Collections.sort(sorted);
        List<Long> gaps = new ArrayList<>();
        for (int i = 1; i < sorted.size(); i++) {
            gaps.add(sorted.get(i) - sorted.get(i - 1));
        }
        return gaps.get(gaps.size() / 2);
    }
}
