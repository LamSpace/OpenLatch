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

// client LockType 枚举（非 protocol 形态）
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 进程级杀 Leader 演练（变更 s3-leader-discovery-failover 4.1/4.2，P2-14
 * S3 退出门；详设 §10"故障演练"、§11 验收 1/2 的计时部分）：
 * {@code kill -9} 三节点集群的当值 Leader，计时"杀 → 客户端首次成功业务"
 * 恢复窗口，并断言 §8 双场景与不变式。
 *
 * <p><b>运行门控</b>：{@code @Tag("drill")}，默认构建排除；执行需先
 * {@code mvn -s <settings> -pl openlatch-server -am package} 产出 shaded jar，
 * 再以 {@code mvn verify -Pdrill -pl openlatch-client -Dit.test=LeaderKillDrillIT}
 * 触发（jar 缺失时显式告警跳过，与 {@code ClientProcessKillIT} 同纪律）。
 *
 * <p><b>场景覆盖</b>（详设 §8 行为表，杀 Leader 行）：
 * <ol>
 *   <li>恢复计时：kill -9 Leader → 存活客户端经重定向/种子发现在 &lt;10s 内
 *       完成一次全新授予（含选举 + 客户端改道）；</li>
 *   <li>home=死主：持锁连接断开 → 失锁回调触发（锁本应随会话清理，§8 行 1）；</li>
 *   <li>杀 Follower：多数派仍满足，Leader 上业务无感（授予持续成功）；</li>
 *   <li>不变式：单驱动线程持锁审计——同键不出现双活持有者；停载后锁表经
 *       租约兜底清空（下一驱动以全新授予成功即证）。双授强检查器随
 *       P2-19 混沌测试横扩（此处以单驱动串行 + token 审计覆盖最小面）。</li>
 * </ol>
 *
 * <p>产物：结构化演练报告写入 {@code docs/failover-drill-<日期>.md}。
 */
@Tag("drill")
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class LeaderKillDrillIT {

    /** 端口就绪/事件等待统一时限（秒）。 */
    private static final long WAIT_SECONDS = 20;
    /** 恢复预算阈值（详设 §2.4：Leader 故障到恢复服务 < 10s）。 */
    private static final long RECOVERY_BUDGET_MS = 10_000;

    /** 节点句柄。 */
    private record Node(int id, Process process, int accessPort, int raftPort) {
    }

    @Test
    void killLeaderRecoversWithinTenSeconds() throws Exception {
        Path jar = requireServerJar();
        List<Node> nodes = startCluster(jar);
        OpenLatchClient client = null;
        try {
            Node leader = waitLeader(nodes);

            client = OpenLatchClient.builder()
                    .seeds(nodes.stream().map(n -> "127.0.0.1:" + n.accessPort()).toList())
                    .requestTimeout(Duration.ofSeconds(5))
                    .reconnectInitialBackoff(Duration.ofMillis(100))
                    .reconnectMaxBackoff(Duration.ofSeconds(2))
                    .defaultWaitTimeout(Duration.ofSeconds(20))
                    .build();
            client.connectAsync().get(WAIT_SECONDS, TimeUnit.SECONDS);

            // 场景 2：home=未来死主——持锁 + 失锁回调挂接。短租约（3s）使
            // 断连失锁裁决（lostAt = 上次续租 + 租期）落在观察窗内。
            LinkedBlockingQueue<String> lost = new LinkedBlockingQueue<>();
            client.addLockLostListener((key, cause) -> lost.offer(key));
            LockGrant g = client.acquireAsync(new AcquireSpec("drill-hold", LockType.REENTRANT,
                    Thread.currentThread().threadId(), 3_000, -1)).get(WAIT_SECONDS, TimeUnit.SECONDS);

            // ——计时开始：kill -9 当值 Leader（不触发优雅让位）——
            long t0 = System.currentTimeMillis();
            leader.process().destroyForcibly();
            leader.process().waitFor(10, TimeUnit.SECONDS);

            // 恢复（端到端计时）：重试环。按 §6.2 断连快速失败契约，落在死
            // 车道上的尝试会立即失败（调用方重试）；重试直到命中已重连的
            // home/获取车道并被客户端内建重定向与发现机制授予。单次等待
            // 取满内部预算上限（defaultWaitTimeout 20s），绝不抛弃在途请求
            // （抛弃会让迟到的孤儿授予被看门狗无限续租）。
            long recoveryMs = -1;
            long deadline = t0 + RECOVERY_BUDGET_MS * 6; // 观察上限 60s，判定线 10s
            final String recoveryKey = "drill-recover";
            LockGrant again = null;
            int attempt = 0;
            while (System.currentTimeMillis() < deadline && again == null) {
                attempt++;
                try {
                    again = client.acquireAsync(new AcquireSpec(recoveryKey,
                            LockType.REENTRANT, Thread.currentThread().threadId(), 30_000, -1))
                            .get(20, TimeUnit.SECONDS);
                    recoveryMs = System.currentTimeMillis() - t0;
                } catch (Exception retryLater) {
                    Thread.sleep(50);
                }
            }
            assertThat(again).as("端到端恢复：授予在观察窗内完成（尝试 %d 次）", attempt).isNotNull();
            assertThat(recoveryMs).as("端到端恢复 < 10s（详设 §2.4/§11-2）")
                    .isLessThan(RECOVERY_BUDGET_MS);

            // 旧锁：home 宕机 → 失锁回调（§8 行 1；宽限期 = 3s 租约）
            String lostKey = lost.poll(10, TimeUnit.SECONDS);
            boolean holdLostReported = "drill-hold".equals(lostKey);
            assertThat(holdLostReported).as("home=死主：失锁回调触发").isTrue();

            // 不变式收口：恢复授予释放后，同键可全新授予（锁表无泄漏残留）
            client.releaseAsync(recoveryKey, again.leaseToken(),
                    Thread.currentThread().threadId()).join();
            LockGrant fresh = client.acquireAsync(new AcquireSpec(recoveryKey,
                    LockType.REENTRANT, Thread.currentThread().threadId(), 1_000, -1))
                    .get(WAIT_SECONDS, TimeUnit.SECONDS);
            assertThat(fresh.leaseToken()).isPositive();
            client.releaseAsync(recoveryKey, fresh.leaseToken(),
                    Thread.currentThread().threadId()).join();

            appendReport("## 场景 A：kill -9 当值 Leader（初始 Leader：节点%d）\n\n"
                    .formatted(leader.id())
                    + "| 指标 | 值 | 判定 |\n|---|---|---|\n"
                    + "| kill → 首次成功授予 | " + recoveryMs + " ms | < 10000 ms ✅ |\n"
                    + "| 死主会话失锁回调 | " + (holdLostReported ? "触发" : "未触发") + " | 触发 ✅ |\n"
                    + "| 停载后同键全新授予 | 成功 | 无泄漏 ✅ |\n\n");
        } finally {
            if (client != null) {
                client.shutdown();
            }
            for (Node n : nodes) {
                if (n.process().isAlive()) {
                    n.process().destroyForcibly();
                }
            }
        }
    }

    /**
     * 场景 B：杀单个 Follower——多数派仍满足（3 容忍 1），Leader 混合负载
     * 无感（§8 行 3）。独立三节点集群：与场景 A 不可共用（连杀两节点即
     * 失去多数派）。
     */
    @Test
    void followerKillIsImperceptibleToLeaderTraffic() throws Exception {
        Path jar = requireServerJar();
        List<Node> nodes = startCluster(jar);
        OpenLatchClient client = null;
        try {
            Node leader = waitLeader(nodes);
            client = OpenLatchClient.builder()
                    .seeds(nodes.stream().map(n -> "127.0.0.1:" + n.accessPort()).toList())
                    .requestTimeout(Duration.ofSeconds(5))
                    .defaultWaitTimeout(Duration.ofSeconds(20))
                    .build();
            client.connectAsync().get(WAIT_SECONDS, TimeUnit.SECONDS);

            // 基线混合负载：单键获取+释放 ×5
            driveTraffic(client, 5, "base");

            Node victim = nodes.stream()
                    .filter(n -> n != leader && !isLeaderNode(n))
                    .findFirst().orElseThrow();
            victim.process().destroyForcibly();
            victim.process().waitFor(10, TimeUnit.SECONDS);

            long t1 = System.currentTimeMillis();
            driveTraffic(client, 20, "after"); // 杀 Follower 后 20 轮全部即时成功
            appendReport("## 场景 B：kill -9 单 Follower（节点%d，初始 Leader：节点%d）\n\n"
                    .formatted(victim.id(), leader.id())
                    + "| 指标 | 值 | 判定 |\n|---|---|---|\n"
                    + "| 杀 Follower 后 20 轮获取+释放 | " + (System.currentTimeMillis() - t1)
                    + " ms 全成功 | 多数派满足、无感 ✅ |\n\n");
        } finally {
            if (client != null) {
                client.shutdown();
            }
            for (Node n : nodes) {
                if (n.process().isAlive()) {
                    n.process().destroyForcibly();
                }
            }
        }
    }

    /** 单键混合负载驱动：rounds 轮「获取→释放」，任一失败即断言失败。 */
    private static void driveTraffic(OpenLatchClient client, int rounds, String keyPrefix)
            throws Exception {
        for (int i = 0; i < rounds; i++) {
            String key = keyPrefix + "-traffic";
            LockGrant g = client.acquireAsync(new AcquireSpec(key, LockType.REENTRANT,
                    Thread.currentThread().threadId(), 30_000, -1)).get(5, TimeUnit.SECONDS);
            assertThat(g.leaseToken()).as("%s 轮 %d 授予", keyPrefix, i).isPositive();
            client.releaseAsync(key, g.leaseToken(), Thread.currentThread().threadId())
                    .get(5, TimeUnit.SECONDS);
        }
    }

    /** 定位 shaded 可执行 jar；缺失时显式告警（不静默跳过）。 */
    private static Path requireServerJar() {
        Path jar = locateServerJar();
        if (jar == null) {
            System.err.println("[WARN] LeaderKillDrillIT SKIPPED: openlatch-server executable "
                    + "shade jar not found; run 'mvn -s <settings> -pl openlatch-server -am package' "
                    + "before '-Pdrill' to make this P2-14 fault-injection case effective.");
        }
        assumeTrue(jar != null, "openlatch-server shaded jar not built; run package first");
        return jar;
    }

    /** 演练报告追加（同日单文件，两场景各写一节）。 */
    private static void appendReport(String section) throws IOException {
        Path out = Path.of("docs", "failover-drill-"
                + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE) + ".md");
        Files.createDirectories(out.getParent());
        if (!Files.exists(out)) {
            Files.writeString(out, "# 杀 Leader 演练报告（s3 P2-14）\n\n- 生成：3 节点本机 shaded jar，"
                    + "election-timeout 800ms\n\n");
        }
        Files.writeString(out, section, java.nio.file.StandardOpenOption.APPEND);
        System.out.println("[DRILL] report appended: " + out.toAbsolutePath());
    }

    // ---------- 集群装配 / 探测 ----------

    /** 以 shaded jar 起三节点集群（显式端口 + client-addresses 映射 + 短选举超时）。 */
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
            Path dir = Files.createTempDirectory("openlatch-drill-node-");
            Path cfg = Files.createTempFile("openlatch-drill", ".properties");
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
            Process p = new ProcessBuilder("java", "-Dopenlatch.config=" + cfg,
                    "-jar", jar.toAbsolutePath().toString())
                    .redirectErrorStream(true)
                    .start();
            // 排空子进程输出，防管道缓冲满阻塞被杀进程。
            drain(p);
            waitForPort(access[i]);
            nodes.add(new Node(i + 1, p, access[i], raft[i]));
        }
        return nodes;
    }

    /** 异步排空进程 stdout/stderr。 */
    private static void drain(Process p) {
        Thread t = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(
                    new InputStreamReader(p.getInputStream()))) {
                while (r.readLine() != null) {
                    // 丢弃（演练不解析日志）
                }
            } catch (IOException ignored) {
                // 进程终止即结束
            }
        }, "drill-drain-" + p.pid());
        t.setDaemon(true);
        t.start();
    }

    /** 轮询探测当前 Leader（HELLO 提示自报：hint>0 且地址为本节点接入地址）。 */
    private static Node waitLeader(List<Node> nodes) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            for (Node n : nodes) {
                if (!n.process().isAlive() || !isLeaderNode(n)) {
                    continue;
                }
                return n;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("30s 内未探测到 Leader");
    }

    /**
     * 单节点 Leader 判定：原始 socket 发 HELLO，读 {@code leader_hint}
     * 与该节点配置 id 比对。不经 OpenLatchClient——带重定向的客户端会把
     * Follower 探测也引向真主，无法反证本机角色。
     */
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
                                    .setClientName("drill-role-probe"))
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

    /** 定位 openlatch-server shaded jar（与 ClientProcessKillIT 同策略）。 */
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

    private static void waitForPort(int port) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(WAIT_SECONDS);
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket()) {
                ignored.connect(new InetSocketAddress("127.0.0.1", port), 200);
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
    }
}
