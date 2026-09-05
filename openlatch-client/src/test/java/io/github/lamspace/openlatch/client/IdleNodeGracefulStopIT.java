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

import io.github.lamspace.openlatch.server.ClusterConfig;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 长空闲优雅关停回归（spec"集群节点生命周期/长空闲后优雅关停有界"，soak 缺陷
 * {@code phase2-release-closure} D7）：三节点 in-JVM 集群建立后完全静默
 * 65s（越过 Ratis cached 工作线程 60s 空闲回收时限），逐节点优雅
 * {@code stop()} 并断言单节点有界完成。
 *
 * <p><b>为何 65s 静默是判据本体</b>：缺陷机制是"proxy cached 池零 worker 时刻
 * fire-and-forget 派发关停任务 → 永不被执行 → 关停挂 1 天"——修复（固定池钉死）
 * 前的任何时刻关停都可能中招，空闲窗口是触发前件的唯一变量；本用例把该窗口
 * 变确定性。修复前运行：首节点 stop 超时红（180s 类级）；修复后：三节点均应在
 * 秒级完成。
 *
 * <p>运行门控：{@code @Tag("drill")}，默认构建排除；执行
 * {@code mvn -s <settings> -pl openlatch-client verify -Pdrill -Dit.test=IdleNodeGracefulStopIT}。
 */
@Tag("drill")
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class IdleNodeGracefulStopIT {

    /** 静默时长：cached 池 worker 空闲回收时限 60s + 余量。 */
    private static final long IDLE_MS = 65_000L;
    /** 单节点优雅关停判定上界（修复后实测秒级；上界留部署期抖动余量）。 */
    private static final long STOP_BOUND_MS = 15_000L;

    private final List<OpenLatchServer> servers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (OpenLatchServer s : servers) {
            try {
                s.stop();
            } catch (RuntimeException ignored) {
                // best effort
            }
        }
        servers.clear();
    }

    /**
     * 建 3 节点集群 → 完全静默 65s → 逐节点 stop，断言每个关停有界完成。
     * 选主完成后即摘除一切客户端连接面（本用例不建 SDK 连接），确保关停时刻
     * 复制面与请求面均无在途调度。
     */
    @Test
    void gracefulStopAfterLongIdleIsBounded() throws Exception {
        startCluster();
        awaitQuorumUp();
        Thread.sleep(IDLE_MS);
        for (OpenLatchServer s : servers) {
            long t0 = System.currentTimeMillis();
            s.stop();
            long elapsed = System.currentTimeMillis() - t0;
            assertThat(elapsed).as("静默 65s 后优雅 stop 有界完成（缺陷回归）").isLessThan(STOP_BOUND_MS);
        }
    }

    // ---------- 装配（沿 ClientChaosIT 口径，负载与杀点留给混沌档） ----------

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private void startCluster() throws IOException {
        int n = 3;
        int[] accessPorts = new int[n];
        int[] raftPorts = new int[n];
        for (int i = 0; i < n; i++) {
            accessPorts[i] = freePort();
            raftPorts[i] = freePort();
        }
        List<String> peers = new ArrayList<>();
        List<String> addrs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            peers.add((i + 1) + "@127.0.0.1:" + raftPorts[i]);
            addrs.add((i + 1) + "@127.0.0.1:" + accessPorts[i]);
        }
        for (int i = 0; i < n; i++) {
            Path dir = Files.createTempDirectory("openlatch-idle-stop-node-");
            ClusterConfig cc = new ClusterConfig(true, i + 1, peers, addrs, raftPorts[i],
                    dir.toString(), 100_000L, 1_500L);
            cc.validate();
            ServerConfig sc = new ServerConfig(accessPorts[i], 1, 30_000L, 30_000L,
                    100L, 3_600_000L, 500L, 1_500L, 512, 4096, 1024);
            OpenLatchServer server = new OpenLatchServer(sc, cc);
            server.start();
            servers.add(server);
        }
    }

    /** 等多数派在线且选主完成（任一节点自报 Leader 提示指向正 id）。 */
    private void awaitQuorumUp() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 40_000;
        while (System.currentTimeMillis() < deadline) {
            boolean leaderUp = servers.stream().anyMatch(s -> {
                var cluster = s.cluster();
                return cluster != null && cluster.subsystem().isLeader();
            });
            if (leaderUp) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("40s 内未完成初始选主");
    }
}
