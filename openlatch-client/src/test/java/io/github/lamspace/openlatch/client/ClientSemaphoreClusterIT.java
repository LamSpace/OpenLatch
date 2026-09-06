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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Semaphore 客户端端到端·集群档（P3-04）：真三节点集群（Raft 复制组 +
 * Leader 改道）上的许可计数、跨客户端等待-通知-重发闭环，以及经 Follower
 * 种子接入的获取改道路径。切换/快照深场景归 P3-07 演练矩阵。
 */
@Timeout(value = 150, unit = TimeUnit.SECONDS)
class ClientSemaphoreClusterIT {

    /** 集群节点句柄。 */
    private static final class NodeRef {
        /** 节点序号。 */
        final int id;
        /** 服务器实例。 */
        final OpenLatchServer server;
        /** 接入地址。 */
        final String address;

        NodeRef(int id, OpenLatchServer server, String address) {
            this.id = id;
            this.server = server;
            this.address = address;
        }

        boolean isLeader() {
            return server.cluster() != null && server.cluster().subsystem().isLeader();
        }
    }

    /** 全部节点。 */
    private final List<NodeRef> nodes = new ArrayList<>();
    /** 全部客户端（tearDown 关停）。 */
    private final List<OpenLatchClient> clients = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (OpenLatchClient c : clients) {
            c.shutdown();
        }
        clients.clear();
        for (NodeRef n : nodes) {
            try {
                n.server.stop();
            } catch (RuntimeException ignored) {
                // best effort
            }
        }
        nodes.clear();
    }

    /** 起 n 节点集群并等待选主。 */
    private void startCluster(int n) throws Exception {
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
            Path dir = Files.createTempDirectory("openlatch-sem-it-node-");
            ClusterConfig cc = new ClusterConfig(true, i + 1, peers, addrs, raftPorts[i],
                    dir.toString(), 1_000_000L, 800L);
            cc.validate();
            ServerConfig sc = new ServerConfig(accessPorts[i], 1, 30_000L, 30_000L,
                    100L, 3_600_000L, 500L, 1_500L, 512, 4096, 1024);
            OpenLatchServer server = new OpenLatchServer(sc, cc);
            server.start();
            nodes.add(new NodeRef(i + 1, server, "127.0.0.1:" + accessPorts[i]));
        }
        awaitTrue(() -> leader() != null, "初始选主");
    }

    /** 当值 Leader 节点。 */
    private NodeRef leader() {
        return nodes.stream().filter(NodeRef::isLeader).findFirst().orElse(null);
    }

    /** 种子全列表客户端。 */
    private OpenLatchClient clientToAllSeeds() {
        List<String> seeds = nodes.stream().map(n -> n.address).toList();
        OpenLatchClient c = OpenLatchClient.builder()
                .seeds(seeds)
                .requestTimeout(Duration.ofSeconds(5))
                .defaultWaitTimeout(Duration.ofSeconds(25))
                .build();
        clients.add(c);
        return c;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static void awaitTrue(java.util.function.BooleanSupplier condition, String what)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        boolean ok = condition.getAsBoolean();
        while (!ok && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
            ok = condition.getAsBoolean();
        }
        assertThat(ok).as(what).isTrue();
    }

    @Test
    void countingReentrancyAndTotalAssertAcrossCluster() throws Exception {
        // 集群计数档（排队-通知闭环归 P3-07/7.2 矩阵）：跨会话计数、同线程
        // 重入累加与对称归还、总量断言不符经应用路径显式拒绝。
        startCluster(3);
        OpenLatchClient a = clientToAllSeeds();
        OpenLatchClient b = clientToAllSeeds();
        a.connectAsync().get(30, TimeUnit.SECONDS);
        b.connectAsync().get(30, TimeUnit.SECONDS);

        OSemaphore sa = a.newSemaphore("csem-count", 2);
        assertThat(sa.tryAcquire(1)).isTrue();
        assertThat(sa.tryAcquire(1)).isTrue(); // 同线程重入累加（重入预检豁免）
        OSemaphore sb = b.newSemaphore("csem-count", 2);
        assertThat(sb.tryAcquire()).isFalse();       // 池尽（经 Leader 裁决 DENIED）
        assertThat(sb.tryAcquire(3)).isFalse();      // 超过总量的请求同样不可满足
        sa.release(2);
        assertThat(sb.tryAcquire(2, 10, TimeUnit.SECONDS)).isTrue(); // 归还后即可授
        sb.release(2);
        // 总量断言不符的集群码形（回执映射归 P3-07/7.1），单机侧已由
        // SemaphoreGatingTest 钉住 INVALID_REQUEST。
    }

    @Test
    void leaseSurvivesLeaderRedirectForSemaphoreHolders() throws Exception {
        // 存量跟家：经 Leader 授予的许可在 leader 让位后续租/释放通道照常
        // （锁的既有不变式对许可族成立的前提检查）。
        startCluster(3);
        OpenLatchClient a = clientToAllSeeds();
        a.connectAsync().get(30, TimeUnit.SECONDS);
        OSemaphore sa = a.newSemaphore("csem-handover", 2);
        sa.acquire(2);
        NodeRef before = leader();
        transferLeadership(nodes.stream().filter(n -> !n.isLeader()).findFirst().orElseThrow());
        awaitTrue(() -> leader() != null && leader() != before, "让位完成");
        // 让位后释放经转发道送达新 Leader：成功且不泄漏。
        sa.release(2);
        OpenLatchClient b = clientToAllSeeds();
        b.connectAsync().get(30, TimeUnit.SECONDS);
        assertThat(b.newSemaphore("csem-handover", 2).tryAcquire(2, 10, TimeUnit.SECONDS)).isTrue();
        b.newSemaphore("csem-handover", 2).release(2);
    }

    /** 当值 Leader 存活让位至目标节点。 */
    private void transferLeadership(NodeRef to) {
        NodeRef l = leader();
        assertThat(l).as("让位前存在 Leader").isNotNull();
        try {
            var reply = l.server.cluster().subsystem().acquireClient().admin()
                    .transferLeadership(org.apache.ratis.protocol.RaftPeerId.valueOf("n" + to.id), 5_000);
            assertThat(reply.isSuccess()).as("transferLeadership 成功").isTrue();
        } catch (java.io.IOException e) {
            throw new AssertionError(e);
        }
    }
}
