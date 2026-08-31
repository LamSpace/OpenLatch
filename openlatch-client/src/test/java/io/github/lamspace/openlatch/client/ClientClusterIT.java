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
import org.apache.ratis.protocol.RaftPeerId;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 客户端 × 真集群端到端 IT（变更 s3-leader-discovery-failover 3.5，
 * spec"Leader 发现与故障转移"核心场景）：同 JVM 起三节点
 * {@link OpenLatchServer}（真 TCP 接入 + Raft 复制组 + client-addresses
 * 提示映射），驱动真 {@link OpenLatchClient}。恢复时限放宽（防 CI 抖动），
 * 断言语义正确性为主；进程级 {@code kill -9} 计时归 4.x 演练。
 */
@Timeout(value = 150, unit = TimeUnit.SECONDS)
class ClientClusterIT {

    /** 一个集群节点的测试床句柄。 */
    private static final class NodeRef {
        final int id;
        final OpenLatchServer server;
        final String address;

        NodeRef(int id, OpenLatchServer server, String address) {
            this.id = id;
            this.server = server;
            this.address = address;
        }

        boolean isLeader() {
            return server.cluster() != null && server.cluster().subsystem().isLeader();
        }

        String address() {
            return address;
        }
    }

    /** 全部节点（关停清单）。 */
    private final List<NodeRef> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (NodeRef n : nodes) {
            try {
                n.server.stop();
            } catch (RuntimeException ignored) {
                // best effort
            }
        }
        nodes.clear();
    }

    // ---------- 集群装配 ----------

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** 起 n 节点集群（选举超时 800ms，接入端口显式分配以填 client-addresses）。 */
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
            Path dir = Files.createTempDirectory("openlatch-client-it-node-");
            ClusterConfig cc = new ClusterConfig(true, i + 1, peers, addrs, raftPorts[i],
                    dir.toString(), 1_000_000L, 800L);
            cc.validate();
            ServerConfig sc = new ServerConfig(accessPorts[i], 1, 30_000L, 30_000L,
                    100L, 3_600_000L, 500L, 1_500L, 512, 4096, 1024);
            OpenLatchServer server = new OpenLatchServer(sc, cc);
            server.start();
            nodes.add(new NodeRef(i + 1, server, "127.0.0.1:" + accessPorts[i]));
        }
        awaitTrue(this::hasLeader, "初始选主");
    }

    private NodeRef leader() {
        return nodes.stream().filter(NodeRef::isLeader).findFirst().orElse(null);
    }

    private NodeRef follower() {
        return nodes.stream().filter(x -> !x.isLeader()).findFirst().orElseThrow();
    }

    private void stopNode(NodeRef node) throws IOException {
        node.server.stop();
    }

    /** 当值 Leader 存活让位至目标节点（Ratis transferLeadership）。 */
    private void transferLeadership(NodeRef to) throws IOException {
        NodeRef l = leader();
        assertThat(l).as("让位前存在 Leader").isNotNull();
        var reply = l.server.cluster().subsystem().acquireClient().admin()
                .transferLeadership(RaftPeerId.valueOf("n" + to.id), 5_000);
        assertThat(reply.isSuccess()).as("transferLeadership 成功").isTrue();
    }

    /** 轮询等待条件成立。 */
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

    private boolean hasLeader() {
        return leader() != null;
    }

    private static AcquireSpec spec(String key, long leaseMs) {
        return new AcquireSpec(key, LockType.REENTRANT, Thread.currentThread().getId(),
                leaseMs, -1);
    }

    private OpenLatchClient clientTo(String... seeds) {
        return OpenLatchClient.builder()
                .seeds(java.util.Arrays.asList(seeds))
                .requestTimeout(Duration.ofSeconds(5))
                .defaultWaitTimeout(Duration.ofSeconds(25))
                .build();
    }

    // ---------- spec 场景：启动直连 / NOT_LEADER 重定向（真集群） ----------

    @Test
    void heldLockAndServiceRecoverWhenHomeLeaderKilled() throws Exception {
        // 客户端直连当前 Leader（home 即被杀节点）——kill 后 home 连接断裂，
        // 重连经种子轮询落到存活 Follower，新获取经 NOT_LEADER 重定向到新主。
        // 覆盖 ClientProcessKillIT 的集群等价物：home=被杀 Leader 的恢复路径。
        startCluster(3);
        NodeRef l = leader();
        LinkedBlockingQueue<String> lost = new LinkedBlockingQueue<>();
        // 种子表含全部节点（Leader 居首使 home 连到将死主）：home 断裂后重连
        // 必须能在其余种子中找到存活节点（客户端持全量种子的真实部署形态）。
        String[] seeds = new String[3];
        seeds[0] = l.address();
        int si = 1;
        for (NodeRef n : nodes) {
            if (n != l) {
                seeds[si++] = n.address();
            }
        }
        try (OpenLatchClient client = clientTo(seeds)) {
            client.addLockLostListener((key, cause) -> lost.add(key));
            client.connectAsync().get(10, TimeUnit.SECONDS);
            client.acquireAsync(spec("homekill", 3_000)).get(20, TimeUnit.SECONDS);

            stopNode(l); // 杀掉 home 所连的 Leader

            // 旧锁：home 宕机 → 失锁回调（宽限 = 3s 租约）
            assertThat(lost.poll(20, TimeUnit.SECONDS)).as("失锁回调").isEqualTo("homekill");

            // 恢复：重试环直至落到存活节点获得新会话并完成一次授予（快速失败
            // 为断连既定语义，需调用方重试——详设 §6.2/§6.3）。
            awaitTrue(() -> leader() != null, "新主选出");
            long t0 = System.currentTimeMillis();
            long rdeadline = t0 + 25_000;
            LockGrant g = null;
            int tries = 0;
            while (System.currentTimeMillis() < rdeadline) {
                tries++;
                try {
                    g = client.acquireAsync(spec("homekill", 30_000)).get(3, TimeUnit.SECONDS);
                    break;
                } catch (Exception retry) {
                    Thread.sleep(100);
                }
            }
            assertThat(g).as("home 被杀后经重连+重定向恢复授予（尝试 %d 次）", tries).isNotNull();
            assertThat(g.leaseToken()).isPositive();
            client.releaseAsync("homekill", g.leaseToken(), Thread.currentThread().getId()).join();
        }
    }

    @Test
    void acquireSucceedsThroughSeedFollowerAndRedirectsOnLeaderKill() throws Exception {
        startCluster(3);
        NodeRef f = follower();
        try (OpenLatchClient client = clientTo(f.address())) {
            client.connectAsync().get(10, TimeUnit.SECONDS);
            // 启动直连：经种子 F 发现 Leader 并授予
            LockGrant g = client.acquireAsync(spec("sk", 30_000)).get(20, TimeUnit.SECONDS);
            assertThat(g.leaseToken()).isPositive();

            // 杀 Leader：下一次获取经 NOT_LEADER 提示/强制发现跟随新主，预算内成功
            stopNode(leader());
            client.releaseAsync("sk", g.leaseToken(), Thread.currentThread().getId())
                    .exceptionally(x -> null).join();
            long t0 = System.currentTimeMillis();
            LockGrant g2 = client.acquireAsync(spec("sk", 30_000)).get(25, TimeUnit.SECONDS);
            assertThat(System.currentTimeMillis() - t0).isLessThan(20_000L);
            assertThat(g2.leaseToken()).isPositive();
        }
    }

    // ---------- spec 场景"failover 期间持锁不丢"（§8 行 2 端到端） ----------

    @Test
    void heldLockSurvivesAliveLeaderStepdown() throws Exception {
        startCluster(3);
        NodeRef l = leader();
        NodeRef other = follower();
        LinkedBlockingQueue<String> lost = new LinkedBlockingQueue<>();
        try (OpenLatchClient client = clientTo(l.address())) {
            client.addLockLostListener((key, cause) -> lost.add(key));
            client.connectAsync().get(10, TimeUnit.SECONDS);
            // 直连 Leader 获取：会话 home 在 L（租约 6s，看门狗 2s 周期）
            LockGrant g = client.acquireAsync(spec("keep", 6_000)).get(20, TimeUnit.SECONDS);

            // L 存活让位：连接/会话/锁全部保留，续租经 L 的转发车道送达新主
            transferLeadership(other);
            awaitTrue(() -> !l.isLeader(), "L 降级为 Follower");
            Thread.sleep(7_000); // 跨一个完整租约周期：若续租断链，锁将到期并触发丢失回调

            assertThat(lost).as("让位窗口内不得误判丢锁").isEmpty();
            client.releaseAsync("keep", g.leaseToken(), Thread.currentThread().getId())
                    .get(10, TimeUnit.SECONDS); // 降级节点上释放经转发车道成功
        }
    }

    // ---------- 隔离 Leader（无多数派）：续租失败兜底判定丢锁 ----------

    @Test
    void isolatedLeaderRenewFailuresAdjudicateLockLoss() throws Exception {
        startCluster(3);
        NodeRef l = leader();
        LinkedBlockingQueue<String> lost = new LinkedBlockingQueue<>();
        try (OpenLatchClient client = clientTo(l.address())) {
            client.addLockLostListener((key, cause) -> lost.add(key));
            client.connectAsync().get(10, TimeUnit.SECONDS);
            client.acquireAsync(spec("iso", 6_000)).get(20, TimeUnit.SECONDS);

            // 杀两个 Follower（先取两个不同 id，避免重复停同一节点）：
            // Leader 存活但失去多数派，续租条目无法提交。
            List<NodeRef> followers = new ArrayList<>(nodes.stream()
                    .filter(x -> !x.isLeader()).toList());
            assertThat(followers).as("两个 Follower").hasSize(2);
            stopNode(followers.get(0));
            stopNode(followers.get(1));
            // 看门狗连续两次超时（2s 周期 + 请求超时上界）内必判丢锁
            assertThat(lost.poll(30, TimeUnit.SECONDS)).as("隔离 Leader 上续租连败必判丢锁")
                    .isEqualTo("iso");
        }
    }

    // ---------- spec 场景"等待者跨 failover 重新排队" ----------

    @Test
    void waiterRequeuesOnNewLeaderAfterFailover() throws Exception {
        startCluster(3);
        NodeRef l = leader();
        NodeRef f = follower();
        LinkedBlockingQueue<String> lostA = new LinkedBlockingQueue<>();
        try (OpenLatchClient a = clientTo(l.address()); OpenLatchClient b = clientTo(f.address())) {
            a.addLockLostListener((key, cause) -> lostA.add(key));
            a.connectAsync().get(10, TimeUnit.SECONDS);
            b.connectAsync().get(10, TimeUnit.SECONDS);
            LockGrant ga = a.acquireAsync(spec("wq", 6_000)).get(20, TimeUnit.SECONDS);
            assertThat(ga.leaseToken()).isPositive();

            // B 排队（经种子重定向至 Leader 的队列）
            var bFirst = b.acquireAsync(spec("wq", 30_000));
            Thread.sleep(1_500);

            // 杀 Leader：等待队列不复制（§4.4），B 的连接断开使其等待以"服务不可用"
            // 快速失败（断连不自动重试为既定语义）；A 因 home 宕机收失锁回调
            // （短租约使 lostAt 落在窗口内）。
            stopNode(l);
            assertThatThrownBy(() -> bFirst.get(25, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ServerUnavailableException.class);
            assertThat(lostA.poll(25, TimeUnit.SECONDS)).isEqualTo("wq");

            // 应用侧重试：向新主重新排队（A 的会话被新主批量清理后 wq 可授）。
            awaitTrue(() -> {
                NodeRef nl = leader();
                return nl != null && nl != l;
            }, "新主选出");
            LockGrant gb = b.acquireAsync(spec("wq", 30_000)).get(25, TimeUnit.SECONDS);
            assertThat(gb.leaseToken()).isPositive(); // 位次重置后仍按新主队列获授
        }
    }
}
