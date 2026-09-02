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
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 混沌用例（详设 §10"混沌"层，S4/P2-19/§11-6；design Open Question 落点：
 * 检查器通道 = 客户端共享 key 持有审计 + 全副本 digest 收敛 + 停载空表）：
 * 真 SDK × 真 in-JVM 三节点集群，随机杀/重启节点并让快照窗口（低阈值 +
 * 小 segment）与杀点交错。
 *
 * <p><b>不变式</b>：
 * <ol>
 *   <li>任何时刻同 key 至多一写持有者——两客户端竞争同一 key 集，授予成功
 *       即登记持有，互斥违规（failover 双授）会令两客户端同时"持有成功"；</li>
 *   <li>无锁泄漏——停载（全部客户端 shutdown，会话清理落地）+ 一租约期后，
 *       全部存活副本锁表为空；</li>
 *   <li>快照恢复一致——混沌全程各存活副本与在役 Leader 摘要收敛。</li>
 * </ol>
 *
 * <p>恢复语义由 SDK 自动承担（断连重连、种子发现、NOT_LEADER 改道），断言
 * 只落在不变式上；租约取短值压缩用例时长（语义与租约长短无关，详设 §4.3）。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClientChaosIT {

    private static final String[] KEYS = {"ck-0", "ck-1", "ck-2", "ck-3"};
    /** 竞争租约（毫秒）：停载后"一租约期空表"判定的租期基准。 */
    private static final long LEASE_MS = 2_000L;
    /** 混沌时长（毫秒）。 */
    private static final long CHAOS_MS = 18_000L;
    /** 固定种子（可复现）。 */
    private static final long SEED = 20260902L;

    private static final class NodeRef {
        final int id;
        final String address;
        final ClusterConfig cc;
        final ServerConfig sc;
        OpenLatchServer server;

        NodeRef(int id, String address, ClusterConfig cc, ServerConfig sc, OpenLatchServer server) {
            this.id = id;
            this.address = address;
            this.cc = cc;
            this.sc = sc;
            this.server = server;
        }

        /** 运行时在位（stop 后 cluster 置空、restart 后重建）。 */
        boolean isAlive() {
            try {
                return server.cluster() != null;
            } catch (RuntimeException e) {
                return false;
            }
        }
    }

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

    @Test
    void randomKillRestartNeverBreaksMutualExclusionOrLeaks() throws Exception {
        startCluster(3, 60L); // 低阈值：混沌窗口内交错快照生成与追赶
        Random rnd = new Random(SEED);
        AtomicBoolean stop = new AtomicBoolean();
        Map<String, Long> holders = new ConcurrentHashMap<>();
        AtomicLong conflicts = new AtomicLong();
        AtomicLong grants = new AtomicLong();
        List<OpenLatchClient> clients = new ArrayList<>();
        List<Thread> drivers = new ArrayList<>();

        // 混沌压载：两客户端竞争共享 key（立即式 waitMs=0：占用即 DENIED）。
        for (int d = 0; d < 2; d++) {
            final long tid = 40_000L + d;
            OpenLatchClient client = clientTo();
            clients.add(client);
            client.connectAsync().get(15, TimeUnit.SECONDS);
            Thread th = Thread.ofPlatform().name("chaos-driver-" + d).start(() -> {
                int i = 0;
                while (!stop.get()) {
                    String key = KEYS[i++ % KEYS.length];
                    LockGrant g;
                    try {
                        g = client.acquireAsync(new AcquireSpec(key,
                                LockType.REENTRANT, tid, LEASE_MS, 0))
                                .get(3, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        // DENIED（键忙）/断连/无 Leader：调用方重试的合法语义。
                        continue;
                    }
                    // 授予成功：登记本线程持有。冲突=另一线程已持有同 key（双授）。
                    Long prev = holders.put(key, tid);
                    if (prev != null && prev != tid) {
                        conflicts.incrementAndGet();
                    }
                    grants.incrementAndGet();
                    try {
                        Thread.sleep(120);
                        client.releaseAsync(key, g.leaseToken(), tid).get(3, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        // 释放失败（连接断/锁已随会话清理）：锁可能已消失，
                        // 但仍须摘除本登记——否则留下幻影持有误报冲突。
                    } finally {
                        holders.remove(key, tid);
                    }
                    if (Thread.interrupted()) {
                        return;
                    }
                }
            });
            drivers.add(th);
        }

        // 混沌控制器：随机杀存活节点 / 重启停机节点，保证 ≥1 存活、快照窗口交错。
        long deadline = System.currentTimeMillis() + CHAOS_MS;
        while (System.currentTimeMillis() < deadline) {
            int alive = (int) nodes.stream().filter(NodeRef::isAlive).count();
            int pick = rnd.nextInt(100);
            NodeRef target;
            if (alive > 1 && pick < 50) {
                // 50%：杀一台（降到 1 也允许——多数派缺席窗由重启恢复）。
                target = nodes.stream().filter(NodeRef::isAlive).findFirst().orElse(null);
                if (target != null) {
                    stopNode(target);
                }
            } else if ((target = nodes.stream().filter(n -> !n.isAlive()).findFirst().orElse(null)) != null
                    && pick < 95) {
                // 45%：重启一台停机节点（原数据目录 + 原端口，RECOVER）。
                restartNode(target);
            } else {
                Thread.sleep(150); // 让位路径混沌占比小，本轮仅退避
            }
            Thread.sleep(120);
        }

        // 收尾：全部节点在场，驱停。
        for (NodeRef n : nodes) {
            if (!n.isAlive()) {
                restartNode(n);
            }
        }
        awaitTrue(this::hasLeader, "混沌后选出 Leader");
        stop.set(true);
        for (Thread th : drivers) {
            th.join(15_000);
        }
        for (OpenLatchClient c : clients) {
            c.shutdown();
        }
        awaitDigestsAgree();
        // 停载 + 一租约期（含余量）：全部存活副本锁表为空。
        Thread.sleep(LEASE_MS + 1_500L);
        for (NodeRef n : nodes) {
            assertThat(lockCount(n)).as("节点%d 混沌停载后无锁泄漏", n.id).isZero();
        }
        assertThat(conflicts.get()).as("任何时刻同 key 至多一写持有者").isZero();
        assertThat(grants.get()).as("混沌窗口内系统确实持续授予").isGreaterThan(0);
    }

    // ---------- 集群装配 / 杀 / 重启 / 探测 ----------

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private void startCluster(int n, long snapshotThreshold) throws Exception {
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
            Path dir = Files.createTempDirectory("openlatch-chaos-node-");
            ClusterConfig cc = new ClusterConfig(true, i + 1, peers, addrs, raftPorts[i],
                    dir.toString(), snapshotThreshold, 800L);
            cc.validate();
            ServerConfig sc = new ServerConfig(accessPorts[i], 1, 30_000L, 30_000L,
                    100L, 3_600_000L, 500L, 1_500L, 512, 4096, 1024);
            OpenLatchServer server = new OpenLatchServer(sc, cc);
            server.start();
            nodes.add(new NodeRef(i + 1, "127.0.0.1:" + accessPorts[i], cc, sc, server));
        }
        awaitTrue(this::hasLeader, "初始选主");
    }

    private void stopNode(NodeRef n) throws IOException {
        n.server.stop();
    }

    /** 原数据目录 + 原端口重启（成员记录由 Raft 日志/快照保留，RECOVER）。 */
    private void restartNode(NodeRef n) throws IOException {
        OpenLatchServer fresh = new OpenLatchServer(n.sc, n.cc);
        fresh.start();
        n.server = fresh;
    }

    private NodeRef leader() {
        return nodes.stream().filter(NodeRef::isAlive)
                .filter(x -> x.server.cluster() != null
                        && x.server.cluster().subsystem().isLeader())
                .findFirst().orElse(null);
    }

    private boolean hasLeader() {
        return leader() != null;
    }

    private int lockCount(NodeRef n) {
        var core = n.server.cluster().core();
        return core == null ? -1 : core.shadow().lockCount();
    }

    /** 全部存活副本与在役 Leader 摘要收敛。 */
    private void awaitDigestsAgree() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            NodeRef l = leader();
            if (l != null) {
                String base = digest(l);
                boolean ok = nodes.stream().filter(NodeRef::isAlive)
                        .allMatch(x -> digest(x).equals(base));
                if (ok) {
                    return;
                }
            }
            if (System.currentTimeMillis() >= deadline) {
                throw new AssertionError("混沌后副本摘要未收敛");
            }
            Thread.sleep(150);
        }
    }

    private static String digest(NodeRef n) {
        return n.server.cluster().core().digest();
    }

    private OpenLatchClient clientTo() {
        List<String> seeds = nodes.stream().map(x -> x.address).toList();
        return OpenLatchClient.builder()
                .seeds(seeds)
                .requestTimeout(Duration.ofSeconds(5))
                .defaultWaitTimeout(Duration.ofSeconds(15))
                .build();
    }

    private static void awaitTrue(java.util.function.BooleanSupplier cond, String what)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 40_000;
        while (!cond.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(100);
        }
        assertThat(cond.getAsBoolean()).as(what).isTrue();
    }
}
