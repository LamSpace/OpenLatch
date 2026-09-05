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
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

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
import java.util.concurrent.atomic.AtomicInteger;
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
 *
 * <p><b>负载时长两档</b>：缺省运行执行 ~18s 有界窗口（{@link #CHAOS_MS}，
 * S4 交付的常规回归语义，零变化）；设置系统属性
 * {@value #SOAK_PROPERTY}={@code N}（正整数分钟）则切换为字面 ≥N 分钟持续
 * 负载 soak（详设 §10 建议 ≥10 分钟、发布级独占环境单轮补跑口径），两档共享
 * 同一驱动逻辑、随机种子与不变式断言，唯一变量是墙钟预算；soak 开启时常规
 * 回归用例禁用，二选一互斥。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClientChaosIT {

    private static final String[] KEYS = {"ck-0", "ck-1", "ck-2", "ck-3"};
    /** 竞争租约（毫秒）：停载后"一租约期空表"判定的租期基准。 */
    private static final long LEASE_MS = 2_000L;
    /** 混沌时长（毫秒），缺省常规回归档。 */
    private static final long CHAOS_MS = 18_000L;
    /**
     * soak 档开关系统属性：值为正整数分钟时启用字面持续负载 soak 并禁用
     * 常规短窗口回归；未设或非正值时维持缺省 ~18s 语义。由 failsafe 透传
     * 至 fork JVM（命令行 {@code -D} 即达）。
     */
    static final String SOAK_PROPERTY = "openlatch.chaos.soak-minutes";
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

    /**
     * 常规混沌回归：~18s 有界窗口，随机杀/重启与低阈值快照交错，断言三大
     * 不变式（互斥、无泄漏、摘要收敛）。语义与 S4 交付逐字节一致；当
     * {@value #SOAK_PROPERTY} 为正整数分钟时本用例禁用，soak 档由
     * {@link #soakUnderSustainedLoad()} 接管（二选一互斥，防双份长负载）。
     */
    @Test
    @DisabledIfSystemProperty(named = SOAK_PROPERTY, matches = "[1-9][0-9]*")
    void randomKillRestartNeverBreaksMutualExclusionOrLeaks() throws Exception {
        runChaos(CHAOS_MS);
    }

    /**
     * 字面持续负载 soak（详设 §10"混沌"层时长口径，发布级单轮补跑）：负载
     * 窗口为 {@value #SOAK_PROPERTY} 分钟（墙钟预算，正整数），驱动逻辑、
     * 随机种子与不变式断言同常规回归，唯一变量是预算——随机性与语义面与短
     * 窗口等价，长窗口旨在暴露罕见时序交错。结束时向 stdout 打印
     * {@code [CHAOS]} 汇总行（实际墙钟/杀/重启/授予/冲突计数）供取证入库。
     *
     * <p><b>调用者义务</b>：仅当 {@value #SOAK_PROPERTY} 设为正整数分钟时
     * 启用；静息态独占执行（热机 CPU 争抢扭曲计时面，S3 观察在案）；预算
     * 超过 50 分钟须同步放大本用例 {@code @Timeout}。
     */
    @Test
    @EnabledIfSystemProperty(named = SOAK_PROPERTY, matches = "[1-9][0-9]*")
    @Timeout(value = 60, unit = TimeUnit.MINUTES)
    void soakUnderSustainedLoad() throws Exception {
        runChaos(Long.getLong(SOAK_PROPERTY) * 60_000L);
    }

    /**
     * 混沌驱动引擎（两档共享）：三节点 in-JVM 集群 + 两客户端共享 key 竞争
     * × 随机杀/重启交错（低阈值快照穿插），负载墙钟 {@code chaosMs} 后收尾
     * 并执行不变式断言。
     *
     * @param chaosMs 负载窗口墙钟毫秒数（常规档 {@value #CHAOS_MS}；soak 档
     *                为分钟预算换算值）
     */
    private void runChaos(long chaosMs) throws Exception {
        startCluster(3, 60L); // 低阈值：混沌窗口内交错快照生成与追赶
        Random rnd = new Random(SEED);
        AtomicBoolean stop = new AtomicBoolean();
        Map<String, Long> holders = new ConcurrentHashMap<>();
        AtomicLong conflicts = new AtomicLong();
        AtomicLong grants = new AtomicLong();
        AtomicInteger kills = new AtomicInteger();
        AtomicInteger restarts = new AtomicInteger();
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
        long loadStart = System.currentTimeMillis();
        long deadline = loadStart + chaosMs;
        while (System.currentTimeMillis() < deadline) {
            int alive = (int) nodes.stream().filter(NodeRef::isAlive).count();
            int pick = rnd.nextInt(100);
            NodeRef target;
            if (alive > 1 && pick < 50) {
                // 50%：杀一台（降到 1 也允许——多数派缺席窗由重启恢复）。
                target = nodes.stream().filter(NodeRef::isAlive).findFirst().orElse(null);
                if (target != null) {
                    stopNode(target);
                    kills.incrementAndGet();
                }
            } else if ((target = nodes.stream().filter(n -> !n.isAlive()).findFirst().orElse(null)) != null
                    && pick < 95) {
                // 45%：重启一台停机节点（原数据目录 + 原端口，RECOVER）。
                restartNode(target);
                restarts.incrementAndGet();
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
        // 负载窗口与扰动计数汇总（soak 取证入库用；短窗口档同样打印，口径统一）。
        System.out.printf("[CHAOS] windowBudgetMs=%d loadWallMs=%d kills=%d restarts=%d grants=%d conflicts=%d%n",
                chaosMs, System.currentTimeMillis() - loadStart, kills.get(), restarts.get(),
                grants.get(), conflicts.get());
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
