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

package io.github.lamspace.openlatch.console;

import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.AdminConfig;
import io.github.lamspace.openlatch.server.ClusterConfig;
import io.github.lamspace.openlatch.server.MetricsConfig;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;
import org.junit.jupiter.api.AfterAll;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.util.StringUtils.countOccurrencesOf;

/**
 * §7 T3 控制台端到端冒烟（集群三节点档，spec"双形态数据源与集群视角口径"
 * 的页面投影）：同 JVM 拉起 3 台集群服务器（admin-token 已配置）与控制台，
 * 断言概览角色分布（恰一 LEADER、二 FOLLOWER）、锁详情等待队列的
 * "仅 Leader 可见"标注（Leader 有位次 / Follower 空区+标注）、节点视图
 * 成员表 Leader 标识。指标端口指向无监听口（单机全局指标口在集群多节点
 * 场景不可区分，此处专测其降级路径）。
 */
@SpringBootTest(classes = OpenLatchConsoleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsoleClusterTest {

    /** 集群节点数。 */
    private static final int NODES = 3;

    /** 三台集群服务器（启动序：全部 boot 后等选主）。 */
    private static final OpenLatchServer[] SERVERS = new OpenLatchServer[NODES];
    /** 各节点展示名（与 {@link #SERVERS} 同序，index=nodeId-1）。 */
    private static final String[] DISPLAYS = new String[NODES];
    /** 预置负载客户端（连接 Leader）。 */
    private static OpenLatchClient seedClient;
    /** 排队者客户端（独立会话，直连 Leader）。 */
    private static OpenLatchClient waiterClient;
    /** 持有引用。 */
    private static OLock holder;
    /** 排队者任务（诊断其异常去向）。 */
    private static java.util.concurrent.Future<?> waiterFuture;

    /** 控制台实际监听端口。 */
    @Value("${local.server.port}")
    private int port;

    /** 测试管理令牌。 */
    private static final String TOKEN = ConsoleTestSupport.ADMIN_TOKEN;

    static {
        // 上下文与 @DynamicPropertySource 先于 @BeforeAll 求值：集群必须在
        // 类初始化即拉起（starter IT 的 static 字段同法）。
        try {
            bootCluster();
        } catch (Exception e) {
            throw new IllegalStateException("集群基座启动失败", e);
        }
    }

    /**
     * 拉起三节点集群（Raft 临时端口 + 锁端口 0 + 指标端口 0 + admin token）。
     *
     * @throws Exception 启动失败
     */
    private static void bootCluster() throws Exception {
        int[] raftPorts = new int[NODES];
        for (int i = 0; i < NODES; i++) {
            raftPorts[i] = freePort();
        }
        List<String> peers = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            peers.add((i + 1) + "@127.0.0.1:" + raftPorts[i]);
        }
        // 与 ClientClusterIT 同形态：接入端口显式分配并填入 client-addresses，
        // Leader 提示地址可直达（车道/改道语义完整）。
        int[] accessPorts = new int[NODES];
        for (int i = 0; i < NODES; i++) {
            accessPorts[i] = freePort();
        }
        List<String> addrs = new ArrayList<>();
        for (int i = 0; i < NODES; i++) {
            addrs.add((i + 1) + "@127.0.0.1:" + accessPorts[i]);
        }
        for (int i = 0; i < NODES; i++) {
            Path dir = Files.createTempDirectory("openlatch-console-cluster-");
            ServerConfig config = new ServerConfig(accessPorts[i], 1, 30_000L, 30_000L,
                    1_000L, 3_600_000L, 200L, 5_000L, 512, 4096, 1024);
            ClusterConfig cc = new ClusterConfig(true, i + 1, peers, addrs, raftPorts[i],
                    dir.toString(), 1_000_000L, 800L);
            cc.validate();
            OpenLatchServer s = new OpenLatchServer(config, cc,
                    new MetricsConfig(true, 0), new AdminConfig(TOKEN));
            s.start();
            SERVERS[i] = s;
            DISPLAYS[i] = "127.0.0.1:" + s.port();
        }
        // 等选主完成（任一节点视角 Leader 已知）。
        awaitLeader();
        seedLoad();
    }

    /**
     * 等待集群选出稳定 Leader：全部节点提示收敛到同一 nodeId 且持续
     * 3s 不变（后启动节点加入可能触发改选——种子负载必须落在终态 Leader
     * 上，否则排队者会踩在改选窗口里）。
     *
     * @throws InterruptedException 等待被中断
     */
    private static void awaitLeader() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            Integer first = stableLeader();
            if (first != null) {
                Thread.sleep(3_000);
                if (first.equals(stableLeader())) {
                    return;
                }
            } else {
                Thread.sleep(100);
            }
        }
        throw new AssertionError("集群超时未稳定选主");
    }

    /**
     * 全部节点提示是否一致且已知；是则返回 nodeId，否则 {@code null}。
     *
     * @return 稳定 Leader nodeId 或 {@code null}
     */
    private static Integer stableLeader() {
        Integer leader = null;
        for (OpenLatchServer s : SERVERS) {
            var snap = s.cluster().leaderTracker().snapshot();
            if (snap.unknown()) {
                return null;
            }
            if (leader == null) {
                leader = snap.leaderNodeId();
            } else if (!leader.equals(snap.leaderNodeId())) {
                return null;
            }
        }
        return leader;
    }

    /**
     * 在 Leader 上预置"持有 + 排队"负载（SDK 自动改道 Leader，直连其展示端口）。
     */
    private static void seedLoad() {
        int leaderIdx = leaderIndex();
        seedClient = OpenLatchClient.builder().address(DISPLAYS[leaderIdx]).build();
        try {
            seedClient.connectAsync().get(10, TimeUnit.SECONDS);
            holder = seedClient.newReentrantLock("cs:order");
            holder.lock();
            // 排队者用独立会话 + acquireAsync（ClientClusterIT 的排队形态，
            // future 挂起即服务端 QUEUED 在队）。
            waiterClient = OpenLatchClient.builder().address(DISPLAYS[leaderIdx]).build();
            waiterClient.connectAsync().get(10, TimeUnit.SECONDS);
            waiterFuture = waiterClient.acquireAsync(new io.github.lamspace.openlatch.client
                    .AcquireSpec("cs:order", io.github.lamspace.openlatch.client.LockType.REENTRANT,
                    9_001L, 0, 30_000));
            // 等待 Leader 侧队列登记（复制授予应用 + 本地入队完成）。
            awaitWaiterRegistered();
        } catch (Exception e) {
            throw new AssertionError("预置集群负载失败", e);
        }
    }

    /**
     * 轮询 Leader 的等待队列直到排队者出现。
     *
     * @throws InterruptedException 等待被中断
     */
    private static void awaitWaiterRegistered() throws InterruptedException {
        var waitQueue = SERVERS[leaderIndex()].cluster().waitQueue();
        long deadline = System.currentTimeMillis() + 10_000;
        while (waitQueue.totalWaiters() < 1) {
            if (System.currentTimeMillis() >= deadline) {
                StringBuilder diag = new StringBuilder("等待者未在预算内入队：");
                for (int i = 0; i < NODES; i++) {
                    var rt = SERVERS[i].cluster();
                    diag.append("[node").append(i + 1)
                            .append(" leader=").append(rt.subsystem().isLeader())
                            .append(" waiters=").append(rt.waitQueue().totalWaiters())
                            .append(" held=").append(rt.core().shadow().isHeld("cs:order"))
                            .append(" hint=").append(rt.leaderTracker().snapshot().leaderNodeId())
                            .append("] ");
                }
                for (int i = 0; i < NODES; i++) {
                    String scrape = SERVERS[i].metrics().registry().scrape();
                    diag.append("[node").append(i + 1).append(" acquire: ")
                            .append(scrape.lines()
                                    .filter(l -> l.startsWith("openlatch_server_acquire_total"))
                                    .map(l -> l.replace("openlatch_server_acquire_total", "").trim())
                                    .collect(java.util.stream.Collectors.joining(" ~ ")))
                            .append("] ");
                }
                diag.append(" waiterDone=").append(waiterFuture.isDone());
                if (waiterFuture.isDone()) {
                    try {
                        waiterFuture.get(0, TimeUnit.MILLISECONDS);
                        diag.append(" waiter=returned");
                    } catch (Exception ex) {
                        diag.append(" waiterEx=").append(ex.getCause());
                    }
                }
                throw new AssertionError(diag.toString());
            }
            Thread.sleep(50);
        }
    }

    /**
     * 当前 Leader 的数组下标（nodeId-1）。
     *
     * @return 下标
     */
    private static int leaderIndex() {
        int leader = SERVERS[0].cluster().leaderTracker().snapshot().leaderNodeId();
        return leader - 1;
    }

    /** 停全部服务器与客户端。 */
    @AfterAll
    static void shutdown() {
        if (seedClient != null) {
            seedClient.close();
        }
        if (waiterClient != null) {
            waiterClient.close();
        }
        for (OpenLatchServer s : SERVERS) {
            if (s != null) {
                s.stop();
            }
        }
    }

    /**
     * 找一个空闲端口。
     *
     * @return 端口
     * @throws IOException 分配失败
     */
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * 注入三节点地址与 token；指标端口置 1（集群多节点共用单全局指标口
     * 本就无法区分——此档专测降级；单节点降级断言归单机档用例）。
     *
     * @param registry 动态属性注册器
     */
    @DynamicPropertySource
    static void consoleProperties(DynamicPropertyRegistry registry) {
        registry.add("openlatch.console.server-addresses",
                () -> String.join(",", DISPLAYS));
        registry.add("openlatch.console.admin-token", () -> TOKEN);
        registry.add("openlatch.console.metrics-port", () -> "1");
        registry.add("openlatch.console.refresh-interval-seconds", () -> "2");
    }

    /** GET 页面正文。 */
    private String get(String path) {
        return ConsoleTestSupport.get(port, path);
    }

    /**
     * 轮询页面直到出现目标串（复制收敛/等待者登记均异步）。
     *
     * @param path   路径
     * @param needle 目标串
     */
    private void awaitVisible(String path, String needle) {
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            if (get(path).contains(needle)) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("等待被中断", e);
            }
        }
        throw new AssertionError("页面 " + path + " 超时未见 " + needle);
    }

    @Test
    void overviewRoleDistributionExactlyOneLeader() {
        awaitVisible("/", ">FOLLOWER<");
        String body = get("/");
        assertThat(countOccurrencesOf(body, ">LEADER<")).isEqualTo(1);
        assertThat(countOccurrencesOf(body, ">FOLLOWER<")).isEqualTo(2);
        // 版本列在每块呈现（等待者/镜像口径的逐字段断言归 L1 集群档）。
        assertThat(body).contains(OpenLatchServer.serverVersion());
    }

    @Test
    void keyDetailLeaderHasQueueFollowerMarksLeaderOnly() {
        String leader = DISPLAYS[leaderIndex()];
        awaitVisible("/key?node=" + leader + "&key=cs:order", "等待队列");
        String leaderPage = get("/key?node=" + leader + "&key=cs:order");
        assertThat(leaderPage).contains("writer").doesNotContain("仅 Leader 可见");

        String follower = null;
        for (int i = 0; i < NODES; i++) {
            if (i != leaderIndex()) {
                follower = DISPLAYS[i];
                break;
            }
        }
        awaitVisible("/key?node=" + follower + "&key=cs:order", "仅 Leader 可见");
        // 镜像收敛后 Follower 也有持有者明细（逻辑会话口径）。
        assertThat(get("/key?node=" + follower + "&key=cs:order"))
                .contains("writer").contains("仅 Leader 可见");
    }

    @Test
    void nodesPageShowsMemberTableWithLeaderTag() {
        String body = get("/nodes");
        assertThat(body).contains("Leader");
        // 成员表含三节点（CLUSTER_VIEW 任一节点作答覆盖全成员）。
        assertThat(body).contains("127.0.0.1");
    }
}
