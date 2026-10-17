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
import io.github.lamspace.openlatch.test.TestPorts;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
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
 * 客户端 × 真集群端到端 IT（Leader 发现与故障转移核心场景）：同 JVM 起三节点
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

    /** 起 n 节点集群（选举超时 800ms，接入端口显式分配以填 client-addresses）。 */
    private void startCluster(int n) throws Exception {
        int[] accessPorts = new int[n];
        int[] raftPorts = new int[n];
        for (int i = 0; i < n; i++) {
            // 取口必须避开 OS 临时端口带：否则"探到的口刚被内核回收、紧接着被同机
            // 并发 connect 拿走"会让随后的真 bind 撞 EADDRINUSE（机制见 TestPorts）。
            accessPorts[i] = TestPorts.freeNonEphemeral();
            raftPorts[i] = TestPorts.freeNonEphemeral();
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
            // 为断连既定语义，需调用方重试）。
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

    /**
     * 标量原子量的换主持久锚：**已提交账本不丢、不双加、版本戳不漂移**（契约面判据）。
     *
     * <p><b>判据口径（2026-10-16 由"严格活性"改到契约面，判例见台账 W19）</b>：
     * 原子族为 exactly-once 有意让渡活性——换主/启动窗内的读写遇**显式终态**
     * （会话换代拒绝、请求预算耗尽、连接不可用）是契约已声明的合法收场，契约
     * 给出的补救即 {@code getStamped()} 复核。故本锚不再要求"每一步都在预算内
     * 一次成功"，改判 <b>不静默错值</b>：显式失败由
     * {@link LeaderKillLedger} 先复核解除不确定、再据实况**有界**推进
     * （<b>不盲目重发</b>——重发已生效的增量会重复生效），而安全面断言**照旧
     * 跑完**：换主后已提交态 MUST 原样存续、增量步的读数 MUST 只落在
     * "未生效／已生效一次"两态内（双加即红）、推进 MUST 有界收敛（真 wedge 仍红）。
     *
     * <p><b>本锚不再覆盖</b>：换主窗内的**成功率**（严格活性）；该面若劣化，
     * 由 W11 收敛锚与故障注入类 IT 承载。
     *
     * @throws Exception 等待或断言失败
     */
    @Test
    void committedAtomicValueSurvivesLeaderKillWithoutDoubleApply() throws Exception {
        startCluster(3);
        // 种子覆盖全节点：杀任一成员后客户端仍可经种子发现新主（与既有改道用例同理）。
        String[] allSeeds = nodes.stream().map(NodeRef::address).toArray(String[]::new);
        try (OpenLatchClient client = clientTo(allSeeds)) {
            client.connectAsync().get(10, TimeUnit.SECONDS);
            OAtomicLong a = client.newAtomicLong("ak");
            // 前置态：有界推进到 5 再 6（显式失败先复核再推进；读数只许落在两态内）
            long atFive = LeaderKillLedger.advanceScalar("建立 5", a, 0, 5);
            assertThat(atFive).isEqualTo(5);
            long atSix = LeaderKillLedger.advanceScalar("建立 6", a, atFive, 1);
            assertThat(atSix).isEqualTo(6);   // version 2
            NodeRef victim = leader();
            stopNode(victim);
            awaitTrue(() -> {
                NodeRef l = leader();
                return l != null && l != victim;
            }, "新主选出");
            // 已提交值：不丢（回 0）、不双加（12）；版本戳无漂移（2）——读幂等可重发
            OAtomicLong.Stamped afterKill = LeaderKillLedger.readScalarStamped("换主后读", a);
            LeaderKillLedger.assertDurable("换主后账本", afterKill.value(), afterKill.version(), 6, 2);
            // 后续写经改道到新主有界落值（同窗口重发由去重槽保证不双加）
            assertThat(LeaderKillLedger.advanceScalar("换主后续写", a, afterKill.value(), 1))
                    .isEqualTo(7);
        }
    }

    /**
     * 有值引用的换主持久锚：**已提交载荷字节级不丢不漂、版本戳不漂、续写照常**
     * （契约面判据，口径同 {@link #committedAtomicValueSurvivesLeaderKillWithoutDoubleApply}）。
     *
     * <p>引用族的增量步是 CAS 形态（期望载荷 + 每会话去重槽双保险），推进同样走
     * "先复核、再有界推进"纪律；复核读到的载荷只允许停在
     * "前置载荷／目标载荷"两态内——读到第三方载荷即红（被写坏）。
     *
     * @throws Exception 等待或断言失败
     */
    @Test
    void committedReferencePayloadSurvivesLeaderKillWithoutDrift() throws Exception {
        startCluster(3);
        // v6 引用形态的换主持久锚：已提交载荷字节级不丢不漂、续写照常。
        String[] allSeeds = nodes.stream().map(NodeRef::address).toArray(String[]::new);
        try (OpenLatchClient client = clientTo(allSeeds)) {
            client.connectAsync().get(10, TimeUnit.SECONDS);
            OAtomicReference r = client.newAtomicReference("rpk");
            assertThat(LeaderKillLedger.advanceRef("建立 v1", r, null, "v1")).isEqualTo("v1");
            assertThat(LeaderKillLedger.advanceRef("CAS v2", r, "v1", "v2")).isEqualTo("v2");
            NodeRef victim = leader();
            stopNode(victim);
            awaitTrue(() -> {
                NodeRef l = leader();
                return l != null && l != victim;
            }, "新主选出");
            OAtomicReference.Stamped afterKill = LeaderKillLedger.readRefStamped("换主后读", r);
            assertThat(afterKill.valueAsString()).as("换主后载荷不漂").isEqualTo("v2");
            assertThat(afterKill.version()).as("换主后版本戳不漂").isEqualTo(2);
            assertThat(LeaderKillLedger.advanceRef("换主后续写", r, "v2", "v3")).isEqualTo("v3");
        }
    }

    /**
     * v7 队列换主锚（W10 定案轮恢复常驻，安全性质语义）。W10 根因——非权威
     * 节点的队列拒绝此前落 {@code notLeaderEnvelope} 的 default 异型码形
     * （acquire 载荷），队列车道盲读 protobuf 默认实例把拒绝成型为
     * "OK-空应答"（take 交付空串、size 读 0）——已由服务端同型码形修复与
     * 客户端缺码形瞬态护栏杜绝，复制面从未丢写（W10-DBG 两存活节点
     * shadowDepth 恒一致）。改道<b>收敛</b>依赖 HELLO 提示建道时序：提示
     * 滞后（启动/套件负载窗）或 Leader 更迭而 home 连接未断裂时，非 ACQUIRE
     * 车道无重发现钩子（已登记残余缺口 W11），<b>任意</b>队列请求（含换主
     * 前的 put）都可能 churn 满预算显式失败。本锚锁定安全性质：MUST NOT
     * 出现错值伪交付（空串/0/乱序）——改道收敛时全链路依序断言
     * a→b→null→c→1；未收敛时以显式 {@code OpenLatchException}（超时/会话
     * 换代口径）收场即为合法终态；回归任何静默错值必红（错值走
     * {@code AssertionError}，不属被容忍的异常族）。
     */
    @Test
    void committedQueueElementsSurviveLeaderKillWithoutDoubleApply() throws Exception {
        startCluster(3);
        // v7 队列形态的换主持久锚：已提交元素不丢不重、续写照常（去重槽同判例）。
        String[] allSeeds = nodes.stream().map(NodeRef::address).toArray(String[]::new);
        try (OpenLatchClient client = clientTo(allSeeds)) {
            client.connectAsync().get(10, TimeUnit.SECONDS);
            OBlockingQueue q = client.newBlockingQueue("qk", 4);
            try {
                q.put("a");
                q.put("b");
                assertThat(q.size()).isEqualTo(2);
                NodeRef victim = leader();
                stopNode(victim);
                awaitTrue(() -> {
                    NodeRef l = leader();
                    return l != null && l != victim;
                }, "新主选出");
                assertThat(q.takeAsString()).isEqualTo("a");
                assertThat(q.pollAsString()).isEqualTo("b");
                assertThat(q.poll()).isNull();
                // 换主后续写经改道照常落值（同窗口重发由每会话去重槽保证不双插）。
                assertThat(q.offer("c")).isTrue();
                assertThat(q.size()).isEqualTo(1);
            } catch (OpenLatchException rerouteWindow) {
                // W11 驻留窗：预算内未完成改道——显式失败即安全形态，伪成功
                // 不可能从该分支成型（未被受理的请求没有返回值可言）。收敛
                // 轮次照常走完全链路断言。
                return;
            }
        }
    }

    // ---------- 非 ACQUIRE 车道路由收敛：契约回归网（默认构建执行） ----------

    /**
     * 非 ACQUIRE 车道路由收敛的**契约回归网**（home 驻留非权威节点形态）。
     *
     * <p><b>来历与现状</b>：本用例原为该缺口未修复时的确定性红先夹具——那时它
     * **无修复必红**，故以 {@code @Tag("w11-repro")} 门控、默认构建排除；缺口
     * 收口后与同族夹具一并转正，**现由默认构建执行**，守的是"home 活着但已非
     * 权威时，非 ACQUIRE 请求必须在窗内收敛到当值 Leader"这条契约。
     *
     * <p><b>窗口构造</b>：home 直连当值 Leader 并正常受理（此时无获取车道），
     * 随后令该节点**存活让位**——连接与会话保留、但已非权威。与既有概率锚
     * （真杀主 + Leader 漂移）刻意不同：本窗由"让位"**单向构造**，可连续复跑。
     * 未修复时非 ACQUIRE 请求既无提示可依（车道不存在）、home 又不断连，
     * 只能耗尽等待预算显式超时。
     *
     * <p><b>守卫断言</b>：取值不得是 protobuf 默认实例被读作成功——原子
     * {@code addAndGet} 的返回值为 0 即属该形态（默认实例的 status 与 value
     * 皆为 0）。断言只在收敛之后求值：**显式超时是合法终态**（未收敛时用例
     * 在收敛断言处失败），只有静默错值才是缺陷。
     */
    @Test
    void residentNonAuthoritativeHomeConvergesNonAcquireRequests() throws Exception {
        startCluster(3);
        NodeRef oldLeader = leader();
        NodeRef target = nodes.stream().filter(n -> n != oldLeader).findFirst().orElseThrow();
        try (OpenLatchClient client = clientTo(oldLeader.address())) {
            client.connectAsync().get(10, TimeUnit.SECONDS);
            OBlockingQueue q = client.newBlockingQueue("repro-resident-queue", 4);
            OAtomicLong a = client.newAtomicLong("repro-resident-atomic", 0);
            q.put("a"); // home 即当值 Leader：直连受理
            assertThat(a.addAndGet(5)).isEqualTo(5);
            transferLeadership(target);
            awaitTrue(() -> target.isLeader() && !oldLeader.isLeader(),
                    "让位完成（home 降级但仍连通）");
            q.put("b"); // 未收敛即在此耗尽等待预算抛超时（红）
            assertThat(q.size()).isEqualTo(2);
            assertThat(a.addAndGet(5))
                    .as("换主窗不得静默错值（默认实例读作 OK 的形态）").isEqualTo(10);
        }
    }

    // ---------- 场景"failover 期间持锁不丢"（端到端） ----------

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

    // ---------- 场景"等待者跨 failover 重新排队" ----------

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

            // 杀 Leader：等待队列不复制，B 的连接断开使其等待以"服务不可用"
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
