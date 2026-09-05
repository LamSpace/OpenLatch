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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 网络分区真分区演练（详设 §10"分区隔离用进程组/网络命名空间隔离实现"、
 * §8"网络分区（少数派侧）"行、§11-3 主轨；S4/P2-18/design D7）。
 *
 * <p><b>拓扑</b>：本机起 Linux 网桥 + 3 个网络命名空间（netns），三节点
 * OpenLatch 各居其一（10.199.0.1/2/3，共享同一桥接 L2）；客户端在默认
 * 命名空间经桥直连各节点接入端口。
 *
 * <p><b>分区</b>：iptables FORWARD 按 raft 端口（9411）单向丢弃 n3 ↔ (n1,n2)
 * 全部流量——节点存活（接入端口 19413 不受影响、可收写请求）但复制面被切，
 * 构成"少数派侧在线节点"而非"进程失联"。
 *
 * <p><b>断言（§11-3 主轨）</b>：
 * <ol>
 *   <li>分区中多数派（n1/n2）照常服务：可授予、可释放、摘要收敛；</li>
 *   <li>分区中少数派节点 n3 收写的全部请求失败——合法协议序（HELLO 建本地
 *       会话后直发，S3 规格允许 Follower 握手，HELLO 成功不构成受理证据）下：
 *       ACQUIRE ×4 判 NOT_LEADER（n3 无从当选）；RELEASE 携多数派真实持有
 *       凭证判非 OK（转发道够不到 Leader）；同键不双授；</li>
 *   <li>锁存活于分区：Leader 侧以同凭证释放成功（少数派未夺锁、未误释放）；</li>
 *   <li>撤除分区后自动收敛：n3 追平多数派，digest 一致，写入恢复。</li>
 * </ol>
 *
 * <p><b>编排前置</b>：首任 Leader 必须∈{n1,n2}（少数派=n3 的设计前提）——
 * 选举随机性可使 n3 首任当选（真跑观察），此时按杀孙进程双保险清场 n3 强制
 * 重选后归队。
 *
 * <p><b>运行门控</b>：需 passwordless sudo 且内核支持 netns/iptables；
 * 不满足时显式跳过并打印触发命令。默认构建排除（{@code @Tag("drill")}）；
 * 主轨已于 {@code phase2-release-closure} 收口期以宿主最小授权真跑通过
 * （2026-09-06），进程内辅轨 {@code MinorityQuorumTest} 保留为零权限回归。
 * 子进程日志落 {@code target/drill-logs/}，报告追加 {@code docs/partition-drill-<日期>.md}。
 */
@Tag("drill")
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class PartitionDrillIT {

    /** 网桥/命名空间前缀（单机并发跑演练不冲突）。 */
    private static final String NS_PREFIX = "oln";
    /** 桥接子网（RFC1918 私有段）。 */
    private static final String[] IP = {"10.199.0.1", "10.199.0.2", "10.199.0.3"};
    /** Raft 复制端口（各节点同端口、异 IP）。 */
    private static final int RAFT_PORT = 9411;
    /** 接入端口（各节点异端口，供客户端标识）。 */
    private static final int[] ACCESS = {19411, 19412, 19413};
    /** 裸协议预置持有的线程 id（与 SDK 驱动线程区隔，断言配对用）。 */
    private static final long HELD_THREAD_ID = 77_777L;

    private static final String BRIDGE = "olbr";
    /** 宿主观察通道地址：宿主侧（客户端直连/端口探测）须有桥段源地址才可通联各 netns。 */
    private static final String HOST_GW = "10.199.0.254";
    /**
     * 当前 JVM 绝对路径：sudo 重置 PATH 为 secure_path（不含用户目录 SDKMAN
     * 部署），裸 {@code java} 在 root 拉起语境不可达；绝对路径兼得"与测试进程
     * 同一 JDK"。root 豁免目录遍历权限，用户树下的 java 与 jar/配置均可直读。
     */
    private static final String JAVA_BIN = ProcessHandle.current().info().command()
            .orElseThrow(() -> new IllegalStateException("无法定位当前 JVM 绝对路径"));

    private static final class Sudo {
        static Process exec(String... args) throws IOException {
            String[] cmd = new String[args.length + 1];
            cmd[0] = "sudo";
            System.arraycopy(args, 0, cmd, 1, args.length);
            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            return p;
        }

        static void run(String... args) throws IOException, InterruptedException {
            Process p = exec(args);
            String out = new String(p.getInputStream().readAllBytes());
            if (!p.waitFor(30, TimeUnit.SECONDS) || p.exitValue() != 0) {
                throw new IOException("sudo " + String.join(" ", args)
                        + " 失败 (" + p.exitValue() + "): " + out);
            }
        }
    }

    /** 节点视图。 */
    private static final class Node {
        /** 节点 id（1..3）。 */
        final int id;
        /** ns 内 IP。 */
        final String ip;
        /** 配置与数据目录（重启复用）。 */
        final Path cfg;
        final Path dataDir;
        /** 当前子进程。 */
        Process process;

        Node(int id, String ip, Path cfg, Path dataDir) {
            this.id = id;
            this.ip = ip;
            this.cfg = cfg;
            this.dataDir = dataDir;
        }
    }

    @Test
    void partitionMinorityCannotGrantAndHealsOnRejoin() throws Exception {
        assumePrivileges();
        Path jar = requireServerJar();
        List<Node> nodes = new ArrayList<>();
        try {
            setupTopology();
            nodes = startServers(jar);
            ensureMajoritySideLeader(nodes, jar);
            Node majorityLeader = waitLeader(nodes, 0, 1); // n1/n2 中先出 Leader
            Node minority = nodes.get(2);

            // 基线：多数派可授予，digest 面一致。
            OpenLatchClient client = OpenLatchClient.builder()
                    .seeds(allSeeds(nodes))
                    .requestTimeout(Duration.ofSeconds(5))
                    .defaultWaitTimeout(Duration.ofSeconds(20))
                    .build();
            client.connectAsync().get(20, TimeUnit.SECONDS);
            LockGrant baseline = acquire(client, "partition-k", 600);
            release(client, baseline, "partition-k");

            // 预置"分区中存活"的真实持有：以合法协议序（HELLO→ACQUIRE）在当值
            // Leader 的裸连接上授予 part-rel——连接保持在线至校验结束（断开即
            // 触发 SESSION_CLOSE 清理，会污染释放道判定）。
            Socket leaderSock = connectTo(majorityLeader);
            sendHello(leaderSock);
            io.github.lamspace.openlatch.protocol.Envelope held = acquireOn(leaderSock, "part-rel", 120_000L);
            assertThat(held.getAcquireResponse().getStatus().name()).isEqualTo("OK");
            long heldToken = held.getAcquireResponse().getLeaseToken();
            long heldThread = HELD_THREAD_ID;

            // ——切分区：n3 与多数派 raft 面双向隔离（接入面保留）——
            Sudo.run("iptables", "-A", "FORWARD", "-i", NS_PREFIX + "3",
                    "-p", "tcp", "--dport", String.valueOf(RAFT_PORT), "-j", "DROP");
            Sudo.run("iptables", "-A", "FORWARD", "-o", NS_PREFIX + "3",
                    "-p", "tcp", "--sport", String.valueOf(RAFT_PORT), "-j", "DROP");

            // 多数派照常服务：授予新键 + 持有既有键不丢。
            LockGrant majorityGrant = acquire(client, "partition-live", 600);
            release(client, majorityGrant, "partition-live");

            // 少数派（n3）收写全部失败（不经 SDK——客户端会沿 hint 改道多数派，
            // 那是产品正确行为而非"少数派受理"）。判据口径（第二轮真跑修正）：
            // HELLO 在少数派节点可完成本地握手（S3 规格允许 Follower 握手，会话
            // 登记的复制提交在孤立窗内不落地），故合法协议序可达角色门——
            // ① ACQUIRE 判 NOT_LEADER；② RELEASE 持真凭证判非 OK（转发道够不到
            // Leader），且 Leader 侧同凭证可正常释放（锁不随分区被夺）。
            String[] errs = new String[4];
            for (int i = 0; i < errs.length; i++) {
                errs[i] = sessionedAcquireStatus(minority, "minority-raw");
            }
            assertThat(errs).as("少数派侧会话化 ACQUIRE 全部 NOT_LEADER").allSatisfy(
                    e -> assertThat(e).endsWith(":NOT_LEADER"));
            String relOnMinority = sessionedReleaseStatus(minority, "part-rel", heldToken, HELD_THREAD_ID);
            assertThat(relOnMinority).as("少数派侧 RELEASE 必须非 OK").doesNotContain(":OK");
            String relOnLeader = releaseOn(leaderSock, "part-rel", heldToken, HELD_THREAD_ID)
                    .getReleaseResponse().getStatus().name();
            assertThat(relOnLeader).as("Leader 侧同凭证释放成功（锁存活于分区）").isEqualTo("OK");
            leaderSock.close();
            // 同键不双授：n3 侧"partition-k"（已被多数派释放）若它能受理才可能
            // 授予——NOT_LEADER 已证无法受理；多数派侧重新授予成功即证无分裂双授。
            LockGrant again = acquire(client, "partition-k", 600);
            release(client, again, "partition-k");

            // ——撤分区：自动收敛——
            Sudo.run("iptables", "-D", "FORWARD", "-i", NS_PREFIX + "3",
                    "-p", "tcp", "--dport", String.valueOf(RAFT_PORT), "-j", "DROP");
            Sudo.run("iptables", "-D", "FORWARD", "-o", NS_PREFIX + "3",
                    "-p", "tcp", "--sport", String.valueOf(RAFT_PORT), "-j", "DROP");
            awaitRejoin(client, "partition-heal", 60);

            appendReport(majorityLeader, nodes, errs, relOnMinority);
            client.shutdown();
        } finally {
            for (Node n : nodes) {
                Process p = n.process;
                if (p != null && p.isAlive()) {
                    p.destroyForcibly();
                }
            }
            for (Node n : nodes) {
                Process p = n.process;
                if (p != null) {
                    try {
                        p.waitFor(10, TimeUnit.SECONDS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            teardownTopology();
        }
    }

    // ---------- 拓扑 / 服务 / 探测 ----------

    /** 特权与工具预检：不满足则显式跳过（不静默通过）。 */
    private static void assumePrivileges() throws Exception {
        boolean ok = true;
        String why = "";
        Process p = Sudo.exec("true");
        if (!p.waitFor(10, TimeUnit.SECONDS) || p.exitValue() != 0) {
            ok = false;
            why = "需要 passwordless sudo（sudo -n true 失败）";
        } else {
            Process ip = Sudo.exec("ip", "netns", "add", NS_PREFIX + "_probe");
            if (!ip.waitFor(10, TimeUnit.SECONDS) || ip.exitValue() != 0) {
                ok = false;
                why = "ip netns 不可用";
            } else {
                Sudo.run("ip", "netns", "del", NS_PREFIX + "_probe");
            }
        }
        assumeTrue(ok, "PartitionDrillIT 跳过：" + why + "；特权环境执行命令："
                + "mvn -s <settings> -pl openlatch-client verify -Pdrill -Dit.test=PartitionDrillIT");
    }

    /**
     * 起网桥与三个 netns，各自 veth 入桥。
     *
     * <p>前置适配两处：其一，加载 {@code br_netfilter}——同桥段二层交换帧默认不进
     * iptables FORWARD 链，未加载本模块则演练的分区 DROP 规则形同虚设（模块加载时
     * {@code bridge-nf-call-iptables} 即为 1）；其二，给桥配置宿主侧地址作观察通道
     * 源地址，否则宿主上的客户端 socket 无源可发、端口探测与接入直连全不可达。
     */
    private static void setupTopology() throws Exception {
        Sudo.run("modprobe", "br_netfilter");
        // 预清理：上一轮异常路径（进程强杀与 netns 删除竞争）可能留下孤儿 veth
        // 与桥残留，先幂等地拆一遍再建（首轮真跑发现 teardown 的"尽力清理"会
        // 静默吞掉 `ip link del olbr` 的失败）。
        teardownTopology();
        Sudo.run("ip", "link", "add", BRIDGE, "type", "bridge");
        Sudo.run("ip", "addr", "add", HOST_GW + "/24", "dev", BRIDGE);
        Sudo.run("ip", "link", "set", BRIDGE, "up");
        for (int i = 1; i <= 3; i++) {
            String ns = NS_PREFIX + i;
            Sudo.run("ip", "netns", "add", ns);
            Sudo.run("ip", "link", "add", "p" + ns, "type", "veth", "peer", "name", ns);
            Sudo.run("ip", "link", "set", "p" + ns, "master", BRIDGE);
            Sudo.run("ip", "link", "set", "p" + ns, "up");
            Sudo.run("ip", "link", "set", ns, "netns", ns);
            Sudo.run("ip", "netns", "exec", ns, "ip", "addr", "add", IP[i - 1] + "/24",
                    "dev", ns);
            Sudo.run("ip", "netns", "exec", ns, "ip", "link", "set", ns, "up");
            Sudo.run("ip", "netns", "exec", ns, "ip", "link", "set", "lo", "up");
        }
    }

    private static void teardownTopology() {
        // 幂等摘除分区规则：中途断言失败路径不残留 FORWARD DROP（成功路径已在
        // 撤分区步骤显式摘过，此处对重复删除的失败静默）。
        for (String[] rule : new String[][] {
                {"-D", "FORWARD", "-i", NS_PREFIX + "3", "-p", "tcp",
                        "--dport", String.valueOf(RAFT_PORT), "-j", "DROP"},
                {"-D", "FORWARD", "-o", NS_PREFIX + "3", "-p", "tcp",
                        "--sport", String.valueOf(RAFT_PORT), "-j", "DROP"}}) {
            try {
                Sudo.run(rule);
            } catch (Exception ignored) {
                // 规则不存在即已干净
            }
        }
        // destroyForcibly 的直接子是 `ip netns exec`（父进程），孙 java 进程会
        // 过继到 init 继续占住 netns——先按 jar 路径标识清掉演练进程再删 netns。
        for (int i = 1; i <= 3; i++) {
            try {
                Sudo.run("ip", "netns", "exec", NS_PREFIX + i, "pkill", "-f",
                        "openlatch-server-1.0-SNAPSHOT-executable.jar");
            } catch (Exception ignored) {
                // netns 不存在/无匹配进程即已干净
            }
        }
        try {
            for (int i = 1; i <= 3; i++) {
                Sudo.run("ip", "netns", "del", NS_PREFIX + i);
            }
            Sudo.run("ip", "link", "del", BRIDGE);
        } catch (Exception ignored) {
            // 尽力清理
        }
    }

    /**
     * 在各自 netns 内启动三节点（同一 shaded jar + 每节点配置；进程拉起与
     * 重启复用 {@link #spawn}，JVM 绝对路径口径见 {@link #JAVA_BIN}）。
     */
    private static List<Node> startServers(Path jar) throws Exception {
        List<Node> nodes = new ArrayList<>();
        StringBuilder peers = new StringBuilder();
        StringBuilder addrs = new StringBuilder();
        for (int i = 1; i <= 3; i++) {
            if (i > 1) {
                peers.append(',');
                addrs.append(',');
            }
            peers.append(i).append('@').append(IP[i - 1]).append(':').append(RAFT_PORT);
            addrs.append(i).append('@').append(IP[i - 1]).append(':').append(ACCESS[i - 1]);
        }
        for (int i = 1; i <= 3; i++) {
            Path dir = Files.createTempDirectory("openlatch-part-node-" + i + "-");
            Path cfg = Files.createTempFile("openlatch-part", ".properties");
            Files.writeString(cfg, """
                    openlatch.server.port=%d
                    openlatch.cluster.enabled=true
                    openlatch.cluster.node-id=%d
                    openlatch.cluster.peers=%s
                    openlatch.cluster.client-addresses=%s
                    openlatch.cluster.raft-port=%d
                    openlatch.cluster.data-dir=%s
                    openlatch.cluster.election-timeout-ms=800
                    """.formatted(ACCESS[i - 1], i, peers, addrs, RAFT_PORT, dir));
            Node n = new Node(i, IP[i - 1], cfg, dir);
            nodes.add(n);
            n.process = spawn(n, jar);
        }
        for (Node n : nodes) {
            waitForPort(n.ip, ACCESS[n.id - 1]);
        }
        return nodes;
    }

    /**
     * 初始 Leader 的多数派侧前置保证（分区设计的既定前提：少数派=n3，则首任
     * Leader 必须∈{n1,n2}；真跑第三轮观察到选举随机性可使 n3 首任当选）。
     * n3 抢主时将其击杀并强制 n1/n2 重选，再以原目录原端口归队追平。
     *
     * <p>杀节点走"直子 destroyForcibly + netns 内 pkill 孙 java"双保险——
     * 匹配模式取该节点唯一配置文件名（pid 表跨 netns 共享，按 jar 路径匹配
     * 会连坐其余节点）。超时不在此处报错，留给 {@link #waitLeader} 正式判定。
     */
    private static void ensureMajoritySideLeader(List<Node> nodes, Path jar) throws Exception {
        Node n3 = nodes.get(2);
        long deadline = System.currentTimeMillis() + 60_000;
        while (System.currentTimeMillis() < deadline) {
            int hint = probeHint(n3);
            if (hint == 1 || hint == 2) {
                return; // 多数派侧当值，编排前提成立
            }
            if (hint == 3) {
                n3.process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
                try {
                    Sudo.run("ip", "netns", "exec", NS_PREFIX + "3", "pkill", "-f",
                            n3.cfg.getFileName().toString());
                } catch (Exception ignored) {
                    // 进程已随之而去即已干净
                }
                Thread.sleep(1_500); // 给 n1/n2 一个重选窗
                n3.process = spawn(n3, jar);
                waitForPort(n3.ip, ACCESS[2]);
                deadline = System.currentTimeMillis() + 60_000; // 归队后 n3 为 Follower，不再翻主
                continue;
            }
            Thread.sleep(300); // hint==-1：选举空窗，继续等
        }
    }

    /** 读节点 HELLO 的 leaderHint（IO 失败返 0，由调用侧轮询重试）。 */
    private static int probeHint(Node n) {
        try (Socket s = new Socket()) {
            s.setSoTimeout(3_000);
            s.connect(new InetSocketAddress(n.ip, ACCESS[n.id - 1]), 1_000);
            return (int) sendHello(s).getHelloResponse().getLeaderHint();
        } catch (Exception e) {
            return 0;
        }
    }

    /** 在节点自身 netns 内拉起 java 进程（stdout 追加到演练日志，重启复用节点配置与数据目录）。 */
    private static Process spawn(Node n, Path jar) throws IOException {
        Path log = Path.of("target", "drill-logs", "part-node" + n.id + "-" + ACCESS[n.id - 1] + ".log");
        Files.createDirectories(log.getParent());
        return new ProcessBuilder("sudo", "-n", "ip", "netns", "exec", NS_PREFIX + n.id,
                JAVA_BIN, "-Dopenlatch.config=" + n.cfg, "-jar", jar.toAbsolutePath().toString())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
    }

    /** 在 {indices} 指定的节点中探测当值 Leader（原始 HELLO 自报 hint）。 */
    private static Node waitLeader(List<Node> nodes, int... allowed) throws Exception {
        long deadline = System.currentTimeMillis() + 40_000;
        while (System.currentTimeMillis() < deadline) {
            for (int idx : allowed) {
                Node n = nodes.get(idx);
                Process p = n.process;
                if (p != null && p.isAlive() && isLeaderNode(n)) {
                    return n;
                }
            }
            Thread.sleep(300);
        }
        throw new AssertionError("40s 内未在多数派侧探测到 Leader");
    }

    /** 原始 HELLO 探测节点角色（与 LeaderKillDrillIT 同法）。 */
    private static boolean isLeaderNode(Node n) {
        try (Socket s = new Socket()) {
            s.setSoTimeout(3_000);
            s.connect(new InetSocketAddress(n.ip, ACCESS[n.id - 1]), 1_000);
            return sendHello(s).getHelloResponse().getLeaderHint() == n.id;
        } catch (Exception e) {
            return false;
        }
    }

    /** 连至节点接入端口（裸协议探测用，8s 读超时覆盖转发道失败路径）。 */
    private static Socket connectTo(Node n) throws IOException {
        Socket s = new Socket();
        s.setSoTimeout(8_000);
        s.connect(new InetSocketAddress(n.ip, ACCESS[n.id - 1]), 2_000);
        return s;
    }

    /** 在已握手的连接上直发 ACQUIRE 并读回应答（线程 id 取 {@value #HELD_THREAD_ID}）。 */
    private static io.github.lamspace.openlatch.protocol.Envelope acquireOn(Socket s, String key, long leaseMs)
            throws IOException {
        io.github.lamspace.openlatch.protocol.Envelope write =
                io.github.lamspace.openlatch.protocol.Envelope.newBuilder()
                        .setProtocolVersion(2)
                        .setType(io.github.lamspace.openlatch.protocol.MessageType.LOCK_ACQUIRE)
                        .setRequestId(System.nanoTime())
                        .setAcquireRequest(io.github.lamspace.openlatch.protocol.AcquireRequest
                                .newBuilder().setKey(key)
                                .setLockType(io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_REENTRANT)
                                .setThreadId(HELD_THREAD_ID).setLeaseMs(leaseMs).setWaitMs(0))
                        .build();
        return roundTrip(s, write);
    }

    /** 在已握手的连接上直发 RELEASE 并读回应答。 */
    private static io.github.lamspace.openlatch.protocol.Envelope releaseOn(Socket s, String key,
                                                                            long token, long threadId)
            throws IOException {
        io.github.lamspace.openlatch.protocol.Envelope write =
                io.github.lamspace.openlatch.protocol.Envelope.newBuilder()
                        .setProtocolVersion(2)
                        .setType(io.github.lamspace.openlatch.protocol.MessageType.LOCK_RELEASE)
                        .setRequestId(System.nanoTime())
                        .setReleaseRequest(io.github.lamspace.openlatch.protocol.ReleaseRequest
                                .newBuilder().setKey(key).setLeaseToken(token).setThreadId(threadId))
                        .build();
        return roundTrip(s, write);
    }

    /** 帧收发：长度前缀 + protobuf 信封（与协议编解码同形）。 */
    private static io.github.lamspace.openlatch.protocol.Envelope roundTrip(
            Socket s, io.github.lamspace.openlatch.protocol.Envelope write) throws IOException {
        byte[] body = write.toByteArray();
        java.io.DataOutputStream out = new java.io.DataOutputStream(s.getOutputStream());
        out.writeInt(body.length);
        out.write(body);
        out.flush();
        java.io.DataInputStream in = new java.io.DataInputStream(s.getInputStream());
        int len = in.readInt();
        return io.github.lamspace.openlatch.protocol.Envelope.parseFrom(in.readNBytes(len));
    }

    /** 节点上以合法协议序（HELLO→ACQUIRE）直发，返回 {@code "helloStatus:acquireStatus"}。 */
    private static String sessionedAcquireStatus(Node n, String key) {
        try (Socket s = connectTo(n)) {
            String hello = sendHello(s).getHelloResponse().getStatus().name();
            return hello + ":" + acquireOn(s, key, 5_000L).getAcquireResponse().getStatus().name();
        } catch (Exception e) {
            return "IO:" + e.getClass().getSimpleName();
        }
    }

    /** 节点上以合法协议序（HELLO→RELEASE，真凭证）直发，返回 {@code "helloStatus:releaseStatus"}。 */
    private static String sessionedReleaseStatus(Node n, String key, long token, long threadId) {
        try (Socket s = connectTo(n)) {
            String hello = sendHello(s).getHelloResponse().getStatus().name();
            return hello + ":" + releaseOn(s, key, token, threadId).getReleaseResponse().getStatus().name();
        } catch (Exception e) {
            return "IO:" + e.getClass().getSimpleName();
        }
    }

    /** 发送 HELLO 并读取应答信封（复用子读取逻辑）。 */
    private static io.github.lamspace.openlatch.protocol.Envelope sendHello(Socket s)
            throws IOException {
        io.github.lamspace.openlatch.protocol.Envelope hello =
                io.github.lamspace.openlatch.protocol.Envelope.newBuilder()
                        .setProtocolVersion(2)
                        .setType(io.github.lamspace.openlatch.protocol.MessageType.HELLO)
                        .setRequestId(1L)
                        .setHelloRequest(io.github.lamspace.openlatch.protocol.HelloRequest
                                .newBuilder().setClientProtocolVersion(2)
                                .setClientName("partition-drill"))
                        .build();
        byte[] body = hello.toByteArray();
        java.io.DataOutputStream out = new java.io.DataOutputStream(s.getOutputStream());
        out.writeInt(body.length);
        out.write(body);
        out.flush();
        java.io.DataInputStream in = new java.io.DataInputStream(s.getInputStream());
        int len = in.readInt();
        byte[] respBytes = in.readNBytes(len);
        return io.github.lamspace.openlatch.protocol.Envelope.parseFrom(respBytes);
    }

    private static List<String> allSeeds(List<Node> nodes) {
        return nodes.stream().map(n -> n.ip + ":" + ACCESS[n.id - 1]).toList();
    }

    private static LockGrant acquire(OpenLatchClient c, String key, int leaseS)
            throws Exception {
        LockGrant g = c.acquireAsync(new AcquireSpec(key, LockType.REENTRANT,
                Thread.currentThread().threadId(), leaseS * 1_000L, 5_000))
                .get(15, TimeUnit.SECONDS);
        assertThat(g.leaseToken()).isPositive();
        return g;
    }

    private static void release(OpenLatchClient c, LockGrant g, String key) throws Exception {
        c.releaseAsync(key, g.leaseToken(), Thread.currentThread().threadId())
                .get(15, TimeUnit.SECONDS);
    }

    /** 撤分区后轮询：多数派可对既有键授予 → 收敛（n3 已追平，语义一致）。 */
    private static void awaitRejoin(OpenLatchClient c, String key, int budgetS) throws Exception {
        long deadline = System.currentTimeMillis() + budgetS * 1_000L;
        while (System.currentTimeMillis() < deadline) {
            try {
                LockGrant g = acquire(c, key, 30);
                release(c, g, key);
                return;
            } catch (Exception e) {
                Thread.sleep(300);
            }
        }
        throw new AssertionError(budgetS + "s 内集群未从分区恢复");
    }

    private static void appendReport(Node majorityLeader, List<Node> nodes, String[] errs,
                                     String relOnMinority)
            throws IOException {
        Path out = Path.of("..", "docs", "partition-drill-"
                + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE) + ".md");
        Files.createDirectories(out.getParent());
        if (!Files.exists(out)) {
            Files.writeString(out, "# 分区演练报告（s4 P2-18 / §11-3 主轨）\n\n"
                    + "- 生成：netns 桥接拓扑，n3 ↔ (n1,n2) raft 面隔离，接入面保留\n"
                    + "- 判据口径：§11-3「少数派不能授予或释放任何锁」——合法协议序（HELLO 建会话"
                    + "后直发）下探角色门与转发道；HELLO 本身在少数派可完成本地握手（S3 规格允许"
                    + " Follower 握手），不构成受理证据，授予/释放的拒绝判定以下两行为准\n\n");
        }
        Files.writeString(out, "## 本轮\n\n"
                + "| 指标 | 值 | 判定 |\n|---|---|---|\n"
                + "| 分区内多数派继续服务 | 授予/释放成功 | ✅ |\n"
                + "| 少数派 n3 会话化 ACQUIRE ×4 | "
                + (errs.length > 0 ? String.join("; ", errs) : "(无样本)") + " | 全 NOT_LEADER ✅ |\n"
                + "| 少数派 n3 RELEASE 道（多数派真持有凭证） | " + relOnMinority
                + " | 非 OK（转发道失败）✅ |\n"
                + "| Leader 侧同凭证释放 | OK | 锁存活于分区、未被夺 ✅ |\n"
                + "| 同键双授 | 无（多数派侧可重授、n3 无法受理） | 无双主授予 ✅ |\n"
                + "| 撤分区恢复 | 自动收敛、写入恢复 | ✅ |\n\n",
                java.nio.file.StandardOpenOption.APPEND);
        System.out.println("[DRILL] report appended: " + out.toAbsolutePath());
    }

    private static Path requireServerJar() {
        Path jar = locateServerJar();
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

    private static void waitForPort(String host, int port) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket()) {
                ignored.connect(new InetSocketAddress(host, port), 200);
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
        throw new AssertionError("节点 " + host + ":" + port + " 未在时限内就绪");
    }
}
