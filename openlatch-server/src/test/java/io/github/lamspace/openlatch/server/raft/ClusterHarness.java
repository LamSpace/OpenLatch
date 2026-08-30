package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.server.ClusterConfig;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * 进程内多节点集群测试基座（详设 §10"复制集成"层，design D8）：以真实
 * {@link RaftSubsystem}（gRPC 传输 + 本机端口）在同 JVM 组装多节点，
 * 接入侧用 {@link EmbeddedChannel} 直驱 {@link ClusterRequestHandler} /
 * {@link SessionCoordinator}——协议级异步桥（EventLoop ↔ 应用线程）全程为真。
 *
 * <p><b>与进程级演练的分工</b>：{@link #stopNode} 关停运行时（等价节点停止
 * 服务对复制面的效果）；真实 {@code kill -9} 计时与分区演练归 S3/S4
 * （P2-14/P2-18，复用 PoC driver）。
 *
 * <p><b>参数取向</b>：选举超时独立可调——"无日志增长"类断言取大值排除
 * NOOP 探针噪声；failover/失联类取小值压缩用例时长。租约下限压至 100ms
 * 使到期用例秒级完成。
 */
final class ClusterHarness implements AutoCloseable {

    /** 单节点视图（运行时可随重启替换，其余身份字段不变）。 */
    static final class Node {
        /** 节点 id。 */
        final int id;
        /** 连接注册表（跨重启保留：AWAIT_NOTIFY 与在线连接断言入口）。 */
        final ServerSessionRegistry registry;
        /** 临时数据目录（重启复用）。 */
        final Path dataDir;
        /** Raft gRPC 端口（重启复用）。 */
        final int raftPort;
        /** 选举超时（重启复用）。 */
        final long electionTimeoutMs;
        /** 当前运行时（{@code null}=已停机）。 */
        volatile ClusterRuntime runtime;

        private Node(int id, ClusterRuntime runtime, ServerSessionRegistry registry,
                     Path dataDir, int raftPort, long electionTimeoutMs) {
            this.id = id;
            this.runtime = runtime;
            this.registry = registry;
            this.dataDir = dataDir;
            this.raftPort = raftPort;
            this.electionTimeoutMs = electionTimeoutMs;
        }

        /** 本节点是否存活（运行时在位且 division 未关闭）。 */
        boolean alive() {
            ClusterRuntime rt = runtime;
            if (rt == null) {
                return false;
            }
            try {
                return rt.subsystem().division().getInfo().isAlive();
            } catch (IOException | RuntimeException e) {
                return false;
            }
        }

        /** 本节点复制状态摘要（停机后读最后内存态）。 */
        String digest() {
            ClusterRuntime rt = runtime;
            if (rt == null) {
                throw new IllegalStateException("node " + id + " stopped");
            }
            return rt.digest();
        }

        /** 本节点是否当值 Leader。 */
        boolean isLeader() {
            ClusterRuntime rt = runtime;
            return rt != null && rt.subsystem().isLeader();
        }

        /** 状态机已应用位点（未应用过为 0）。 */
        long lastApplied() {
            ClusterRuntime rt = runtime;
            if (rt == null) {
                return -1;
            }
            var ti = rt.subsystem().stateMachine().lastApplied();
            return ti == null ? 0 : ti.getIndex();
        }
    }

    /** 全部节点（按 id 升序固定）。 */
    private final List<Node> nodes = new ArrayList<>();
    /** 端口与目录模板（重启复用）。 */
    private final List<String> peerSpecs;
    /** 关停幂等标志。 */
    private boolean closed;

    private ClusterHarness(List<String> peerSpecs) {
        this.peerSpecs = peerSpecs;
    }

    /**
     * 启动 n 节点集群（选举超时默认 1s）并等待初始选主。
     *
     * @param n 节点数（3 或 5）
     * @return 就绪集群
     * @throws IOException 端口/目录分配或装配失败
     */
    static ClusterHarness start(int n) throws IOException {
        return start(n, 1_000L);
    }

    /**
     * 启动 n 节点集群并等待初始选主。
     *
     * @param n                 节点数
     * @param electionTimeoutMs 选举超时（探针周期与 failover 时长的共同旋钮）
     * @return 就绪集群
     * @throws IOException 装配失败
     */
    static ClusterHarness start(int n, long electionTimeoutMs) throws IOException {
        int[] ports = new int[n];
        for (int i = 0; i < n; i++) {
            ports[i] = freePort();
        }
        List<String> peers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            peers.add((i + 1) + "@127.0.0.1:" + ports[i]);
        }
        ClusterHarness h = new ClusterHarness(List.copyOf(peers));
        for (int i = 0; i < n; i++) {
            Path dir = Files.createTempDirectory("openlatch-cluster-" + (i + 1) + "-");
            Node node = new Node(i + 1, null, new ServerSessionRegistry(), dir, ports[i],
                    electionTimeoutMs);
            node.runtime = boot(node, peers, electionTimeoutMs);
            h.nodes.add(node);
        }
        h.awaitTrue(h::hasLeader, 20_000, "初始选主");
        return h;
    }

    /** 按当前存活视图装配并启动一个节点的运行时。 */
    private static ClusterRuntime boot(Node node, List<String> peers, long electionTimeoutMs)
            throws IOException {
        ClusterConfig cc = new ClusterConfig(true, node.id, peers, node.raftPort,
                node.dataDir.toString(), 1_000_000L, electionTimeoutMs);
        cc.validate();
        return ClusterRuntime.create(cc, testServerConfig(), node.registry);
    }

    /** 集群级测试配置：短租约下限/快扫描/常规限额。 */
    private static ServerConfig testServerConfig() {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(0, d.workerThreads(), d.idleTimeoutMs(), 5_000L,
                100L, 3_600_000L, 200L, 1_500L,
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    /** 找一个空闲 TCP 端口。 */
    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** 节点列表（按 id 升序）。 */
    List<Node> nodes() {
        return List.copyOf(nodes);
    }

    /** 指定 id 的节点。 */
    Node node(int id) {
        return nodes.stream().filter(x -> x.id == id).findFirst().orElseThrow();
    }

    /** 当前 Leader 节点（无则 null）。 */
    Node leader() {
        return nodes.stream().filter(Node::isLeader).findFirst().orElse(null);
    }

    /** 集群是否已选出 Leader。 */
    boolean hasLeader() {
        return leader() != null;
    }

    /** 存活节点数。 */
    int aliveCount() {
        return (int) nodes.stream().filter(Node::alive).count();
    }

    /**
     * 停止除 excludeIds 之外的任一存活节点（"杀主+杀一 Follower"编排用）。
     *
     * @param excludeIds 不被选择的节点 id
     */
    void stopNodeOtherThan(int... excludeIds) {
        java.util.Set<Integer> ex = new java.util.HashSet<>();
        for (int id : excludeIds) {
            ex.add(id);
        }
        Node victim = nodes.stream().filter(x -> x.alive() && !ex.contains(x.id))
                .findFirst().orElseThrow(() -> new IllegalStateException("no node to stop"));
        stopNode(victim.id);
    }

    /**
     * 开/关全部存活节点的 NOOP 探针与失联判定轮询（"日志零增长"类断言前
     * 关闭以排除探针条目；生产路径恒开）。
     *
     * @param enabled 是否启用探针
     */
    void setProbesEnabled(boolean enabled) {
        for (Node x : nodes) {
            ClusterRuntime rt = x.runtime;
            if (rt != null) {
                rt.sessionCoordinator().setProbesEnabled(enabled);
            }
        }
    }

    /** 按 id 停止节点（运行时逆序关停，数据目录保留可重启）。 */
    void stopNode(int id) {
        Node x = node(id);
        ClusterRuntime rt = x.runtime;
        x.runtime = null;
        if (rt != null) {
            rt.close();
        }
    }

    /**
     * 以保留数据目录重启已停节点（追赶验证用；调用方负责 await 追平）。
     *
     * @param id 节点 id
     * @throws IOException 重启失败
     */
    void restartNode(int id) throws IOException {
        Node x = node(id);
        if (x.alive()) {
            throw new IllegalStateException("node " + id + " still alive");
        }
        x.runtime = boot(x, peerSpecs, x.electionTimeoutMs);
    }

    /**
     * 选举一个可停的 Follower 并停止之。
     *
     * @return 被停节点 id
     */
    int stopOneFollower() {
        Node victim = nodes.stream().filter(x -> x.alive() && !x.isLeader())
                .findFirst().orElseThrow(() -> new IllegalStateException("no follower to stop"));
        stopNode(victim.id);
        return victim.id;
    }

    /** 轮询等待条件成立（超时抛断言错误）。 */
    void awaitTrue(BooleanSupplier cond, long timeoutMs, String what) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!cond.getAsBoolean()) {
            if (System.currentTimeMillis() >= deadline) {
                throw new AssertionError("超时未满足: " + what);
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(what + "：等待被中断");
            }
        }
    }

    /** 全部存活节点摘要是否一致。 */
    boolean aliveDigestsAgree() {
        List<String> ds = nodes.stream().filter(Node::alive).map(Node::digest).distinct().toList();
        return ds.size() <= 1;
    }

    /** 全部存活节点是否已追平到同一摘要（先要求 Leader 在位）。 */
    boolean aliveAgreeWithLeader() {
        Node l = leader();
        return l != null && nodes.stream().filter(Node::alive).allMatch(x -> x.digest().equals(l.digest()));
    }

    /** 打开一条测试连接（EmbeddedChannel 直驱）。 */
    TestConn connect(Node n) {
        return new TestConn(n);
    }

    /** 测试连接壳：集群路径直驱 + 出站应答读取。 */
    static final class TestConn {
        /** 目标节点。 */
        final Node node;
        /** 嵌入通道。 */
        final EmbeddedChannel channel;
        /** 连接簿记。 */
        final ServerSession session;
        /** 处理器上下文。 */
        final ChannelHandlerContext ctx;

        private TestConn(Node node) {
            this.node = node;
            this.channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
            this.session = new ServerSession(channel);
            this.channel.attr(ServerSession.KEY).set(session);
            this.ctx = channel.pipeline().firstContext();
        }

        /** 集群 HELLO 直驱，等待 HelloResponse。 */
        Envelope hello(long requestId) {
            Envelope msg = Envelope.newBuilder()
                    .setProtocolVersion(1)
                    .setType(MessageType.HELLO)
                    .setRequestId(requestId)
                    .setHelloRequest(HelloRequest.newBuilder()
                            .setClientProtocolVersion(1).setClientName("harness"))
                    .build();
            node.runtime.sessionCoordinator().handleHello(ctx, session, msg);
            return awaitOutbound(10_000);
        }

        /** 单次集群写请求直驱（在途记账同真实 handler），返回应答。 */
        Envelope request(Envelope msg) {
            session.tryBeginRequest(1024);
            switch (msg.getType()) {
                case LOCK_ACQUIRE -> node.runtime.requestHandler().handleAcquire(session, msg, ctx);
                case LOCK_RELEASE -> node.runtime.requestHandler().handleRelease(session, msg, ctx);
                case LEASE_RENEW -> node.runtime.requestHandler().handleRenew(session, msg, ctx);
                default -> throw new IllegalArgumentException("not a write: " + msg.getType());
            }
            return awaitOutbound(10_000);
        }

        /** 弹跑 EventLoop 挂起任务并读取下一个出站信封（超时抛断言错误）。 */
        Envelope awaitOutbound(long timeoutMs) {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (System.currentTimeMillis() < deadline) {
                channel.runPendingTasks();
                Envelope out = channel.readOutbound();
                if (out != null) {
                    return out;
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("等待应答被中断");
                }
            }
            throw new AssertionError("应答超时（timeoutMs=" + timeoutMs + "）");
        }

        /** 尽力读一条出站消息（无则 null）——"不应有应答/无推送"断言用。 */
        Envelope pollOutbound() {
            channel.runPendingTasks();
            return channel.readOutbound();
        }

        /** 模拟连接断开（集群断连传播路径，等价 ServerSessionHandler.channelInactive）。 */
        void disconnect() {
            if (session.markClosed()) {
                node.registry.remove(session.sessionId());
                if (session.isHandshaken()) {
                    node.runtime.sessionCoordinator().submitClose(session.sessionId());
                }
            }
            channel.close();
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (Node x : nodes) {
            ClusterRuntime rt = x.runtime;
            x.runtime = null;
            if (rt != null) {
                try {
                    rt.close();
                } catch (RuntimeException ignored) {
                    // best effort
                }
            }
        }
    }
}
