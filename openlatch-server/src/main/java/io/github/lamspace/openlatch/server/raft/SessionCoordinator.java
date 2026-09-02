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

package io.github.lamspace.openlatch.server.raft;

import com.google.protobuf.ByteString;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.SessionPayload;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.ChannelHandlerContext;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 会话集群协调器（详设 §5.2，P2-08）：会话的集群登记、断连传播与接入
 * 节点失联批量清理。
 *
 * <p><b>会话 id 组合（§5.2 规则 1）</b>：接入节点 HELLO 时分配
 * {@code sessionId = (nodeId << 32) | localSeq}，nodeId 高位保证全局唯一
 * 且自描述归属（失联批量清理直接按高位路由，无需额外注册表）。
 *
 * <p><b>HELLO 语义（design D12）</b>：先提交 {@code SESSION_OPEN} 并等待
 * 应用回执（即多数派确认）再回 {@code HelloResponse}——握手成功即保证
 * 该会话已进入复制状态，客户端随后写请求不会因"未登记"被竞态拒绝。
 * 提交通道经内部 RaftClient 池自动寻主，HELLO 落在任意节点皆可成立。
 *
 * <p><b>断连传播（§5.2 规则 3）</b>：接入节点检测到断开提交
 * {@code SESSION_CLOSE}；失败按固定退避重试（副本侧幂等，重复提交无害），
 * 最终兜底是节点失联批量清理与租约到期。
 *
 * <p><b>失联批量清理（§5.2 规则 4，design D5 + S4 加固）</b>：Leader 以
 * {@code election-timeout-ms} 为周期提交 NOOP 探针（兼作租约活性位点），
 * 同周期轮询 {@code Division.getCommitInfos()} 的 per-peer commitIndex：
 * 某 peer 连续 {@value #STALL_TOLERANCE} 个周期<b>零推进</b>且未越过 Leader
 * 位点即判失联，对复制状态中归属该节点的全部会话逐条补发
 * {@code SESSION_CLOSE}。S4 加固的判据说明：仅"未越过 Leader 位点"不够——
 * commitInfo 缓存滞后（选举/修复窗口内可达一个刷新周期）而 Leader 位点随
 * 探针持续前移，会让<b>正在回放的存活副本</b>被误判失联、误清其存活会话
 * （违 §11-2"存活会话锁不丢"）；真失联节点的定义性特征正是零推进。
 * 滞后上界 ≈ 4×选举超时；成员被移除时不经此判定，由
 * {@code onMemberRemoved} 显式触发同一车道。
 *
 * <p><b>线程模型</b>：HELLO/断连处理在连接 EventLoop；条目应用回调
 * （{@link #onEntryApplied}）在状态机应用线程（经网关转发，非阻塞）；
 * 探针/轮询在单守护调度线程（非 Leader 空转短路）。重试定时器共用该调度器。
 */
public final class SessionCoordinator {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(SessionCoordinator.class);

    /** 失联判定的连续停滞周期容忍数（design D5 防抖，M=3）。 */
    static final int STALL_TOLERANCE = 3;

    /** SESSION_CLOSE 重试间隔（毫秒）。 */
    private static final long CLOSE_RETRY_MS = 500L;
    /** SESSION_CLOSE 重试上限（次）。 */
    private static final int CLOSE_RETRY_MAX = 8;

    /** 装配子系统（commitInfos 轮询与角色）。 */
    private final RaftSubsystem subsystem;
    /** 复制网关（提交通道）。 */
    private final ReplicationGateway gateway;
    /** 语义内核（liveSessions 消费）。 */
    private final LockStateMachineCore kernel;
    /** 连接注册表。 */
    private final ServerSessionRegistry registry;
    /** 本节点 id（sid 高位来源）。 */
    private final int nodeId;
    /** 选举超时（探针/轮询周期）。 */
    private final long probeIntervalMs;
    /** 默认租约（HelloResponse 携带，供客户端看门狗参考）。 */
    private final long defaultLeaseMs;
    /** Leader 提示单源（HELLO 应答的 leader_hint/leader_address 来源，design D3）。 */
    private final LeaderTracker leaderTracker;

    /** 本节点 localSeq 发号器（低 32 位，从 1 起）。 */
    private final AtomicLong localSeq = new AtomicLong(1);
    /** 关闭重试在途表：sid → 剩余重试次数。 */
    private final Map<Long, AtomicInteger> closeRetries = new ConcurrentHashMap<>();
    /** per-peer 停滞计数：peerId → 连续"零推进且未越位"周期数。 */
    private final Map<String, Integer> stallCounts = new ConcurrentHashMap<>();
    /** peer 上次观测 commitIndex（零推进判据的记忆位，S4 加固）。 */
    private final Map<String, Long> lastPeerCommit = new ConcurrentHashMap<>();
    /** 已判失联并触发批量清理的节点 id（防重复扫描提交）。 */
    private final Set<Integer> cleanedNodes = ConcurrentHashMap.newKeySet();
    /** 探针开关（测试/诊断用）：关闭后仅停探针与失联判定，不影响提交通道。 */
    private volatile boolean probesEnabled = true;
    /** 探针/轮询调度器。 */
    private final ScheduledExecutorService probeScheduler;

    /**
     * 构造协调器并启动探针调度（非 Leader 时各 tick 空转短路）。
     *
     * @param subsystem     Raft 子系统
     * @param gateway       复制网关
     * @param kernel        状态机内核
     * @param registry      连接注册表
     * @param config        服务器配置（默认租约展示值）
     * @param leaderTracker Leader 提示单源（HELLO 应答填充 leader 字段）
     */
    public SessionCoordinator(RaftSubsystem subsystem, ReplicationGateway gateway,
                              LockStateMachineCore kernel, ServerSessionRegistry registry,
                              ServerConfig config, LeaderTracker leaderTracker) {
        this.subsystem = subsystem;
        this.gateway = gateway;
        this.kernel = kernel;
        this.registry = registry;
        this.nodeId = subsystem.clusterConfig().nodeId();
        this.probeIntervalMs = subsystem.clusterConfig().electionTimeoutMs();
        this.defaultLeaseMs = config.defaultLeaseMs();
        this.leaderTracker = leaderTracker;
        gateway.setSessionCoordinator(this);
        this.probeScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "openlatch-session-probe");
            t.setDaemon(true);
            return t;
        });
        this.probeScheduler.scheduleAtFixedRate(this::probeTickSafely,
                probeIntervalMs, probeIntervalMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 分配本节点新逻辑会话 id：{@code (nodeId << 32) | localSeq}。
     *
     * @return 全局唯一逻辑会话 id
     */
    public long allocateSessionId() {
        return ((long) nodeId << 32) | (localSeq.getAndIncrement() & 0xFFFFFFFFL);
    }

    /**
     * 集群 HELLO 路径：分配 sid → 提交 SESSION_OPEN → 应用回执后于本连接
     * EventLoop 写回 {@code HelloResponse}（成功激活登记；失败回可重试错误，
     * 连接保持可重发 HELLO——门闩未置已握手）。两条路径均随附
     * {@link LeaderTracker} 单源的 leader 提示（v2 客户端据此直连/改道；
     * 提示取应答构造时刻的快照，选举空窗以 -1/空串呈现）。
     *
     * @param ctx     连接上下文
     * @param session 连接簿记
     * @param msg     HELLO 信封（requestId 回显用）
     */
    public void handleHello(ChannelHandlerContext ctx, ServerSession session, Envelope msg) {
        long sid = allocateSessionId();
        ByteString payload = SessionPayload.newBuilder().setSessionId(sid).build().toByteString();
        gateway.submit(RaftEntryType.SESSION_OPEN, payload)
                .whenComplete((r, err) -> ctx.channel().eventLoop().execute(() -> {
                    if (!ctx.channel().isActive()) {
                        // 竞态：回执抵达前连接已断（channelInactive 已先行或后行于本回调，
                        // 两种序都被覆盖：后行者见 handshaken=false 仅摘注册表，
                        // 先行者已由本处补提交关闭，副本侧幂等）。
                        submitClose(sid);
                        return;
                    }
                    LeaderTracker.Snapshot leader = leaderTracker.snapshot();
                    if (err != null) {
                        // 提交失败但条目可能已在途提交（leader 切换竞态）：补发
                        // 关闭保证不留下"无连接持有"的孤儿会话登记（副本侧幂等）。
                        log.warn("SESSION_OPEN failed for sid={} on node {}", sid, nodeId, err);
                        submitClose(sid);
                        ctx.writeAndFlush(Envelope.newBuilder()
                                .setProtocolVersion(msg.getProtocolVersion())
                                .setType(MessageType.HELLO)
                                .setRequestId(msg.getRequestId())
                                .setHelloResponse(HelloResponse.newBuilder()
                                        .setStatus(StatusCode.NOT_LEADER).setSessionId(0)
                                        .setLeaderHint(leader.leaderNodeId())
                                        .setLeaderAddress(leader.leaderAddress()))
                                .build());
                        return;
                    }
                    session.activate(sid, msg.getProtocolVersion());
                    registry.register(session);
                    ctx.writeAndFlush(Envelope.newBuilder()
                            .setProtocolVersion(msg.getProtocolVersion())
                            .setType(MessageType.HELLO)
                            .setRequestId(msg.getRequestId())
                            .setHelloResponse(HelloResponse.newBuilder()
                                    .setStatus(StatusCode.OK)
                                    .setSessionId(sid)
                                    .setServerProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                                    .setDefaultLeaseMs(defaultLeaseMs)
                                    .setLeaderHint(leader.leaderNodeId())
                                    .setLeaderAddress(leader.leaderAddress()))
                            .build());
                }));
    }

    /**
     * 断连传播入口（接入节点 EventLoop 调用）：提交 SESSION_CLOSE 并登记重试。
     *
     * @param sessionId 逻辑会话 id
     */
    public void submitClose(long sessionId) {
        closeRetries.computeIfAbsent(sessionId, k -> new AtomicInteger(CLOSE_RETRY_MAX));
        doCloseSubmit(sessionId);
    }

    /**
     * 单次关闭提交；失败由调度器退避重试，副本侧幂等（未登记会话空操作）。
     *
     * @param sessionId 逻辑会话 id
     */
    private void doCloseSubmit(long sessionId) {
        ByteString payload = SessionPayload.newBuilder().setSessionId(sessionId).build().toByteString();
        gateway.submit(RaftEntryType.SESSION_CLOSE, payload)
                .whenComplete((r, err) -> {
                    if (err == null) {
                        closeRetries.remove(sessionId); // 已应用（幂等收敛）
                        return;
                    }
                    AtomicInteger left = closeRetries.get(sessionId);
                    if (left != null && left.decrementAndGet() > 0) {
                        probeScheduler.schedule(() -> doCloseSubmit(sessionId),
                                CLOSE_RETRY_MS, TimeUnit.MILLISECONDS);
                    } else {
                        closeRetries.remove(sessionId);
                        log.warn("SESSION_CLOSE give up after retries: sid={} (兜底：失联批量清理/租约到期)",
                                sessionId, err);
                    }
                });
    }

    /**
     * 条目应用回调（经网关转发，状态机应用线程）：仅观测 SESSION_CLOSE 落地
     * 以提前终结本地重试。
     *
     * @param entry  已应用条目
     * @param result 回执
     */
    public void onEntryApplied(io.github.lamspace.openlatch.protocol.raft.RaftLogEntry entry,
                               io.github.lamspace.openlatch.protocol.raft.ApplyResult result) {
        if (entry.getType() == RaftEntryType.SESSION_CLOSE) {
            try {
                SessionPayload p = SessionPayload.parseFrom(entry.getCommandPayload());
                if ((p.getSessionId() >>> 32) == nodeId) {
                    closeRetries.remove(p.getSessionId());
                }
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
                log.warn("session entry unparsable on close-observe (seq={})", entry.getSeq());
            }
        }
    }

    /**
     * Leadership 变更钩子（经网关转发）：新任期重置失联判定状态。
     *
     * @param isLeader 本节点当前是否 Leader（探针 tick 自裁决角色，此处仅清态）
     */
    public void onLeaderChanged(boolean isLeader) {
        stallCounts.clear();
        cleanedNodes.clear();
        // 新任期基线重置：选举/修复窗口的 commitInfo 缓存滞后不得计入停滞。
        lastPeerCommit.clear();
    }

    /**
     * 探针/轮询 tick（调度线程）：仅 Leader 实质执行——提交 NOOP、读取
     * per-peer commitInfos、累计停滞并触发批量清理。异常只记日志不断调度。
     */
    private void probeTickSafely() {
        try {
            probeTick();
        } catch (RuntimeException e) {
            log.error("session probe tick failed", e);
        }
    }

    /**
     * 开关 NOOP 探针与失联判定轮询（测试基座在无 failover 断言的用例中
     * 关闭，以排除探针条目对"日志零增长"类断言的干扰；生产恒开）。
     *
     * @param enabled 是否启用探针
     */
    public void setProbesEnabled(boolean enabled) {
        this.probesEnabled = enabled;
    }

    /** {@link #probeTickSafely()} 的无保护实现。 */
    private void probeTick() {
        if (!subsystem.isLeader() || !probesEnabled) {
            stallCounts.clear();
            cleanedNodes.clear();
            lastPeerCommit.clear();
            return;
        }
        gateway.submit(RaftEntryType.NOOP, ByteString.EMPTY);
        long leaderCommit = -1;
        Map<String, Long> peerCommit = new ConcurrentHashMap<>();
        try {
            for (CommitInfoProto ci : subsystem.division().getCommitInfos()) {
                String pid = ci.getServer().getId().toStringUtf8();
                if (pid.equals(subsystem.clusterConfig().selfPeerId())) {
                    leaderCommit = ci.getCommitIndex();
                } else {
                    peerCommit.put(pid, ci.getCommitIndex());
                }
            }
        } catch (java.io.IOException e) {
            return; // 子系统关停中
        }
        if (leaderCommit < 0) {
            return;
        }
        for (Map.Entry<String, Long> pe : peerCommit.entrySet()) {
            long commit = pe.getValue();
            Long prev = lastPeerCommit.put(pe.getKey(), commit);
            if (commit >= leaderCommit) {
                stallCounts.remove(pe.getKey());
                continue;
            }
            // 零推进判据（S4 加固）：滞后但在推进（选举/回放修复中）的存活
            // 副本不判失联；真失联节点的定义特征即 commitIndex 完全不动。
            if (prev != null && commit > prev) {
                stallCounts.remove(pe.getKey());
                continue;
            }
            int stalls = stallCounts.merge(pe.getKey(), 1, Integer::sum);
            if (stalls >= STALL_TOLERANCE) {
                handleLostPeer(pe.getKey());
            }
        }
    }

    /**
     * 成员移除的显式失联清理（详设 §7.4，S4/P2-17）：被移除节点即刻从
     * commitInfos 消失，基于停滞计数的失联判定对其永不可见——出组时必须
     * 显式走同一批量清理车道（§5.2 规则 4 语义：每会话一条 SESSION_CLOSE
     * 经日志落地，各副本一致收敛）。
     *
     * <p>幂等：与探针判定路径共用 {@code cleanedNodes} 去重（先失联清理过
     * 的节点再移除为空操作）；仅 Leader 有意义，非 Leader 调用为空操作
     * （提交通道会随 Leadership 语义失败，由调用方在 Leader 侧编排）。
     *
     * @param nodeId 被移除的节点 id
     */
    public void onMemberRemoved(int nodeId) {
        if (!subsystem.isLeader()) {
            return;
        }
        handleLostPeer("n" + nodeId);
    }

    /**
     * peer 判失联：对复制状态中归属该节点的每个会话补发 SESSION_CLOSE
     * （每会话一条条目，§5.2 规则 4）。同一节点只触发一轮（cleanedNodes
     * 去重；对端恢复后其后续会话由新 HELLO 重建）。
     *
     * @param peerId 失联 peer 的 Raft 成员 id（"n&lt;nodeId&gt;"）
     */
    private void handleLostPeer(String peerId) {
        int lostNode;
        try {
            lostNode = Integer.parseInt(peerId.substring(1));
        } catch (IndexOutOfBoundsException | NumberFormatException e) {
            return;
        }
        if (!cleanedNodes.add(lostNode)) {
            return;
        }
        int cleaned = 0;
        for (Long sid : kernel.shadow().liveSessions()) {
            if ((sid >>> 32) == lostNode) {
                submitClose(sid);
                cleaned++;
            }
        }
        log.info("peer lost: node {} stalled -> batch SESSION_CLOSE for {} sessions", lostNode, cleaned);
    }

    /**
     * 关停：停止探针与重试调度。幂等。
     */
    public void close() {
        probeScheduler.shutdownNow();
    }
}
