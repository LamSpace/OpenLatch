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

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.server.ClusterConfig;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Raft 子系统装配（详设 §3.2 {@code RaftSubsystem}，design D6/D7/D11）：
 * 持有本节点的 Ratis {@link RaftServer}、状态机与内部提交通道
 * （{@link RaftClient} 池），把集群能力绑定到 OpenLatch 节点生命周期。
 *
 * <p><b>职责</b>：配置 → Ratis 装配（gRPC 传输、单 Raft 组、存储目录、选举
 * 超时透传）；向 {@code ReplicationGateway} 提供条目提交通道与角色查询。
 * 不含锁语义（{@link LockStateMachineCore}）与协议映射
 * （{@code ClusterRequestHandler}）。
 *
 * <p><b>状态机组件</b>：内核与状态机在本子系统构造（registry 回调可能多次
 * 调用——每分一次组，S2 单组、实例幂等复用），装配方经 {@link #core()} /
 * {@link #stateMachine()} 取用并在 gateway 建成后回挂观察者。
 *
 * <p><b>快照装配（S4，§7/P2-15）</b>：Ratis 自动触发按
 * {@code openlatch.cluster.snapshot-threshold} 开启（未快照位点差越过阈值
 * 即在应用线程产出快照）；快照文件保留数钉为 2（详设 §7.2"保留最近 2 份"，
 * 不随外部配置浮动——保留语义与恢复判据耦合）；日志截断由库侧按快照位点
 * 协同完成。手动触发见 {@link #triggerSnapshot()}。
 *
 * <p><b>线程模型</b>：{@link #start()} 前不可用（除构造期字段）；启动后
 * {@link #isLeader()}/{@link #acquireClient()} 任意线程可调（Ratis 自身并发安全）；
 * {@link #close()} 幂等。客户端池轮转仅为摊开 Ratis 单 ClientId 串行化
 * （PoC 摩擦档案），不承诺池内顺序。
 *
 * <p><b>生命周期</b>：由 {@code OpenLatchServer} 在集群模式下于开放接入端口
 * <b>之前</b>调用 {@link #start()}（spec"Raft 子系统生命周期绑定"）；关停反序。
 */
public final class RaftSubsystem {

    /** 日志器：装配与关停诊断。 */
    private static final Logger log = LoggerFactory.getLogger(RaftSubsystem.class);

    /** 内部提交客户端池大小（PoC 摩擦：同 ClientId 在途串行，池化摊开，design D11）。 */
    private static final int CLIENT_POOL_SIZE = 4;

    /** 集群 Raft 组 UUID（按组名派生，全节点一致）。 */
    private static final UUID GROUP_UUID =
            UUID.nameUUIDFromBytes("openlatch-cluster".getBytes(StandardCharsets.UTF_8));

    /** 集群配置（已校验）。 */
    private final ClusterConfig clusterConfig;
    /** 锁语义内核（与状态机共享，gateway/驱动方消费）。 */
    private final LockStateMachineCore core;
    /** 状态机适配器（registry 首次回调构造，此后复用同一实例）。 */
    private final LockStateMachine stateMachine;

    /** 组 id。 */
    private final RaftGroupId groupId;
    /** 全成员 RaftPeer 列表。 */
    private final List<RaftPeer> peers;
    /** 本节点 peer id。 */
    private final RaftPeerId selfPeerId;

    /** Ratis 服务，start 后非空、close 后置回 {@code null}。 */
    private RaftServer server;
    /** 内部提交客户端池（design D11），未启动为 {@code null}。 */
    private RaftClient[] clients;
    /** 客户端池轮转下标。 */
    private final AtomicInteger clientIdx = new AtomicInteger();

    /**
     * 构造子系统（不启动）。
     *
     * @param clusterConfig 集群配置（{@code enabled=true}；{@code false} 时不应构造本类）
     * @param coreConfig    锁语义配置（注入状态机内核）
     * @throws IOException 配置映射失败（地址非法等装配期错误）
     */
    public RaftSubsystem(ClusterConfig clusterConfig, CoreConfig coreConfig) throws IOException {
        if (!clusterConfig.enabled()) {
            throw new IllegalStateException("RaftSubsystem 仅在集群启用时构造");
        }
        this.clusterConfig = clusterConfig;
        this.core = new LockStateMachineCore(coreConfig);
        this.stateMachine = new LockStateMachine(core);
        this.groupId = RaftGroupId.valueOf(GROUP_UUID);
        this.selfPeerId = RaftPeerId.valueOf(clusterConfig.selfPeerId());
        this.peers = parsePeers(clusterConfig.peers());
    }

    /**
     * 启动 Raft 服务与内部客户端池。
     *
     * <p>存储目录已存在时以 RECOVER 启动（Ratis 摩擦档案：默认 FORMAT 在重启
     * 非空目录即失败），否则 FORMAT 新建。
     *
     * <p><b>线程池装配契约</b>：proxy/server/client 三组池钉为非缓存固定池
     * （常驻 worker）——Ratis 3.3.0 的组关停派发是 fire-and-forget 进 cached
     * proxy 池，而 cached 池 worker 空闲 60s 全部退出后，优雅关停的派发任务
     * 可能永不被执行（关停链挂至库内 1 天超时），空闲节点必中；固定池从构造
     * 上消除该前提（soak 取证见 observations 档案）。尺寸 4 覆盖单分组启动
     * 派发与本服务内部客户端池（{@value #CLIENT_POOL_SIZE}）并发度，属保守
     * 容量而非调优参数。
     *
     * @throws IOException 启动失败（端口占用、存储不可写等）
     */
    public void start() throws IOException {
        RaftProperties props = new RaftProperties();
        GrpcConfigKeys.Server.setPort(props, clusterConfig.raftPort());
        RaftServerConfigKeys.setStorageDir(props, List.of(new File(clusterConfig.dataDir())));
        long electionMs = clusterConfig.electionTimeoutMs();
        RaftServerConfigKeys.Rpc.setTimeoutMin(props,
                TimeDuration.valueOf(Math.max(1, electionMs / 2), TimeUnit.MILLISECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(props,
                TimeDuration.valueOf(electionMs, TimeUnit.MILLISECONDS));
        // S4：自动触发按 snapshot-threshold 开启；保留 2 份（详设 §7.2）。
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(props, true);
        RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(
                props, clusterConfig.snapshotThreshold());
        RaftServerConfigKeys.Snapshot.setRetentionFileNum(props, 2);
        // 截断推进至本节点快照位点（快照位点恒为已应用⊆已提交，对多数派安全）。
        // 库默认的"按全体 peer 提交位取 min"会被任一长期缺席节点卡死——日志
        // 无上界增长且严重落后场景永远走不到安装流（§7.3-2 依赖截断制造位点差）。
        RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(props, true);
        // 线程池钉死（soak 缺陷修复，契约见本方法 Javadoc）：cached 池的 worker
        // 空闲 60s 回收，而 Ratis 3.3.0 RaftServerProxy.close 以 fire-and-forget
        // 把组关停任务派发进 proxy 池（不 join）——零 worker 时刻派发即永不被执行，
        // 状态机更新器收不到停止信号，同线程的 shutdownAndWait 挂 1 天。
        RaftServerConfigKeys.ThreadPool.setProxyCached(props, false);
        RaftServerConfigKeys.ThreadPool.setProxySize(props, 4);
        RaftServerConfigKeys.ThreadPool.setServerCached(props, false);
        RaftServerConfigKeys.ThreadPool.setServerSize(props, 4);
        RaftServerConfigKeys.ThreadPool.setClientCached(props, false);
        RaftServerConfigKeys.ThreadPool.setClientSize(props, 4);
        if (clusterConfig.logSegmentBytes() > 0) {
            // Raft 库语义透传（同 election-timeout-ms 口径）：小 segment 使截断
            // 粒度落在测试可驱动的条目量级（S4 追赶用例）。
            org.apache.ratis.util.SizeInBytes seg =
                    org.apache.ratis.util.SizeInBytes.valueOf(clusterConfig.logSegmentBytes());
            RaftServerConfigKeys.Log.setSegmentSizeMax(props, seg);
            RaftServerConfigKeys.Log.setPreallocatedSize(props, seg);
        }

        RaftGroup group = RaftGroup.valueOf(groupId, peers);
        // 摩擦档案（P2-02）：重启目录非空必须 RECOVER，否则 "Failed to FORMAT"。
        boolean storageExists = java.nio.file.Files.exists(
                java.nio.file.Path.of(clusterConfig.dataDir(), groupId.getUuid().toString()));
        server = RaftServer.newBuilder()
                .setServerId(selfPeerId)
                .setGroup(group)
                .setProperties(props)
                .setOption(storageExists ? RaftStorage.StartupOption.RECOVER
                        : RaftStorage.StartupOption.FORMAT)
                .setStateMachineRegistry(pid -> stateMachine)
                .build();
        server.start();

        clients = new RaftClient[CLIENT_POOL_SIZE];
        for (int i = 0; i < clients.length; i++) {
            RaftProperties clientProps = new RaftProperties();
            // 单请求超时放宽到 10s：默认 1s 在切主窗口内易耗尽可能重试提交，
            // 快速失败语义由网关的 Leadership 事件收尾保证，不靠客户端超时。
            org.apache.ratis.client.RaftClientConfigKeys.Rpc.setRequestTimeout(
                    clientProps, TimeDuration.valueOf(10_000, TimeUnit.MILLISECONDS));
            clients[i] = RaftClient.newBuilder()
                    .setProperties(clientProps)
                    .setRaftGroup(group)
                    .build();
        }
        log.info("Raft subsystem started: self={}, raftPort={}, peers={}",
                selfPeerId, clusterConfig.raftPort(), clusterConfig.peers());
    }

    /**
     * 关停：先摘内部客户端池（阻断新提交），再关 Ratis 服务。幂等。
     */
    public void close() {
        if (clients != null) {
            for (RaftClient c : clients) {
                try {
                    c.close();
                } catch (Exception ignored) {
                    // best effort
                }
            }
            clients = null;
        }
        if (server != null) {
            try {
                server.close();
            } catch (Exception ignored) {
                // best effort
            }
            server = null;
        }
        log.info("Raft subsystem stopped: self={}", selfPeerId);
    }

    /**
     * 手动触发一份快照（详设 §7.2"手动管理命令"落点，S4/design D6）：
     * 直调本节点状态机的 {@code takeSnapshot}，语义与自动触发一致
     * （applyLock 内一致性副本 + 锁外落盘）。仅供运维脚本与测试在阈值
     * 之外主动产快照；不等待库侧截断/清理——那由下一轮自动快照周期顺带
     * 完成（手动位点已推进库侧 latest 引用，截断按 max 位点收敛）。
     *
     * <p><b>线程注记</b>：可在任意线程调用；与在途应用经内核 {@code applyLock}
     * 与状态机实例锁串行，快照内容为"调用时刻已应用位点"的完整状态。
     * 角色无关（Leader/Follower 均可——各副本快照自产）。
     *
     * @return 快照位点（已应用索引）；无有效位点（尚未应用任何条目）时 {@code -1}
     * @throws IOException 服务未启动、分组不可达或落盘失败
     */
    public long triggerSnapshot() throws IOException {
        return ((LockStateMachine) server.getDivision(groupId).getStateMachine())
                .takeSnapshot();
    }

    /**
     * 成员变更（详设 §7.4，S4/P2-17/design D6）：以目标投票者/监听者全集
     * 提交 Ratis 单步配置变更（{@code AdminApi.setConfiguration}）。
     *
     * <p><b>输入形态</b>：与配置键 {@code peers} 同族的 {@code id@host:port}
     * 列表（端口为 Raft 复制端口）；监听者条目仅要求 {@code id@host:port}
     * 可达，其后续升票由运维以全集列表再次调用完成（listener 追赶→升
     * voter 的两段流程见部署文档）。
     *
     * <p><b>多数派护栏</b>（spec"成员变更运维"，机械拒绝而非仅文档约定）：
     * 以<b>本节点视角</b>的当前投票者集为基线做差集校验——单次调用对投票者
     * 的净变更 MUST ≤ 1 个成员，且 MUST NOT 同时含加与删。两条件联合保证
     * 旧/新多数派恒相交（单步变更安全前提）；违反抛
     * {@link IllegalArgumentException}。监听者集合不受此护栏约束
     * （监听者不参与投票，不改变多数派）。
     *
     * <p><b>调用位置</b>：从视图新鲜的成员节点调用（本方法读本地
     * {@code getRaftConf()} 做基线；请求经内部客户端路由至当值 Leader 提交）。
     * 返回成功即配置条目已提交（Ratis 单步变更语义）。
     *
     * @param voterSpecs   目标投票者全集（{@code id@host:raftPort}）
     * @param listenerSpecs 目标监听者全集（可为空列表）
     * @throws IllegalArgumentException 净变更超一个成员或同时加减；spec 形态非法
     * @throws IOException              提交失败（无 Leader、被拒、超时）
     */
    public void setMembers(List<String> voterSpecs, List<String> listenerSpecs)
            throws IOException {
        List<RaftPeer> voters = parsePeers(voterSpecs);
        List<RaftPeer> listeners = parsePeers(listenerSpecs);
        java.util.Set<RaftPeerId> current = new java.util.HashSet<>();
        // getCurrentPeers() 无参形态即投票者全集（Ratis 语义：非 LISTENER 在册成员）。
        for (RaftPeer p : division().getRaftConf().getCurrentPeers()) {
            current.add(p.getId());
        }
        java.util.Set<RaftPeerId> target = new java.util.HashSet<>();
        for (RaftPeer p : voters) {
            target.add(p.getId());
        }
        java.util.Set<RaftPeerId> added = new java.util.HashSet<>(target);
        added.removeAll(current);
        java.util.Set<RaftPeerId> removed = new java.util.HashSet<>(current);
        removed.removeAll(target);
        if (!added.isEmpty() && !removed.isEmpty() || added.size() + removed.size() > 1) {
            throw new IllegalArgumentException(
                    "成员变更多数派护栏：单次仅可加或删一个投票者（added=" + added
                            + ", removed=" + removed + "）——先加新节点并等待追赶完成，再移除旧节点（§7.4）");
        }
        var reply = acquireClient().admin().setConfiguration(voters, listeners);
        if (!reply.isSuccess()) {
            Throwable failure = reply.getException();
            throw new IOException("setConfiguration failed: " + failure, failure);
        }
        log.info("membership changed: voters={}, listeners={}", voterSpecs, listenerSpecs);
    }

    /**
     * 移除一个投票者（出组）：以本节点视图当前成员全集为基线，剔除
     * {@code n<nodeId>} 后提交单步变更。被移除节点的会话清理由调用方经
     * {@code ClusterRuntime.removeMember} 的同车道完成（出组成员不再出现于
     * commitInfos，失联判定对其永不可见——必须显式触发）。
     *
     * @param nodeId 要移除的节点 id
     * @throws IllegalArgumentException 目标不在当前投票者集合
     * @throws IOException              提交失败
     */
    public void removeVoter(int nodeId) throws IOException {
        List<String> voters = new ArrayList<>();
        List<String> listeners = new ArrayList<>();
        var conf = division().getRaftConf();
        for (RaftPeer p : conf.getCurrentPeers()) {
            voters.add(specOf(p));
        }
        for (RaftPeer p : conf.getCurrentPeers(
                org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER)) {
            listeners.add(specOf(p));
        }
        if (!voters.removeIf(s -> s.startsWith(nodeId + "@"))) {
            throw new IllegalArgumentException("节点 " + nodeId + " 不在当前投票者集合: " + voters);
        }
        setMembers(voters, listeners);
        log.info("voter removed: n{}", nodeId);
    }

    /**
     * {@link RaftPeer} 折算回 {@code id@host:port} spec（"n&lt;id&gt;" → id）。
     *
     * @param peer 成员
     * @return spec 字符串
     */
    private static String specOf(RaftPeer peer) {
        return peer.getId().toString().substring(1) + "@" + peer.getAddress();
    }

    /**
     * 本节点当前是否为 Leader（角色查询，业务线程任意时刻可调）。
     *
     * @return Leader 为 {@code true}；未启动/关停后为 {@code false}
     */
    public boolean isLeader() {
        try {
            return server.getDivision(groupId).getInfo().isLeader();
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    /**
     * 当前 Leader 的节点 id（客户端提示/诊断；未知或本节点为 Leader 时语义
     * 由调用方裁决——本方法返回 {@code null} 表示角色信息不可得）。
     *
     * @return Leader peer id 字符串，不可得为 {@code null}
     */
    public String leaderPeerId() {
        try {
            RaftPeerId lid = server.getDivision(groupId).getInfo().getLeaderId();
            return lid == null ? null : lid.toString();
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    /**
     * 轮转取用一个内部提交客户端（design D11）。
     *
     * @return 池内客户端；未启动为 {@code null}
     */
    public RaftClient acquireClient() {
        RaftClient[] pool = clients;
        if (pool == null) {
            return null;
        }
        return pool[Math.floorMod(clientIdx.getAndIncrement(), pool.length)];
    }

    /**
     * 锁语义内核（digest、影子表、应用入口的消费方为 gateway 与测试）。
     *
     * @return 内核
     */
    public LockStateMachineCore core() {
        return core;
    }

    /**
     * 状态机适配器（gateway 建成后回挂 {@link ApplyObserver} 用）。
     *
     * @return 状态机
     */
    public LockStateMachine stateMachine() {
        return stateMachine;
    }

    /**
     * 本子系统消费的 Raft 组 id。
     *
     * @return 组 id
     */
    public RaftGroupId groupId() {
        return groupId;
    }

    /**
     * 集群配置（只读）。
     *
     * @return 配置
     */
    public ClusterConfig clusterConfig() {
        return clusterConfig;
    }

    /**
     * 分区的公开视图（peer commitInfos 轮询——失联检测入口，design D5）。
     *
     * @return Ratis division 句柄
     * @throws IOException 服务未启动或组不存在
     */
    public org.apache.ratis.server.RaftServer.Division division() throws IOException {
        return server.getDivision(groupId);
    }

    /**
     * 解析 {@code id@host:port} 列表为 Ratis 成员。
     *
     * @param specs 全成员列表（含本节点，{@link ClusterConfig#validate()} 已保证）
     * @return RaftPeer 列表
     */
    private static List<RaftPeer> parsePeers(List<String> specs) {
        List<RaftPeer> out = new ArrayList<>();
        for (String s : specs) {
            int at = s.indexOf('@');
            int colon = s.lastIndexOf(':');
            String id = s.substring(0, at).trim();
            String host = s.substring(at + 1, colon).trim();
            int port = Integer.parseInt(s.substring(colon + 1).trim());
            out.add(RaftPeer.newBuilder()
                    .setId(RaftPeerId.valueOf("n" + id))
                    .setAddress(host + ":" + port)
                    .build());
        }
        return List.copyOf(out);
    }
}
