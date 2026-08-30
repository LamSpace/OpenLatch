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
 * <p><b>快照边界（S2）</b>：Ratis 自动快照触发显式关闭——状态机未实现
 * {@code takeSnapshot}（§7 归 S4/P2-15）；{@code snapshot-threshold} 配置照常
 * 解析与校验，仅在 S4 装配时生效。
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
        // S2 无快照实现：显式关闭自动触发（S4/P2-15 落地后由 snapshot-threshold 驱动）。
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(props, false);

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
