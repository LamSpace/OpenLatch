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

import io.github.lamspace.openlatch.protocol.ClusterView;
import io.github.lamspace.openlatch.protocol.NodeInfo;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ClusterConfig;

/**
 * Leader 提示的单源视图（详设 §3.2 {@code LeaderTracker}，s3 design D3）。
 *
 * <p><b>职责</b>：把 Raft 层的 Leadership 变更事件折算为「当前 Leader 的
 * nodeId + 接入地址」快照，供 HELLO 应答、{@code NOT_LEADER} 随附提示、
 * {@code CLUSTER_VIEW} 作答三处消费方共用同一数据源——三处报告的 Leader
 * 身份恒一致（spec"单一数据源一致性"）。
 *
 * <p><b>与权威受理判定解耦</b>：本视图仅表达「谁是 Leader」的提示，写请求
 * 能否受理由 {@code ReplicationGateway.isLeaderAuthoritative()} 独立裁决。
 * 提示滞后至多令客户端一次改连落空（以 {@code -1}/{@code ""} 降级并走种子
 * 发现），MUST NOT 使非 Leader 节点误受理其不该受理的写（spec"降级不误受理"）。
 *
 * <p><b>线程模型</b>：{@link #onLeaderChanged} 由 {@link LockStateMachine}
 * 的事件线程（Ratis 通知线程）单写；读取（{@link #snapshot()}）发生在各连接
 * EventLoop 与其他线程。字段 {@code volatile}，读写无锁；写入是一次性替换
 * 不可变 {@link Snapshot}，读方永不见半更新状态。构造后地址映射不可变。
 *
 * <p><b>初值</b>：组网完成但尚未收到任何 Leadership 事件时为「未知」
 * （nodeId {@code -1}、地址空串），与选举空窗呈现一致。
 */
public final class LeaderTracker {

    /** 哨兵：Leader 未知（选举中或事件未达）。 */
    public static final int UNKNOWN_NODE_ID = -1;

    /** 本集群配置（接入地址映射来源，构造后不变）。 */
    private final ClusterConfig clusterConfig;
    /** 当前 Leader 快照（volatile 一次性替换；nodeId -1 + 地址空 = 未知）。 */
    private volatile Snapshot snapshot = new Snapshot(UNKNOWN_NODE_ID, "");

    /**
     * 构造跟踪器。
     *
     * @param clusterConfig 集群配置（提供 {@code client-addresses} 地址映射）
     */
    public LeaderTracker(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    /**
     * Leadership 变更事件入口（挂到
     * {@link LockStateMachine#setLeaderIdentityListener}）。
     *
     * @param leaderNodeId 新 Leader 的数值 nodeId；{@code -1} 表示暂无 Leader
     *                     （选举中），快照降级为「未知」
     */
    public void onLeaderChanged(int leaderNodeId) {
        if (leaderNodeId == UNKNOWN_NODE_ID) {
            snapshot = new Snapshot(UNKNOWN_NODE_ID, "");
            return;
        }
        snapshot = new Snapshot(leaderNodeId, clusterConfig.clientAddress(leaderNodeId));
    }

    /**
     * 当前 Leader 快照（提示填充与 {@code CLUSTER_VIEW} 作答的统一读取点）。
     *
     * @return 不可变快照；{@code leaderNodeId() == }{@value #UNKNOWN_NODE_ID}
     *         表示未知（选举中/事件未达）
     */
    public Snapshot snapshot() {
        return snapshot;
    }

    /**
     * 依本地视图构造 {@code CLUSTER_VIEW} 载荷（详设 §6.2，任意节点可答，
     * 只读、不产生日志条目）：成员表取 {@code peers} 配置（已校验的
     * {@code id@host:port} 形态），地址取 {@code client-addresses} 映射
     * （缺项空串——客户端以各节点自报兜底，design D4），{@code is_leader}
     * 按本跟踪器当时快照判定；选举空窗时无任何条目为 leader。
     *
     * @return 集群视图消息
     */
    public ClusterView clusterView() {
        int leader = snapshot.leaderNodeId();
        ClusterView.Builder view = ClusterView.newBuilder().setStatus(StatusCode.OK);
        for (String peer : clusterConfig.peers()) {
            int id = Integer.parseInt(peer.substring(0, peer.indexOf('@')).trim());
            view.addNodes(NodeInfo.newBuilder()
                    .setNodeId(id)
                    .setAddress(clusterConfig.clientAddress(id))
                    .setIsLeader(id == leader));
        }
        return view.build();
    }

    /**
     * Leader 身份快照值对象。
     *
     * @param leaderNodeId 当前 Leader 的 nodeId（{@code -1} 为未知）
     * @param leaderAddress 其接入地址 {@code host:port}；未知或未配置地址映射时为空串
     */
    public record Snapshot(int leaderNodeId, String leaderAddress) {

        /**
         * 是否处于「Leader 未知」状态（选举空窗或事件未达）。
         *
         * @return 未知返回 {@code true}
         */
        public boolean unknown() {
            return leaderNodeId == UNKNOWN_NODE_ID;
        }
    }
}
