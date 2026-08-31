# cluster-node-lifecycle delta — s3-leader-discovery-failover

## MODIFIED Requirements

### Requirement: 集群配置体系
系统 SHALL 在 Phase 1 Properties 体系上并入以下配置键并逐键校验：`openlatch.cluster.enabled`（默认 `false`）、`openlatch.cluster.node-id`（启用时必填）、`openlatch.cluster.peers`（启用时必填，`id@host:raftPort` 列表）、`openlatch.cluster.client-addresses`（可选，`id@host:port` 列表——各节点客户端接入地址映射，供 Leader 提示与 `CLUSTER_VIEW` 作答；配置时 MUST 逐项格式合法且 nodeId 唯一）、`openlatch.cluster.raft-port`（默认 `9411`）、`openlatch.cluster.data-dir`（默认 `./data`）、`openlatch.cluster.snapshot-threshold`（默认 `1000000`）、`openlatch.cluster.election-timeout-ms`（默认 `3000`，按 Raft 层语义透传）。`enabled=true` 而必填项缺失或非法时，启动 MUST 失败并给出可定位的错误信息，MUST NOT 静默降级为单机模式。

#### Scenario: 缺必填项启动失败
- **WHEN** `openlatch.cluster.enabled=true` 且未配置 `node-id` 或 `peers`
- **THEN** 进程启动失败，错误信息指明缺失的配置键

#### Scenario: 单机默认零配置
- **WHEN** 不提供任何 `openlatch.cluster.*` 配置启动
- **THEN** 服务以单机模式正常启动，与 Phase 1 行为一致

#### Scenario: 地址映射未配置不阻塞启动
- **WHEN** `enabled=true` 且未配置 `client-addresses`
- **THEN** 节点正常组网服务，Leader 提示中 `leader_address` 为空字符串，客户端以种子发现兜底可完成故障转移

#### Scenario: 地址映射非法启动失败
- **WHEN** `client-addresses` 含格式非法项或重复 nodeId
- **THEN** 进程启动失败，错误信息指明具体非法项

## ADDED Requirements

### Requirement: 非 Leader 节点的查询应答

集群中任意存活节点（含 Follower 与 Leadership 空窗期的节点）SHALL 应答 HELLO、PING 与 `CLUSTER_VIEW` 查询：HELLO 在 Follower 上 MUST 正常建立会话（会话登记经复制通道至当值 Leader，与本 Requirement 引入前一致），`CLUSTER_VIEW` MUST 依据本地配置与 Leadership 视图作答、不依赖本节点为 Leader、不产生日志条目。本节点为 Leader 时 HELLO 提示 MUST 指向自身。

#### Scenario: Follower 可握手可查询
- **WHEN** 客户端连接 Follower 完成 HELLO 并发送 `CLUSTER_VIEW` 与 PING
- **THEN** 握手成功获得会话、`CLUSTER_VIEW` 返回成员表、PING 正常应答；期间集群写日志不因这些查询增长

#### Scenario: Leader 自指提示
- **WHEN** 客户端直连当值 Leader 完成 HELLO
- **THEN** 响应的 `leader_hint` 为该 Leader 自身 nodeId，`leader_address` 为其接入地址（已配置时）
