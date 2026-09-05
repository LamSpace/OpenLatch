# cluster-node-lifecycle Specification

## Purpose

定义集群节点的装配、配置与生命周期契约：`openlatch.cluster.*` 配置体系、Raft 子系统与接入服务的启停绑定，以及"同一二进制、默认配置 = Phase 1 单机行为"的回退保证。

## Requirements

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

### Requirement: Raft 子系统生命周期绑定
启用集群时，Raft 子系统 SHALL 与接入服务同生命周期：启动顺序为"先完成 Raft 组网与状态机初始化，后开放客户端接入端口"；关停顺序为"先拒新请求并了结在途（含以可重试错误完成未决请求），后关停 Raft 层并刷写持久化"。节点在追赶/未就绪期间 MUST NOT 应答任何写请求成功。优雅关停 MUST 在有界时间内完成，且该有界性 MUST NOT 依赖复制库线程池在关停时刻的调度状态（装配层 SHALL 将复制库的服务端线程池钉为非缓存常驻 worker，杜绝"关停派发任务落在零工作线程队列而永不被执行"的调度前提）。

#### Scenario: 关停无悬挂请求
- **WHEN** 节点关停时存在已提交未应用的在途写请求
- **THEN** 每个在途请求均收到应答（应用结果或可重试错误）后关停完成，无请求无限等待

#### Scenario: 未就绪不接受写入
- **WHEN** 节点刚启动、复制状态尚未追平
- **THEN** 该节点对写请求一律以拒绝/重定向错误应答，握手与只读连接可正常建立

#### Scenario: 长空闲后优雅关停有界
- **WHEN** 集群节点在无复制流量、无请求调度的空闲时段（超过库 cached 工作线程的空闲回收时限）之后执行优雅关停（`OpenLatchServer.stop()`）
- **THEN** 关停在有界时间内完成（状态机更新器收到停止信号并排空已提交条目，关停链无无限等待）；同场景在持续负载交错下亦成立

### Requirement: 单机模式回退保证
`openlatch.cluster.enabled=false` 时，同一二进制 MUST 表现出与 Phase 1 完全一致的行为：不启动任何 Raft 组件、不监听 `raft-port`、写请求处理路径不经过复制、既有锁语义与时序不变。本变更 MUST NOT 改动 `openlatch-core` 的任何公开行为契约。

#### Scenario: Phase 1 回归全绿
- **WHEN** 以默认（单机）配置运行 Phase 1 全部既有测试
- **THEN** openlatch-core / openlatch-server / openlatch-client 既有用例全部通过，无行为差异

#### Scenario: 单机不触碰 Raft 资源
- **WHEN** `enabled=false` 启动且 `data-dir` 不存在
- **THEN** 不创建 Raft 日志/快照目录，进程端口仅含接入端口

### Requirement: 非 Leader 节点的查询应答

集群中任意存活节点（含 Follower 与 Leadership 空窗期的节点）SHALL 应答 HELLO、PING 与 `CLUSTER_VIEW` 查询：HELLO 在 Follower 上 MUST 正常建立会话（会话登记经复制通道至当值 Leader，与本 Requirement 引入前一致），`CLUSTER_VIEW` MUST 依据本地配置与 Leadership 视图作答、不依赖本节点为 Leader、不产生日志条目。本节点为 Leader 时 HELLO 提示 MUST 指向自身。

#### Scenario: Follower 可握手可查询
- **WHEN** 客户端连接 Follower 完成 HELLO 并发送 `CLUSTER_VIEW` 与 PING
- **THEN** 握手成功获得会话、`CLUSTER_VIEW` 返回成员表、PING 正常应答；期间集群写日志不因这些查询增长

#### Scenario: Leader 自指提示
- **WHEN** 客户端直连当值 Leader 完成 HELLO
- **THEN** 响应的 `leader_hint` 为该 Leader 自身 nodeId，`leader_address` 为其接入地址（已配置时）

### Requirement: 成员变更运维
系统 SHALL 提供集群成员变更的封装操作：新增节点（经追赶纳入复制组）与移除节点。被移除节点上登记的会话 SHALL 按会话失联的既定语义以日志条目批量清理，各副本一致收敛。运行为约束（先加新节点并等待追赶完成再移除旧节点；禁止同时变更多数派成员）SHALL 写入部署运维文档，且变更操作 MUST NOT 中断多数派在服务期间的锁语义（持有与租约不丢）。

#### Scenario: 加节点追赶纳入
- **WHEN** 以空数据目录新增一个节点并执行成员变更
- **THEN** 该节点追赶（含快照安装）至与既有副本全量一致后成为复制组成员，可被选为 Leader

#### Scenario: 删节点会话清理
- **WHEN** 移除一个持有活跃会话与锁的节点
- **THEN** 该节点全部会话的关闭以日志条目落地，其持有的锁被释放，存活副本摘要一致

#### Scenario: 变更期间已确认锁不丢
- **WHEN** 成员变更进行中对集群持有的锁执行续租/释放
- **THEN** 已确认的持有关系保持，操作按正常语义应答
