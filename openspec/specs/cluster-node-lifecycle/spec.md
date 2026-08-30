# cluster-node-lifecycle Specification

## Purpose

定义集群节点的装配、配置与生命周期契约：`openlatch.cluster.*` 配置体系、Raft 子系统与接入服务的启停绑定，以及"同一二进制、默认配置 = Phase 1 单机行为"的回退保证。

## Requirements

### Requirement: 集群配置体系
系统 SHALL 在 Phase 1 Properties 体系上并入以下配置键并逐键校验：`openlatch.cluster.enabled`（默认 `false`）、`openlatch.cluster.node-id`（启用时必填）、`openlatch.cluster.peers`（启用时必填，`id@host:raftPort` 列表）、`openlatch.cluster.raft-port`（默认 `9411`）、`openlatch.cluster.data-dir`（默认 `./data`）、`openlatch.cluster.snapshot-threshold`（默认 `1000000`）、`openlatch.cluster.election-timeout-ms`（默认 `3000`，按 Raft 层语义透传）。`enabled=true` 而必填项缺失或非法时，启动 MUST 失败并给出可定位的错误信息，MUST NOT 静默降级为单机模式。

#### Scenario: 缺必填项启动失败
- **WHEN** `openlatch.cluster.enabled=true` 且未配置 `node-id` 或 `peers`
- **THEN** 进程启动失败，错误信息指明缺失的配置键

#### Scenario: 单机默认零配置
- **WHEN** 不提供任何 `openlatch.cluster.*` 配置启动
- **THEN** 服务以单机模式正常启动，与 Phase 1 行为一致

### Requirement: Raft 子系统生命周期绑定
启用集群时，Raft 子系统 SHALL 与接入服务同生命周期：启动顺序为"先完成 Raft 组网与状态机初始化，后开放客户端接入端口"；关停顺序为"先拒新请求并了结在途（含以可重试错误完成未决请求），后关停 Raft 层并刷写持久化"。节点在追赶/未就绪期间 MUST NOT 应答任何写请求成功。

#### Scenario: 关停无悬挂请求
- **WHEN** 节点关停时存在已提交未应用的在途写请求
- **THEN** 每个在途请求均收到应答（应用结果或可重试错误）后关停完成，无请求无限等待

#### Scenario: 未就绪不接受写入
- **WHEN** 节点刚启动、复制状态尚未追平
- **THEN** 该节点对写请求一律以拒绝/重定向错误应答，握手与只读连接可正常建立

### Requirement: 单机模式回退保证
`openlatch.cluster.enabled=false` 时，同一二进制 MUST 表现出与 Phase 1 完全一致的行为：不启动任何 Raft 组件、不监听 `raft-port`、写请求处理路径不经过复制、既有锁语义与时序不变。本变更 MUST NOT 改动 `openlatch-core` 的任何公开行为契约。

#### Scenario: Phase 1 回归全绿
- **WHEN** 以默认（单机）配置运行 Phase 1 全部既有测试
- **THEN** openlatch-core / openlatch-server / openlatch-client 既有用例全部通过，无行为差异

#### Scenario: 单机不触碰 Raft 资源
- **WHEN** `enabled=false` 启动且 `data-dir` 不存在
- **THEN** 不创建 Raft 日志/快照目录，进程端口仅含接入端口
