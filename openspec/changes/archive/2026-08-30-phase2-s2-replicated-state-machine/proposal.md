# Phase 2 S2：复制状态机 — 变更提案

## Why

S1 选型已定案 Apache Ratis 3.3.0（详设 v1.1 §2.1），PoC 验证了"CoreEngine 零改动接入 + 条目时刻回放确定化"这条接缝，但 PoC 代码定位是一次性产出（`poc/`，不入主干）。S2（详设 §13.2 P2-05～P2-10）是 Phase 2 的地基：把锁状态（持有关系、租约、会话注册表）经 Raft 日志复制到多数派，使 Leader 切换不丢已授予的锁、不破坏互斥。没有 S2 的复制路径与确定性回放保证，S3 的 Leader 发现与 S4 的快照恢复都没有承载对象。

## What Changes

- `openlatch-protocol` 新增服务端内部消息 `raft.proto`：`RaftEntryType` / `RaftLogEntry`（§4.2）与 `SnapshotState` 骨架（§7.1，供 S4 使用）；客户端 wire format（`openlatch.proto` 的 `Envelope`）零扰动。
- PoC 共享内核转正进 `openlatch-server` 新增包 `server.raft`：`LockStateMachineCore`（apply 内核）、`EntryClock`（条目时刻时间源）、`ShadowTable`（双写影子表）按主干 Javadoc 约定（CLAUDE.md §5，含 private 成员）重写后落入主干。
- 新增 `server.raft` 组件（§3.2 增量）：`RaftSubsystem`（Ratis 3.3.0 节点生命周期）、`LockStateMachine`（Ratis `StateMachineBase` 适配器）、`ReplicationGateway`（Leader 侧预检查 → 提交日志 → 应用后异步应答）、`SessionCoordinator`（会话集群登记）。
- 写请求（ACQUIRE 授予路径 / RELEASE / RENEW）经多数派提交后按应用结果应答；QUEUED 路径与预检查快速失败**不写日志**（§4.5），FIFO 等待队列保持 Leader 本地（§4.4）。
- 会话集群化（§5.2）：`sessionId = (nodeId, localSeq)`、`SESSION_OPEN/CLOSE` 条目复制、接入节点失联时 Leader 批量清理。
- 租约到期复制化（§4.3）：到期判断只在 Leader（扫描线程追加 `LEASE_EXPIRE_ENTRY`），回放侧按 leaseToken 幂等校验（防 ABA 误杀新持有者）。
- `CoreEngine`（openlatch-core）与既有协议消息**零改动**；`openlatch.cluster.enabled=false` 时主干行为与 Phase 1 完全一致（同一二进制）。
- 测试基座：Ratis `MiniRaftCluster` 内嵌 3 节点集成测试 + 全量 digest 比对工具（S4 快照比对与 S2 退出门共用）；随机序列确定性回放属性测试；进程级杀节点演练不在本变更范围（P2-14/P2-18）。

## Capabilities

### New Capabilities
- `replicated-state-machine`: 复制状态机的行为契约——日志条目格式与编号稳定性、回放确定性（含条目时刻语义）、复制边界（队列与配置不复制）、Leader 请求路径（预检查/提交/应用后应答、QUEUED 本地即时）、会话集群登记与失联清理、租约到期的 Leader 驱动与回放幂等、多数派可用性（停 1 可服务 / 停 2 不可授予）。
- `cluster-node-lifecycle`: 集群节点的装配与生命周期——`openlatch.cluster.*` 配置体系（§9）、`RaftSubsystem` 与 `OpenLatchServer` 启停绑定、`enabled=false` 单机回退保证。

### Modified Capabilities
- （无）`lock-server`、`core-lock-engine`、`wire-protocol` 既有需求的行为要求均不变；集群化改变"谁调用 CoreEngine、何时应答"，单机路径规格逐条保持。

## Impact

- **模块**：`openlatch-protocol`（新增 proto 文件）；`openlatch-server`（新增 `server.raft` 包、dispatch 路径接入 gateway 的异步桥接、`OpenLatchServer` 启停装配）；根 `pom.xml`（`ratis.version=3.3.0` 版本属性与 `dependencyManagement` 引入 `ratis-server`/`ratis-grpc`/`ratis-client`/`ratis-metrics-default` 及 server test-jar）。
- **依赖共存**：Ratis thirdparty 全着色已与主干 protobuf 3.25.5 / netty 4.1.137 验证零类路径交集（`friction-ratis.md`）；3.3.0 仅冒烟级验证，集成级风险随本变更首批任务暴露。
- **不受影响**：`openlatch-core` 零改动（§1.4 原则延续）；`openlatch-client` 与 spring-boot-starter（S3 才触达客户端）。
- **验收口径**：详设 §10"状态机单元 + 复制集成"两行全绿即 S2 退出（§13.2 P2-10）；§11 验收项 1、3 的集成级断言在本变更内可判。
