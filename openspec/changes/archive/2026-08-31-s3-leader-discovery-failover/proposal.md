# Proposal: s3-leader-discovery-failover

## Why

Phase 2 S2 交付了复制状态机（多数派复制、会话集群登记、租约到期驱动），但集群尚不可用：Leader 切换后客户端无处可去——协议无 leader 提示、Follower 对写请求回无提示的占位 `NOT_LEADER`、客户端仍只会连单地址。S3（P2-11～P2-14）补上"Leader 发现与客户端故障转移"，使详设 §11 验收 2（杀 Leader 恢复 < 10s、存活会话锁不丢）可测、可达。

## What Changes

- **协议 v2（增量，非 BREAKING）**：`Envelope.protocol_version` 升 2，服务端同时接受 v1/v2 客户端；`HelloResponse` 复用既有预留字段 `leader_hint=5` 承载 Leader nodeId、新增 `leader_address=6`；`AcquireResponse`/`ReleaseResponse`/`LeaseRenewResponse` 各新增 `leader_node_id`/`leader_address` hint 字段（详设 §6.2 漏定义的"随附 leaderHint"载体）；新增 `MessageType.CLUSTER_VIEW=7` 与 `ClusterView`/`NodeInfo` 消息；启用 Phase 1 预留码 `NOT_LEADER`。v1 字段编号与语义零变更，新字段对 v1 客户端为未知字段、按既有容忍规则忽略。
- **Follower 角色语义定形**（核心裁决）：写请求分车道——ACQUIRE（新授予/排队，Leader 本地态）在 Follower 一律 `NOT_LEADER` + hint 引导改连；RELEASE/RENEW（纯 token/归属校验）经 Follower 的既有转发车道（与 `SESSION_OPEN` 同路）提交 Leader、以 home 会话身份复制执行——由此"Leader 切换后存活会话的锁可续可解"成立（验收 2），v1 客户端存量锁操作亦不退化。选举空窗（新 Leader 未出）回 `NOT_LEADER + leader_node_id = -1` 区别于"你不是 Leader"。
- **服务端 LeaderTracker**：保留 Ratis `notifyLeaderChanged` 的新 Leader 身份（现被折算为 boolean 丢弃），维护 `{leaderNodeId, leaderAddress}`，供 HELLO/`NOT_LEADER` 应答与 `CLUSTER_VIEW` 作答；新增配置键 `openlatch.cluster.client-addresses`（`id@host:port` 列表）提供节点接入地址映射。
- **客户端 Leader 发现与故障转移**：`OpenLatchClient.Builder` 新增种子列表配置（既有单地址入口保持兼容）；启动 HELLO hint 直连、运行中 `NOT_LEADER` 重定向重放、连续 3 次 `NOT_LEADER` 强制种子发现、断连先试原地址后轮询种子；存量锁按其 `sessionId` 路由回 home 连接，新获取走 Leader 连接；发现失败在请求超时（默认 5s）内快速抛错。
- **杀 Leader 演练自动化（S3 退出门）**：进程级 3 节点演练（`@Tag("drill")`，独立 profile），计时"kill → 客户端首次成功业务" < 10s，断言存活会话锁不丢（续租不断、failover 后释放成功）与等待者重排队，全程双授/泄漏不变式检查。

不含：ForwardingProxy 新类（转发复用 `ReplicationGateway` 既有车道，不另立组件）；spring-boot-starter 的 seeds 透传（留待后续）；快照/成员变更（S4 范围）。

## Capabilities

### New Capabilities

（无——全部落在既有 capability 的需求增量上。）

### Modified Capabilities

- `wire-protocol`: 协议版本需求从"固定 1、非 1 拒绝"改为"1|2 接受、其他拒绝"；新增 v2 leader 提示字段、`CLUSTER_VIEW` 消息与 `NOT_LEADER` 启用语义；v1 兼容性冻结要求。
- `replicated-state-machine`: "Leader 写请求路径"需求扩展 Follower 侧契约——ACQUIRE 拒绝+hint、RELEASE/RENEW 转发车道、选举空窗区分、LeaderTracker 提示一致性。
- `client-sdk`: 新增"Leader 发现与故障转移"需求（种子直连、重定向重放、强制发现、home 连接路由、快速失败）；"构建与连接建立"的必填地址放宽为种子列表。
- `cluster-node-lifecycle`: 集群配置体系新增 `openlatch.cluster.client-addresses` 键及校验；`CLUSTER_VIEW` 在追赶期可作答的只读例外。

## Impact

- **代码**：`openlatch-protocol/openlatch.proto`（v2 增量）；`openlatch-server`（`ServerSessionHandler` HELLO 填 hint、`ClusterRequestHandler` 分车道、`server.raft` 新增 `LeaderTracker`、`ClusterConfig` 新键、`OpenLatchServer.PROTOCOL_VERSION=2` 与版本门）；`openlatch-client`（`ClientConfig` seeds、`ConnectionManager` 目标选择与重定向、`OpenLatchClient` NOT_LEADER 拦截与双车道路由、`RequestMultiplexer`/`Watchdog` 错误语义、`PROTOCOL_VERSION=2`）；`openlatch-examples` 或 server 测试域新增演练驱动。
- **测试**：协议冻结用例（新增 `openlatch.proto` Phase 1 基线冻结，对齐 `RaftProtoContractFreezeTest` 先例）；`ClusterHarness` 扩角色感连接入口；client 测试引入集群 harness（test-jar 依赖）；新增 drill profile。
- **文档**：详设 v1.2 修订回写（hint 字段编号定稿、§6.2 补响应 hint、§6.1 v1 口径、新配置键入 §9 表）；一致性声明与 failover 行为写入用户文档。
- **依赖**：无新增外部依赖（Ratis 3.3.0 已就位）；`CoreEngine`（openlatch-core）零改动延续。
