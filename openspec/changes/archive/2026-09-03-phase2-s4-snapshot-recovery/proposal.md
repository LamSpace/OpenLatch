# Proposal: phase2-s4-snapshot-recovery

## Why

详设 §1.1 目标 3（"快照 + 日志追赶支持节点重启与新节点加入"）尚未落地：S2 将 `takeSnapshot` 留为返回 `-1` 的桩、显式关闭 Ratis 自动触发，节点重启只能全量回放日志，落后过多/新加入节点无追赶通道。S4（P2-15～P2-19）补齐快照生成/加载/安装、成员变更与容错演练，是 §11 验收第 3–6 项与 Phase 2 发布的收口段。

## What Changes

- **快照生成（P2-15）**：`LockStateMachine.takeSnapshot` 实装——apply 锁内取 `ShadowTable.toProto()` 一致性副本、锁外写盘（tmp→原子 rename，复用 `SnapshotState` proto 骨架）；`snapshot-threshold` 接入 Ratis 自动触发（替换 S2 的显式关闭），保留最近 2 份；新增 `RaftSubsystem.triggerSnapshot()` 作为手动触发面（测试/运维脚本驱动，不引入管理端点）。
- **快照加载与追赶（P2-16）**：`initialize` 启动加载最新快照 + Ratis 原生日志回放；实现 `loadSnapshot`/`pause` 流安装，落后节点与新节点经 Leader 快照安装后增量追赶；追赶期间写请求 `NOT_LEADER`、`CLUSTER_VIEW` 可答（§7.3-3）；10 万条目基准（大小/落盘/加载耗时、恢复 <30s）与 `StateComparisons` 全量比对收口。
- **`CoreEngine` 快照重建入口（§7.1 授权增量）**：新增 core 原生值对象 + `restoreFrom` 入口（含 token 发号器跳界、到期堆回填、会话登记），**既有方法体零改动**——回灌路线已被 PoC 证伪（快照含历史释放空洞时无法复现 token 序列），直接重建是唯一正确路线（详见 design D1）；快照随附**发号水位**（`SnapshotState.next_lease_token`，S4 增补字段），否则重建副本与未截断副本对同一尾部日志发出不同凭证、跨副本永久分叉（design D10）。
- **失联判定加固（实施中发现的真实缺陷）**：停滞计数改以"commitIndex 零推进"为必要条件，选举/回放修复窗口内的暂时滞后不再误清存活会话（design D12，spec"失联判定的进度保护"）。
- **成员变更（P2-17）**：`setConfiguration` 封装；被移除节点会话沿用 §5.2 规则 4 批量清理；"先加后删、禁止同时变更多数派"写入部署运维文档。
- **演练与验收闭环（P2-18/19）**：netns 真分区脚本 + in-JVM 近似用例（§11-3）；滚动重启错误率 <1%（§11-5）；≥10 分钟随机杀节点混沌 + 不变式检查器（§11-6）；§11 逐项证据收集、退出检查单与详设 v1.4 回写（含 core "零改动"口径切换为"§7.1 授权纯新增"）。

**非目标**：不改客户端、不改 wire protocol（快照/日志消息为服务端内部，`SnapshotState` 编号已冻结）、不做多 Raft 组/分片、不做读优化。

## Capabilities

### New Capabilities

- `snapshot-recovery`: 快照内容/序列化、触发与保留、生成期间服务不中断、启动恢复与日志回放、落后/新增节点的快照安装追赶、追赶期间对外行为、10 万条目基准与全量比对判据。

### Modified Capabilities

- `core-lock-engine`: **新增** Requirement"快照状态重建入口"——恢复锁条目/持有计数/租约/会话登记后，后续授予/释放/续租/到期扫描与未截断日志的副本一致；token 发号不复用快照已发凭证。
- `cluster-node-lifecycle`: **新增** Requirement"成员变更运维"——加节点追赶、删节点会话清理、先加后删流程约束经封装 API 可执行。
- `replicated-state-machine`: **新增** Requirement"失联判定的进度保护"——零推进为失联必要条件，修复窗口不误清存活会话（D12）。

## Impact

- `openlatch-core`：新增重建入口（新增值对象 + `CoreEngine` 新增一个方法；既有方法体零改动，退出证据口径相应切换）。
- `openlatch-server`：`server.raft` 包内 `LockStateMachine`（takeSnapshot/loadSnapshot/initialize/pause）、`LockStateMachineCore`（重建 + SnapshotState 翻译）、`RaftSubsystem`（自动触发/保留配置、triggerSnapshot、成员变更封装）。
- 测试：`ClusterHarness` 扩展（快照等待/安装观测）；`StateComparisons` 复用为 P2-16 比对工具；新增单测/属性测试/集成测试与演练脚本（产出 `docs/` 演练报告，格式沿 `failover-drill-2026-08-31.md`）。
- 文档：`docs/OpenLatch-Phase2-详细设计说明书.md` v1.4 回写；部署运维文档增成员变更章节。
- 依赖：无新增（Ratis 3.3.0 原生 SnapshotStorage/AdminApi 能力范围内）。
