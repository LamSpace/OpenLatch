# Proposal: phase2-leader-stall-followup

## Why

Phase 2 签署发布（`docs/Phase2-验收报告.md` 标准 5 ⚠️ 条件通过）随附一项在案 P1：**leader 复制停摆**——"旧 leader 带脏条目重启归群"语境下，新 leader 任期 NOOP 可能永不提交，写面 `LeaderNotReady` 阻塞 200+ 秒不自愈、无自愈路径（差分归因与全部取证见 `openspec/changes/archive/2026-09-06-phase2-release-closure/defects/leader-replication-stall-ratis-3.3.0.md`）。当前仅有运维缓解（"先从不先主"推荐序），不解决无人值守可用性缺口。收口期还登记了一项外观偏差：孤立节点 RELEASE 道实测回 `NOT_HELD`，与 proto 注释"NOT_LEADER（仅转发失败路径）"码形不一致，客户面可能被误读为锁已丢。本 change 兑现验收报告承诺的跟进计划。

## What Changes

- **T1 确定性复现与根因定位（spike 先行）**：构造最小化复现（in-JVM 3 节点：杀当值 Leader→其带脏条目重启归群→高频采样观察停摆率），双 jstack 复刻现场定位到库内单点路径（`LeaderStateImpl` sender 激活 / `GrpcLogAppender` INCONSISTENCY 回退后重试推进 / `startupLogEntry` 生命周期三者之一），结论入 observations 档案。
- **T2 产品侧自愈看门狗（若 T1 判定可承受）**：leader 角色下任期提交停滞超阈 → 主动 `transferLeadership` 让位于日志最落后的健康对侧（失败兜底步序见 design D3）；含防误伤振荡保护（选举空窗/追赶窗豁免）、可观测面（log/事件）。规格化：撤销本 change `skip_specs`，`cluster-node-lifecycle`（或 `replicated-state-machine`，随机制落点）ADDED 需求"复制停滞自愈"——沿 `phase2-release-closure` 4.3 先例执行中落定。
- **T3 Ratis 3.3.1 升级评估**：跟踪上游发版；若发布且相关路径（close/leader 选举/复制）变更，升级跑全套 `-Pdrill` + 10 分钟 soak 对照，可根治则**以升级替代 T2**（移除钉死/看门狗中不必要者，装配契约随实调整）；未根治则以 T1 最小复现向 Apache Ratis 提报 issue。
- **T4 NOT_HELD 码形对齐**：定位孤立节点 RELEASE 道返回 `NOT_HELD` 的门序成因（会话本地建立→归属判先于转发失败），定夺"实现改回 NOT_LEADER/可重试码"或"注释与部署文档记录码形"——倾向后者为底、前者为客户面语义优先（design D5）。
- 不做：多 Raft 组、ReadIndex、T2 的自动 failover 编排（属 Phase 3 运维面，如需要另行立项）。

无 **BREAKING**：协议字段/错误码枚举不增不改（T4 仅改返回值形状或文档）；公开 API 不动。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

（暂空——T2/T4 规格化在执行中落定时撤销 `skip_specs` 并补 delta；机制未定前不虚构需求。当前阶段为证据采集 + spike + 条件修复，`skip_specs: true`。）

## Impact

- **测试**：新增最小复现用例（`@Tag("drill")` 档，非默认门禁——高频采样耗时长）；T2 落定后新增自愈回归 + `ClientChaosIT` soak 对照轮。
- **产品代码**：条件性——T2 触及 `server.raft`（看门狗挂 `SessionCoordinator`/新类）；T4 触及 `ClusterRequestHandler` 错误映射或 `openlatch.proto` 注释。
- **依赖**：T3 可能引入 ratis 3.3.1 版本 bump（当前 3.3.0 全摩擦档案在 S2/S4 observations 为基线）。
- **文档**：详设 §12 风险 5 状态更新；部署文档运维指引修订（人工序 → 自愈说明或保留人工序）；`defects/` 档案终态（closed/converted）。
