# Tasks: phase2-leader-stall-followup

spike 型 change：组 1 结论驱动组 2/3 取舍（D4 升级可行则组 2 降级）。决策 D1–D6 见 design.md。

## 1. 确定性复现与根因定位（T1，spike）

- [ ] 1.1 最小复现用例（D1：`@Tag("drill")` in-JVM 3 节点——SIGKILL 语义停当值 Leader→带脏条目重启归群→"新 leader 任期起 commitIndex 零推进 + 持续 `LeaderNotReadyException`"判定→K 轮采样命中率）
  - verify：复现基座建立（命中率 >0 且显著，采样数据入档）；构造不中则回退滚动演练重放（脚本已有），复跑 ≥4 轮取证
- [ ] 1.2 根因定位：停摆现场双 jstack（沿收口档案方法）+ 3.3.0 源码对账（`LeaderStateImpl` sender 激活 / `GrpcLogAppender` INCONSISTENCY 回退后推进 / `startupLogEntry` 生命周期，三选一收口），结论写 `observations-leader-stall-rootcause.md`
  - verify：给出唯一候选路径与机理陈述，解释"重启可复位 / 让位可复位"何者成立（喂给 D3）
- [ ] 1.3 3.3.1 状态调查（D4 前置）：Maven Central 版本事实 + 相关路径 changelog/commit diff（close/LeaderStateImpl/LeaderElection/GrpcLogAppender）
  - verify：三态结论之一：可升级根治 / 可升级未根治 / 未发布；决定组 2、组 3 走向

## 2. 修复路径落地（依 1.3 三选一）

- [ ] 2A.1（若"可升级根治"）升级 ratis 3.3.1：依赖 bump、S2/S4 摩擦档案基线全套重验（`clean verify` + `-Pdrill` + 10 分钟 soak），停摆采样（1.1 用例）复跑归零
  - verify：全绿 + 停摆命中 0/K 轮；详设 §2 选型版本口径更新
- [ ] 2B.1（若"可升级未根治"或"未发布"）看门狗实现（D2 检测 + D3 动作 + 防误伤，常量钉死 D6）+ delta spec 规格化（撤销 `skip_specs`，落点 capability 随机制定）+ 确定性单测（模拟停摆态断言 ≤T_stall+ε 内让位/重启触发、正常选举窗零误伤）
  - verify：单测全绿；1.1 复现用例复跑 K 轮全部自愈；soak 对照轮绿（自愈事件计入观察记录，错误率按自愈前后分段如实报）
- [ ] 2C.1（若 1.1/回退重放始终不可复现）change 降级收口：仅完成组 3 + 组 4，P1 维持运维缓解现状，档案记录"复现不可得"与复核条件
  - verify：验收报告标准 5 复核注记

## 3. 上游提报（若非 2A）

- [ ] 3.1 整理最小复现（1.1 用例 + 1.2 根因陈述）向 Apache Ratis 提 JIRA/GitHub issue，附 3.3.0 版本与环境事实；跟踪编号回写本档案
  - verify：提报链接入档

## 4. NOT_HELD 码形对齐（T4，独立可做）

- [ ] 4.1 定位 `ClusterRequestHandler` RELEASE/RENEW 门序现实现（归属判定与转发失败的先后关系），核对冻结测试面改动半径（`OpenlatchProtoContractFreezeTest`）
  - verify：成因陈述入档（哪一道门、何种连接状态下回 NOT_HELD）
- [ ] 4.2 定夺并落地（D5）：默认注释/文档对齐（proto 注释 + 部署文档码形说明）；若客户面误读风险被 4.1 证实（转发失败先于归属判定可达），改实现回 NOT_LEADER/可重试码并补契约用例
  - verify：分区演练报告同凭证观测与新码形一致（重跑 run 一次）或文档口径闭环

## 5. 收口

- [ ] 5.1 `defects/leader-replication-stall-ratis-3.3.0.md` 状态更新（resolved/mitigated/deferred + 链接）；详设 §12 风险 5、部署文档运维指引同步
- [ ] 5.2 验收报告标准 5 复核改判（若自愈/升级落地：⚠️→✅ 或注记"停摆已由 X 承载"）+ 全量门禁（`clean verify` + `-Pdrill` 全套）
- [ ] 5.3 合入主干并归档本 change
