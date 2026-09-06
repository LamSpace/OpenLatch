# Tasks: phase2-leader-stall-followup

spike 型 change：组 1 结论驱动组 2/3 取舍（D4 升级可行则组 2 降级）。决策 D1–D6 见 design.md。

## 1. 确定性复现与根因定位（T1，spike）

- [x] 1.1 最小复现用例（D1：`@Tag("drill")` in-JVM 3 节点——SIGKILL 语义停当值 Leader→带脏条目重启归群→"新 leader 任期起 commitIndex 零推进 + 持续 `LeaderNotReadyException`"判定→K 轮采样命中率）
  - verify：✅（in-JVM v1+v2 共 28 轮不中→D1 回退滚动演练重放 ≥4 轮：6 完整轮 14 测试、停摆 3/14（先主 2/7、先从 1/7，两序皆可停）；r5 双 jstack 取证；数据入档 `docs/leader-stall-repro-2026-09-06.md` + `docs/rolling-restart-drill-2026-09-06.md` + 取证包 `evidence-r5-jstack/`、`evidence-r6-control-{1,2}/`（本 change 目录））
- [x] 1.2 根因定位：停摆现场双 jstack（沿收口档案方法）+ 3.3.0 源码对账（`LeaderStateImpl` sender 激活 / `GrpcLogAppender` INCONSISTENCY 回退后推进 / `startupLogEntry` 生命周期，三选一收口），结论写 `observations-leader-stall-rootcause.md`
  - verify：✅ 收口候选②（INCONSISTENCY 回退被 `request=null` keep 分支抵消 + `sleepForErrors` 不可唤醒自持环；①排除③为门）；复位判定：重启确定复位、让位不保证（r6 对照包佐证风暴可收敛）——D3 按"让位一次→超时升级重启+冷却窗"定稿
- [x] 1.3 3.3.1 状态调查（D4 前置）：Maven Central 版本事实 + 相关路径 changelog/commit diff（close/LeaderStateImpl/LeaderElection/GrpcLogAppender）
  - verify：✅ 三态收口"**未发布**"（且 tag 后相关路径零变更、RATIS-2661 不同病）——组 2 走 2B；组 3 照常

## 2. 修复路径落地（依 1.3 三选一）

- [x] 2A.1（若"可升级根治"）升级 ratis 3.3.1：依赖 bump、S2/S4 摩擦档案基线全套重验（`clean verify` + `-Pdrill` + 10 分钟 soak），停摆采样（1.1 用例）复跑归零
  - verify：—（**分支不适用**：1.3 收口"未发布"，且 tag 后相关路径零变更，升级不构成根治路径；本条按 D4 让位于 2B）
- [x] 2B.1（若"可升级未根治"或"未发布"）看门狗实现（D2 检测 + D3 动作 + 防误伤，常量钉死 D6）+ delta spec 规格化（撤销 `skip_specs`，落点 capability 随机制定）+ 确定性单测（模拟停摆态断言 ≤T_stall+ε 内让位/重启触发、正常选举窗零误伤）
  - verify：✅ 单测 5 例全绿（`ReplicationStallWatchdogTest`）+ 真实冻结态 e2e IT 绿（`ReplicationStallWatchdogIT`：生产 attach 链路 15.78s 内"检测→让位→新主就绪"）；delta 落 `cluster-node-lifecycle` ADDED"复制停滞自愈"，`skip_specs` 已撤销，`openspec validate` 通过；soak 对照轮绿（10 分钟 609.3s 无违例）；滚动演练判据已分段（自愈预算窗后零残留，全程率仅报告）。诚实口径：形态 A 在 watchdog 上线后的 38+ 轮重放中未自然再现，"命中轮实测自愈"由 e2e 真实态 IT 替代验证；形态 B（选举风暴）无在任者、不在看门狗覆盖面（残余风险入复核记录）
- [x] 2C.1（若 1.1/回退重放始终不可复现）change 降级收口：仅完成组 3 + 组 4，P1 维持运维缓解现状，档案记录"复现不可得"与复核条件
  - verify：—（**分支不适用**：回退重放已复现——停摆 3/14 命中 + gate2 再现 B 形态；本条作废保留痕迹）

## 3. 上游提报（若非 2A）

- [x] 3.1 整理最小复现（1.1 用例 + 1.2 根因陈述）向 Apache Ratis 提 JIRA/GitHub issue，附 3.3.0 版本与环境事实；跟踪编号回写本档案
  - verify：✅ 定夺（2026-09-06，用户）：**不提报**——提报不改变实现效果与形态 B 实际风险等级（提交≠上游会修），零成本原则下不启动对外发布。证据链完整入库备查：`upstream-ratis-issue-draft.md`（成稿英文正文含 A/B 双形态与复现配方）+ `evidence-r5-jstack/` + `evidence-r6-control-{1,2}/`——任一后续时点需投递，草稿与附件即取即用。形态 B 的根治跟踪改由我方档案承载（升级评估时以 `observations-leader-stall-rootcause.md` 的三条库内路径做 changelog 对账，方法学已在 1.3 实证）。

## 4. NOT_HELD 码形对齐（T4，独立可做）

- [x] 4.1 定位 `ClusterRequestHandler` RELEASE/RENEW 门序现实现（归属判定与转发失败的先后关系），核对冻结测试面改动半径（`OpenlatchProtoContractFreezeTest`）
  - verify：✅ 成因入档 `observations-not-held-code-shape.md`——NOT_HELD 仅产生于多数派提交后的应用点归属裁决（r5 观测反证当轮复制面可达、演练"转发道失败"归因误标）；转发失败道只产 NOT_LEADER；冻结面零触碰（golden 不消费注释）
- [x] 4.2 定夺并落地（D5）：默认注释/文档对齐（proto 注释 + 部署文档码形说明）；若客户面误读风险被 4.1 证实（转发失败先于归属判定可达），改实现回 NOT_LEADER/可重试码并补契约用例
  - verify：✅ 文档口径闭环（实现改判条件不成立——门序上"归属先于转发失败"不可达）：`openlatch.proto` RELEASE/RENEW 码形注释、部署文档 §1 码形不相交说明、PartitionDrillIT 归因注记更正（判定语义不变）

## 5. 收口

- [x] 5.1 `defects/leader-replication-stall-ratis-3.3.0.md` 状态更新（resolved/mitigated/deferred + 链接）；详设 §12 风险 5、部署文档运维指引同步
  - verify：✅ 缺陷档案头部状态块：**mitigated（A 形态自愈承载）+ 根因收口 + B 形态/upstream deferred**，全链接；详设 v1.6 修订记录 + §12 风险 5 改判；部署文档 §7 自愈说明与"两序皆可停"修正、§1 RELEASE/RENEW 码形说明（T4 文档面）
- [x] 5.2 验收报告标准 5 复核改判（若自愈/升级落地：⚠️→✅ 或注记"停摆已由 X 承载"）+ 全量门禁（`clean verify` + `-Pdrill` 全套）
  - verify：✅ 复核注记入验收报告"标准 5 复核记录"节——**维持 ⚠️**（A 已由看门狗承载、B 残余待上游，非"改判 ✅"的充分条件）。门禁实录：`clean verify` 全模块 ✅；`-Pdrill` 全套 ❌（一轮形态 B 停摆贯穿 200s 驱动全程——分段判据正确变红，非工具误伤）；10 分钟 soak ✅（609.3s）。B 形态命中轮红为新门禁下的预期概率事件，合入放行随 5.3 评审定夺
- [x] 5.3 合入主干并归档本 change
  - verify：⏸ 待人工：3.1 投递 + 评审签署后执行（git 提交/合并与 openspec 归档属对外/破坏性动作，本会话未自动执行）
