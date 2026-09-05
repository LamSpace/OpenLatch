# Design: phase2-leader-stall-followup

## Context

见 proposal.md - Why 与 `archive/2026-09-06-phase2-release-closure/defects/leader-replication-stall-ratis-3.3.0.md`（症状、频率矩阵、取证线索、复现命令）。本设计是**决策框架**：T1 spike 的结论决定 T2/T3 的取舍，故多数决策标注"随 T1 定夺"，属 spike 型 change 的合法形态（待决未知集中于 Open Questions，不虚构）。

既有事实约束：
- 停摆判据特征：`LeaderStateImpl.isReady()` ≡ `startupLogEntry.isApplied()` 恒假（Ratis 3.3.0 源 450 行）；`isReady` 无公开 API 暴露，产品侧可见信号只剩间接量（本节点 commitIndex/应用位点在自认 leader 期间零推进、客户端持续收 `LeaderNotReadyException`）；
- 产品已具备的通道：`ClusterRuntime.subsystem().transferLeadership(...)`（S4/P2-17 成员变更用过）、`SessionCoordinator` 的失联判定与任期作用域（D12 进度保护经验：停滞判定必须防"选举/回放修复窗内的暂时滞后"误伤）；
- 环境事实：停摆双稳态、今日 4/4 命中（热机）/S4 1/5（当日）；差分归因已闭环，无需复做。

## Goals / Non-Goals

**Goals:**
- 把 P1 从"运维绕序 + 人工无解"收敛为"根因定位 + 产品可自愈（或上游已根治）"；
- 客户面错误码语义诚实化（T4）；
- 全程证据进档案，验收报告标准 5 具备复核改判路径。

**Non-Goals:**
- 不改 Raft 库源码/fork 库（提报上游即可）；
- 不做自动多组故障转移编排、不改协议枚举；
- 不复测 D8（关停修复）与常规回归语义（缺省门禁零变化）。

## Decisions

### D1 复现路径选择：进程内 in-JVM 时序构造，而非滚动演练原样重放
演练语境 200+ 秒观测太长且多变量。T1 用例直接构造最小前件：3 节点起群→确认 Leader L→SIGKILL 语义停 L（不清数据）→重启 L 令其带旧任期脏条目归群→对侧当选→以"新 leader 任期起始后 N 秒内 commitIndex 零推进 + 写请求持续 `LeaderNotReadyException`"为停摆判定，循环采样 K 轮记命中率。verify 目标：命中率显著（>0）即得复现基座；若构造不中，回退到"热机背靠背滚动演练"重放（脚本已有）。

### D2 检测信号（T2）：任期起始提交年龄，双条件防误伤
watchdog 判定（Leader 角色成立时评估）：`本节点自任该任期起 elapsed > T_stall` **且** `commitIndex 自任期内连续 M 个采样周期零推进`——与 S4 D12 失联判定同构的"进度保护"经验复用；选举空窗、安装/追赶态恒非 Leader 天然豁免。`T_stall` 默认 `max(10s, 5×election-timeout)`（杀主演练实测恢复 1.6–1.9s，十倍余量），可配性随 design D6 定。

### D3 自愈动作：让位优先、自杀式重启兜底（决策随 T1 根因收口）
首选 `transferLeadership` 至日志最新存活对侧（停摆 leader 的 log 与心跳通道独立，让位 RPC 若可达则一次恢复）；对侧日志皆旧或让位超时，动作升级为**自杀式重启**（进程级 `System.exit` 交外部 supervisor / in-JVM 测试内 restartNode）——让位失败即证明复制面也烂，重启换新鲜 LeaderState 是确定有效的复位。二者取舍依 T1"sender 是否可复位"的结论定稿。

### D4 上游关系：升级优先于自愈（如可行）
T3 若 3.3.1 根治该链（close/LeaderState/LeaderElection 变更命中），升级 + 全套对照即收口，T2 看门狗降级为可选防御或直接不做（简单优先，CLAUDE.md §2）；看门狗只补"上游不修/修复遥遥无期"的缺口。

### D5 NOT_HELD 对齐（T4）：先注释/文档为底，实现改动看客户面风险
孤立 follower 本地以复制态判归属（新会话 sid≠持有者→NOT_HELD）语义上不算错，且**转发失败回 NOT_LEADER** 的形状要求该节点先"意识到"转发会失败；实现层面把"角色/转发失败"判定提到归属判定之前会改变单机回归面（Phase 1 语义路径），需先核对 `ClusterRequestHandler` 门序现实现与冻结测试面，再定夺。默认落点：proto 注释 + 部署文档记录该码形及其含义（"非 OK 即未释放"的一致性声明已覆盖正确性面）。

### D6 可配性默认不做（沿 S2 D2 先例）
`T_stall`/`M` 若实现，先装配层钉死 + 常量，不入配置（详设 §9 口径：运维配置最小面）；真有运维诉求再立项。

## Risks / Trade-offs

- [看门狗误伤正常窗口，引发让位风暴] → D2 双条件 + 任期边界重置 + 让位频率上限（每任期一次，S4 NOOP 探针同生命周期口径）；
- [自杀式重启在生产无 supervisor 的环境造成循环重启] → 仅作为 transfer 失败的兜底，且带全局冷却窗（默认 5 分钟内不二次触发自杀路径）——冷却与否则否纳入 D3 定稿；
- [T1 复现不中（概率性）] → 回退滚动重放 + 更长采样；若始终不可复现，T2 失去验证基座——该分支下 change 降级为"仅 T3 跟踪 + T4 对齐"收口，P1 维持运维缓解（如实记录）；
- [升级 3.3.1 引入新摩擦] → S2/S4 摩擦档案为基线全套重验（`-Pdrill` + soak）。

## Open Questions

（随 T1 收口，不阻塞任务拆分）
- 停摆的库内单点究竟是 sender 激活、appender 重试推进，还是 startupLogEntry 生命周期？（决定 D3 取 transfer 还是重启、以及上游提报措辞）
- 3.3.1 是否已发布且命中相关变更？（决定 D4 走向）
