# 在册事项(Watchlist)

> 被动、等触发器的登记事项——**没有预定要做的活儿**。触发后按"动作"列另立 OpenSpec
> change 处理,处理完从本表移除。历史决策与诊断证据以"记录位置"列为准。
>
> English: passive, trigger-based follow-ups only; nothing scheduled. On trigger, open an
> OpenSpec change per the "action" column, then remove the row.

| # | 事项 | 现状与结论 | 触发 → 动作 | 记录位置 |
|---|---|---|---|---|
| W1 | 演练首写 `request N timed out`(1/15 观测) | 领先解释为端口竞态族其他显形,现场日志已失、未定案;修复后本地/CI 多轮未复现 | 再现 → 按"红先保 `target/drill-logs/` 再清理"纪律取现场,另立 change 根因排查,**不猜** | `archive/2026-09-12-drill-port-allocation-race-fix`(tasks 3.2);记忆档 |
| W2 | `freePort()` 探针式取口的其余 7 处持有者(ClientProcessKillIT / IdleNodeGracefulStopIT / ClientChaosIT / ClientClusterIT / ClientSemaphoreClusterIT / ClientHandshakeTest / ScriptedServer) | 默认 verify 无观测失败;多为单节点/轻连接,TOCTOU 暴露面小 | 任一处出现 `bind EADDRINUSE` / "接入端口未就绪" / "未探测到 Leader" → 按 `drill-port-allocation-race-fix` 同款带外取样改造该处(红一次改一处) | 同上 change(tasks 3.3) |
| W3 | drill CI 自动化恢复 | schedule 已暂停(2026-09-14):ubuntu-latest 2 vCPU 不满足时序断言的算力前提;评审人决定**不接 self-hosted**。现状 = `workflow_dispatch` 手动随跑,本地复跑随时可行 | 将来接受 self-hosted 或 ≥4 vCPU 档 → 一行 PR(`runs-on` + 解注释 schedule) | `.github/workflows/drill.yml` 头注;`archive/2026-09-14-ci-github-actions/ci-first-runs.md` |
| W4 | Ratis 升级评估对账 | 复制停摆(形态 A 自愈已承载)与选举风暴残余(形态 B,已签署不向上游提报)均定夺由"升级评估对账"承载 | 评估/升级 Apache Ratis 版本时:对照 `archive/2026-09-06-phase2-release-closure/defects/` 与 leader-stall 根因档案复核两形态是否随版本修复;若修复 → 可评估移除/放宽服务端停摆看门狗并回写部署指南 | Phase 2 验收报告·遗留与偏差记录;`cluster-node-lifecycle` 规格 |
| W6 | 原子变量 GET 读数路径的 Raft 日志膨胀观察(v4 裁决:读亦全量经提交) | GET 条目重放零迁移、状态不腐;单机内嵌基线下 `addAndGet` 与 `get` 吞吐无数量级差(见 `target/benchmark/` 基线报告) | 触发(任一:生产观测 ATOMIC 读/写比显著>1 的热点 key、日志规模或快照时长成为运维压力、用户报读放大) → 另立 change 评估免日志读路径(follower-read / read-index;属 `add-oatomic-long` 设计 D1 明示的 non-goal) | `openspec/changes/add-oatomic-long/design.md` D1;`target/benchmark/` 基线报告 |
| W5 | 发布窗口挂账(定版/Central) | 评审人定夺暂不发布;发布宣告签署位留白属正常状态 | 决定发布时:定版 1.0-SNAPSHOT→release、Maven Central 发布流程、`发布宣告`签署、README Website/release badge/`Not yet published` 表述同步更新、About Website 字段换正式站 | `docs/quality/` 各报告发布宣告节;README「要求」表 |
| W7 | 循环屏障全命令进日志的日志膨胀观察(v5 裁决:到场/离场/动作了结皆入日志) | 屏障相基准无数量级劣化:2 方 8358 世代/s、P50 0.11ms、携 action 同量级(`target/benchmark/` 基线报告);到场频率受 parties 整批天然约束,与 ATOMIC 读放大(W6)同型但暴露面更窄 | 触发(任一:生产观测热点 barrier key 的世代轮转远超预期、该 key 日志规模或快照时长成为运维压力) → 另立 change 评估"等待席位不入日志"的替代路径(判例:锁/Latch 等待队列不入日志;`add-obarrier` 设计风险节已声明按本表流程评估) | `openspec/changes/archive/2026-09-20-add-obarrier/design.md` 风险节;`target/benchmark/benchmark-baseline-2026-09-20.md` |

## 维护规约

- 收口 change 时若产生新的"不阻塞出阶段"观察项,归档前**必须**登记入本表(带记录位置列);
- 本表只增删行、不写长文——细节永远沉淀在"记录位置"列指向的档案里;
- 例行守护(PR 门禁 build+citation-check、benchmark weekly、防假绿断言)不属本表,它们已自动化。
