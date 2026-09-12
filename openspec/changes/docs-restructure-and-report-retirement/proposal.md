# Proposal: docs-restructure-and-report-retirement

## Why

`docs/` 现状 21 个文件平铺，混居三种性质的内容与一个例外：永久设计产物（概要设计、Phase1–3 详设、实施计划）、阶段验收快照（三份验收报告）、测试持续追加的 dated 证据工件（10 个演练/基准报告），以及唯一一篇用户向部署指南。杂乱根因是"测试把仓库当报告汇"——四个演练 IT 按设计把报告 append 到仓库根 `docs/`，`BenchmarkMain` 默认写 `docs/benchmark-baseline-<date>.md`：每跑一次 `-Pdrill` 或基准就脏一次工作树，文件只增不减；README 基准段直链 dated 文件，门户内容绑在速朽证据上。评审定夺：**过时报告不再入库**（切断增殖，历史证据迁 openspec 归档，常态报告归 CI artifacts）、**poc/ 归档退役**（方案 C：tag 锚定 + 工作树删除；选型已定案，PoC 仅剩审计价值且顶层无 README 致意歧义）、**README 重写**——其头部仍以"Phase 1 / MVP"定位并逐项声明"不含集群/FairLock/Semaphore/CountDownLatch/控制台/TLS/认证"，Phase 2 提示停留在"S4 进行中"，全部与 Phase 3 收口的实际交付矛盾；"Restart = full release"对集群+快照部署已是误导表述；无 badge、无架构图、无兼容性矩阵。Spring Boot 兼容性须诚实声明：当前 starter 依赖 Boot 4 独有的 `spring-boot-starter-aspectj`、以 Boot 4.0.3 BOM 编译，且全库 `release=25`——仅支持 Boot 4.x + Java 25；评审定夺 Boot 3 暂不立项，README 明说现状并引导 Boot 3 用户手动装配。

## What Changes

- docs 三分重组（`git mv` 保历史，全文引用路径修正）：`docs/design/` ← 概要设计、Phase1–3 详设、总体实施计划与验证方案、raft-selection-report（选型结论是活的架构决策记录，非"过时报告"）；`docs/quality/` ← 三份验收报告；`docs/guide/` ← 用户向（本变更仅将《Phase2 集群部署与故障转移》移入 `docs/guide/zh/`，体系化建设属 bilingual-user-guide）；新增 `docs/README.md` 说明三区定位与 dated 报告归档政策。
- 报告退出入库：4 个演练 IT（LeaderKill/Partition/RollingRestart/LeaderStallRepro）报告落点 `../docs/` → `../target/drill-reports/`（同日单文件追加命名保留；`-D` 覆盖系统属性保留；drill-logs 本就在 target——口径拉齐）；`BenchmarkMain` 默认输出同迁；`git rm` `docs/` 下 10 个 dated 报告，其中被验收报告/详设引用的按引用方迁至对应 `openspec/changes/archive/<change>/evidence/`（先例：s4-exit-checklist、observations-* 已在归档目录承载证据），引用文本路径改写至归档位（链接改指可点处，事实叙述不动）。
- poc 归档退役：`git tag archive/raft-selection-poc`（钉住 poc 完整存在的最后提交）→ `git rm -r poc/`；`raft-selection-report.md`"证据可复现"节改写为"checkout 该 tag 后 `summarize.py` 可复现"；REMOVED 主规格 capability `raft-selection-poc`（整条退役，历史经归档保留）。
- README 重写（EN/CN 对称）：定位更新为 Phase 3 收口全貌（Raft 集群、FairLock/Semaphore/CountDownLatch、管理控制台、TLS/mTLS、Token 认证、协议 v3）；新增 badge 行（按 ci-github-actions 最终 workflow 名）、架构图、兼容性矩阵（Java 25；Spring Boot 4.x only + Boot 3 手动装配指引；协议 v1/v2/v3 协商与"不做隐式兼容"）；修正"Restart = full release"为单机/集群分列表述（集群经快照恢复）；Known Limitations 与 Semantics 章节语义保留；Benchmark 段改为"本地跑 `BenchmarkMain` 生成 + 最新常态结果见 CI artifacts"；Documentation 索引指向 `docs/guide/`（用户）与 `docs/design/`（明示内部材料）。
- 非目标：用户指南内容扩充（bilingual-user-guide）；注释与 pom 引用清理（javadoc-internal-citation-cleanup）；workflow 实体（ci-github-actions）。

## Capabilities

### New Capabilities

- `documentation-structure`: docs/ 三分区与内容归宿；运行时报告不落入库的落点纪律（演练/基准 → `target/` + CI artifacts）；历史 dated 证据的 openspec 归档迁移；README 门户正确性（现状定位、兼容性诚实声明、双语对称）；poc 归档与选型证据的 tag 锚定。

### Removed Capabilities

- `raft-selection-poc`: 选型 PoC 随 `poc/` 退役出工作树；结论存于 `docs/design/raft-selection-report.md`，原始数据经 tag `archive/raft-selection-poc` 锚定可复现。

## Impact

- 文件：docs/ 约 11 个 `git mv` + 10 个 `git rm`/迁移 + 新增 `docs/README.md`；`poc/` 整目录删除 + 1 tag；4 个 DrillIT + BenchmarkMain 各一处落点常量与类级 Javadoc 文字修改（报告路径属副作用输出，断言逻辑零触碰）；README EN/CN 全文重写；验收报告/详设中指向旧路径的链接文字修正。
- 验证：`-pl openlatch-client,openlatch-server -am test-compile` 过 + 全量 `clean verify` 绿；落点端到端（`target/drill-reports/` 实际产出）由评审人桌面 `-Pdrill` 手工复跑与 CI drill job 首跑双通道背书（记为退出判据，本环境不假装完成）；markdown 死链检查（引用旧 `docs/xxx.md` 路径全仓 grep 清零）。
- 依赖与顺序：报告路径文本与 ①（保留 docs 路径句）及 ②（drill artifact 上传路径）需对账，本变更落地后 ② 的 artifact path 按新落点更新；badge URL 依赖 ② 的 workflow 定名；`docs/guide/zh/` 骨架被 ④（bilingual-user-guide）扩充分承。
