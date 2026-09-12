# docs 目录说明

## 分区

| 分区 | 性质 | 内容 |
|---|---|---|
| `design/` | 永久设计产物（内部材料） | 概要设计、Phase 1–3 详细设计说明书、总体实施计划与验证方案、Raft 选型决策报告 |
| `quality/` | 阶段验收快照（内部材料） | Phase 1–3 验收报告（含带日期的复核记录追加） |
| `guide/` | 用户文档（面向外部，中英双语按 `guide/<lang>/` 组织） | 当前：`zh/OpenLatch-Phase2-集群部署与故障转移.md`；体系化指南建设中 |

## dated 运行时报告的归宿（2026-09-12 起）

故障演练与基准报告（`*-drill-<日期>.md`、`leader-stall-repro-<日期>.md`、`benchmark-baseline-<日期>.md`）**不再入库**：

- 本地运行：落各模块 `target/drill-reports/`、仓库根 `target/benchmark/`（`mvn clean` 即清）；
- 常态分布：GitHub Actions 的 drill / benchmark job 以 run artifacts 交付（run 页可下载，保留 30/60 天）；
- 历史证据：`quality/` 验收报告曾以文件名引用（反引号叙述）的 dated 报告，其原件已迁至对应 `openspec/changes/archive/<change>/evidence/` 目录归档——**验收报告与详设修订记录中的原句不改写**，引用处按"文件名 + 日期"陈述，原件位置以归档目录为准；
- `poc/raft-selection/` 已于 2026-09-12 归档退役（tag `archive/raft-selection-poc`），文档中的历史路径叙述以此为准。

验收判据的**关键数字**在各轮复核记录中已全文录入，不依赖原始报告文件即可复核。
