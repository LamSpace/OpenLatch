# dated 报告迁移动作映射表（任务 3.1 台账）

引用方检索方式：`grep -rl "<文件名>" docs/ README*.md CLAUDE.md openspec/`（含 archive 区）。

| 原 docs/ 路径 | 引用方 | 去向 |
|---|---|---|
| benchmark-baseline-2026-08-29.md | Phase1-验收报告 | `archive/2026-08-29-phase1-audit-remediation/evidence/` |
| benchmark-baseline-2026-09-10.md | Phase3-验收报告、README×2 | `archive/2026-09-10-phase3-release-closure/evidence/`（README 引用由第 5 节重写移除） |
| failover-drill-2026-08-31.md | Phase2 详设、Phase2-验收报告、部署指南 | `archive/2026-08-31-s3-leader-discovery-failover/evidence/` |
| failover-drill-2026-09-05.md | Phase2-验收报告 | `archive/2026-09-06-phase2-release-closure/evidence/` |
| failover-drill-2026-09-06.md | **无引用方** | `git rm`（git 历史保真，可自 `bc15625~` 前史恢复） |
| partition-drill-2026-09-06.md | Phase2 详设、Phase2-验收报告 | `archive/2026-09-06-phase2-release-closure/evidence/` |
| rolling-restart-drill-2026-09-02.md | Phase2 详设、Phase2-验收报告 | `archive/2026-09-03-phase2-s4-snapshot-recovery/evidence/` |
| rolling-restart-drill-2026-09-05.md | Phase2-验收报告 | `archive/2026-09-06-phase2-release-closure/evidence/` |
| rolling-restart-drill-2026-09-06.md | phase2-leader-stall-followup 工件 | `archive/2026-09-06-phase2-leader-stall-followup/evidence/` |
| leader-stall-repro-2026-09-06.md | phase2-leader-stall-followup 工件 | `archive/2026-09-06-phase2-leader-stall-followup/evidence/` |

引用文本处置：docs 内部引用全部为反引号叙述（无 markdown 超链接），按定夺不改写原文，
位置语义由 `docs/README.md`"dated 报告的归宿"一节统一承载；README 双语的超链接在
第 5 节门户重写中整体消失。标准 5 第二轮复核记录（ff34d99）所引 2026-09-12 当日报告
按新政策自始未入库，证据留档评审机 `/tmp/drill-evidence-2026-09-12/formal-b35f549/`。
