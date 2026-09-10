# Proposal: phase3-release-closure

## Why

Phase 3 的四个工作项已全部实现并归档（`phase3-t1-extended-lock-types`、`phase3-t2-metrics`、`phase3-t3-admin-console`、`phase3-t4-security`，P3-01～P3-17），但**阶段收口层缺失**——实现做完了，收口账没做：

1. **无阶段验收报告**：Phase 1/2 均有 `docs/PhaseN-验收报告.md`，Phase 3 没有。T4 的 `t4-acceptance-evidence.md` 只覆盖详设 §8 六项且是单变更视角、散落在 change 目录，不构成阶段级汇总载体（DoD §5.4-2 要求"该阶段验收清单逐项有对应的自动化用例或验收记录"）。
2. **详设未收口**：`docs/OpenLatch-Phase3-详细设计说明书.md` 仍挂 **v1.0 / 待评审**。实现期各章勘误（§2.1 初始定型通道、§3.4 双路径埋点、§4.2/§4.3 管理协议与排序、§5/§10.4 T4）虽已回写正文，但缺版本升版、修订记录与状态改判——Phase 1/2 收口均有此动作（Phase 2 升 v1.5/已验收）。
3. **基准未按 DoD §5.4-5 复核**：最近基线为 `docs/benchmark-baseline-2026-08-29.md`（Phase 1）；Phase 3 引入扩展锁类型与 TLS，属性能相关面，DoD 要求"与上一里程碑对比无显著退化，退化 >20% 需说明原因"，Phase 2 收口报告有基准汇总段。
4. **README 文档面未同步**（DoD §5.4-4）：`README_CN.md`"文档"节只列 Phase 1 两份；"已知局限（Phase 1）"标题与内容未按现状改判（集群/扩展锁/控制台/TLS 已实现，"无集群"等条目已不成立）。
5. **T4 记账未回填**：`archive/2026-09-10-phase3-t4-security/tasks.md` 的 4.6（归档+发布）实际已在提交 `c9b4313` 内完成却未勾选；3.6（集群档认证用例）为显式延后项、措辞含"另立小变更评估"，留作空框。

参照 Phase 2 先例（`phase2-release-closure`），阶段发布需要一个收口 change 把这些逐项闭合成可追溯证据文档，并宣告 Phase 3 发布。

## What Changes

- **新增 `docs/Phase3-验收报告.md`**（仿 Phase 2 结构）：总体计划 §5.3 Phase 3 六项 + 详设 §8 六项逐项闭环（判据 / 证据路径 / 结果），DoD §5.4 五条自检，缺陷与遗留如实记录段，发布宣告尾注。证据以引用汇总（各 change 的 `t*-acceptance-evidence.md`、演练报告、构建记录），不重复造。
- **基准复核**：复跑 `BenchmarkMain` 产出 `docs/benchmark-baseline-<date>.md`（Phase 3 现状），与 2026-08-29 基线逐项对比，>20% 退化写明原因；对比结果入验收报告汇总段。
- **详设收口**（纯文档）：`docs/OpenLatch-Phase3-详细设计说明书.md` 版本 v1.0 → v1.1，状态"待评审" → "已验收"，补修订记录条目（指向实现期各章勘误与本次收口）。不改动已定稿的契约语义。
- **README 双语同步**（纯文档）：文档节补 Phase 2/3 详设与验收报告链接；"已知局限（Phase 1）"按现状改判（Phase 2/3 已交付的集群、扩展锁、控制台、TLS 不再列为局限）。
- **T4 记账回填**：`archive/2026-09-10-phase3-t4-security/tasks.md` 的 4.6 补记归档 commit 后勾选；3.6 改记为"已评估·结构保证 + 单机覆盖，集群夹具另立小变更"，不留空框。
- **集群档认证零副作用夹具（T4 3.6 的实证）不在本变更范围**：拆为后续独立小变更评估（需新建三节点 `OpenLatchServer` 夹具，会引入新的集群测试装配，超出纯收口半径）。本变更在报告与 tasks 中如实标注该口径。

无 **BREAKING**：零产品代码、协议与公开 API 变更；仅文档、证据与 change 记账。默认行为、测试缺省语义逐字节不变。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

（无。本变更为纯阶段收口——验收报告、详设版本/状态回写、README 同步、基准复核与 change 记账，不含任何 spec 级行为变更；`.openspec.yaml` 已设 `skip_specs: true`。）

## Impact

- **新增文档**：`docs/Phase3-验收报告.md`、`docs/benchmark-baseline-<date>.md`。
- **修改文档**：`docs/OpenLatch-Phase3-详细设计说明书.md`（版本/状态/修订记录）、`README.md` / `README_CN.md`（文档节与已知局限节）。
- **change 记账**：`openspec/changes/archive/2026-09-10-phase3-t4-security/tasks.md`。
- **不受影响**：`openlatch-core` / `openlatch-protocol` / `openlatch-server` / `openlatch-client` / `openlatch-spring-boot-starter` / `openlatch-console` / `openlatch-examples` 全部产物与源码；既有测试缺省语义；CI 门禁。
- **环境面**：基准复跑为常规 `mvn` 构建（无特权）；`-Pdrill` 与手工安全走查按仓库纪律留 CI/运维环境（无 TTY 沙箱不可靠）。
