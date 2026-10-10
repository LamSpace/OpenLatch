# Proposal

## Why

工程改进第四项评估（`evaluate-structured-concurrency`，已落地）之后，`ROADMAP.md` 已无任何"未启动/进行中/挂起"的活跃演进方向——一档/二档/三档原语与工程改进前三项全部"已落地"，第四项（结构化并发）转入 WATCHLIST（W16）。该文件作为"演进路线"的使命已完成。

但它身兼的另一职是**活的治理资产**：**决策记录（裁决的单一事实源）+ 冻结的不做清单 + 每原语落地纪律**，且被 durable spec 规范性引用——`openspec/specs/replicated-state-machine/spec.md` 四条常驻守卫均写着"改动本条款 MUST 先经 ROADMAP 决策记录登记"。故不能物理删除，而应**改旗易帜**：`ROADMAP.md → DECISIONS.md`，剥离已死的演进路线表、保留活的治理内容，并改写指向它的活引用。

## What Changes

- **文件改名**：`ROADMAP.md` → `DECISIONS.md`（用 `git mv` 保留文件历史）。
- **剥离死表**：删除"一档/二档/三档/工程改进"四张表（全部"已落地"或已退役）与"状态图例"节；**保留**决策记录、不做清单、每原语落地纪律、维护规约。
- **改写文件头**：定位由"演进路线（Roadmap）"改为"决策账本（Decision Ledger）"；维护规约中"本表/表格"措辞与"与 WATCHLIST 分工"句据实校正（分工句随路线图表移除而失效）。
- **改写活引用**：`openspec/specs/replicated-state-machine/spec.md` **×4** 条守卫（"MUST 先经 ROADMAP 决策记录登记" → 指向 `DECISIONS.md`）；`WATCHLIST.md` **W5**（×2 处）。
- **不动历史引用**：约 30 个归档 change 的 ROADMAP 引用保持原样（记当时为真）。
- 零代码改动、零对外 API 面、**无 BREAKING**。
- **规格 delta：有**——`replicated-state-machine` 四条守卫条款文本 MODIFIED（引用目标更名）；本 change **非** `skip_specs`。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `replicated-state-machine`: 四条常驻守卫条款（`topic 零复制日志边界` / `条件复制边界——await 折叠既有条目、signal 家族零日志` / `phaser 操作复制边界与重放确定性` / `timer 操作复制边界与重放确定性`）正文中的规范性引用由"改动本条款 MUST 先经 ROADMAP 决策记录登记"更新为指向 `DECISIONS.md`。断言的编号项与全部 `#### Scenario` **逐字不变**——仅治理文档的引用目标更名，无行为面变化。

## Impact

- **文档/治理**：`ROADMAP.md → DECISIONS.md`（改名 + 剥离死表 + 文件头/维护规约校正）；`WATCHLIST.md` W5 两处引用。
- **规格**：`openspec/specs/replicated-state-machine/spec.md` ×4 条 MODIFIED（delta 见 `specs/`）；归档时 sync。
- **代码/协议/发布面**：零变化；不改 client/server/core/console/starter/examples 任何源码，不触 wire/proto，不升协议版本门。
