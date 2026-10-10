# Proposal

## Why

工程改进第四项（结构化并发）的评估。按第三项（上下文传播）模式落地：先勘察落点、据实校正定位、定义转正触发器，再收敛为一次纯评估的裁决记录。

勘察结论：**库内当前不存在结构化并发的落点（R1 空集）**——需要"单逻辑操作 fork 多个并发子任务、再全部汇合"的形状才谈得上 `StructuredTaskScope` 的价值，而客户端每次调用是单 future、屏障/闩/相位器/定时器的 `register + send` 是刻意有序的两步（先登记本地通知槽再发 RPC，防丢唤醒竞态）、服务端 topic 扇出在 Raft apply 线程内**串行** `enqueue + pump`（并发化会破坏状态机确定性）、复制扇出则封装在 Ratis 内部。同时 `StructuredTaskScope` 在 JDK 25 仍为预览（第五预览 JEP 505），JDK 26 为第六预览（JEP 525）且 API 仍在改名，而本库禁止库产物启用 `--enable-preview`，故**当前不可用、亦无处可用**。

本 change 不实现任何东西，价值在于把上述结论钉成常驻记录、定义解除挂起的触发器，并把该事项从 ROADMAP（主动方向表）移入 WATCHLIST（被动触发表）——与"无落点、纯等外部信号"的定性吻合。

## What Changes

- **定位校正**：本项定性为 **R1（内部采纳，非对外契约）**；勘察结论 = 库内无 `fork + join` 落点（客户端单 future、屏障/闩/相位器/定时器 `register + send` 刻意有序、服务端 topic 扇出在 apply 线程内串行、复制扇出在 Ratis 内）。
- **转正触发器定义**：JDK 某版本 `StructuredTaskScope` 转正（JEP final，不再标注 `@PreviewFeature`）**且** API 在随后的 ≥2 个发布中保持稳定（无改名/增删）——门槛严于单纯"转正"，因 JDK 25→26 之间方法仍在改名。
- **基线代价裁决**：若转正落在 JDK 27，**接受** `maven.compiler.release` 由 25 抬升至 27 的兼容代价。
- **预览禁令裁决**：`--enable-preview` 对已发布库**无例外窗口**，全域禁令保持（库本体不可用，开例外亦兑现不了价值）。
- **与第三项耦合记录**：`EntryClock` 的 ScopedValue 继承面**仅**经 `StructuredTaskScope.fork` 开放，故采纳结构化并发即令第三项的边界矩阵绊线（"域内新建子线程不继承条目时刻"）**按设计翻红**——任何未来落地 change 必须显式对账该绊线。
- **治理迁移**：`WATCHLIST.md` 登记 W16（被动触发器）；`ROADMAP.md` 决策记录节补 2026-10-10 行。路线图表格行的退役与 `ROADMAP.md → DECISIONS.md` 改名**不由本 change 承担**，留给后续独立治理 change。
- 零代码改动、零规格 delta（纯评估/治理，`skip_specs: true`）、零对外 API 面、无 BREAKING。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

（无——本 change 不改变任何规格级行为：纯评估与治理记录，按 OpenSpec 纯重构/文档规则设 `skip_specs: true`。评估结论本身沉淀在本 change 的 `design.md` 与 ROADMAP 决策记录行，不构成对外契约条款。）

## Impact

- **治理/文档**：`WATCHLIST.md`（新增 W16 行）；`ROADMAP.md`（决策记录节补 2026-10-10 行）。
- **代码**：零变化——不触 client/server/core/console/starter/examples 任何源码。
- **规格与协议**：零变化——不触 wire/proto、不升协议版本门、不改任何 specs；`skip_specs: true`。
- **发布面**：零感知——Maven Central 发布链与运行时要求（仍 JDK 25）不变。
