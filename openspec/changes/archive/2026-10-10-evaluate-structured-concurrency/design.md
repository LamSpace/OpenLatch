# Design

## Context

动机与定位见 proposal.md - Why，不重复。此处只沉淀决定评估结论的现状事实链（本机 JDK 25.0.3 实测 + `src.zip` 核验）。

- **JDK 预览轨迹（硬事实）**：`StructuredTaskScope` 自 JDK 19 的 incubator（JEP 428）起长期未转正，中间经历多轮预览；JDK 25 为**第五预览**（JEP 505，`open()`/`Joiner` 大改），JDK 26 为**第六预览**（JEP 525，`anySuccessfulResultOrThrow → anySuccessfulOrThrow` 改名、新增 `Joiner.onTimeout()`）。本机 `.../25.0.3-oracle/lib/src.zip` 中 `java.util.concurrent.StructuredTaskScope` 仍标注 `@PreviewFeature`（`@since 21`）。**结论：仍未转正，且 API 仍在改名。**
- **发布纪律（硬约束）**：ROADMAP 决策记录"库产物面不得启用 `--enable-preview`"为硬线。`--enable-preview` 需**编译期与运行期同时**开启，对已发布库等于强迫所有下游用户也开 preview——不可兑现，故无例外窗口。
- **与第三项的耦合（实测）**：JEP 506 final 版 ScopedValue 的继承面**仅**经 `StructuredTaskScope.fork` 开放（`java.lang.ScopedValue` Inheritance 节明述）。第三项已据此把关：`EntryClock.APPLY_NOW` 的传播边界与 `ThreadLocal` **逐格相同**，并在 `StateMachineDeterminismTest` 布下边界矩阵绊线（断言"作用域内新建子线程**不**继承条目时刻"）。**采纳结构化并发 = 第一次在某 `withApplyNow` 作用域内 `fork` = 该绊线按设计翻红。**
- **落点勘察（src/main 实测）**：见 D1 表。

## Goals / Non-Goals

**Goals**

- 把"库内无结构化并发落点（R1 空集）"的勘察结论钉成常驻记录。
- 定义解除挂起的**转正触发器**、**基线代价**与**预览禁令裁决**。
- 记录与第三项 `EntryClock` 边界绊线的耦合，供未来落地 change 对账。

**Non-Goals**（设计级边界，proposal 所列之外）

- 不实现任何代码——预览 API 不可用且无落点。
- 不推进 `ROADMAP.md → DECISIONS.md` 改名与路线图表格退役（留给后续独立治理 change）。
- 不把结构化并发定义为对外契约（定性 R1）；双语指南零触碰。
- 不设 JDK 发布盯梢（定时/自动化）——改由 WATCHLIST 被动触发表承担。

## Decisions

**D1 定性 R1 + 落点空集。**
需要"单逻辑操作 fork ≥2 并发子任务、再全部汇合"的形状，`StructuredTaskScope` 才有价值。逐候选勘察（src/main）：

| 候选落点 | fork + join？ | SC 契合度 |
|---|---|---|
| 客户端 RPC（`RequestMultiplexer`） | 否，单 `CompletableFuture` | — |
| 屏障/闩/相位器/定时器 `await`（`register + sendWithId`） | 否，**刻意有序**两步 | — （并发的，防丢唤醒竞态反而引入 bug） |
| 服务端 topic 扇出（`TopicRegistry.publish`） | 否，apply 线程内**串行** `enqueue + pump` | **负**（并发化破坏状态机确定性） |
| 服务端复制（`ReplicationGateway.submit`） | 在 Ratis 内部 | 非我方可动 |
| 测试/基准夹具（`BenchmarkMain` 线程池） | 是 | 边缘（非发布面、非落点） |

结论：**库内当前无落点**。**替代读法 R2（对外承诺"SDK 可用于 StructuredTaskScope 子任务"）**：否决——库内无 fan-out API 面，无从承诺，且沿第二项虚拟线程契约为"调用方线程形态"，结构化并发属调用方自组织，无 SDK 契约可立。

**D2 转正触发器：JEP final 且 API 连续 ≥2 发布稳定。**
门槛严于单纯"转正"——因 JDK 25→26 之间方法仍在改名，刚 final 即采纳等于踩在小改之上。**替代**：仅要求"转正"——否决，门槛过松。

**D3 基线代价：接受 `maven.compiler.release` 25→27 抬升。**
转正落 JDK 27 概率高；抬升属发布策略与用户兼容面决策，用户已裁决接受。

**D4 预览禁令无例外窗口。**
全域禁令保持（见 Context 理由）。**替代**：仅对非发布模块（examples/tests）开 preview——否决，库本体仍不可用，开例外亦兑现不了价值。

**D5 与第三项的耦合：采纳即令边界绊线按设计翻红。**
第三项 `StateMachineDeterminismTest` 的边界矩阵断言"作用域内新建子线程不继承条目时刻"（JEP 506 final 语义：仅 `fork` 继承）。未来落地 change **必须显式对账**：要么接受 `EntryClock` 边界变宽（同步改绊线 + `EntryClock` Javadoc），要么约定"apply 路径永不许 fork"。本 design 与 W16 触发动作列共同承载此约束，防将来撞上。

**D6 治理：登入 WATCHLIST + ROADMAP 决策记录补行。**
该事项定性"无落点、纯等外部信号"，与 WATCHLIST"被动、等触发器、没有预定要做的活儿"天然对齐，故**移出 ROADMAP 主动方向表**。路线图表格行的物理退役与文件改名由后续独立治理 change 承担。

## Risks / Trade-offs

- [被动等待期可能很长（第六预览已至、第七预览在望，转正无期）] → WATCHLIST 被动触发，不投人力。
- [未来 JDK 若把 ScopedValue 继承面从结构化并发放宽到普通 `Thread.start`] → 第三项边界绊线即红（该绊线本就为此而设）；与本项耦合记录互为照应。
- [未来落地 change 忽略与第三项绊线的耦合] → D5 显式记录，W16 动作列要求"落地须对账第三项边界矩阵"。
- [后来者误以为结构化并发"随时可上"] → proposal/design 明写预览轨迹 + 禁令，杜绝误判。

## Migration Plan

纯评估：无部署、无回滚、无数据/协议迁移。裁决随本 change 归档即生效；WATCHLIST/ROADMAP 编辑即治理动作。`-Pdrill` 与基准**不需要**（零运行时变化，判例对齐第三项与影子修复）。

## Open Questions

无。定性（D1）、触发器（D2）、基线代价（D3）、禁令（D4）、耦合（D5）均为已定决策。
