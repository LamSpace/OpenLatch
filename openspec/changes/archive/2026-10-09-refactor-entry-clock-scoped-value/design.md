# Design

## Context

动机与定位修正见 proposal.md - Why，不重复。此处只沉淀影响方案的现状约束（src/main 实测）：

- `EntryClock` 是全仓库唯一真 `ThreadLocal` 用法：`private static final ThreadLocal<Long> APPLY_NOW`，实现 `io.github.lamspace.openlatch.core.Clock`，`nowMs()` = `get()!=null ? get() : System.currentTimeMillis()`。
- 唯一写入点在 `LockStateMachineCore.apply`（第 271–301 行），`synchronized (applyLock)` 内、形状为 `setApplyNow(0) → try { setApplyNow(t); switch(15 arm) } catch (InvalidProtocolBufferException | RuntimeException) → error(...) finally { clearApplyNow(); }`。`setApplyNow(0)` 与 `setApplyNow(t)` 紧邻，中间仅 `entry.getWallClockMs()`（protobuf getter，无时钟读），故 `setApplyNow(0)` 的可观测值为恒被覆盖的死写。
- 读侧全部同线程：`CoreEngine` 十几处 `clock.nowMs()` 均在 `core.apply(...)` 调用栈内；无跨线程读点。
- 各 apply arm（`applySessionOpen`/`applyAcquire`/…）声明 `throws InvalidProtocolBufferException`（受检）——异常面由 `apply` 的单 catch 收口为 `INTERNAL_ERROR` 回执。
- apply 路径**不 fork 线程**：`applyTransaction` 的 observer 在 `core.apply(...)` 之外同步调用；`CompletableFuture.completedFuture(...)` 是已完成 future，无异步派发。
- 基线：JDK 25（`maven.compiler.release=25`），ScopedValue 已转正（JEP 506）；`--enable-preview` 全域禁用，`StructuredTaskScope` 仍预览故不可用。

**关键约束（实测，决定 D4/D5）**：JEP 506 final 版 ScopedValue 的继承面**已收窄为结构化并发**——`src.zip` 中 `java.base/java/lang/ScopedValue.java`（Inheritance 节）明述：绑定"captured when creating a `StructuredTaskScope` and inherited by all threads started in that task scope with the `fork` method"。实测探针（作用域内新建线程）三路皆**不继承**：`Thread.ofPlatform().start`、`Thread.ofVirtual().start`、`new Thread().start`、`newVirtualThreadPerTaskExecutor`。故 **ScopedValue 与 ThreadLocal 在本项目使用面上的传播边界逐格相同**。

## Goals / Non-Goals

**Goals**

- 以有界作用域（作用域退出即还原）替代 `APPLY_NOW` 的 `ThreadLocal` 设/清配对，结构性消除"漏清即泄漏陈旧条目时刻"的隐患面。
- **行为保持**：回放确定性、`Clock` 接口、`CoreEngine`/`SystemClock`、wire/proto、协议版本门、规格全部零变化。
- 把作用域传播边界钉为常驻绊线（防未来改动改变边界或重新引入跨线程泄漏）。

**Non-Goals**（设计级边界，proposal 所列之外）

- 不动客户端：会话身份全程显式传递，无 ambient 需求（评估结论，见 ROADMAP 决策记录）。
- 不改 `Clock` 接口签名、不合并/改写 `SystemClock`。
- 不引入 `StructuredTaskScope`（预览，禁令）——因此**不追求**"作用域内新建子线程继承条目时刻"（JDK 25 下仅结构化并发可达，本项目不可用）。
- 不新增对外 API、指标或配置项；不加规格 delta（纯重构，`skip_specs: true`）。
- 不跑 `BenchmarkMain`：时间源读取不在 SDK 热路径，机制替换测不出显著差异。

## Decisions

**D1 机制：ScopedValue 替代 ThreadLocal。**

`EntryClock` 内部字段改为 `private static final ScopedValue<Long> APPLY_NOW = ScopedValue.newInstance();`，`nowMs()` 判定由 `get()!=null` 改为 `APPLY_NOW.isBound() ? APPLY_NOW.get() : System.currentTimeMillis()`。绑定只能经 `where(KEY, v).call(...)` 的有界区间，作用域退出即还原，`finally clearApplyNow()` 面结构性消失。**替代方案**：(a) 保留 ThreadLocal + 防御性断言——否决，不解决结构性面；(b) 自造"不可变上下文 + 显式捕获"封装——否决，重现轮子且无 JDK 保证；(c) 把 `now` 作为参数显式穿透 `CoreEngine` 十几处——否决，撕动锁语义核心的公开签名面，收益不成比。

**D2 作用域入口形状与异常边界。**

新增静态入口 `withApplyNow(long ms, ApplyOp op)`，其中 `ApplyOp` 为自定义函数接口 `@FunctionalInterface interface ApplyOp { ApplyResult run() throws InvalidProtocolBufferException; }`，两者均**包内可见**（消费者仅同包的 `LockStateMachineCore` 与测试，不扩对外面）。`apply` 的 `switch` 抽为私有 `applyBody(RaftLogEntry, long t) throws InvalidProtocolBufferException`；`apply` 收敛为：

```
synchronized (applyLock) {
    try {
        long t = entry.getWallClockMs();
        return EntryClock.withApplyNow(t, () -> applyBody(entry, t));   // 示意
    } catch (InvalidProtocolBufferException | RuntimeException e) {
        return error("apply failed: type=" + entry.getType(), entry, e);
    }
}
```

`withApplyNow` 内部 `ScopedValue.where(APPLY_NOW, ms).call(op::run)` 的宽 `Exception` 收窄翻译：`InvalidProtocolBufferException`/`RuntimeException` 原样重抛，其余包为 `IllegalStateException`（`ApplyOp` 契约下不可达）。**理由**：catch 留在作用域调用之外，保持既有 `catch (InvalidProtocolBufferException | RuntimeException)` 面**逐字不变**，不扩大到 `Exception`。**替代方案**：(a) catch 移入 lambda——否决，让作用域与错误映射耦合；(b) `withApplyNow` 声明 `throws Exception` + 调用处宽 catch——否决，行为面被动扩大。

**D3 折叠 `setApplyNow(0)`。**

ScopedValue 不可"设后覆写"，单次 `where(t)` 绑定隐含折叠那一步死写，故 `applyBody` 内不再有任何"标记 0"。此折叠是转换的**必然结果**，非顺手清理——design 显式记录。

**D4 传播边界：不变（原设计的"变宽"判断被实测证伪）。**

初版设计据 JEP 506 **预览期**语义判断"作用域内新建子线程会继承条目时刻（语义变宽）"。实测（见 Context）证伪：final 版继承面已收窄至 `StructuredTaskScope.fork`，本项目禁用结构化并发故不可达。**故作用域边界在两种机制下逐格相同**：域内同线程可见条目时刻；域外线程（含域内新建子线程）与作用域退出后的同线程，均回落系统时钟。本项因此是**严格行为保持的纯重构**，不引入任何语义变宽，亦不新增规格条款。

**D5 测试形态：常驻边界矩阵绊线（非红先）。**

行为零变化 ⇒ **没有可红先的锚点**（同断言在旧新实现下同值）。测试策略据实调整为：

1. **边界矩阵**（四格：域内同线程可见 / 域外同线程回落 / 既有及域内新建子线程回落 / 并发作用域互不串扰）作为**常驻回归绊线**——老新实现下皆绿，价值在"未来任何改动改变边界或重新引入跨线程泄漏即红"，与影子修复的 `LockType` 全值矩阵、客户端虚拟线程的钉扎/扇出绊线同哲学；此处矩阵是**防复发**而非一次性验证。
2. **行为保持网**：既有 `StateMachineDeterminismTest`（digest 判据）全量保持绿，是本次替换行为不变的真相源。

**D6 ROADMAP 归属治理。**

第 52 行原措辞"客户端内部会话/追踪上下文传递评估"与实际落点（服务端 apply 时钟）不符。按维护规约，先在"决策记录"节补行固定评估结论（客户端无 ambient 需求；唯一真 ThreadLocal 在服务端 `EntryClock`），再据实校正表格行描述与状态。

## Risks / Trade-offs

- [作用域边界依赖 JDK 版本语义：若未来 JDK 把继承面从结构化并发放宽到普通 `Thread.start`，边界会变宽（届时子线程将读到条目时刻）] → D4 边界矩阵绊线钉死当前逐格行为；`EntryClock` Javadoc 显式声明边界以 JDK 25 语义为准；JDK 升级时该绊线即红，迫使重新评估（正是绊线的价值）。
- [异常收窄翻译若写错会改变错误回执面] → D2：`withApplyNow` 只做 IPBE/RuntimeException 原样重抛 + 兜底包裹；catch 面逐字不变；`StateMachineDeterminismTest` 的"applyFailures 为零"与坏条目回执用例是回归网。
- [`setApplyNow(0)` 折叠若被误读为无关清理] → D3 记录为转换必然结果；apply 期已静态核实无 arm 落在该两行之间。
- [ScopedValue 读路径比 ThreadLocal 稍重？] → 无：无 map 查找，且读取不在瓶颈；不为此跑基准（Non-Goal）。

## Migration Plan

库内机制变更随版本发布，无部署次序、无数据/协议迁移、无快照格式变化。回滚 = 还原 `EntryClock` 字段与 `apply` 入口两处 + 测试，无复制状态影响。`-Pdrill` 不需要（无进程/时序/线程形态变化，判例对齐影子修复）；如需 CI 要求则记入归档过程记录。

## Open Questions

无。作用域入口异常收窄（D2）与传播边界（D4）均为已定决策，不构成可后置未知。
