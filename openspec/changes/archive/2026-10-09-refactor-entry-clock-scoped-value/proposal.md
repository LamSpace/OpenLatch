# Proposal

## Why

全仓库唯一的真 `ThreadLocal` 用法是服务端 `EntryClock.APPLY_NOW`——apply 期向应用调用路径注入"条目携带时刻"以保回放确定性。它以 `set`/`try-finally 清值` 配对，忘清即静默把陈旧条目时刻泄漏给该线程的后续读取（无任何报错）。ScopedValue 在 JDK 25 已转正（JEP 506），提供结构性消除：作用域退出即还原，无需 `finally` 纪律。

本项同时修正 ROADMAP 对该项的定位：评估结论为**客户端并无 ambient 上下文需求**——会话身份全程显式传递（`SessionContext` 经 `Route` 捕获 + `Thread.currentThread().threadId()` 显式读），不存在措辞所设想的"线程本地袋"；故落点收敛为服务端唯一真 ThreadLocal 的机制替换，而非客户端会话/追踪上下文改造。

**行为零变化（已实测核验）**：JEP 506 的 final 版中，ScopedValue 绑定**仅**被 `StructuredTaskScope.fork` 派生的线程继承；`Thread.start`/`ofPlatform`/`ofVirtual` 及线程池一律不继承（`StructuredTaskScope` 仍预览、被本项目 `--enable-preview` 禁令排除）。故作用域传播边界在 ThreadLocal 与 ScopedValue 下**逐格相同**（子线程与域外线程均回落系统时钟）——本项是**严格行为保持的纯内部机制替换**。

## What Changes

- `EntryClock` 的 `ThreadLocal<Long> APPLY_NOW` 替换为 `ScopedValue<Long>`；`nowMs()` 的 `get()!=null` 判定改为 `isBound()` 判定，未绑定回落系统时钟——**行为保持**。
- 新增有界作用域入口（`withApplyNow(long, ApplyOp)`），把 `LockStateMachineCore.apply` 的 `setApplyNow(0) → setApplyNow(t) → 15-arm switch → finally clearApplyNow()` 收敛为单次作用域调用；`finally clearApplyNow()` 面与"设值—清值"配对纪律结构性消失。
- 折叠 `setApplyNow(0)` 这一冗余写（紧随其后即被覆盖，中间无时钟读）——ScopedValue 不可"设后覆写"，此为转换的**必然结果**，非顺手清理。
- 传播边界**不变**：作用域外线程（含作用域内新建的子线程）与作用域退出后的同线程均回落物理时钟——与 ThreadLocal 逐格一致；以常驻边界矩阵绊线钉死（防未来改动重新引入跨线程泄漏或改变边界）。
- 无对外 API 变化、无 wire/proto 变化、协议版本门不升。
- **无规格 delta**（纯内部机制替换，行为零变化）：`replicated-state-machine` 等规格逐字不动；按 OpenSpec 纯重构规则设 `skip_specs: true`。

## Impact

- **代码**：`openlatch-server` 的 `raft/EntryClock.java`（机制替换 + 作用域入口）、`raft/LockStateMachineCore.java`（apply 入口收敛 + `applyBody` 抽取 + 注释同步）、`raft/LockStateMachine.java`（类级注释中 "EntryClock 的 thread-local" 一句同步）、`StateMachineDeterminismTest`（边界矩阵 + 陈旧注释同步）。
- **契约面**：`Clock` 接口、`CoreEngine`、`SystemClock` 零改动；对外无 API、无 wire、无快照格式变化；规格零改动。
- **治理/文档**：`ROADMAP.md` 第 52 行归属校正 + "决策记录"节补评估结论行；`EntryClock`/`LockStateMachine` 的 Javadoc 按注释纪律同步（含 `show=private`）；双语指南零触碰（非用户面）。
