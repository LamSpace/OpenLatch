# Tasks

## 1. 夹具：作用域边界矩阵（常驻回归绊线）

- [x] 1.1 作用域边界矩阵四格（置入 `StateMachineDeterminismTest`）：① 应用作用域内同线程读到条目携带时刻；② 作用域退出后同线程回落系统时钟、不残留陈旧时刻；③ 既有（非派生）线程回落系统时钟；④ 两线程各自作用域以不同携带时刻应用序列互不串扰、摘要与串行执行一致——验证：**现行 ThreadLocal 实现**下四格全绿（行为保持证据）
- [x] 1.2 新增"域内新建子线程**不**继承条目时刻"格：作用域内 `Thread.ofPlatform().start(...)` 读取时间源，断言读到系统时钟（JEP 506 final 语义：ScopedValue 仅经 `StructuredTaskScope.fork` 继承，本项目禁用结构化并发故不可达）——验证：两种机制下该格皆绿、逐格一致（**无红先锚点**：行为零变化，纯行为保持；此格修改前的"继承"断言为错误前提，已按实测证伪反转）

## 2. 实现：EntryClock 机制替换（含 Javadoc 同步）

- [x] 2.1 `EntryClock`：`APPLY_NOW` 由 `ThreadLocal<Long>` 改 `ScopedValue<Long>`；`nowMs()` 判定改 `isBound()`；新增包内 `ApplyOp` 函数接口与 `withApplyNow(long, ApplyOp)`（`ScopedValue.where(...).call(...)` 的宽 `Exception` 收窄：IPBE/RuntimeException 原样重抛、其余包 `IllegalStateException`）；删除 `setApplyNow`/`clearApplyNow`——验证：编译通过，任务 1.1/1.2 全绿
- [x] 2.2 `LockStateMachineCore.apply`：`switch` 抽为私有 `applyBody(RaftLogEntry, long t) throws InvalidProtocolBufferException`；入口收敛为 `try { return EntryClock.withApplyNow(t, () -> applyBody(entry, t)); } catch (InvalidProtocolBufferException | RuntimeException e) { return error(...); }`，catch 面**逐字不变**，`finally clearApplyNow()` 与 `setApplyNow(0)` 死写随转换消失——验证：`StateMachineDeterminismTest` 全量绿（含 `applyFailures` 为零、坏条目回执）
- [x] 2.3 `StateMachineDeterminismTest` 收尾：`inApplyScope` 辅助方法适配 `withApplyNow`；`concurrentAppliesDoNotCrossLeakEntryClock` 陈旧注释（"thread-local"）同步为"有界作用域"。**注**：原任务点名的该用例实际不碰 setter（仅走 `replay()` 摘要），确认无需重构即编译；真正耦合 setter 的 `entryClockFallsBackToSystemClockOutsideApply` 已由 1.1 边界矩阵取代——验证：全量绿
- [x] 2.4 `LockStateMachine` 类级注释中 "`EntryClock` 的 thread-local" 一句同步为有界作用域措辞——验证：只读注释即可懂契约，`mvn -s /home/lam/repo/settings.xml -pl openlatch-server javadoc:javadoc` 无缺失报错（`EntryClock` 类级/方法级 Javadoc 已在 2.1 内完成）

## 3. 治理：产物收窄为纯重构与 ROADMAP 归属校正

- [x] 3.1 change 产物收窄：`proposal.md` 去 `Capabilities` 节、补"无规格 delta"、纠正传播边界措辞；`design.md` D4/D5 改写为"边界不变、无红先"；删除 `specs/` delta；`.openspec.yaml` 置 `skip_specs: true`——验证：`openspec validate --strict` 通过
- [x] 3.2 `ROADMAP.md`：先在"决策记录"节补评估结论行（客户端无 ambient 上下文需求、会话身份全程显式传递；全仓库唯一真 `ThreadLocal` 在服务端 `EntryClock`，本项以其替代兑现 ScopedValue 采用），再据实校正工程改进第 52 行描述与状态——验证：决策行先于表格改动（维护规约次序），措辞只读自洽、内部代号零引用
- [x] 3.3 提交前 `bash scripts/check-source-citations.sh` 自查——验证：脚本零命中（非零退出即逐 `file:line` 修正至零）

## 4. 集成验证

- [x] 4.1 `mvn -s /home/lam/repo/settings.xml clean verify` 全模块——验证：7/7 模块 BUILD SUCCESS、0 失败 0 错误（唯一既有环境门槛跳过项与本项无关；首轮 client 侧 surefire fork 启动瞬时错误经复跑证伪为环境性 flake）
- [x] 4.2 `-Pdrill` 判定并留痕：本项无进程/时序/线程形态变化（判例对齐影子修复），判**不需要**并记因入归档过程记录——验证：判定写入归档证据/过程记录（`evidence/drill-decision.md`）
- [x] 4.3 归档 change 至 `openspec/changes/archive/<date>-refactor-entry-clock-scoped-value/` 并回填 ROADMAP 状态为 `已落地`+归档链接——验证：归档目录存在、ROADMAP 链接可点
