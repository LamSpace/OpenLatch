# 钉扎观测窗与预热轮判例（任务 3.2）

- 日期：2026-10-07
- 现象：初版观测窗（无预热）读到 10 次 `jdk.VirtualThreadPinned` 事件。
- 事件栈归因（JFR 事件原文）——全部为首触类初始化伪影，与 SDK 桥接无关：
  - `pinnedReason = "VM call to io.github.lamspace.openlatch.protocol.AtomicOpRequest.<clinit> on stack"`
  - `pinnedReason = "Waited for initialization of ...BarrierAwaitRequest by another thread"`（`blockingOperation = "Object.wait"`）
  - `pinnedReason = "Freeze or preempt failed (2)"`（`BuiltinClassLoader.loadClassOrNull` 于栈上）
  - 时长 0.004 ms – 0.86 ms（远低于 JFR 默认 20ms 阈值，故需 `withoutThreshold()`）
- 判定：虚拟线程执行 `<clinit>` 期间不可卸载（JVM 语义），首次装载 protobuf 生成类必然产生该事件；桥接等待本体（`future.get`/`Thread.sleep`/`writeAndFlush`）零事件——JEP 491"monitored 区阻塞不钉扎"结论获实测坐实。
- 处置：观测窗前加同负载预热轮（键前缀 `warm`，完成类初始化），窗口轮（前缀 `run`）只测稳态；稳态若仍有事件即红并附事件栈原文（诊断输出常驻，判读成本=0）。
- 预热轮失败同样视为红（非静默跳过）。
