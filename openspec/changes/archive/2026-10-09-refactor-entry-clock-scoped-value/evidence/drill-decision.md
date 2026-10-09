# `-Pdrill` 判定记录

**判定：不需要执行 `-Pdrill` 故障演练。**

## 依据

- 本项为**纯内部机制替换**：`EntryClock.APPLY_NOW` 由 `ThreadLocal<Long>` 改为 `ScopedValue<Long>`，`LockStateMachineCore.apply` 的应用入口把 `set/clear` 配对收敛为有界作用域调用。线程形态**不变**（apply 仍为单条平台线程串行），无进程拓扑、无时序面、无 wire/proto/协议版本门变化。
- 判例对齐 `fix-shadow-sweep-phaser`（纯镜像投影修复，未跑演练）：判由同为"无进程/时序/线程形态面变化"。
- 行为保持由既有回放确定性套件兜底：`StateMachineDeterminismTest` 全量（含"同一序列两次回放逐字段一致""回放与物理时钟无关""并发应用互不串扰"）在替换前后均绿——时间源的任何行为漂移都会经复制状态摘要 `ShadowTable.digest()` 翻红。

## 结论

演练对本项不提供额外信号，故不执行；如需 CI 强制，可后续按需补跑单轮次并在此登记。
