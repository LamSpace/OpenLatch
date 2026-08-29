# SOFAJRaft 接入摩擦日志（P2-03，详设 §2.3 第 5 项）

版本：**jraft-core 1.4.1**（bolt 1.6.12），JDK 25.0.3。
胶水 LOC：适配器 306 行（含注释）。

## API 侵入 / 文档缺口（按遭遇顺序）

1. **`RouteTable.init()` 已消失**：1.4.1 只有 `RouteTable.getInstance()`；网上样例与官方文档示例大量停留在旧签名（编译期踩）。
2. **RocksDB 存储把 `file://` 前缀当字面路径**：`setLogUri("file:///...")` 报 `mkdir file:///...: 没有那个文件或目录`——文档示例两种写法混用，实际必须裸绝对路径（meta/snapshot 同理）。
3. **`Node#snapshotSync` 禁止从状态机回调外调用**（`IllegalStateException: You can't trigger snapshot synchronously out of StateMachine's callback methods`）：外部手动触发只能走 `CliService.snapshot`，且是异步 RPC——需自行轮询落盘文件出现才能确认完成。
4. **`NodeOptions` 无 `setSnapshotThreshold`**：1.4.1 的自动快照触发口径与 1.3 文档不符（仅剩 `snapshotIntervalSecs`/`snapshotLogIndexMargin`）。PoC 因此不依赖各库自动快照，两候选统一"手动全节点 SNAP + delta 追赶"对称流程。
5. **`CliService` 实现类命名混乱**：`rpc.impl.cli.CliClientServiceImpl` 是 RPC client 不是服务；真实现为 `core.CliServiceImpl`。
6. **`Status.ok()` 实为 `isOk()`**；`SnapshotWriter.getPath()` 挂在基类 `Snapshot`（grep 方法表要查父类）。
7. **Task 无 `setPeerId`**（1.4.x 精简），leader 提案只能本地 `node.apply`。
8. **无 `bootstrapPeers`**：1.4.1 的 `CliService` 不提供该入口；静态 `initialConf` 组网可用（PoC 采用）。

## 依赖共存（§2.2 高权重维度）

- **protobuf 不着色**：jraft-core 1.4.1 编译期 pin **3.25.5，与主干当前 pin 恰好一致**（本次幸运）；但主干任何 protobuf 升级都会直接波及 Raft 层——结构性共存风险高于 Ratis 的 thirdparty 方案。
- **bolt 依赖非着色的 `io.netty:netty-all`**：被主干 netty-bom 统一顶到 4.1.137（高于 bolt 自身编译版本）；PoC 实测可用，但 io_uring 原生库加载在 Java 25 触发 restricted-method 告警（需 `--enable-native-access=ALL-UNNAMED` 消除，未消除也不影响运行）。
- 传递足迹明显更大：rocksdbjni 8.8.1 + jna 5.14 + hessian + disruptor + metrics-core + commons-*（主干若引入 JRaft，SBOM 与 fat-jar 体积都要评估）。

## Java 25 兼容

- 冒烟/杀主/5k 快照全绿，无启动阻塞项；仅上述 native-access 告警。

## 状态机集成方式（§2.2 高权重维度）

- `StateMachine` 接口 + `StateMachineAdapter` 即"外部状态机 + 日志回放"，`CoreEngine` 零改动（FSMCaller 单线程 apply 语义符合预期）。
- 快照 onSnapshotSave/onSnapshotLoad 直接拿 `writer.getPath()` 目录写自定义二进制文件，集成心智负担低于 Ratis 的 storage 接线。
