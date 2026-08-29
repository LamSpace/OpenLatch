# Ratis 接入摩擦日志（P2-02，详设 §2.3 第 5 项）

版本：ratis-server / ratis-grpc / ratis-client / ratis-metrics-default **3.2.2**，JDK 25.0.3。
胶水 LOC：适配器 321 行（含注释）；共享逻辑均在 harness（两库对等）。

## API 侵入 / 文档缺口（按遭遇顺序）

1. **metrics 实现是独立构件**：仅引 `ratis-server` 时启动即 `ClassNotFoundException: MetricRegistriesImpl`。文档未突出，必须在依赖表显式加 `ratis-metrics-default`。
2. **`Message.valueOf` 无 ByteBuffer 重载**（3.2.2）：只有 shaded-ByteString / String / AbstractMessage。第一反应写 `Message.valueOf(ByteBuffer)` 编译失败。
3. **proto 命名反直觉**：条目载荷是 `LogEntryProto.getStateMachineLogEntry().getLogData()`——`getData()`/`getLogEntryType()` 不存在（oneof 用 `hasStateMachineLogEntry()` 判别）。
4. **重启语义**：单组 `setGroup` 默认 `StartupOption=FORMAT`，重启非空目录抛 `Failed to FORMAT`；需自行按目录存在性选 `RECOVER/FORMAT`。且 `StartupOption` 枚举**没有 CREATE**（与 javadoc 惯例直觉不符）。
5. **`RaftGroupId.toString()` 带 `#` 前缀**，落盘目录名是裸 UUID——存在性判断要用 `getUuid().toString()`（第一次踩中导致重启失败）。
6. **快照临时文件 API 不可见**：`getTmpSnapshotFile` 是 `protected`，状态机侧只能自造 tmp + 原子 rename 才能满足 Ratis 的 `*.tmp` 忽略约定。
7. **follower 手动快照无公开入口**：`SnapshotManagementApi.create` 面向 leader；follower 本地触发要 `server.getDivision(gid).getStateMachine()` 下转型直调 `takeSnapshot()`。
8. **同 ClientId 串行排队**：leader 对单一客户端的在途请求顺序处理，bulk 阶段吞吐被钉在 ~500/s，与复制带宽无关——必须多 `RaftClient` 实例池（本 PoC 用 8）才能测真实批量追赶能力。API 无提示。
9. **io() 与 async() 回执语义**：`io().send` 阻塞至 apply 完成，线程占用与 JRaft 异步链不对等，改 `async().send`（属测量公平性调整，两侧同步记录）。

## 依赖共存（§2.2 高权重维度）

- **protobuf / Netty / gRPC 全部经 `ratis-thirdparty-misc` 着色**：与主干 protobuf 3.25.5、netty-bom 4.1.137 **零类路径交集**（dependency:tree 验证）。这是 Ratis 的显著加分项——主干 server 升级 Netty 不会波及 Raft 层，反之亦然。
- 传递依赖极小（thirdparty + commons 系）。

## Java 25 兼容

- 冒烟/杀主/5k 与 100k 快照全绿；无 `--add-opens` 需求。
- 唯一的 Unsafe 弃用告警来自主干自己的 protobuf-java，与 Ratis 无关。

## 状态机集成方式（§2.2 高权重维度）

- `BaseStateMachine` + `SimpleStateMachineStorage` 即达"外部状态机 + 日志回放"形态，`CoreEngine` 零改动（EntryClock 注入成立，applier 单线程语义符合预期）。
- 快照截断回放依赖 `getLatestSnapshot()` 正确接线，PoC 以 REBUILD 计数 + 追赶耗时验证了链路（victim 重启 `rebuilds=1, failures=0`）。
