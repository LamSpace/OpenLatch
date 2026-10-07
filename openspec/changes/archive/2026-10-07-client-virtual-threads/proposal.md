# Proposal

## Why

演进路线"工程改进"第二项（客户端虚拟线程化）的可行性前提已全部就位：`maven.compiler.release=25` 且三条 CI 工作流均为 JDK 25，JEP 491（JDK 24 转正）消灭了 "synchronized 块内阻塞钉死载体线程"，而客户端调用线程路径（protobuf 编解码 + `CompletableFuture.get` + `Thread.sleep` 轮询 + 非阻塞 `writeAndFlush`）零 JNI——即阻塞 API 桥接面在虚拟线程下已无钉扎源。但这一亲和性目前只是"事实成立"，没有契约、没有测试、没有文档；同时 OTopic 每订阅一条平台线程派发线程是全 SDK 唯一"按逻辑实体 1:1 消耗平台线程"的形态，扇出规模下即资源线性膨胀。本 change 把事实钉成契约、把唯一热点改为虚拟线程。

## What Changes

- OTopic 订阅派发线程由平台线程改为**虚拟线程**（每订阅一条，串行回调承诺与"不占用 Netty EventLoop"承诺逐项不变）——本 change 唯一的运行时代码改动点。
- client-sdk 规格新增**虚拟线程亲和**要求：调用方阻塞 API（`lock`/`acquire`/`await`/队列与原子操作族）可被虚拟线程安全高效调用；线程归属键、中断语义、失锁回调执行器、EventLoop/共享定时器线程形态的保持性承诺。
- 新增 `ClientVirtualThreadsIT`：1 万虚拟线程等待者规模见证（同规模平台线程必 OOM，虚拟线程形态为存在性证明）、归属矩阵、中断语义对齐、OTopic 扇出下平台线程数绊线、JFR `jdk.VirtualThreadPinned` 事件零断言（自包含，不依赖 CI 日志抓取）。
- 全量 `clean verify` 回归（既有客户端 40+ 测试文件为回归基线）+ `BenchmarkMain` 改动前后对比 + `-Pdrill` 演练一轮（用户裁决：跑）。
- 双语用户指南 03（虚拟线程小节 + OTopic 派发线程措辞）、10（兼容性：虚拟线程列为受支持用法面）与相关契约 Javadoc 同步；ROADMAP 该项状态行进退。
- 明确 Non-Goal：Netty EventLoop 与共享 `HashedWheelTimer` 保持平台线程（单条、精度、零收益不动）；锁丢失回调执行器保持专用单线程平台线程（序列化契约已入规格与指南，改虚拟只赔契约稳定）；不引入 `ScopedValue`（后续独立项）与 `StructuredTaskScope`（仍预览，发布禁令线）；调用方桥接形态不改全异步。
- 零新 wire 类型、零协议版本门升级、零公开 API 签名变化、无 BREAKING。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `client-sdk`: 新增"虚拟线程亲和"要求——调用方阻塞 API 的虚拟线程安全/效率承诺、线程归属键与中断语义的形态无关性、OTopic 交付派发线程虚拟线程化（串行性不变、平台线程数不随订阅数增长）、EventLoop/共享定时器/失锁回调执行器保持平台线程形态。既有 OTopic API 条款（"SDK dispatcher 线程、MUST NOT 占用 Netty EventLoop"）与新形态相容，不触发 MODIFIED。

## Impact

- 代码：`openlatch-client`（`RemoteTopic.Subscription` 派发线程构造一处；相关 Javadoc——`OpenLatchClient` 类级线程模型概括、`OTopic`/`RemoteTopic` 交付线程描述）。
- 测试：`openlatch-client/src/test` 新增 `ClientVirtualThreadsIT`；既有 IT/单测全量回归；`-Pdrill` 演练；`BenchmarkMain`（examples 模块）前后对比。
- 规格与文档：`openspec/specs/client-sdk`（归档时 sync 新需求）；`docs/guide/{zh,en}/03-client-sdk.md`、`docs/guide/{zh,en}/10-compatibility.md`；`ROADMAP.md` 工程改进表第二行状态。
- 依赖与协议：零变化——不触 wire/proto/服务端/core/console/starter；Maven Central 发布链的运行时要求不变（仍 JDK 25）。
