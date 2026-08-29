# Proposal: javadoc-contract-realignment

## Why

全项目 JavaDoc 的**机械完整性**已由构建背书（`maven-javadoc-plugin` 3.12.0，`doclint=all` + `show=private` + `failOnWarnings`，六模块 `javadoc:javadoc` 全绿，含私有成员零缺失）。但六路并行审计（覆盖 65 个主源文件 + 43 个测试文件）合成 **≈73 项质量层发现**：注释与实现的语义漂移集中在「枚举式承诺」句式（「仅…」「一律…」「A/B/C 三类」「XX 时返回 YY」），其中主源码 **5 处 HIGH 为公开/wire 级契约假陈述**——按文档编码的调用方会直接踩坑（如 `OpenLatchException` 承诺"锁丢失 status() 返回 null"而实现携带服务端状态码；`Outcome.DENIED` 前置条件漏掉 FIFO 待重发窗口）。根因是行为演进（phase1-audit-remediation、M4 等）未依 CLAUDE.md §5.2 同步注释，且此类漂移无机械防线。本变更将注释质量拉回与机械层同等的可信度：**只读注释即可懂契约，且契约必为真**。

## What Changes

- **G1 主源码 HIGH 语义失真修复（5 处，共 7 点同族）**：core `DENIED` 前置条件（`Outcome` 常量+类注释、`CoreEngine.acquire` 排队语义、`AcquireCommand.queueIfBusy` 统一措辞）；client `OpenLatchException` status() 枚举如实化；`OpenLatchClient.shutdown()` 尽力释放陈述由"待补齐"改写为现状契约；server `helloResponse` 删除虚构的"仅 OK 携带"路径条件；`ServerSession` 类级补线程模型。
- **G2 `OLock` 契约面补全至 JDK Lock 基准**：`lock`/`tryLock`/`tryLock(t,unit)` 补 `@throws`（`ServerUnavailableException`、`OpenLatchException`、负值 `IllegalArgumentException`）、`tryLockAsync` 负值语义、类级新增「租约与自动续租」段（/3 周期、连续超时判失、持有语义以客户端存活为前提）。
- **G3 线程模型维度补写（会话/连接层 7 类）**：`ServerSessionHandler`、`RequestDispatcher`、`NotifyEventBridge`、`ServerSessionRegistry` 类级 T 段；`ConnectionManager` 锁纪律声明如实化（限定受锁字段清单）+ 类级线程模型段；`HeldLockRegistry` T 维度补全。
- **G4 协议入口方法级契约注释**：`ServerSessionHandler` 四个 Netty 生命周期 override、`NotifyEventBridge.notifyHead`（request_id=0/丢弃分支/可调用线程）、`EnvelopeCodecHandler.exceptionCaught`（触发源边界修正）、`ServerChannelInitializer.initChannel`、`OpenLatchServer.port()` 调用者义务；`markClosed` 注释限定 EventLoop 串行前提（代码 CAS 化不在本变更范围，design 另录待办）。
- **G5 低级失真与引用卫生 + 测试虚假承诺必改**：stale 设计基线引用（`Outcome` REJECT_KEY 比对、`AcquireCommand` 误挂"D3"）、`{@code}` 畸形、衍字病句、"快照/视图""命中/条目"措辞、`LeaseManager` 锁序口径对齐设计 v1.2、`ServerBootstrapFactory` boss=1 归属修正、client `LockType.SIMPLE` 补队列满拒绝分支；测试侧约 12 处注释/名字/类文档对代码的虚报（`ServerSessionTest`"限额不可触发"、`CoreEngineConcurrencyTest` 四不变量、`ClientTestServers`"一律临时端口"、`MessageLegalityTest.channel` 参数目的描述反、`OpenLatchStarterIT` 中断言异常名不符等）一律修复。
- **G6 测试源码私有成员全覆盖补齐**：按已裁定标准（字面 CLAUDE.md §5，废弃归档 D2 测试精简档），42 个测试文件的全部私有字段/方法/构造器/内部 record 补 Javadoc（≈24 点，含 `TestProtocolClient` 私有字段、`TestServers` 私有构造器、core/server 测试套件 `acquire` 工厂与 `setUp`/`tearDown` 等），深度按载体分量取简洁档。
- **G7 验证门**：全量 `mvn -s /home/lam/repo/settings.xml clean verify`（机械层防回归 + 测试不受影响）+ `git diff` 纯注释核验 + 对每条被改写的枚举式承诺做反向对码清单（仿 deepen D4 防失真法）。
- **零行为变化**：仅触碰注释与 Javadoc 标签；无 API、无协议、无运行时语义改动，无 BREAKING。

## Capabilities

### New Capabilities

无。

### Modified Capabilities

无——纯文档整改，不改变任何 spec 级行为；`skip_specs: true`（与归档 javadoc 变更 normalize/deepen 同款裁定：行为不变则规格不变）。

## Impact

- **受影响文件 ≈47 个、修改点 ≈73 处**：
  - core main 5（Outcome、CoreEngine、LeaseManager、SessionRegistry、AcquireCommand）；client main 8（OpenLatchException、OpenLatchClient、ConnectionManager、OLock、RemoteLock、HeldLockRegistry、LockDeniedException、AcquireSpec）；server main 8（ServerSessionHandler、ServerSession、NotifyEventBridge、RequestDispatcher、EnvelopeCodecHandler、ServerChannelInitializer、ServerBootstrapFactory、OpenLatchServer）；starter 1（OpenLatchAspect）；examples 2（WatchdogExample、ExampleServers）。
  - 测试侧：core 6、client 4、protocol 1、server 12、starter 1。
- **不受影响**：代码逻辑、协议、构建配置、`openspec/specs/` 主规格。
- **验证成本**：单轮全量 verify（≈构建期 javadoc 门自动复跑）。
- **评审特征**：diff 应全部为 `/** … */` 与 `// …` 行；任何非注释行改动视为错误。
