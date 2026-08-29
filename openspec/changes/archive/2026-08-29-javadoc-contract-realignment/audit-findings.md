# 审计取证清单（apply 阶段逐条复核用）

来源：2026-08-29 六路并行审计（每路代理逐文件全文精读 + 对码核验 + javadoc↔proto↔设计说明书三方对照）。
编号前缀：`cc` core 主源码；`cl` client 主源码；`sv` server 主源码 + ProtocolCodecTest；`st` starter+examples 主源码；`te-cc` client+core 测试；`te-sv` server+starter 测试；`x` 跨模块交叉核验。
**注**：所有行号为审计时点快照，实施时按 tasks.md 纪律以代码为准复核。机械层（doclint+show=private）零缺失为本次 BUILD SUCCESS 背书结论，不在本清单内。

## cc｜openlatch-core/src/main（7 项）

| # | 位置 | 类别/严重度 | 失真/缺口 | 修复指向 |
|---|---|---|---|---|
| cc-F1 | `result/Outcome.java:35`（+类注释 :25） | WRONG/HIGH | DENIED 写"锁被占用且立即式"，实际 `waiters` 非空（队首已通知待重发窗口）亦 DENIED（`LockEntry:160-162,190`） | design D2 canonical 句 |
| cc-F2 | `CoreEngine.java:223,259` | SHALLOW-METHOD/MED | release/renew"判定顺序穷尽"未列 null-key → `CHM.get(null)` NPE 的调用者义务 | 补"key 须非 null（协议层校验），否则 NPE" |
| cc-F3 | `CoreEngine.java:156` | WRONG/MED | acquire 排队语义与 cc-F1 同源失真 | canonical 同文 |
| cc-F4 | `Outcome.java:20-21` | WRONG/LOW | 类注释比对"§4.2 单值 REJECT_KEY"基线已不存在（设计 v1.2 已列双值） | 改指现文或删对比句 |
| cc-F5 | `lease/LeaseManager.java:25` | WRONG/LOW | "独立于条目锁/无交叉持锁"未涵盖条目锁→堆锁嵌套（`CoreEngine:196,274`），落后设计 §4.9.3 v1.2 | 补锁序不成环一句 |
| cc-F6 | `session/SessionRegistry.java:56` | WRONG/LOW | "与 remove 在同一 key 原子互斥"之 key 二义（实为 map 表项=sessionId，非锁键） | 改"同一 sessionId 表项" |
| cc-F7 | `command/AcquireCommand.java:31` | WRONG/LOW | "（见设计说明书 D3）"误挂——详设无 D 编号，实为 m1 design.md D3 | 改出处 |

核实为真（不修复）：CoreConfig 默认值==§5.7、夹取顺序、acquire/release/renew 判定顺序、规则 3-7 内联编号、expireDue 陈旧校验、HeapEntry tie-break、AWAIT_NOTIFY 映射等全量核验通过。

## cl｜openlatch-client/src/main（11 项）

| # | 位置 | 类别/严重度 | 失真/缺口 | 修复指向 |
|---|---|---|---|---|
| cl-F1 | `OpenLatchException.java:26-27` | WRONG/HIGH | "锁丢失不携带状态码 status() 返回 null"被 `Watchdog:185`、`RemoteLock:141`（携带 INVALID_TOKEN/NOT_HELD/SESSION_EXPIRED）证伪 | design D2 canonical |
| cl-F2 | `OpenLatchClient.java:279` | WRONG/HIGH | shutdown"尽力释放待补齐"——`releaseAllHeldBestEffort()`（:285/:363-383）已实现生效 | canonical 现状句 |
| cl-F3 | `internal/ConnectionManager.java:105-106` | WRONG/MED | "全部状态字段读写在此锁内"被 :257-259/:266-268/:410/:414 锁外访问证伪；类级无 T 段 | D4 锁纪律如实化+T 段 |
| cl-F4 | `OLock.java:58,65,75` | MISSING/MED | 三获取方法未枚举 ServerUnavailableException（`OpenLatchClient:196-198`→`RemoteLock:239-241`）与 OpenLatchException/超时上抛 | design D3 清单 |
| cl-F5 | `OLock.java:67-75` | MISSING/MED | 负 waitTime→IAE（`RemoteLock:114-116`）未声明；tryLockAsync 负值（`AcquireSpec:48-50`）未记 | design D3 |
| cl-F6 | `OLock.java:22-41` | SHALLOW-CLASS/MED | 缺租约/自动续租维度（/3 周期、连续 2 超时判失、持有以进程存活为前提），"只读注释即可懂契约"不达标 | design D3 |
| cl-F7 | `internal/HeldLockRegistry.java:297-298` | WRONG/LOW | "不可变快照"实为 `unmodifiableCollection` 弱一致只读视图（:303） | 改"只读视图（弱一致遍历）" |
| cl-F8 | `internal/LockDeniedException.java:25-27` | WRONG/LOW | "acquireAsync 转换为 OpenLatchException"不实——原样以本异常完成（`AwaitTracker:335`，构造器自带 DENIED） | 改"原样收到" |
| cl-F9 | `RemoteLock.java:32` | WRONG/LOW | `{@code{{@code tryLock}}` 双括号畸形 | 修标签 |
| cl-F10 | `OLock.java:27`（+`RemoteLock.java:82`） | WRONG/LOW | "以待等待总超时"衍字病句 | 两处统一"以等待总超时" |
| cl-F11 | `AcquireSpec.java:40` | MISSING/LOW | 校验清单漏 lockType requireNonNull（:44） | 补"锁类型非空" |

核实为真：Watchdog /3 与连续 2 阈值、superseded 让位、孤儿归还三时序、±20% 抖动、requestId 起 1、ClientConfig 默认值==§6.7 等逐项吻合。

## sv｜openlatch-server/src/main + ProtocolCodecTest（15 项）

| # | 位置 | 类别/严重度 | 失真/缺口 | 修复指向 |
|---|---|---|---|---|
| sv-F1 | `net/ServerSessionHandler.java:201` | WRONG/HIGH | helloResponse"OK 时另携带 default_lease_ms"不实——:214-218 全路径无条件携带（含版本不匹配拒绝 :189） | design D2 canonical |
| sv-F2 | `session/ServerSession.java:29` | SHALLOW-CLASS/HIGH | 并发簿记类（AtomicInteger+3 volatile，IO/扫描线程交叉访问）类注释零线程模型 | D4 T 段 |
| sv-F3 | `ServerSession.java:92` | WRONG/MED | markClosed 承诺"幂等只执行一次"，实现为 volatile 读-判-写，依赖未声明的 EventLoop 串行前提 | 注释限定前提（D5，不改代码） |
| sv-F4 | `ServerSessionHandler.java:103,109,143,158` | MISSING/MED | 四个 Netty 协议入口 override 无方法级 Javadoc（OVERLOADED 不计在途等契约只在码内） | D4 方法注释 |
| sv-F5 | `NotifyEventBridge.java:54` | MISSING/MED | notifyHead 无注释：request_id=0（proto:47）、静默丢弃（:56-58）、失败不抛、线程不定 | D4 |
| sv-F6 | `net/EnvelopeCodecHandler.java:41` | MISSING/MED | exceptionCaught 无注释；类注释触发源低估（实含下游入站异常传播至 head） | D4 |
| sv-F7 | `dispatch/RequestDispatcher.java:47` | SHALLOW-CLASS/MED | "纯函数可单测"未写 dispatch 于连接 EventLoop 同步执行、并发进入边界 | D4 |
| sv-F8 | `protocol/.../ProtocolCodecTest.java:30` | MISSING/MED | 私有 `roundTrip` 无注释（吞 InvalidProtocolBufferException→AssertionError 语义只在码内） | G6 |
| sv-F9 | `NotifyEventBridge.java:33` | SHALLOW-CLASS/LOW | 线程来源"可能来自扫描线程"不完整（AWAIT_NOTIFY 亦可来自释放 IO 线程） | D4 |
| sv-F10 | `ServerChannelInitializer.java:67` | MISSING/LOW | initChannel 无注释；出站顺序不变式仅行内（:69-71）；channels.add 用途未述 | D4 |
| sv-F11 | `ServerSessionHandler.java:71` | SHALLOW-CLASS/LOW | @Sharable 隐含但未写"回调仅本 Channel EventLoop、单连接串行"（sv-F3 成立前提） | D4 |
| sv-F12 | `net/ServerBootstrapFactory.java:25` | WRONG/LOW | "boss 1 线程"系调用方选择（`OpenLatchServer:108`）被写成工厂契约 | 归属修正 |
| sv-F13 | `session/ServerSessionRegistry.java:26` | SHALLOW-CLASS/LOW | 跨线程结构无类级 T 段（并发安全藏在字段注释） | D4 |
| sv-F14 | `OpenLatchServer.java:138` | MISSING/LOW | port() 未写调用者义务（start() 前 serverChannel==null → NPE） | D4 |
| sv-F15 | `ProtocolCodecTest.java:38-256` | MISSING/LOW | 11 个 @Test 无场景行（个别仅靠行内注释 171/185/212/238） | G6 |

核实为真：javadoc↔proto 全量对照（request_id=0、auth_token 拒绝、wait_ms 折算、OK/QUEUED 有效字段、状态码映射、protocolVersion=1、ServerConfig.validate 全规则）吻合。

## st｜starter + examples 主源码（5 项）

| # | 位置 | 类别/严重度 | 失真/缺口 | 修复指向 |
|---|---|---|---|---|
| st-F1 | `spring/OpenLatchAspect.java:59-60,226-228` | WRONG/MED | 丢失-静默枚举漏 SESSION_EXPIRED（实现 :237-240 判入），与 :75"原样传播"（仅获取期）形成读者矛盾 | 补枚举+限定释放侧 |
| st-F2 | `OpenLatchAspect.java:57-62,210-212` | WRONG/MED | 释放二分法漏第三分支：InterruptedException→恢复标志+debug 日志+放行不抛 | 类④+方法补分支 |
| st-F3 | `examples/WatchdogExample.java:34-36` | WRONG/LOW | 失锁归因"续租持续失败"不实——server.stop() 走断连 lostAt 定时裁决（断连期 tick 跳过不计数） | 机制改写 |
| st-F4 | `OpenLatchAspect.java:306-311` | WRONG/LOW | "表达式缓存命中数"实为条目数（与自身 @return 亦矛盾） | 改"条目数" |
| st-F5 | `examples/ExampleServers.java:45-55` | MISSING/LOW | startFastExpiry 枚举漏第 4 位参隐含 minLease 1s→500ms 下调 | 补注 |

核实为真：属性名/默认值（9410/5s/30s/200ms/10s）、waitTime 三分支、@Order(0) 事务外层、build 即异步连接、读写互斥与 FIFO、SIMPLE 自锁、蓄水池/中位口径等均对码为真；零 MISSING（覆盖）、零 OVERDOC。

## te-cc｜client+core 测试（16 项）

| # | 位置 | 类别/严重度 | 失真/缺口 | 修复指向 |
|---|---|---|---|---|
| te-cc-F1 | `core/CoreEngineConcurrencyTest.java:35` | WRONG/HIGH | 类注释虚报"互斥/无丢失/无重复入队/无孤儿等待者"四不变量，断言仅覆盖前两条（后两条实于 IdempotencyLimitTest） | 收敛+指路 |
| te-cc-F2 | `CoreEngineConcurrencyTest.java:110` | MISSING/MED | `runConcurrent` 无注释（屏障起跑、错误收集后统一断言、60s join 期限） | G6 |
| te-cc-F3 | `client/ClientTestServers.java:23` | WRONG/MED | "一律绑定临时端口(0)"被 `ClientReconnectTest:42-43` 固定 19410 证伪 | 改"默认建议…自行传入" |
| te-cc-F4 | `client/ClientWatchdogIT.java:37` | WRONG/LOW | HOLD_MS 注释"远超租约期验证续租"——常量未被引用，检查点硬编码 {4000,7000,9500} | 如实化+死常量待办 |
| te-cc-F5 | `CoreEngineSessionIdempotencyLimitTest.java:100` | WRONG/LOW | 测试名"…RemovesWaiters"过度承诺（k3 等待者移除未断言） | Javadoc 限定+改名待办 |
| te-cc-F6 | 同上:105 行内 | WRONG/LOW | "a 持有 k1（写）"实为 REENTRANT | 改标注 |
| te-cc-F7 | `TestSupport.java:27,58` | MISSING/LOW | `now`、`events` 私有字段无注释 | G6 |
| te-cc-F8 | `TestSupport.java:56,101` | MISSING/LOW | 两个 `Event` record 无注释 | G6 |
| te-cc-F9 | `CoreEngineConcurrencyTest.java:38,106` | MISSING/LOW | `KEY`/`Body` 无注释 | G6 |
| te-cc-F10 | `CoreEngineLeaseTest.java:35-37,46` | MISSING/LOW | 3 私有字段+`acquire` 工厂（隐藏 leaseMs=30_000/queue=true 时序前提） | G6 |
| te-cc-F11 | `CoreEngineMutexReentrantTest.java:33-35,44` | MISSING/LOW | 同 te-cc-F10 模式 | G6 |
| te-cc-F12 | `CoreEngineReadWriteFairnessTest.java:33-35,44` | MISSING/LOW | 同模式 + 两测试绕过工厂自设租约（:198,211,229） | G6 |
| te-cc-F13 | `CoreEngineSessionIdempotencyLimitTest.java:33-35,44` | MISSING/LOW | 同模式 | G6 |
| te-cc-F14 | 同上:66 | MISSING/LOW | `new CoreConfig(...,2)` 第 6 位参=队列上限无注释 | G6 |
| te-cc-F15 | `RequestMultiplexerTest.java:41` | SHALLOW-CLASS/LOW | 类注释四维度清单漏 supersede/超时竞争（D3，:181,206） | 6.5 |
| te-cc-F16 | `OLockSyncWrapperTest.java:34` | SHALLOW-CLASS/LOW | 清单（5.2–5.5）漏 5 个测试的分组：simple/读写/lockAsync/D4 监听器/过期后解锁 | 6.5 |

## te-sv｜server+starter 测试（18 项）

| # | 位置 | 类别/严重度 | 失真/缺口 | 修复指向 |
|---|---|---|---|---|
| te-sv-F1 | `session/ServerSessionTest.java:24` | WRONG/HIGH | "限额实际不可触发"（m2 D4 旧文）被同模块 `InflightOverloadTest`（写完成背压，:41-45）与 `ServerSessionHandler:139` 证伪 | 按现行实现改写 |
| te-sv-F2 | `spring/OpenLatchStarterIT.java:179` | WRONG/MED | 行内"第二个…独自超时"实断言 BrokenBarrierException（首持有者 2s 超时破屏障，`AnnotatedService:119`）；方法注释亦未记该异常 | 6.3 |
| te-sv-F3 | `net/MessageLegalityTest.java:47` | WRONG/MED | channel() 注释"事件不抛错的静默监听"恰反——notifyThrows=true 即抛 IllegalStateException 驱动第三维度；"通道对"误述单通道返回 | 6.3 |
| te-sv-F4 | `net/FramingTest.java:33` | SHALLOW-CLASS/MED | 维度清单漏出站编码+长度前缀（:95-115，双消息写出解释只在码内） | 6.5 |
| te-sv-F5 | `FramingTest.java:38` | MISSING/MED | `newFramingChannel` 入站-only pipeline 组合未注释（出站测试须自建通道之因） | G6 |
| te-sv-F6 | `dispatch/RequestDispatcherTest.java:52` | MISSING/MED | `newSession`——15 测试之承重夹具，`activate(core.sessionOpened())` 登记防 SESSION_EXPIRED 短路的语义未写（对照 :163-164 故意绕过） | G6 |
| te-sv-F7 | `net/HandshakeTest.java:46` | MISSING/MED | `setUp`——fireChannelActive 装 KEY attribute、no-op 通知桥两承重步骤未注释 | G6 |
| te-sv-F8 | `ServerConfigTest.java:29` | MISSING/LOW | 维度清单漏配置文件缺失快速失败（:105）与 port=0 绑定对（:131-152） | 6.5 |
| te-sv-F9 | `FramingTest.java:45` | MISSING/LOW | `frame` 手拼 §3.1 帧格式（4 字节长度头）与解码器参数关联未注释 | G6 |
| te-sv-F10 | `RequestDispatcherTest.java:58,72,82` | MISSING/LOW | 三个 Envelope 构造器无注释（InflightOverloadTest:96-101 为房规范本） | G6 |
| te-sv-F11 | `HandshakeTest.java:56,67,76` | MISSING/LOW | `readOutboundEnvelope`（断言唯一存在+强转耦合）、`hello`/`acquire` builders 无注释 | G6 |
| te-sv-F12 | `DisconnectEndToEndTest.java:37,44` | MISSING/LOW | acquire builder 硬编码（重入式/threadId=1/leaseMs=0→服务端默认租约，:61 注释成立之因）与 tearDown 无注释 | G6 |
| te-sv-F13 | `NotifyEndToEndTest.java:39,46,60` | MISSING/LOW | 同族 builders（此处 leaseMs 显式传入之差异未注释）+ tearDown | G6 |
| te-sv-F14 | `SmokeIT.java:111,146,152` | MISSING/LOW | `awaitListening`（轮询/静默成功/超时 AssertionError/吞 IOException）等无注释（同文件 findShadedJar 有——不一致） | G6 |
| te-sv-F15 | `OpenLatchServerTest.java:37` | MISSING/LOW | `configOnPort` 无注释且重复 `TestServers.config`（重复本身另录待办） | G6 |
| te-sv-F16 | `MessageLegalityTest.java:60,69,79,88` | MISSING/LOW | 4 helpers 无注释（覆盖不一致：acquire 行内倒是解释了 wait_ms -1/0） | G6 |
| te-sv-F17 | `OpenLatchServerTest.java:85` | WRONG/LOW | 名字"…and_bounded"无时长断言支撑（@Timeout 是整测兜底） | 6.4（改名待办） |
| te-sv-F18 | `SmokeIT.java:42` | WRONG/LOW | "failsafe 在 verify 阶段运行"不实——IT 在 integration-test 执行、verify 仅校验（pom.xml:110-118） | 6.5 |

## x｜跨模块交叉核验（本会话直接取证）

| # | 位置 | 类别/严重度 | 失真/缺口 | 修复指向 |
|---|---|---|---|---|
| x-1 | `client/LockType.java:25-26` | WRONG/LOW | SIMPLE"再次获取将排队等待自身直至租约到期"未提队列满即拒分支（core 侧为"排队或拒绝"） | 补"或队列满即拒" |

## 不修复登记（越界/非本变更）

- 代码侧改进（design D5 待办）：`markClosed` CAS 化；`HOLD_MS` 死常量、`ClientFaultInjectionIT.fastExpiry` 与 `ClientTestServers.fastExpiryConfig` 重复、`configOnPort` 与 `TestServers.config` 重复；两处测试改名（`…and_bounded`、`…RemovesWaiters`）。
- `OpenLatchAspectTest.java:224` "LATEx" 缩略语未展开——可读性小疵，未立目。
