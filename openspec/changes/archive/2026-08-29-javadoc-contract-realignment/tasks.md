# Tasks: javadoc-contract-realignment

实施纪律（贯穿全部任务，design D6）：每条改写前**先按所引发现（F#，取证详见本目录 `audit-findings.md`）复核涉事代码分支**；复核不成立的发现标记「不修复 + 理由」于本文件行尾，禁止盲改。所有新措辞若命中 design D2 canonical 句，**逐字复制**。仅改注释（含测试内联行注释）；不改方法名、常量、代码逻辑（越界项见 design D5 待办）。

## 1. 基线

- [x] 1.1 确认 `git status` 干净；跑 `mvn -s /home/lam/repo/settings.xml clean verify` 全绿作为基线（与 D6-③ 批间比对同源）

## 2. core 主源码（G1+G5，批 1）

- [x] 2.1 `Outcome`：`DENIED` 常量与类注释按 design D2 canonical 句改写（cc-F1/cc-F4 复核）；类注释 "REJECT_KEY 细分" 比对基线改指设计说明书 v1.2 §4.2 现文（cc-F4）
- [x] 2.2 `CoreEngine`：`acquire` 排队语义段用 canonical 句同文（cc-F3）；`release`/`renew` 的 @param 补 null-key 调用者义务（cc-F2）
- [x] 2.3 `AcquireCommand`：`queueIfBusy` 用 canonical 句同文（cc-F1 同族）；"设计说明书 D3" 误挂改为 m1 design.md D3 或 §4.4（cc-F7）
- [x] 2.4 `LeaseManager`：类注释补"条目锁→堆锁单向嵌套、堆锁内不取条目锁、不成环"（cc-F5，对齐设计 §4.9.3 v1.2）
- [x] 2.5 `SessionRegistry`：`touchIfPresent` "与 remove 原子互斥"的 "key" 消歧为 sessionId 表项（cc-F6）
- [x] 2.6 `mvn -s /home/lam/repo/settings.xml -pl openlatch-core verify` 绿

## 3. client 主源码（G1+G2+G3+G5，批 2）

- [x] 3.1 `OpenLatchException`：status() 枚举段按 design D2 canonical 改写（cl-F1）
- [x] 3.2 `OpenLatchClient`：`shutdown()` 方法注释与类级生命周期段按 canonical 现状句改写（cl-F2）
- [x] 3.3 `OLock`：类级补「租约与续租」段、`lock`/`tryLock`×2 补 @throws（ServerUnavailableException / OpenLatchException / 负值 IAE）、`tryLockAsync` 补负值语义、"以待等待"衍字修正（cl-F4/F5/F6/F10，内容按 design D3 清单）
- [x] 3.4 `RemoteLock`：`{@code{{@code tryLock}}` 畸形修正（cl-F9）、:82 衍字修正、三个获取方法实现差异句与接口新 @throws 对齐（不重复展开）
- [x] 3.5 `ConnectionManager`：`stateLock` 字段注释如实化为受锁字段清单 + volatile 通道说明（cl-F3）；类级补 T 段（定时器线程/EventLoop/调用方三线程交互与字段归属）
- [x] 3.6 `HeldLockRegistry.entries()` "不可变快照"→"只读视图（弱一致遍历）"（cl-F7）；`LockDeniedException` "转换"措辞改为原样收到（cl-F8）；`AcquireSpec` 紧凑构造器校验清单补 lockType 非空（cl-F11）
- [x] 3.7 `LockType.SIMPLE` 常量注释补"队列满即拒"分支，消除与 core 侧「排队或拒绝」的表述落差（本会话交叉核验项）
- [x] 3.8 `mvn -s /home/lam/repo/settings.xml -pl openlatch-client verify` 绿

## 4. server 主源码（G1+G3+G4+G5，批 3）

- [x] 4.1 `ServerSession`：类级补 T 段——各字段（inflight AtomicInteger/3 volatile）的保护方式与 EventLoop/扫描线程访问矩阵（sv-F2）；`markClosed` 限定"仅该连接 EventLoop 串行调用"前提、不改代码（sv-F3，D5）
- [x] 4.2 `ServerSessionHandler`：`helloResponse` 按 canonical 句改写（sv-F1）；`channelActive`/`channelRead0`/`channelInactive`/`userEventTriggered` 各补方法级 Javadoc（职责+触发 EventLoop+endRequest 记账与 markClosed 去重分支，sv-F4）；类级补 T 段（sv-F11）
- [x] 4.3 `NotifyEventBridge`：`notifyHead` 补方法级 Javadoc（request_id=0 语义、会话缺失静默丢弃、写失败仅 debug、可调用线程，sv-F5）；类级线程来源改为"释放→连接 IO 线程；到期/清扫→扫描线程"（sv-F9）
- [x] 4.4 `RequestDispatcher`：类级补 T 段（EventLoop 同步调用、无状态可并发进入、串行性由条目锁保证，sv-F7）
- [x] 4.5 `EnvelopeCodecHandler`：`exceptionCaught` 补方法注释；类注释触发源扩为"分帧/反序列化失败及下游入站异常传播"（sv-F6）
- [x] 4.6 `ServerChannelInitializer.initChannel` 补方法注释（accept 注册线程、出站顺序不变式上收、channels 登记用途，sv-F10）
- [x] 4.7 `ServerBootstrapFactory` boss=1 归属修正为"调用方传入（本应用取 1）"（sv-F12）；`OpenLatchServer.port()` 补调用者义务"start() 前调用 NPE"（sv-F14）
- [x] 4.8 `mvn -s /home/lam/repo/settings.xml -pl openlatch-server verify` 绿

## 5. starter + examples 主源码（G5，批 4）

- [x] 5.1 `OpenLatchAspect`：类注释④与 `isLostLockFailure` 枚举补 SESSION_EXPIRED（注明仅释放侧）（st-F1）；类注释④与 `releaseAfterBusiness` 补"中断→恢复标志、放行不上抛"第三分支（st-F2）；`cachedExpressionCount` 摘要改"条目数"（st-F4）
- [x] 5.2 `WatchdogExample` 场景 2 机制归因改为"断连后按 lostAt 定时裁决；半开连接另有连续续租超时判定"（st-F3）；`ExampleServers.startFastExpiry` 补"minLease 钳制降至 500ms"（st-F5）
- [x] 5.3 `mvn -s /home/lam/repo/settings.xml -pl openlatch-spring-boot-starter,openlatch-examples verify` 绿

## 6. 测试源码——语义必改项（§2 清单，批 5）

- [x] 6.1 `ServerSessionTest` 类注释按现行实现改写：计数语义经直调锁定 + 限额可经写完成背压触发（handler 级由 InflightOverloadTest 覆盖）（te-sv-F1，HIGH）
- [x] 6.2 `CoreEngineConcurrencyTest` 类注释收敛为本文件实际断言的两条不变量 + 补两场景交错维度（8×500 快路径 / 8×200 排队通知）+ 指向重复入队/孤儿等待者真实覆盖文件（te-cc-F1，HIGH）；`ClientTestServers` "一律"改"默认建议…固定端口用例自行传入"（te-cc-F3）
- [x] 6.3 `MessageLegalityTest.channel()` 注释改为如实描述 notifyThrows 双分支 + @param/@return（te-sv-F3）；`OpenLatchStarterIT:179` 行内与方法注释改为"屏障 broken → BrokenBarrierException"（te-sv-F2）
- [x] 6.4 名不符实三例按 D5 只改文档、登记代码待办：`CoreEngineSessionIdempotencyLimitTest.sessionClosedReleasesHoldsAndRemovesWaiters` 补方法 Javadoc 如实限定断言范围（te-cc-F5）；`OpenLatchServerTest.stop_is_idempotent_and_bounded` 补注释说明"bounded 由 @Timeout 承载、无逐次时长断言"（te-sv-F17）；`ClientWatchdogIT.HOLD_MS` 注释如实化（当前检查点 ~9.5s，常量未被引用→待办）（te-cc-F4）；`CoreEngineSessionIdempotencyLimitTest:105` "k1（写）"→"k1（REENTRANT）"（te-cc-F6）
- [x] 6.5 维度清单补全：`FramingTest` 类注释补出站编码维度（te-sv-F4）；`SmokeIT` failsafe 阶段改"integration-test 执行、verify 校验"（te-sv-F18）；`ServerConfigTest` 清单补配置文件缺失/port=0 两维（te-sv-F8）；`RequestMultiplexerTest` 补 supersede/超时竞争维度（te-cc-F15）；`OLockSyncWrapperTest` 补 simple/读写/异步/监听器生命周期组（te-cc-F16）

## 7. 测试源码——G6 私有成员全覆盖（字面 §5，批 6）

- [x] 7.1 core 测试：4 个 CoreEngine* 套件的 `clock`/`listener`/`engine` 字段与 `acquire` 工厂（注明固定 leaseMs=30_000 与 queue 旗标，te-cc-F10~F13）；`TestSupport` 的 `MutableClock.now`、两处 `events` 字段、`Event` record ×2（te-cc-F7/F8）；`CoreEngineConcurrencyTest` 的 `KEY`/`Body`/`runConcurrent`（屏障协议+错误收集语义，te-cc-F2/F9）；`queueDepthLimitRejected` 场景行注明第 6 位参=队列上限 2（te-cc-F14）
- [x] 7.2 server 测试：`FramingTest.newFramingChannel`/`frame`（te-sv-F5/F9）；`HandshakeTest.setUp`/`readOutboundEnvelope`/builders（te-sv-F7/F11）；`RequestDispatcherTest.setUp`/`newSession`（activate 登记前提！）/builders ×3（te-sv-F6/F10）；`Disconnect`/`Notify` 的 builders 与 tearDown（te-sv-F12/F13）；`SmokeIT.awaitListening`/`findFreePort`/`acquire`（te-sv-F14）；`OpenLatchServerTest.configOnPort`（te-sv-F15）；`MessageLegalityTest` builders + `readOut`（te-sv-F16）
- [x] 7.3 按字面 §5 追加点位：`TestProtocolClient` 私有字段（:54-62）、`TestServers` 私有构造器（复核后补）；`ProtocolCodecTest.roundTrip`（sv-F8）+ 11 个 @Test 一行场景注释（sv-F15）
- [x] 7.4 复核 starter 测试与 client 测试其余文件无遗漏私有成员（两路代理报告基本干净，发现新缺口一并补）

## 8. 验证（G7）

- [x] 8.1 全量 `mvn -s /home/lam/repo/settings.xml clean verify` 全绿
- [x] 8.2 纯注释 diff 断言：`git diff --unified=0` 的全部 +/- 行（排除 +++/---）均为注释行（design D7 命令）；违规行逐个回退
- [x] 8.3 反向对码清单：产出「改写后的每条枚举式承诺 → 代码行号证据」对照表，HIGH 7 点全列、MED/LOW 改写句逐条，存为变更目录 `reverse-check.md`
- [x] 8.4 残留旧假陈述 grep 清零：`以待等待`、`待补齐`、`不携带状态码`、`仅 OK 时`、`一律绑定临时端口`、`REJECT_KEY 细分自`（复核后）、`表达式缓存命中`、`不可变快照`、`boss 1 线程`
  - 不修复登记：`TestServers.java:20`「一律绑定临时端口（0）」复核为真（全部调用点实参 0；19410 仅见于配置解析断言用例，不实际 bind），保留；`OpenLatchException` 命中的「不携带状态码」均为限定后的新真陈述。
- [x] 8.5 标记 D5 代码侧待办为后续变更候选（markClosed CAS 化、HOLD_MS/fastExpiry/configOnPort 清理、两处测试改名），不入本变更
