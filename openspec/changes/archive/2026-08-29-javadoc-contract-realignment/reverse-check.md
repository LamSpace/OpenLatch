# 反向对码清单（design D7-3）

每条改写后的枚举式承诺 → 实现证据（行号为 2026-08-29 改写后工作树）。HIGH 全列；主源码部分实施时逐条复核，测试源码部分（批 5/6）完成后续记。

## 主源码

| # | 承诺（改写后注释断言） | 代码证据 | 判定 |
|---|---|---|---|
| 1 | DENIED：立即式在「锁被占用，**或**无持有者但等待队列非空（队首已通知、待重发窗口）」时返回（canonical 四站同文：`Outcome.DENIED`、`Outcome` 类注释、`CoreEngine.acquire` 排队语义、`AcquireCommand.queueIfBusy`） | `LockEntry:161-162` fastPath 要求 `waiters.isEmpty()`；`LockEntry:190-192` `!queueIfBusy → DENIED` 位于快路径/队首重发判定之后，队列非空即不达 | ✅ |
| 2 | `Outcome` 细分基线："协议单值 REJECT_KEY 细分、设计说明书 v1.2 §4.2 已同步为两值" | `Outcome` 注释不再以"相较 §4.2"为对比基线；两常量存在（REJECT_KEY_EMPTY/TOO_LONG） | ✅ |
| 3 | `CoreEngine.release/renew`：key 非 null 调用者义务，违反抛 NPE | `CoreEngine:233/:271` `lockTable.get(cmd.key())` 直入 ConcurrentHashMap.get，null → NPE；协议层校验见 `ServerSessionHandler`/`MessageLegalityTest` 合法性矩阵 | ✅ |
| 4 | `LeaseManager` 锁序：条目锁→堆锁单向嵌套、堆锁内不反向取条目锁、不成环 | 嵌套点 `CoreEngine:201`（acquire 授予 offer）与 `:280`（renew OK 后 offer）均在 `synchronized(e)` 内；`drainExpired` 出锁后逐条再取条目锁（`CoreEngine:303-321` 结构） | ✅ |
| 5 | `SessionRegistry.touchIfPresent` 与 remove 互斥粒度 = sessionId 表项 | `:65` `computeIfPresent(sessionId,…)` vs `:78` `remove(sessionId)`，同为 CHM 同一 key 的原子操作 | ✅ |
| 6 | `OpenLatchException`：超时/断连本地失败 null 状态码；锁丢失**可能**携带 INVALID_TOKEN/NOT_HELD/SESSION_EXPIRED | `Watchdog:184-186`（续租拒绝带 status）、`Watchdog:192-193`（连续失败带 status）、`RemoteLock:145-146`（unlock 前丢失带 `ole.status()`）；纯断连路径 `OpenLatchClient:310-317` LockLostException 无 status（null） | ✅ |
| 7 | `shutdown()`：关停前逐条目尽力释放，单条目至多一个 requestTimeout，失败由租约到期兜底；类级生命周期段同步 | `OpenLatchClient:290` 调用点（closed 置位前）；`:368-388` releaseAllHeldBestEffort 实现（allOf().get(requestTimeout)、exceptionally 吞、随后清空登记） | ✅ |
| 8 | `OLock` 租约与续租段：grantedLeaseMs/3 周期、连续 2 次续租超时判失、断连按失锁时刻裁决 | `Watchdog:58` MAX_CONSECUTIVE_TIMEOUTS=2、`periodMs` /3（复核于 `Watchdog:227-229`）；`OpenLatchClient.onDisconnect` lostAt 定时（`:306-326`） | ✅ |
| 9 | `OLock.lock/tryLock` @throws：ServerUnavailableException（非 ACTIVE 即时拒绝/等待中断连）、OpenLatchException（服务端错误码/传输失败）；`tryLock(t,unit)` 负值 IAE | `OpenLatchClient:196-198` session==null → failedFuture(ServerUnavailableException)；`RemoteLock:244-247` doTryLock RuntimeException 原样上抛；`RemoteLock:118-120` waitTime<0 → IAE | ✅ |
| 10 | `tryLockAsync`：折算毫秒 <-1 同步 IAE；恰为 -1ms 转排队式受总超时兜底 | `RemoteLock.tryLockAsync → new AcquireSpec(..., unit.toMillis(waitTime))`；`AcquireSpec:48-50` waitMs<-1 才抛；`OpenLatchClient:212-218` waitMs=-1 → totalTimeout=defaultWaitTimeout | ✅ |
| 11 | `HeldLockRegistry.entries()` 只读视图（弱一致遍历），非快照 | `:305` `Collections.unmodifiableCollection(entries.values())`；一次性冻结先例 `OpenLatchClient:386` `List.copyOf(...)` | ✅ |
| 12 | `LockDeniedException` 异步路径原样收到、无二次转换 | `AwaitTracker` 直接以 LockDeniedException 完成 future（构造器 `:36-37` 自带 StatusCode.DENIED）；`RemoteLock.doTryLock` 按 `status()==DENIED` 识别 false | ✅ |
| 13 | `AcquireSpec` 紧凑构造器校验四项：key/lockType 非空、lease≥0、wait≥-1 | `:43-50` requireNonNull×2 + 两个区间校验 | ✅ |
| 14 | client `LockType.SIMPLE`：排队等待自身直至租约到期、队列满 REJECT_QUEUE_FULL、立即式直接 DENIED | `LockEntry:142` reentrant=false 无重入分支→排队 `:207`；`:203-204` 队列满；`:190-191` 立即式 | ✅ |
| 15 | `ConnectionManager` 锁纪律：stateLock 仅护 4 字段；channel/session/pendingSession volatile；connectDeadlineMs 锁内写锁外读 | 受锁字段 `:111,119,125,127`（state/connectFuture/currentBackoffMs/reconnectTask，均非 volatile）；volatile `:113,115,117`；写 `:364`（锁内）/读 `:425`（sendHello 锁外） | ✅ |
| 16 | `ServerSession` 线程模型：写方法全在本连接 EventLoop 串行；volatile/AtomicInteger 供跨线程观察读；markClosed 前提限定 | `ServerSessionHandler:162` write listener→同 EventLoop；`ServerSession:96-100` 读-判-写；观察读方 `NotifyEventBridge.get→session.channel()`（扫描线程） | ✅（前提成立；多线程化需 CAS——代码待办已注释披露） |
| 17 | `helloResponse` 恒携带 server_protocol_version 与 default_lease_ms，失败路径 sessionId=0 | `ServerSessionHandler:254-257` 无条件 set（builder 链无分支）；`:212` 拒绝路径传 0 | ✅ |
| 18 | `notifyHead`：request_id=0、request_id_ref=原 ACQUIRE id、会话缺失/非活跃静默丢弃、写失败仅 debug | `NotifyEventBridge:84` setRequestId(0)、`:88` setRequestIdRef；`:78-80` 判空即 return；`:91-96` listener 内 debug | ✅ |
| 19 | `EnvelopeCodecHandler` 触发源：分帧/解码失败 + 一切传播至本站位异常，记 WARN 后断连 | `:44-45` log.warn + ctx.close()；站位 `ServerChannelInitializer:82`（紧随两解码器） | ✅ |
| 20 | `ServerBootstrapFactory` 不约束 boss 线程数（"1"系调用方选择） | `create` 仅 `.group(bossGroup, workerGroup)` 透传；boss=1 在 `OpenLatchServer:108` | ✅ |
| 21 | `OpenLatchServer.port()` start() 前 NPE | 字段 `serverChannel` 初始 null（`:89` 附近），`port()` 直接 `serverChannel.localAddress()` | ✅ |
| 22 | `initChannel`：先登记 channels 再装 pipeline；Prepender 必须比 Encoder 更靠近 head | `ServerChannelInitializer:77-89`（add 顺序与行内注释一致） | ✅ |
| 23 | `OpenLatchAspect` 释放分类三分支：丢失（INVALID_TOKEN/NOT_HELD/SESSION_EXPIRED/断连）静默、其余业务成功抛/业务失败记日志、**中断恢复标志放行不抛** | `OpenLatchAspect:241-246` isLostLockFailure 含 SESSION_EXPIRED；`216-218` InterruptedException 分支（interrupt()+debug，无抛出）；`:258-264` reportReleaseFailure | ✅ |
| 24 | 释放侧 SESSION_EXPIRED 与获取侧"原样传播"不矛盾（获取侧不经 isLostLockFailure） | `acquire`（切面私有方法）ExecutionException 分支仅特判 DENIED 与 LockAcquisitionTimeoutException，其余 RuntimeException（含带 SESSION_EXPIRED 的 OpenLatchException）原样上抛（`:181-194`） | ✅ |
| 25 | `cachedExpressionCount()`＝条目数 | `:317` `expressionCache.size()` | ✅ |
| 26 | `WatchdogExample` 场景 2：断连→lostAt 定时裁决；断连期间续租不计数；连续超时判定仅半开路径 | `OpenLatchClient.onDisconnect`（lostAt 挂时）；`Watchdog.tick` 半开判定路径（复核 `Watchdog:138-141`：非 ACTIVE 跳过不计数）；`st-F3` 修复文本 | ✅ |
| 27 | `ExampleServers.startFastExpiry`：默认租约 2s、**minLease 钳制 500ms**、扫描 200ms | `:53-55` 构造实参序 (…, defaultLease=2_000, minLease=500, maxLease, tick=200, …)，对照 `ServerConfig` record 组件序 | ✅ |
| 28 | client `LockType` 与 core `LockType` 语义一致（会话交叉核验） | 四常量语义逐一对齐；client SIMPLE 补分支后与 core "排队或拒绝"闭合 | ✅ |

## 测试源码（批 5/6，三路代理 + 主会话补差）

| # | 承诺（改写后注释断言） | 代码证据 | 判定 |
|---|---|---|---|
| T1 | `ServerSessionTest` 类注释：计数语义经直调锁定；限额正常同步节奏难触发、**可经写完成背压达到**，handler 级由 `InflightOverloadTest` 覆盖 | `ServerSessionHandler:162`（写完成 listener 才 endRequest，审计行号 :139 因批 3 注释插入漂移）；`InflightOverloadTest:41-45` | ✅ |
| T2 | `CoreEngineConcurrencyTest` 类注释收敛为两不变量（互斥成立、授予无丢失）+ 两场景维度（8×500 立即式快路径 / 8×200 排队+同 requestId 重发）+ 指向 `duplicateAcquireDoesNotEnqueueTwice` | 本文件两用例断言集（inCritical==1、grants==threads×iterations）；被指路用例存在 | ✅ |
| T3 | `runConcurrent` 注释：屏障单发起跑门、Throwable 收集至 synchronized 列表、汇合后测试线程统一断言为空（AssertionError 浮现）、每线程 60s join 期限 | 复核 :111-131 实现后撰写 | ✅ |
| T4 | 四套件 `acquire` 工厂前提**按复核区分**：LeaseTest 工厂 leaseMs 为入参（仅 queueIfBusy=true 固定）；MutexReentrant 固定 30_000、queue 为入参；ReadWriteFairness/IdempotencyLimit 双固定 | 取证原称"四文件同模式均固定 30s"对 LeaseTest 不成立——实施时按代码改写（D6-① 拦截取证误差两例之一） | ✅（修正取证） |
| T5 | `ReadWriteFairnessTest` 工厂绕过点注释为 :211、:229（非 30s）与 :198（立即式 queue=false） | 复核确认 3 处（审计记 2 处），按实际写 | ✅（修正取证） |
| T6 | `MutableClock.now` 初值 1_000_000 为任意非零基点（用例仅相对推进，绝对值不承载语义） | git 史无可考原因，未杜撰；`TestSupport` 全部私有成员按 D1 补齐（含审计漏列的 `QueueingListener.queues`） | ✅（超清单补漏） |
| T7 | `sessionClosedReleasesHoldsAndRemovesWaiters` Javadoc 如实限定断言范围，k3 移除由 `sessionClosedRemovesNotifiedHeadAndRepromotesNewHead` 覆盖；:105 "k1（写）"→"k1（REENTRANT）" | 用例体仅断言 k1 路径与关闭后拒绝；k3 无移除断言 | ✅ |
| T8 | `queueDepthLimitRejected` 注释：CoreConfig 第 6 位参 = maxQueueDepthPerKey = 2 | 与 `CoreConfig` record 组件序核对一致 | ✅ |
| T9 | `ClientTestServers`："默认建议临时端口；固定端口用例（同端口重启重连）自行传入"；`ClientReconnectTest:42` 呼应注释 | `ClientReconnectTest:43` config(19410) 实锤旧"一律"为假 | ✅ |
| T10 | `ClientWatchdogIT.HOLD_MS` 注释：预留参考值、用例检查点硬编码最大 9.5s、常量未被引用（死代码待办） | grep 全目录仅声明处一次命中；:84 `{4000,7000,9500}` | ✅ |
| T11 | `MessageLegalityTest.channel()`：单 EmbeddedChannel；notifyThrows null/false 静默、true 抛 simulated bridge failure 驱动兜底维度；**当前仅以 null 调用，true 分支系参数语义非用例路径** | 复核追加发现（调用点全 null），注释按参数语义撰写不虚称 | ✅ |
| T12 | `OpenLatchStarterIT` 串行证明注释：第二个被授予时屏障已被首个 2s 超时置 broken → `BrokenBarrierException`（非"独自超时"） | `AnnotatedService:116-124` gate.await(2s)；断言 :180-181 hasCauseInstanceOf | ✅ |
| T13 | `FramingTest` 维度清单补出站编码+长度前缀；`newFramingChannel` 入站-only（出站用例自建通道之因）；`frame` 4 字节大端前缀与解码器参数对应 | :95-115 出站用例；解码器实参 0/4/4 | ✅ |
| T14 | `SmokeIT`："failsafe integration-test 阶段执行（package/shade 之后）、verify 校验结果" | `openlatch-server/pom.xml:108-119` 双 goal 绑定 + 本次构建日志实证执行顺序 | ✅ |
| T15 | `ServerConfigTest` 清单补"配置文件缺失快速失败""port=0 临时端口绑定" | :104 / :131-152 用例存在 | ✅ |
| T16 | `OpenLatchServerTest.stop_is_idempotent_and_bounded` Javadoc：bounded 指 @Timeout 整体约束、无逐次时长断言（方法名保留，D5 待办） | 体 :86-89 无时长断言 | ✅ |
| T17 | 请求 builders 一句话+参数硬编码前提（Disconnect：重入/threadId=1/leaseMs=0→服务端默认租约；Notify：leaseMs 显式传入、短租约 100ms 用例依赖；Dispatcher 三 builder 含 waitMs 语义） | 各文件构造实参复核（如 `RequestDispatcherTest:122/:133` 排队/立即用例） | ✅ |
| T18 | `TestProtocolClient` 7 私有字段、`TestServers` 私有构造器按 D1 补齐 | 纯新增注释 | ✅ |
| T19 | `ProtocolCodecTest` roundTrip + 11 个 @Test 一行场景（含 AWAIT_NOTIFY request_id=0/request_id_ref、unknownFields 前向兼容、枚举编号对 §3.2、wait_ms=-1 排队语义对 proto:80） | 逐条断言核实后撰写 | ✅ |
| T20 | 主会话补差：`HandshakeTest` ch/registry/config、`RequestDispatcherTest` core/dispatcher、`Disconnect`/`Notify` server 共 6 私有字段一句话注释（TA2 立目、清单未含，按 D1 全覆盖补齐） | 装配语境读码后撰写 | ✅ |
