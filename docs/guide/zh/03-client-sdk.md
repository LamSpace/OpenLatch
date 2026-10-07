# 03 · 客户端 SDK

## 生命周期与构造

`OpenLatchClient` 实现 `AutoCloseable`：一个实例 = 一条连接（+ 集群形态下的改道车道），内部含 EventLoop、看门狗与重连器，**线程安全、全局共享**，进程退出前 `close()`（或 try-with-resources）。

```java
OpenLatchClient client = OpenLatchClient.builder()
        .seeds("node1:9410", "node2:9410", "node3:9410")   // 集群：全部种子；单机可用 .address("host:port")
        .requestTimeout(Duration.ofSeconds(5))             // 单请求应答超时
        .defaultWaitTimeout(Duration.ofSeconds(30))        // lock() 总兜底
        .connectTimeout(Duration.ofSeconds(3))
        .reconnectInitialBackoff(Duration.ofMillis(200))
        .reconnectMaxBackoff(Duration.ofSeconds(10))
        .workerThreads(1)
        .build();
client.connectAsync().join();   // 可选：主动完成首连；不阻塞后续调用，它们会自动触发连接
```

### 安全与令牌（可选，默认关闭）

| builder | 说明 |
|---|---|
| `tlsEnabled(true)` | 对业务端口启用 TLS |
| `tlsTrustStore(path)` | 服务端证书信任锚（PEM CA） |
| `tlsClientCert / tlsClientKey` | mTLS 客户端证书/私钥（成对提供） |
| `authToken(token)` | HELLO 握手携带的业务令牌（服务端开启认证时必填） |

配置了令牌/ TLS 后，重连与种子发现探针全程沿用同一安全配置。详见 [06 安全](06-security.md)。

## 锁型选择

| 要什么 | 用什么 |
|---|---|
| 同线程可重入的互斥 | `newReentrantLock(key)` |
| 最轻互斥（同线程不可重入！再获取会自排到租约到期） | `newSimpleLock(key)` |
| 读写分离 | `newReadWriteLock(key)` → `.readLock()` / `.writeLock()` |
| 严格到达序的互斥（显式公平承诺） | `newFairLock(key)` |
| N 并发的资源池门闸 | `newSemaphore(key, permits)` |
| 一次性栅栏（等待 N 件事完成） | `newCountDownLatch(key, total)` |

key 是全局命名空间（同 key 同锁），建议 `业务:实体:标识` 式分层命名。

## 同步用法（推荐默认）

```java
OLock lock = client.newReentrantLock("order:123");
lock.lock();                              // 无限等？不存在——defaultWaitTimeout 兜底
try {
    // 临界区
} finally {
    lock.unlock();                        // 已失锁时静默（以回调为准），不抛
}

if (lock.tryLock(2, TimeUnit.SECONDS)) {  // 限时获取
    try { ... } finally { lock.unlock(); }
}
```

- 同步获取超时分别抛 `LockAcquisitionTimeoutException`（等待预算尽）与
  `OpenLatchTimeoutException`（单请求无应答）；`InterruptedException` 原样透传。
- `lock.isHeldByCurrentThread()` 与 `lock.key()` 为本地读，不产生网络请求。

## 失锁处理：写关键状态必配

```java
client.addLockLostListener((key, cause) -> {
    // 全局兜底：记录、告警；正在进行的提交应据此中止
});
lock.onLockLost((key, cause) -> { /* 按锁精确处理 */ });
```

失锁触发场景：租约到期未续、断连致会话关闭、集群 failover 回滚。回调线程是专用单线程，
**不要在回调里做阻塞重活**（投递到业务线程池处理）。

## 信号量与屏障

```java
OSemaphore sem = client.newSemaphore("pool:http", 8);
sem.acquire();                    // 或 tryAcquire(n, timeout, unit)
try { work(); } finally { sem.release(); }        // 未持有而 release 抛 IllegalMonitorStateException

OCountDownLatch latch = client.newCountDownLatch("boot:gates", 3);
latch.init();                     // 定型 total（幂等：同 total 重复 init 合法）
latch.countDown();                // → 返回剩余计数
latch.await(60, TimeUnit.SECONDS);   // 等归零；一次性，归零后条目存续至节点重启
```

屏障注意：条目定型后不随参与者散去而回收（按轮次命名键做隔离，如 `deploy:2026-09-12`）。

## 原子变量（OAtomicLong 族，v4）

```java
OAtomicLong seq = client.newAtomicLong("ids:order");          // 起步 0
OAtomicLong one = client.newAtomicLong("ids:user", 1);         // 非零初值主张：首建以 1 起步
long v = seq.incrementAndGet();                                // 服务端单往返落值（非本地 CAS 循环）

// ABA-free 读改写（推荐形态）：
OAtomicLong.Stamped cur = seq.getStamped();
boolean ok = seq.compareAndSetStamped(cur.value(), cur.version(), cur.value() + 10);

OAtomicBoolean ready = client.newAtomicBoolean("boot:ready");
if (ready.compareAndSet(false, true)) { doInit(); }            // 跨进程"首到者获胜"

OAtomicInteger hits = client.newAtomicInteger("counter:hits");
hits.accumulateAndGet(5, Integer::sum);                        // 客户端带版本 CAS 循环（有重试界限）
```

同一 key 的**形态由首建请求定型**（long/integer/boolean 三选一，互斥互拒），与"同 key
同家族"约定同规则（对齐既有锁/Semaphore/屏障的家族定型纪律）。

语义边界（务必知晓，详见 [01 核心概念 §7](01-concepts.md)）：

1. 每操作一次往返；写与读均经服务端多数派定序（线性一致），代价是 RTT；
2. 值**不绑定会话**——任何客户端死亡不回滚值（与锁/信号量的失锁语义相反）；
   无租约、无看门狗、不触发 `LockLostListener`；
3. 超时抛出（`OpenLatchTimeoutException`）时写效果**可能已生效**——SDK 已以同序号
   重发兜底去重，放弃场景（会话切换）用 `getStamped()` 复核后再决定；
4. JDK 形 `compareAndSet` 存在 ABA；需要"值+历史"双判定时用 `compareAndSetStamped`；
5. 条目常驻：没有删除原子的 API，按轮次/租户给 key 命名并做好基数治理；
6. `newAtomic*(key, initial)` 的初值是**断言**而非赋值：既有条目初值不符时，
   该句柄首个操作抛 `OpenLatchException`（与屏障 total 定型同规则）。

## 有值引用（OAtomicReference，v6）

ATOMIC 家族的载荷形态：一段不透明字节（默认上限 4KB）附版本戳，走 get/set/版本 CAS。

```java
OAtomicReference cfg = client.newAtomicReference("config:switch");   // 初值 null 态
cfg.set("on".getBytes(StandardCharsets.UTF_8));                       // 落字节载荷
cfg.setString("v2");                                                  // String 便利族（UTF-8）

// 跨进程"配置代次切换"（ABA-free）：
OAtomicReference.Stamped cur = cfg.getStamped();
boolean ok = cfg.compareAndSetStamped(cur.value(), cur.version(), s("v3"));

// null 与空字节串是两个可区分的值：
OAtomicReference flag = client.newAtomicReference("boot:ready");
if (flag.compareAndSet(null, s("init"))) { doInit(); }               // 期望 null 建立空态
```

同一 key 的**形态由首建请求定型**（long/integer/boolean/reference 四选一，同族互斥互拒）——
标量键上引用操作、引用键上标量操作均以 `INVALID_REQUEST` 拒绝、零扰动。

语义边界（务必知晓，详见 [01 核心概念 §7.1](01-concepts.md)）：

1. 载荷**不透明**：服务端只存/比字节，永不反序列化——对象编码归应用层；
2. **超限零生效**：写超过服务端 `max-value-bytes`（默认 4KB）的载荷抛
   `OpenLatchException`（拒绝语义），SDK 不截断、不重试，条目值与版本逐项不变；
   钳制下调不追溯存量（既有更大载荷照常可读）；
3. 与标量原子同规则：每操作一次往返、值不绑定会话、超时不确定窗与同序号重发、
   ABA 仅 `*Stamped` 消除、条目与载荷常驻不回收；
4. **无算术面**：不提供 `increment`/`accumulate`（JDK `AtomicReference` 亦无对应物），
   也没有 `weakCompareAndSet`/`compareAndExchange` 族；
5. 需 **v6 握手**：对握版本上限低于 6 的服务端，引用形态请求以显式
   `OpenLatchException`（`INVALID_REQUEST`）失败，不重发、不降级。

## 有界队列与延时队列（OBlockingQueue / ODelayQueue，v7）

跨进程元素搬运原语：有界 FIFO 队列与延时队列（元素带到期时刻）。元素为不透明字节
（同受 `max-value-bytes` 每元素钳制，默认 4KB），容量由首建句柄定型。

```java
OBlockingQueue jobs = client.newBlockingQueue("job:queue", 64);      // 容量主张 64
jobs.put(s("任务A"));                                                 // 阻塞投递（可中断）
if (jobs.offer(s("任务B"))) { ... }                                  // 立即式：队满回 false
byte[] item = jobs.take();                                           // 阻塞消费（可中断）
byte[] maybe = jobs.poll(2, TimeUnit.SECONDS);                       // 带预算消费（本地计时）
List<byte[]> batch = new ArrayList<>();
int n = jobs.drainTo(batch, 32);                                     // 批量摘取（摊薄 RTT）
int depth = jobs.size();                                             // 驻留口径（延时含未到期）

ODelayQueue alarm = client.newDelayQueue("alarm:queue", 16);
alarm.offerDelayed(s("5 分钟后提醒"), 5, TimeUnit.MINUTES);           // 到期前一切消费通道不可见
String due = alarm.takeAsString();                                   // 最早到期先出，同到期按到达序
```

语义边界（务必知晓，详见 [01 核心概念 §9](01-concepts.md)）：

1. **元素绑定 key 不绑定会话**：投递者进程死亡不吞元素（增强于 JDK 同进程堆消散语义）；
   元素存活至被消费，服务端无自动回收——忘删 key 即永久驻留（驻留成本见控制台队列观察）；
2. **双"满"分轨**：元素满时 `offer` 回 `false`、`put` 挂起等空位；等待挂起队列满时挂起
   请求抛 `OpenLatchException`（`OVERLOADED`，沿用锁深度护栏映射）；服务端对挂起不设
   到期期限，客户端超时/中断即本地终态（服务端挂起项由事件与超时清扫收敛，期间其队列
   位次仍占位——被放弃的挂起者不影响他人语义，只影响他人位次）；
3. **幂等由每会话去重槽承载**：超时/改道窗口内同序号自动重发——`put` 不双插、
   `take`/`drainTo` 重放交付**同一份字节**；会话中途切换则放弃（抛异常），用
   `size()`/`peek()` 复核，不盲目换值重试；
4. **元素不可为 null**（判例 JDK `BlockingQueue` rejectNull；与 `OAtomicReference` 的
   null 语义相反，零长度空字节串是合法元素）；无 `iterator`/`contains`/`remove(Object)` 面；
5. **延时形态**：`offerDelayed(e, delay, unit)` 显式命名（JDK `DelayQueue.offer(e,timeout,unit)`
   的"延迟"签名与阻塞队列族"等待预算"同形异义，改名防混读）；绝对到期时刻由服务端
   应用点折算、随复制日志确定化（换主不改判）；到点唤醒精度为服务端 tick 级
   （`ready-tick-ms`，默认 200ms），消费正确性不依赖精度；
6. 需 **v7 握手**：低版本服务上队列请求以显式 `OpenLatchException`（`INVALID_REQUEST`）
   失败，不重发、不降级；`drainTo` 实际摘取数受服务端 `max-drain-bytes` 预算钳制，
   以返回值为准。

## 循环屏障（OBarrier，v5）

```java
OBarrier gate = client.newBarrier("phase:ingest", 3);          // 创建者句柄：定型 parties=3
gate.await();                                                   // 阻塞至本世代合拢（兜底超时约束）
boolean met = gate.await(10, TimeUnit.SECONDS);                  // 限时：超时=本方离场并破障 → false

OBarrier worker = client.newBarrier("phase:ingest");            // 纯加入句柄（不主张 parties）
OBarrier withAction = client.newBarrier("phase:ingest", 3, () -> flushBuffers());
// 三方到场合拢时，最后到场方在自己的 await 调用栈内执行 flushBuffers()；
// 它完成（或抛异常）回报之前，其余两方都不放行。

OBarrier any = client.newBarrier("phase:ingest", 3);
any.breakBarrier();                                             // 显式打破当前世代（幂等，无 reset）
if (any.isBroken()) { ... }                                     // 句柄本地最近所见裁决（无网络）
```

语义边界（务必知晓，详见 [01 核心概念 §8](01-concepts.md)）：

1. 到场合拢、动作两阶段放行均为服务端多数派裁决——每到场/回报一到网络往返；
2. **离场即破障（增强于 JDK）**：任一已到场方超时/中断/进程死亡/`breakBarrier()` 都
   即时打破其当前世代，全体在队他方 `await` 抛 `OBrokenBarrierException`；
3. **破障为世代局部、无粘滞**：下一批到场自然开新世代正常合拢；不提供 `reset()`
   （与 JDK 差异）；`isBroken()` 是句柄本地读数，非实时跨进程一致；
4. 动作由最后到场方在其进程内执行；动作抛异常 ⇒ 本方以该异常终结且同世代全体破障；
5. `await` 超时/中断即破障连带全体收场——对抖动网络建议用 `await()` + 外部监督，
   或加大超时预算；
6. 在途到场遇会话切换不自动重放（到场是有副作用请求），本方抛 `OpenLatchException`，
   旧世代已随会话清理破障；`await` 全程无租约、零续租流量。

## 广播发布/订阅（OTopic，v8）

跨进程广播通道：一个 key 一个通道，每条消息尽力交付给当时在册的全部订阅者。
消息体为不透明字节（同受 `max-value-bytes` 每条钳制，默认 4KB）。

```java
OTopic news = client.newTopic("evt:orders");

news.publish(s("订单已支付"));                              // 同步：受理回执（不代表已送达）
long seq = news.publish("已受理".getBytes(StandardCharsets.UTF_8));
news.publishAsync(s("旁路投递"));                            // 异步：单次发送，不自动重发

OTopicSubscription watch = news.subscribe(m -> {
    handle(m.payload(), m.topicSeq(), m.publisherSessionId());  // 本订阅内 seq 严格升序
});
long lost = watch.droppedCount();                            // 观察：丢弃估计（见下第 2 条）
watch.close();                                               // = news.unsubscribe()（幂等）
```

语义边界（务必知晓，详见 [01 核心概念 §10](01-concepts.md)）：

1. **至多一次**：`publish` 返回仅代表服务端已受理并入 fan-out，不承诺任何订阅者
   收到；断线、缓冲满、换主窗口都会造成丢失，服务端不重投、不逐条通知。需要
   投递必达请用 `OBlockingQueue`；
2. **弱背压 = drop-newest 两级缓冲**：每订阅服务端缓冲（`max-subscription-buffer`，
   默认 256 条）满时丢最新一条并计数，SDK 本地二级缓冲同策略；慢订阅者不反压
   发布者、也不被断开（与 JDK `SubmissionPublisher` 的 onOverflow-close 判例刻意
   不同——断开即会话死亡、连带该会话全部持锁释放）。丢失不逐条通知，仅经
   `droppedCount()` 观察（同任期 `topic_seq` 缺口推断 + 本地溢出计数，跨任期基线
   重置不累计）；
3. **单订阅内、同一 Leader 任期内 seq 严格升序**；跨发布者/跨订阅无全局序；
   换主后 `topic_seq` 重新起算、断档期消息不补投——订阅由 SDK 自动重挂续收；
4. **发布重试去重仅同 Leader 内**：同 `op_seq` 自动重发命中服务端去重槽、不双扇出；
   跨换主重试（含应用层手工重试）可能双投——消费侧幂等是应用义务；
5. **监听器线程模型**：单订阅内**串行回调**（对齐 JDK `Flow.Subscriber.onNext`
   不重入承诺），在 SDK dispatcher 线程执行、绝不占用网络 EventLoop；回调异常
   被吞并记日志、不断续交付；耗时处理请自行转交业务线程池；
6. **订阅绑定会话**：订阅者进程死亡即退订（与队列"死亡不吞元素"相反——队列
   承载驻留数据、topic 承载交付事件）。订阅存续有服务端登记表与缓冲成本，
   长驻方应显式 `unsubscribe()`；
7. 需 **v8 握手**（服务端与客户端双端升级，升级序先服务端后客户端）：v≤7 会话
   发 `TOPIC_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连；消息体必携且不可为
   null（判例队列元素纪律，与引用形态 null 语义相反，零长度合法空消息）；
8. **撞 key**：topic 键与锁/队列/引用等家族键同名时受理端只读探测拒绝
   （`INVALID_REQUEST`，尽力而为、无竞态保证）——"一 key 一形态"对 topic 是
   应用侧契约而非机制互斥。

## 条件变量（OCondition，v9）

与 `OLock` 配套的等待-通知通道，对应 JDK `Condition`。条件身份 = (锁 key,
条件名)：句柄无状态、无需关闭，同名句柄跨进程绑定同一等待集。标准惯用法
（guard loop 是调用方义务）：

```java
OLock lock = client.newReentrantLock("job:dispatch");
OCondition ready = lock.newCondition("ready");            // 命名寻址，可重复创建

lock.lock();
try {
    while (!hasWork()) {          // 永远以谓词复查包裹 await——虚假唤醒允许
        ready.await();            // 受理即全量释放；唤醒返回时持锁、重入 1 级
    }
    takeWork();
} finally {
    lock.unlock();
}

lock.lock();                      // 生产者：signal 必须先持有本锁
try {
    enqueueWork();
    ready.signal();               // 唤醒队首一个等待项（signalAll 全员搬运）
} finally {
    lock.unlock();
}
```

语义边界（务必知晓，详见 [01 核心概念 §11](01-concepts.md)）：

1. **guard loop 是调用方义务**：虚假唤醒允许且不承诺杜绝（JDK 同契约），唤醒
   来源含队首超时清扫促醒、换主重挂窗的已丢 signal、LEAVE/SIGNAL 竞态收敛——
   返回不代表谓词为真，裸 `await()` 即缺陷；
2. **返回时持锁与 1 级重入算术**：`await()`/`await(timeout, unit)` 无论被唤醒、
   超时还是中断，返回（或抛出）前均已重新持有锁——`await(timeout, unit)` 返回
   `false` 时同样持锁（可安全复查谓词再决策），中断在重新入锁后才抛
   `InterruptedException`（JDK 保真）；await 前持有的 N 级重入被一步清零，返回后
   从 1 级起——解锁按"持有 1 级"书写，多出的 N-1 次 `unlock()` 会抛
   `IllegalMonitorStateException`。超时与唤醒同时收束时返回值按先收束者如实呈现
   （返回值是 best-effort，谓词才是真相）；
3. **双层权限异常**：`signal`/`signalAll` 在非持有线程上调用抛
   `IllegalMonitorStateException`，两源同型——本地先行（未持有，零请求）与
   服务端权威复查（持有已丢的窗口，如失锁后继续 signal，线路码 `NOT_HELD`）。
   `await` 的权限**仅本地检查**：服务端不查 await 登记者持有状态（跨换主重挂
   所必需），误用登记的 ghost 受合并护栏钳制、随会话死亡/LEAVE/换主三路回收
   ——显式降级面。空集/无此 name/key 不存在的 signal = 无操作正常返回（JDK 对齐）；
4. **成本模型**：折叠 await 完整闭环至少 2 次网络往返（挂起受理 1 次 + 唤醒后
   重发 1 次；重发环未即授予继续轮转，每轮再加 1 次）；`signal`/`signalAll` 各
   1 次往返即时回执。等待期间服务端仅一条登记，无长连接专属资源；
5. **持有者死亡不代为唤醒（无人 signal 则永睡）**：租约到期/进程死亡的 sweep
   只唤醒**等待队列入队者**，条件等待者不动；等待者自身无租约、无续租义务。
   **生产代码推荐 `await(timeout, unit)` 形态自救**——无限 `await()` 在无人
   signal 的拓扑里就是无限睡眠；
6. **换主分层：等待是承诺、signal 是事件**：等待项随获取车道迁移**自动重挂**
   （同请求标识幂等登记，对应用透明，新 Leader 上照常接收唤醒）；换主窗口内
   发出的 signal 不重放、不补偿（有界窗）。对照 topic 同型句：承诺/登记侧由
   重放与重挂兜底幸存，事件侧（signal/消息）丢失即丢失；
7. **支持面与不提供**：仅 REENTRANT/FAIR/SIMPLE 三互斥形态可挂条件；读写锁
   句柄（`OReadWriteLock` 所得）调 `newCondition` 抛 `UnsupportedOperationException`
   （本地裁决、零请求）。**不提供 `awaitNanos`/`awaitUntil`/`awaitUninterruptibly`
   及任何异步对偶**。FAIR 位次声明：搬运项按搬运时刻入队，被 signal 者不优先于
   搬运前已入队的先辈；
8. **合并护栏与版本门**：条件等待人数与等待队列按"本 key 等待项"合计口径共用
   `max-queue-depth-per-key`——**v9 零新增配置**；超限时 await 收 `OVERLOADED`
   （异常时不持锁，走失锁/重试惯例）。需 v9 握手（升级序先服务端后客户端）：
   v≤8 会话发 `CONDITION_OP` 或携带 `condition` 字段的 ACQUIRE 得
   `INVALID_REQUEST` 消息级拒绝、不断连。

## 相位器（OPhaser，v10）

跨进程按相位会合的泛化结构，对应 JDK `Phaser`。句柄无状态、无需关闭；同 key
句柄（含跨进程）绑定服务端同一账簿。生产者-阶段流水惯用法：

```java
OPhaser ph = client.newPhaser("pipeline");   // 构造零网络
ph.register();                                // 本线程/本参与者注册（返回当前相位）
// ...生产工作...
int 到场相位 = (int) ph.arriveAndAwaitAdvance(); // 到场并等本相位全员到齐
// 返回时该相位已（或即将）合拢；相位号单调递增，下一轮继续 arriveAndAwaitAdvance
ph.arriveAndDeregister();                     // 离场：扣本会话一个配额
```

读数与旁观（不需要参与也能等）：

```java
long seen = ph.getPhase();                    // 一次 QUERY 往返（advisory 读数）
ph.awaitAdvanceInterruptibly(seen, 5, TimeUnit.SECONDS); // 等相位推进越过 seen
```

语义边界（务必知晓，详见 [01 核心概念 §12](01-concepts.md)）：

1. **配额按会话记账，离场只扣本会话**：`arriveAndDeregister` 在无配额的会话上
   调用抛 `OpenLatchException`（`INVALID_REQUEST`）——JDK 匿名 party 的未定义
   行为在此显式化拒绝；会话死亡时其全部未离场配额隐式摘除（应到集合缩小可
   当场合拢，存活方不空转），**已计入的到场事实不撤销**；
2. **注册数跨相位存续、无一次性定型**：与屏障 parties 断言不同，phaser 没有
   "首次主张须匹配"面——`register` 何时都合法、`bulkRegister(n)` 即加 n；
   全员离场后账簿**空转**（相位保持、配额 0），后续注册自当前相位恢复运转；
   **无终止态**（不提供 `isTerminated`/`forceTerminated`，与 JDK 归零即终止相反，
   理由见概念篇）；
3. **无 `onAdvance` 钩子**：相位动作以"返回值判别 + 本地执行"替代——
   `arriveAndAwaitAdvance()` 回显的到场相位号可判"我是否为该相位最后一发"
   （配合 `getArrivedParties()`），但钩子与他人醒转**无先后保证**（不承诺
   "动作先于全体放行"，屏障的两阶段动作在此刻意不采纳）；
4. **返回值与相位号宽度**：各方法相位返回值为 `long`（JDK `int` 在 2^31 个
   相位后回绕，本库账簿单调 long）；`awaitAdvance(long)` 入参是**你已见的
   相位号**，返回是了结时刻的当前相位（严格大于入参）；
5. **成本模型**：每次调用至少 1 个 RTT（注册/到场/离场/查询各 1 次；
   `arriveAndAwaitAdvance` 会合全程 = 到场 1 次 + 唤醒后了结重发 1 次起）；
   等待以分片重发承载保活语义——服务端唤醒丢失（如换主）由下一次重发即刻
   了结或续挂，**无丢失窗**（对照条件"换主窗 signal 不补偿"：这里的等待谓词
   在复制账簿，自愈是结构性的）。高频 `getPhase()` 就是高频网络——应用侧节流；
6. **超时纪律**：`awaitAdvance(long)` 以等待总超时兜底（默认 30s，
   `OpenLatchTimeoutException`），需要 JDK"无限等"语义请外层循环重入；
   `awaitAdvanceInterruptibly(phase, timeout, unit)` 预算耗尽抛
   `TimeoutException`（先尽力 CANCEL 撤销挂起；撤销丢失由护栏与清理三路回收
   兜底，ghost 等待受 `max-queue-depth-per-key` 钳制）；中断收束同路径并保留
   中断位；
7. **不提供清单**：父子分层派生（`parent` 构造不收）、终止面、
   `bulkArriveAndDeregister` 对偶（循环 `arriveAndDeregister` 即可）；读数
   （`getPhase` 族）为 Leader 本地零日志、**不承诺线性化伴随**——与 v4 原子
   GET"读亦经提交"的判例刻意不一致（相位读数返回即刻可能过期）；
8. **护栏与版本门**：`max-parties-per-phaser`（默认 1024、[1,65536]）限单键
   注册总数——超限 `REGISTER` 收 `OVERLOADED`、既有配额零扰动；挂起等待受
   `max-queue-depth-per-key` 合并口径（纯 `awaitAdvance` 超限 `OVERLOADED`；
   `arriveAndAwaitAdvance` 的挂起半程恒宽容）。需 v10 握手（升级序先服务端后
   客户端）：v≤9 会话发 `PHASER_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连。

## 延时触发（OTimer，v11）

定时单次标记：装载一枚未来某刻响一次、全体共见的标记，对应 JDK `java.util.Timer`
的可判定子集。句柄无状态、无需关闭、构造零网络（无"构造即预装"，装载恒显式）。
到期闹钟惯用法：

```java
OTimer t = client.newTimer("daily-report");   // 构造零网络
long 代次 = t.schedule(6, TimeUnit.HOURS);     // 6 小时后响一次，返回新代次
// 另一进程/稍后：
boolean 响了 = t.await(7, TimeUnit.HOURS);      // true=见到标记；false=超时
if (t.isFired()) { /* 全体共见同一次到期，不消耗 */ }
t.schedule(6, TimeUnit.HOURS);                  // 重装载：开新一轮（换代清钟）
t.disarm();                                     // 撤销：当代终结，在等者收异常
```

语义边界（务必知晓，详见 [01 核心概念 §13](01-concepts.md)）：

1. **全体共见、不消耗**：一次 `schedule` 恰一发，到点后任何到达的 `await`/
   `isFired` 恒即刻通过（标记不需被取走）——与 `ODelayQueue` 延时形态的
   "一元素一消费者、take 即改变所有人可见状态"是两台机器：广播单次标记用
   timer，延时交接用队列；
2. **改期以最新代为准**：重装载换代清钟，已挂起的 `await` 以最新代到期时刻为准
   （推后多睡、提前先响皆合法，无"旧代承诺"）；`disarm` 使当代终结并**即时**
   唤醒全体在等者——`await()`/`await(timeout)` 收到当代被撤销时抛
   `OpenLatchException`（"等待不可再满足"异常终态），**不是**布尔 `false`
   （`false` 仅承载超时，两形态不混读）；
3. **装载者死亡钟照响**（死亡语义第三形态）：装载方进程/会话死亡不影响触发——
   账簿绑定 key、死亡零扰动，旁观者照共见；等待位随装载方会话灭但钟不停
   （与屏障"死亡即破障"、phaser"死亡即摘除"并列，是 timer 相对进程本地
   `Timer` 的核心增量）；
4. **无周期、无回调、无绝对时刻**（三不提供）：`schedule(task, delay, period)`
   不提供——服务端周期自触即常驻重发变异，与"到期零条目"裁决相反，客户端
   循环 `schedule` 即等价；无任务载荷/回调（服务端不执行用户代码，触发形态是
   标记位+唤醒）；`scheduleAt(Instant)` 不收（客户端墙钟偏差注入即污染判据，
   绝对到期时刻恒由服务端应用点折算，仅收相对 `delay`）；
5. **成本与精度模型**：`schedule`/`disarm`/每次读数各 1 个 RTT；`await` 含挂起
   期分片保活重发。到点唤醒滞后一个服务端扫描周期（默认 200ms，正确性不依赖
   精度——谓词恒可重判）。装载条目率恒等于用户显式操作频率（到期零条目）；
6. **等待跨换主自愈**：唤醒谓词在复制账簿，换主窗丢失的通知由重发即刻了结
   （已到期 `true`、已撤销异常、未到期续挂），无丢失窗（沿 phaser 同侧证据，
   对照条件 signal 丢失窗）；
7. **advisory 读数与钟偏移**：`isFired`/`isArmed`/`getRemainingMillis` 是
   Leader 本地零日志读数（即刻过期是契约），"已否到期"随判定节点本地时钟——
   跨节点可见偏差 ≤ 节点间时钟偏移（显式降级，管理呈现面不折算、恒呈原始
   `{代次, armed, fire_at_ms}`）；
8. **护栏与版本门**：`max-timer-horizon-ms`（默认 24h）限单次 `delay`——超限
   `INVALID_REQUEST`（参数线，判定唯一在受理点、条目侧不复核）；挂起等待受
   `max-queue-depth-per-key` 合并口径（超限 `OVERLOADED`）；唤醒精度
   `timer-ready-tick-ms`（默认 200ms，与队列 tick 分轨）。需 v11 握手（升级序
   先服务端后客户端）：v≤10 会话发 `TIMER_OP` 得 `INVALID_REQUEST` 消息级
   拒绝、不断连。

## 异步用法

```java
client.acquireAsync(new AcquireSpec(...))
      .thenAccept(grant -> { ... });     // ⚠️ 完成回调在网络线程：只做轻量状态转移
client.releaseAsync(key, leaseToken, threadId, permits);   // 以授予时签发的凭据释放
lock.lockAsync();                        // CompletionStage 形态
lock.tryLockAsync(2, TimeUnit.SECONDS);
```

规则只有一条：**链式回调里不得阻塞**（future 在完成它的网络线程上执行）。锁丢失通知走
独立单线程，同样禁止阻塞。

## 指标接入

builder 传入宿主 `MeterRegistry`（Micrometer，需自备依赖、不传递）即导出客户端四指标
（请求计数/耗时、重连计数、失锁计数），详见 [08](08-observability.md)。

## 常见坑速查

| 症状 | 原因与出路 |
|---|---|
| 同线程再次 `lock()` 简单锁后卡到超时 | SIMPLE 非可重入——换 REENTRANT 或收敛重入路径 |
| 读写锁"持读要写"死等到租约到期 | 无升/降级特判——先释放再取，或改可重入锁 |
| 临界区做着做着收到失锁回调 | 超时/停顿使续租没跑赢租约——回调里中止提交；调大租约或缩短临界区 |
| unlock 不报错但锁其实早没了 | 语义如此：丢失后 unlock 静默——以失锁回调为唯一真相 |
| 集群下偶发 `NOT_LEADER` | 正常改道信号，客户端自动处理；持续出现看 [09](09-troubleshooting.md) |

## 下一步

Spring 工程免写样板 → [04 Spring Boot Starter](04-spring-boot-starter.md)
