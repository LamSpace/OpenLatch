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
