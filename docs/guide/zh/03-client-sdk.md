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
