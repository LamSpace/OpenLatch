## ADDED Requirements

### Requirement: FairLock API

客户端 SHALL 提供 `newFairLock(key)` 创建显式公平承诺的互斥锁：行为与 `newLock`（可重入）逐项等价（互斥、重入、租约、看门狗、丢失通知），并享受服务端公平性承诺（授予顺序等于排队顺序）。

#### Scenario: 公平锁行为等价可重入锁

- **WHEN** 以 `newFairLock` 获取锁后同线程重入、释放、由看门狗续租
- **THEN** 各行为与 `newLock` 一致，服务端按 `LOCK_TYPE_FAIR` 定型

### Requirement: OSemaphore API

客户端 SHALL 提供 `OSemaphore`：`acquire()`、`acquire(n)` 阻塞直至获授（受等待兜底超时约束）；`tryAcquire()` 与 `tryAcquire(timeout)` 立即式/限时式获取；`release()`、`release(n)` 归还许可。许可获取 SHALL 与锁共用租约与看门狗：持有期间由看门狗自动续租，续租失败触发与锁一致的丢失通知；同线程重入按次累加。等待-通知-重发闭环 SHALL 复用 `OLock` 的既有机制（QUEUED → AWAIT_NOTIFY → 幂等重发）。

#### Scenario: 许可耗尽时阻塞并在通知后获授

- **WHEN** 许可全部被占用时线程 `acquire()`，持有者随后释放
- **THEN** 该线程收到通知重发后获授并解除阻塞，全程无 sleep 轮询

#### Scenario: 进程死亡许可不泄漏

- **WHEN** 持有 2 个许可的客户端进程被强杀
- **THEN** 租约到期后 2 个许可归还可用池，等待者获得推进

#### Scenario: tryAcquire 不排队

- **WHEN** 许可不足时调用 `tryAcquire()`
- **THEN** 立即返回 `false`，该请求不出现在服务端等待队列

### Requirement: OCountDownLatch API

客户端 SHALL 提供 `OCountDownLatch`：`await()` 阻塞至屏障归零（受等待兜底超时约束）、`await(timeout)` 限时等待、`countDown()` 与 `countDown(n)` 扣减计数。`newCountDownLatch(key, count)` 创建的创建者句柄在每次请求携带 `total = count` 断言（首次触达即定型，`countDown(0)` 为纯初始化）；`newCountDownLatch(key)` 纯加入句柄不主张初值，对从未定型的屏障被拒。await 等待者 SHALL NOT 持有任何租约、MUST NOT 产生续租流量；断线重连后 SHALL 自动重发 await（幂等），屏障已归零时立即通过。

#### Scenario: 跨进程倒计数放行

- **WHEN** 进程 A 以 count=2 建立屏障并 await，进程 B、C 各 `countDown()`
- **THEN** A 的 await 在计数归零后返回，B、C 的 countDown 各自立即应答剩余计数

#### Scenario: await 无看门狗流量

- **WHEN** 客户端长时间 await（超过一个看门狗周期）
- **THEN** 该会话不因 await 发出任何 LEASE_RENEW 请求
