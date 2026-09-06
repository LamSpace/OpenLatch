# Proposal: phase3-t1-extended-lock-types

## Why

Phase 3 设计说明书 §2/§10.1 的 T1 要求交付三类扩展锁原语（FairLock 显式化、Semaphore、CountDownLatch），覆盖概要设计 §4.1 的 P2 功能项。Phase 1/2 已交付的 `openlatch-core` 仅面向互斥/读写锁（`LockEntry`），协议 v2 无对应类型与消息；且设计说明书 §2.1 存在协议缺口——Semaphore 总许可数与 Latch 初始计数均无设定通道，需在本变更定夺补齐。

## What Changes

- **协议升至 v3**（增量，不破坏 v1/v2）：`LockType` 新增 `LOCK_TYPE_FAIR=4`、`LOCK_TYPE_SEMAPHORE=5`、`LOCK_TYPE_LATCH=6`；`AcquireRequest` 新增 `permits`（请求许可数）、`permits_total`（总许可数，首次定型）；`ReleaseRequest` 新增 `permits`（归还数）；新增 `LATCH_COUNT_DOWN=8`、`LATCH_AWAIT=9` 消息（`LatchAwaitRequest` 携带 `count`，首次定型）；服务端继续接受 v1/v2 客户端，新类型/新消息对 v1/v2 客户端拒绝。
- **core 条目抽象上移**：新增 `KeyEntry` 接口承载生命周期与清理契约（类型判别、`isEmpty`、会话摘除、租约摘除），`LockEntry` 改为实现；`SemaphoreEntry`、`LatchEntry` 新增；`LockTable`/`LeaseManager`/`SessionRegistry`/`CoreEngine` 骨架面向抽象操作。**回归底线：既有 core 测试全绿、互斥锁行为零变化。**
- **跨类型误用显式拒绝**：key 条目由首次请求定型家族（LOCK/SEMAPHORE/LATCH），`FAIR` 与 `REENTRANT` 同族互通；跨家族请求新增 core 侧 `Outcome.REJECT_TYPE_MISMATCH`，协议层统一映射 `INVALID_REQUEST`。
- **Semaphore**：严格 FIFO 仅队首授予（防大请求饥饿）、重入按次累加、租约到期归还该持有者全部许可、超额释放拒绝；看门狗与锁复用。
- **CountDownLatch**：一次性倒计数屏障；`countDown(n)` 走独立消息（进复制日志），归零瞬间向全部 awaiter 广播（复用 `AWAIT_NOTIFY`）；`await` 不入日志、等待者无租约、断连随会话摘除；归零后永久放行、不支持重置。
- **客户端**：`newFairLock(key)`、`OSemaphore`（`acquire/acquire(n)/tryAcquire/release/release(n)`）、`OCountDownLatch`（`await/countDown`）；starter 注解 `type` 新增 `FAIR` 取值（Semaphore/Latch 无声明式语义，不进注解）。
- **集群**：Semaphore 授予/释放/到期与 Latch countDown 走复制日志；两原语等待队列维持 Leader 内存（`WaitQueue` 节点携带请求许可数），与既有锁一致；快照 proto 扩展表达新条目状态。
- **FairLock 公平性回归套件**升格为独立常开 CI 套件（单机/服务端端到端/集群三档断言授予顺序==排队顺序）。

## Capabilities

### New Capabilities

（无——全部为既有能力的增量。）

### Modified Capabilities

- `wire-protocol`: v3 版本门控、`LockType` 三新取值、`permits`/`permits_total` 字段、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息、字段合法性与 v1/v2 拒绝规则。
- `core-lock-engine`: KeyEntry 家族定型与 `REJECT_TYPE_MISMATCH`；Semaphore 授予/重入/到期归还/超额释放语义；Latch 倒计数/归零广播/一次性/无租约语义。
- `client-sdk`: `newFairLock`、`OSemaphore`、`OCountDownLatch` API 与等待-通知-重发闭环的复用扩展。
- `replicated-state-machine`: 新命令的复制边界（countDown 进日志、await 不入日志）、`WaitQueue` 许可数感知的队首推进、failover 下新原语行为与锁一致。
- `snapshot-recovery`: 快照内容清单扩展——Semaphore/Latch 条目状态的序列化与恢复往返。
- `spring-boot-starter`: `@OpenLatch` 注解 `type` 新增 `FAIR` 取值，映射公平互斥语义。

## Impact

- **代码**：`openlatch-protocol`（两个 proto）、`openlatch-core`（lock 包抽象与新条目、`Outcome`/`CoreEngine` 分派）、`openlatch-server`（`RequestDispatcher` 合法性预检与消息路由、`LockStateMachineCore` 新条目应用、`WaitQueue` 许可数、快照 `SnapshotState` 构建、协议版本门控）、`openlatch-client`（新 API、`AwaitTracker`/看门狗复用）、`openlatch-spring-boot-starter`（类型枚举）。
- **测试**：既有全部测试为 P3-01 回归底线；新增 core 语义套件、消息级套件、客户端 IT、公平性常开套件、集群/快照/failover 演练扩展。
- **兼容性**：纯增量；v1/v2 客户端在 Phase 3 服务端行为不变（验收标准 §8-6）；无数据迁移。
- **文档**：设计说明书 §2.1 协议缺口勘误（初始定型通道）需回写。
