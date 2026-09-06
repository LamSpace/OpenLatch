# Tasks: phase3-t1-extended-lock-types

判定基线：每组末尾的验证命令一律 `mvn -s /home/lam/repo/settings.xml`（模块级 `-pl ... -am test`）；新增 Java 源文件按 CLAUDE.md §5 配齐 Javadoc。

## 1. P3-01 KeyEntry 抽象上移（纯重构，独立提交）

- [x] 1.1 新增 `KeyEntry` 接口（`key()/isEmpty()/typeFamily()/removeSession/forceExpire`/租约字段读取，Javadoc 按契约级撰写）；`LockEntry` 改为实现，行为零改动
- [x] 1.2 `LockTable` 泛型改 `KeyEntry`；`CoreEngine` 五个入口（acquire/release/renew/expireDue/sessionClosed）面向抽象操作，锁路径逐行等价
- [x] 1.3 core `Outcome` 新增 `REJECT_TYPE_MISMATCH`、`ReleaseStatus` 新增 `OVER_RELEASE`（本版仅类型不匹配路径可达）；server `RequestDispatcher` 映射两码至 `INVALID_REQUEST`
- [x] 1.4 验证：`git diff` 确认锁语义文件零逻辑改动；既有 core 6 测试类 + server/client 全量测试一字不改全绿（**回归底线**）

## 2. P3-02 FairLock

- [x] 2.1 协议与 core `LockType` 新增 `FAIR`（与 `REENTRANT` 同族互通：条目定型规则、互斥矩阵推广）
- [x] 2.2 客户端 `newFairLock(key)`；starter `@OpenLatch` `type = FAIR` 取值
- [x] 2.3 `FairOrderingSuite`：core 直驱 + server 端到端 + 集群三档，`FAIR`/`REENTRANT` 参数化跑同一"授予序==排队序"矩阵（手工时钟，无 sleep）；确认落 surefire 默认 include、不入 `-Pdrill`
- [x] 2.4 验证：互通/别名用例 + 公平性套件全绿并入 CI 常开

## 3. P3-03 Semaphore（core + 协议）

- [x] 3.1 proto：`LockType.LOCK_TYPE_SEMAPHORE`、`AcquireRequest.permits/permits_total`、`ReleaseRequest.permits`；`MessageLegality` 校验（非 SEMAPHORE 携带 permits>1/permits_total>0 拒绝、SEMAPHORE 建条目必填 permits_total）
- [x] 3.2 `SemaphoreEntry`（D4 规则序：重入→队首足量→DENIED→入队携 permits）；`CoreEngine` 家族分派接入；到期归还全部许可 + `OVER_RELEASE` 拒绝路径
- [x] 3.3 `CoreEngineSemaphoreTest`：D4 场景矩阵（队首不足无人获授、按序授予、重入不受队首约束、立即式、到期整体归还、超额释放、会话关闭不泄漏）+ 类型不匹配矩阵（三家族两两互斥、FAIR≡REENTRANT）
- [x] 3.4 验证：§7 T1 Semaphore 语义用例全绿；v1/v2 请求新类型被消息级拒绝（`HandshakeTest` 扩展）

## 4. P3-04 Semaphore（客户端）

- [x] 4.1 `OSemaphore` API（acquire/acquire(n)/tryAcquire/tryAcquire(timeout)/release/release(n)），QUEUED→AWAIT_NOTIFY→幂等重发闭环复用，看门狗复用
- [x] 4.2 `ClientSemaphoreIT`（单机 + 集群两组，仿 ClientReadWriteIT）：阻塞获授、tryAcquire 不排队、续租失败丢失通知；仿 ClientProcessKillIT 的强杀归还用例
- [x] 4.3 验证：端到端用例全绿

## 5. P3-05 CountDownLatch（core + 协议）

- [x] 5.1 proto：`LockType.LOCK_TYPE_LATCH`、`MessageType.LATCH_COUNT_DOWN=8`/`LATCH_AWAIT=9`、四 payload（`LatchAwaitRequest.count` 首请求定型，0=纯加入）；`Envelope` oneof 扩展
- [x] 5.2 `LatchEntry`（归零广播全体、一次性、`isEmpty` 即回收（D5）、无租约不入到期堆）；`CoreEngine` latch 命令入口（countDown/await）与 `CoreEventListener` 复用
- [x] 5.3 `CoreEngineLatchTest`：归零全体广播、归零后 await 即过、countDown no-op、纯加入拒、断连摘除不影响计数、多轮 expireDue 零触及
- [x] 5.4 验证：CDL 语义用例全绿

## 6. P3-06 CountDownLatch（客户端）

- [ ] 6.1 `OCountDownLatch` API（await/await(timeout)/countDown/countDown(n)）；无看门狗、断线重连自动重发 await
- [ ] 6.2 `ClientLatchIT`：跨进程倒计数放行、await 零 LEASE_RENEW 流量、超时兜底
- [ ] 6.3 验证：端到端用例全绿

## 7. P3-07 集群行为（T1 退出档）

- [ ] 7.1 raft 日志：`LATCH_COUNT_DOWN_ENTRY`；Semaphore 复用 ACQUIRE/RELEASE 载荷透传；`LatchAwait` 不入日志（Leader 本地裁决 + `WaitQueue` 登记）；**回执码形接正**：`REJECT_SEMAPHORE_TOTAL`/`OVER_RELEASE` 在集群 apply 侧的 ApplyStatus 映射（当前 default→INTERNAL_ERROR，P3-04 集群档实证）
- [ ] 7.2 `WaitQueue.Node` 携带 permits（锁恒填 1）；Leader 队首门控推广至许可数判定（"可用 ≥ 队首请求"才通知）——**P3-04 集群档实证缺口**：Semaphore 部分释放（条目未空、池已腾出）当前经 `freedKeys` 无通知通道（锁的 `fullyReleased` 语义正确、许可族需回执携带 `permits_available` 并影子表镜像许可池）； latch 归零 `purgeKey` 式全体广播
- [ ] 7.3 影子表与快照：`SnapshotState` 条目消息新增 family/许可/latch 字段（编号只增）；`StateComparisons` 扩三类条目
- [ ] 7.4 确定性：`StateMachineDeterminismTest` 扩 Semaphore/Latch 条目（同序列两次回放逐字段一致）
- [ ] 7.5 演练扩展：`ClusterSnapshotTest`/`ClusterSnapshotRecoveryTest` 含新条目往返；`LeaderKillDrillIT`/`RollingRestartDrillIT` 各加 Semaphore/Latch 场景（许可不丢不泄漏、awaiter 重发收敛、failover 后计数一致）
- [ ] 7.6 验证：§7 T1 集群用例矩阵全绿 + 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` 全绿 = **T1 退出**

## 8. 文档与验收证据

- [ ] 8.1 Phase 3 设计说明书 §2.1 勘误回写（`permits_total` 字段、`LatchAwaitRequest.count`、D5 回收推论）
- [ ] 8.2 验收对照：详设 §8 的 T1 相关项（1/2/6）逐项证据索引（CI 常开截图/测试报告）挂 change 目录
