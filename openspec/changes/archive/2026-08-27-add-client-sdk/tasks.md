## 1. P1-18 客户端骨架

- [x] 1.1 `openlatch-client/pom.xml` 补齐依赖：netty、`openlatch-protocol`（不得引入 `openlatch-core`）
- [x] 1.2 `ClientConfig` record + `OpenLatchClient.Builder`，§6.7 默认值表逐项落地（address 必填、requestTimeout 5s、defaultWaitTimeout 30s、connectTimeout 3s、退避 200ms/10s、workerThreads 1）
- [x] 1.3 EventLoopGroup、共享 `HashedWheelTimer`、锁丢失回调单线程执行器的创建与关停骨架
- [x] 1.4 验证：编译通过；默认值断言单测全绿

## 2. P1-19 连接与握手

- [x] 2.1 `ConnectionManager` 正向路径：connect → HELLO → `HelloResponse` → ACTIVE；连接超时按 `connectTimeout` 约束
- [x] 2.2 `SessionContext`：sessionId 持有、`AtomicLong` requestId 分配（从 1 起）
- [x] 2.3 验证：对真实服务器的建连握手集成用例通过（含服务端版本拒绝时的失败表现）

## 3. P1-20 请求多路复用

- [x] 3.1 `RequestMultiplexer`：出站收口——登记 `requestId → (future, deadline)` 并挂每请求超时任务
- [x] 3.2 入站响应按 `request_id` 摘除并 complete；未知 id 响应路由给 `AwaitTracker`（D3 补偿入口，此阶段允许其为空实现）
- [x] 3.3 验证：EmbeddedChannel 单测——响应关联、请求超时失败、未知 id 处理全绿

## 4. P1-21 等待跟踪

- [x] 4.1 `AwaitTracker`：QUEUED 挂起（登记 `requestId → (key, threadId, 总超时)`）、等待总超时任务
- [x] 4.2 `AWAIT_NOTIFY` 处理：按 `request_id_ref` 命中挂起项 → 同 requestId 重发；通知晚于超时/失败 → 忽略
- [x] 4.3 重发超时策略（D1）：摘除 inflight 但保持挂起，仅总超时整体失败
- [x] 4.4 孤儿 OK 补偿（D3）：等待完结后短暂保留映射；无主授予 → 发补偿 RELEASE；先写三种孤儿时序的测试再实现
- [x] 4.5 验证：§6.5 边界场景表逐行用例全绿

## 5. P1-22 OLock 同步包装

- [x] 5.1 `HeldLockRegistry`：登记归属（token/grantedLeaseMs/holderThreadId/listeners/lastRenewAtMs），不记重入计数（D4）
- [x] 5.2 `OLock` 实现：`lock()`=限时兜底、`tryLock()` 立即、`tryLock(waitTime)` 限时返回 true/false、`lockAsync`/`tryLockAsync`
- [x] 5.3 `unlock()`：非持锁线程本地抛 `IllegalMonitorStateException`（不发请求）；持锁线程发 RELEASE，按 `fullyReleased` 注销登记
- [x] 5.4 `newReentrantLock`/`newSimpleLock`/`newReadWriteLock` 工厂与 `OReadWriteLock`（readLock/writeLock 映射 LOCK_TYPE）；SIMPLE 自锁警示写入 Javadoc
- [x] 5.5 验证：JUC 风格语义用例全绿（重入计数、非法解锁、isHeldByCurrentThread、限时成功/失败）

## 6. P1-23 看门狗

- [x] 6.1 `Watchdog`：`grantedLeaseMs/3` 周期续租，续租请求超时取 `min(5s, lease/3)`；成功刷新本地到期并重置失败计数
- [x] 6.2 失败判定：`INVALID_TOKEN`/`NOT_HELD`/`SESSION_EXPIRED` 即时失锁；连续 2 次请求超时判失锁；单次超时下周期重试
- [x] 6.3 断连期间跳过发送且不计数（D5）；`fullyReleased` 后注销任务
- [x] 6.4 锁丢失回调：携带 `LockLostException` 原因、走专用执行器、回调异常捕获隔离；全局 + 单锁监听
- [x] 6.5 验证：续租成功路径与三类失败路径用例全绿

## 7. P1-24 重连与锁丢失

- [x] 7.1 断连三连击：挂起 future 快速失败 `ServerUnavailableException`；清空 multiplexer/AwaitTracker；为每个持锁登记 `lostAt` 定时（上次成功续租 + grantedLeaseMs）
- [x] 7.2 指数退避重连循环（200ms×2、上限 10s、±20% 抖动）挂 EventLoop；`shutdown()` 进终态不再重连
- [x] 7.3 `lostAt` 裁决：重连先成功 → 立即触发全部旧锁回调并取消定时；`lostAt` 先到 → 到时回调；回调后清除本地持锁状态
- [x] 7.4 重连成功换新 `SessionContext`（sessionId 更新、requestId 从 1 重来）
- [x] 7.5 预留半开连接测试注入口（可暂停出站写的测试钩子，D7）
- [x] 7.6 `shutdown()`：尽力释放持锁（受限时长）→ 关连接 → 停全部定时与执行器；关停后新请求被拒绝
- [x] 7.7 验证：重连时序用例全绿（含半开连接：看门狗连续超时路径与 `lostAt` 路径分别构造）

## 8. P1-25 集成测试套件（§10.3）

- [x] 8.1 并发互斥：≥16 线程竞争同 key 多轮，临界区计数断言互斥无丢失
- [x] 8.2 tryLock 超时组合：立即式 / 限时成功 / 限时失败
- [x] 8.3 读写并发组合矩阵
- [x] 8.4 看门狗端到端：3s 租约持有 10s 不过期
- [x] 8.5 公平性端到端：授予顺序 == 发起顺序
- [x] 8.6 幂等时序：通知后重发、超时与通知竞争（§6.5 场景表逐项端到端复核）
- [x] 8.7 验证：§10.3 全绿

## 9. P1-26 故障注入套件（§10.4）

- [x] 9.1 持锁中断连：直接关 Channel（不 unlock），短租约下断言一个租约期内其他客户端可获取
- [x] 9.2 等待中断连：排队后关 Channel，断言快速失败且后续授予顺序无残留
- [x] 9.3 杀服务端进程：`ProcessBuilder` 起 shaded jar、`destroyForcibly()`，断言请求超时内失败、重启后自动恢复
- [x] 9.4 半开连接：经 7.5 注入口暂停出站写，断言服务端空闲检测断连与会话清理
- [x] 9.5 验证：§10.4 全绿；M3 退出核对——概要设计 §4.3 标准 1–3 达成
