# client-sdk Specification

## Purpose

为应用提供访问 OpenLatch 锁服务的客户端 SDK：异步内核 + JUC 风格同步包装、全程超时无死等、等待-通知-重发闭环、看门狗续租与锁丢失通知、断连重连与锁丢失裁决、优雅关停。
## Requirements
### Requirement: 客户端构建与连接建立

客户端 MUST 通过 builder 构建，必填服务地址：或单一地址（Phase 1 入口，语义为只含一个种子的种子列表），或种子列表（≥1 个 `host:port`）；二者同配时以种子列表为准，皆缺 MUST 构建失败。其余参数（请求超时默认 5s、等待总超时默认 30s、连接超时默认 3s、重连退避初始 200ms/上限 10s、EventLoop 线程数默认 1）未设置时 MUST 使用默认值。首次使用或显式连接时，客户端 MUST 从种子列表建立连接并完成握手，获得服务端分配的会话；握手完成前发出的业务请求 MUST 不被服务端接受（客户端不得跳过握手直接发业务请求）。

#### Scenario: 默认参数构建

- **WHEN** 应用仅指定服务地址构建客户端并成功连接
- **THEN** 客户端以全部默认参数运行，握手成功并获得会话

#### Scenario: 自定义参数生效

- **WHEN** 应用通过 builder 覆盖请求超时与等待总超时后构建客户端
- **THEN** 客户端按覆盖值执行请求超时与等待兜底

#### Scenario: 种子列表构建

- **WHEN** 应用以多地址种子列表构建客户端，且列表中首个地址不可达
- **THEN** 客户端连接至其余可达种子完成握手，构建参数不因种子形态而改变默认值语义

### Requirement: 异步获取与释放

客户端 MUST 提供异步获取（返回携带租约凭证与实际生效租约的结果）与异步释放接口。获取成功时结果 MUST 携带服务端授予的租约凭证与实际生效租约时长。异步接口的 future MUST 在网络线程上完成；客户端 MUST 以文档明示用户回调不得执行阻塞操作。

#### Scenario: 异步获取成功

- **WHEN** 应用对空闲锁键发起异步获取
- **THEN** future 以携带租约凭证与实际生效租约的结果完成

#### Scenario: 异步释放成功

- **WHEN** 应用以获取时得到的租约凭证异步释放锁
- **THEN** future 正常完成，锁可被其他客户端获取

#### Scenario: 异步释放凭证错误

- **WHEN** 应用以错误租约凭证异步释放锁
- **THEN** future 以明确错误（凭证不匹配）失败，锁不受影响

### Requirement: JUC 风格同步包装语义

客户端 MUST 提供互斥（可重入/不可重入）与读写锁的同步句柄。`lock()` 等价于以待等待总超时兜底的限时获取；`tryLock()` 为立即式；`tryLock(waitTime)` 为限时式；到时未授予 MUST 返回 false（而非抛异常）。非持锁线程调用 `unlock()` MUST 抛 `IllegalMonitorStateException` 且不发送任何释放请求。`isHeldByCurrentThread()` MUST 反映当前线程的持有状态。可重入锁的重入计数 MUST 由服务端维护：客户端每次 `unlock()` 发送一次释放请求，仅当服务端回复计数归零时才结束本地持锁状态，客户端不得本地累计重入次数。不可重入锁的同持有者再次获取将排队等待自身直至租约到期，该行为 MUST 在文档中明确警示。

#### Scenario: lock 受兜底超时约束

- **WHEN** 锁被其他客户端长期持有，当前客户端调用 `lock()`
- **THEN** 在等待总超时（默认 30 秒，可配置）内返回或抛出超时异常，不存在无限阻塞

#### Scenario: tryLock 立即式

- **WHEN** 锁已被占用，当前客户端调用 `tryLock()`
- **THEN** 快速返回 false，不进入等待

#### Scenario: tryLock 限时成功与失败

- **WHEN** 锁在等待时限内被释放 / 锁在等待时限内未被释放
- **THEN** `tryLock(waitTime)` 相应返回 true / false

#### Scenario: 重入与按计数释放

- **WHEN** 同一线程对可重入锁连续获取两次后调用两次 `unlock()`
- **THEN** 第一次释放后仍持锁（其他客户端不可获取），第二次释放后锁可被其他客户端获取

#### Scenario: 非持锁线程解锁被拒

- **WHEN** 未持有锁的线程调用该锁的 `unlock()`
- **THEN** 抛出 `IllegalMonitorStateException`，且服务端未收到释放请求

### Requirement: 全程超时保证

客户端发出的每一个网络请求（获取、释放、续租、握手）MUST 携带超时（默认 5s），超时后对应操作 MUST 以超时异常失败。等待类操作的总时长 MUST 受调用方时限或等待总超时兜底。客户端不存在无超时的阻塞等待路径。同一请求标识因重发等原因重复登记时，MUST 保证不丢失任何已挂起调用方的完成机会：先前登记的在途项 MUST 以超时异常或响应完成，其超时任务 MUST NOT 误伤后来登记的同标识在途项（按条目身份摘除）。

#### Scenario: 请求超时快速失败

- **WHEN** 服务端对某一请求在请求超时时限内无响应（如进程挂起）
- **THEN** 对应操作以超时异常失败，调用方可感知并重试，无线程死等

#### Scenario: 同标识并发登记全部有界完成

- **WHEN** 同一请求标识的在途请求尚未返回期间，同标识请求再次发出（如通知密集触发两次重发），且第一条始终无响应
- **THEN** 两条在途项各自在其超时界限内完成（响应或超时异常），无一永久挂起；后一条的响应/超时不误摘前一条的登记，反之亦然

### Requirement: 等待-通知-重发闭环

排队等待的获取请求在收到服务端的队首通知后，MUST 以原请求标识重发获取请求（服务端幂等，不会二次排队）；重发被授予则完成等待，仍排队则继续挂起等待下一次通知。等待项在等待总时限到达时 MUST 以超时失败；此后到达的通知 MUST 被忽略（不重发）。已超时/已失败的等待若在途重发随后被授予，客户端 MUST 归还该锁（发送释放），不得静默泄漏。重发请求本身超时（无响应）时，MUST 仅结束该次重发、保持挂起等待下一次通知，直到等待总时限到达才整体失败。同一等待收到两次通知而产生的重复授予，MUST 以首个授予为准，重复授予归还。

#### Scenario: 通知后重发授予

- **WHEN** 排队的获取请求收到队首通知，且锁已可授予
- **THEN** 重发请求被授予，等待完成，不产生二次排队

#### Scenario: 超时后通知被忽略

- **WHEN** 等待已因总时限到达而失败，之后到达队首通知
- **THEN** 通知被忽略，不发生重发

#### Scenario: 超时后在途重发被授予则归还

- **WHEN** 等待超时失败前发出的重发在超时之后才被授予
- **THEN** 客户端归还该锁，锁可被其他客户端获取

#### Scenario: 重发无响应保持挂起

- **WHEN** 重发的获取请求在请求超时时限内无响应
- **THEN** 该次重发结束但等待保持挂起，在后续通知到达时可再次重发，直到等待总时限

#### Scenario: 重复通知重复授予归还

- **WHEN** 同一等待先后收到两次通知，两次重发均被授予
- **THEN** 首个授予交付调用方，第二个授予被归还

### Requirement: 看门狗续租与锁丢失通知

持有中的锁 MUST 由客户端自动续租，续租周期为实际生效租约的三分之一。续租成功 MUST 刷新本地到期时间。续租收到明确失效错误（凭证不匹配、未持有、会话失效）MUST 立即判定锁丢失并触发锁丢失回调。续租请求超时 MUST 记录连续失败次数，连续 2 次超时 MUST 判定锁丢失并触发回调；收到服务端过载（`OVERLOADED`）、内部错误或 `NOT_LEADER` MUST 与超时同等对待（计入连续失败，下一周期重试；`NOT_LEADER` 随附的 Leader 提示供故障转移机制改道后续请求）。锁成功释放（计数归零）后 MUST 停止该锁的续租。锁丢失回调携带失效原因，在独立于网络线程的执行器上调用；单个回调抛出的异常 MUST 被捕获，不影响其他锁的续租与回调。客户端支持全局锁丢失监听与单锁锁丢失监听；按 key 注册的监听器 MUST 在该 key 的锁彻底释放（计数归零）后丢弃登记——长生命周期客户端下监听表不随历史 key 基数无界增长；该 key 之后再次被获取时，先前监听器不复活，调用方 MUST 重新注册。

#### Scenario: 长任务续租不过期

- **WHEN** 客户端以较短租约获取锁并持有远超租约期的时长
- **THEN** 看门狗周期续租使锁全程不被服务端到期释放

#### Scenario: 明确失效错误即时回调

- **WHEN** 续租收到凭证不匹配或未持有错误（如服务端重启后锁已不存在）
- **THEN** 立即停止续租并触发锁丢失回调，不等待重试

#### Scenario: 连续两次超时判定丢失

- **WHEN** 续租连续两次请求超时
- **THEN** 判定锁丢失并触发回调；仅一次超时则下一周期继续重试

#### Scenario: 过载错误计入连续失败

- **WHEN** 续租收到 `OVERLOADED`（或 `INTERNAL_ERROR`）响应
- **THEN** 计入连续失败次数，下一周期重试；连续两次即判定锁丢失

#### Scenario: 选举空窗 NOT_LEADER 计入连续失败

- **WHEN** 续租恰逢集群选举空窗收到 `NOT_LEADER`（`leader_node_id = -1`）
- **THEN** 计入连续失败次数，下一周期在新 Leader（或经提示改道后）重试；选举在两个续租周期内完成时锁不丢

#### Scenario: 释放后停止续租

- **WHEN** 客户端成功释放锁（计数归零）
- **THEN** 不再发送该锁的续租请求

#### Scenario: 回调异常隔离

- **WHEN** 某锁的锁丢失回调抛出异常
- **THEN** 异常被捕获并记录，其他锁的续租与回调不受影响

#### Scenario: 监听表不随历史 key 无界增长

- **WHEN** 客户端先后对大量不同 key 注册单锁监听并全部彻底释放
- **THEN** 已释放的 key 不再保留监听器登记；该 key 再次被获取并丢锁时，先前监听器不触发，需调用方重新注册

### Requirement: 断连快速失败与重连

连接断开时，所有挂起中的获取/释放/续租操作 MUST 以"服务不可用"异常快速失败。客户端 MUST 自动重连：指数退避（初始 200ms、倍增、上限 10s、每次 ±20% 抖动），直至成功或客户端关停；集群形态下重连目标 MUST 先试原地址，失败后按序轮询种子列表。重连成功 MUST 获得新会话，旧会话标识的残留响应 MUST 被丢弃。等待中的操作不因断连自动重试，由调用方决定重试。

#### Scenario: 断连时挂起操作快速失败

- **WHEN** 客户端持有挂起的获取请求时连接断开
- **THEN** 该请求以"服务不可用"异常快速失败，不阻塞到超时

#### Scenario: 自动重连恢复服务

- **WHEN** 服务端重启，客户端在退避窗口内重连成功
- **THEN** 后续请求在新会话上正常完成；旧会话的残留响应不影响新请求

#### Scenario: 原地址不可达轮询种子

- **WHEN** 客户端断连且原接入节点进程终止、种子列表中另有存活节点
- **THEN** 重连先试原地址失败后轮询至存活节点完成握手，全程受退避与请求超时约束、无无限等待

### Requirement: 断连期间的持锁丢失裁决

断连发生时，客户端 MUST 为每个本地持锁计算失锁时刻（上次成功续租时刻 + 实际生效租约）。若在失锁时刻前重连成功，旧会话已被服务端清理、旧锁必然失效，客户端 MUST 在重连成功时立即对全部旧持锁触发锁丢失回调；若到失锁时刻仍未重连成功，MUST 在该时刻触发锁丢失回调。锁丢失后客户端对该锁的本地持锁状态 MUST 清除。

#### Scenario: 重连成功即通知锁丢失

- **WHEN** 客户端持锁期间断连，随后在失锁时刻前重连成功
- **THEN** 全部旧持锁立即触发锁丢失回调，本地持锁状态清除

#### Scenario: 失锁时刻到达仍未重连

- **WHEN** 客户端持锁期间断连，直到失锁时刻仍未重连成功
- **THEN** 在该时刻触发锁丢失回调

### Requirement: 优雅关停

`shutdown()` MUST 尽力释放本地持有的锁（发送释放请求，受限时长约束），随后关闭连接并停止重连与全部后台调度。关停后客户端进入终态：新请求 MUST 被拒绝，且不再发起重连。

#### Scenario: 关停释放持锁

- **WHEN** 客户端持有锁时调用 `shutdown()`
- **THEN** 尽力发送释放请求后关闭，其他客户端可获取该锁（至迟在租约到期后）

#### Scenario: 关停后不再重连与受理请求

- **WHEN** 客户端已关停
- **THEN** 不再发起重连；新的获取/释放请求被拒绝

### Requirement: Leader 发现与故障转移

集群形态下客户端 MUST 自动发现并跟随 Leader：

1. **启动发现**：对任一种子完成 HELLO 后，若响应提示的 Leader 有效且非当前连接节点，客户端 SHALL 直连该 Leader 承载后续新获取请求；提示无效（选举中）则退避后重新 HELLO；
2. **NOT_LEADER 重定向**：业务请求收到 `NOT_LEADER` 且提示有效时，客户端 MUST 改连提示所指节点并重放该未完成请求——同会话内重放 MUST 复用原 `requestId`（服务端幂等），因改连产生新会话时 MUST 使用新会话的 `requestId` 空间；
3. **存量锁跟家**：每把已持有锁的续租/释放 MUST 经由该锁获取时的会话所在连接发出（会话归属节点降级不影响其存量锁经转发车道续租/释放）；新获取经由指向当值 Leader 的连接；
4. **强制发现**：连续 3 次业务请求收到 `NOT_LEADER` MUST 触发一次对种子列表的逐一发现（`CLUSTER_VIEW` 或 HELLO），以修复陈旧提示导致的改道失败；
5. **快速失败**：整个发现/重定向过程 MUST 受该请求的请求超时（默认 5s）约束，到时未找到可用 Leader MUST 以异常向调用方失败，由应用决定重试；客户端 MUST NOT 有无超时的故障转移等待路径；
6. **等待中请求**：排队等待的获取在故障转移后 MUST 向新 Leader 重新排队（等待位次重置为既定语义），且重排队后收到队首通知的"通知-重发"闭环保持成立。

单机形态（服务端无 v2 提示能力）下以上机制 MUST 不激活，行为与 Phase 1 一致。

#### Scenario: 启动直连 Leader

- **WHEN** 客户端以种子列表启动，首个建连的节点是 Follower 且 HELLO 提示指向另一节点的 Leader
- **THEN** 客户端随后对空闲 key 的获取在提示所指节点完成（可通过该节点侧观察到授予发生）

#### Scenario: NOT_LEADER 重定向重放

- **WHEN** 客户端向 Follower 发起获取收到 `NOT_LEADER` + 有效提示
- **THEN** 客户端改连提示节点重放该获取，调用方仅感知一次成功应答、无重复授予

#### Scenario: 陈旧提示的强制发现

- **WHEN** 提示所指节点已不可达，客户端连续 3 次请求收到 `NOT_LEADER`
- **THEN** 客户端逐一查询种子列表重新定位 Leader 并完成重放，全程在请求超时预算内或按时失败

#### Scenario: failover 期间持锁不丢

- **WHEN** 客户端持有锁（会话归属节点 F 存活），Leader 被终止并完成切换
- **THEN** 看门狗经 F 的续租经转发车道持续成功，failover 后释放该锁成功；锁在整个窗口不丢失

#### Scenario: 快速失败不无限等待

- **WHEN** 集群全部节点不可达（或选举始终未成立）时客户端发起获取
- **THEN** 请求在请求超时（默认 5s）内以异常失败，不进入无界等待

#### Scenario: 等待者跨 failover 重新排队

- **WHEN** 客户端 A 持锁、客户端 B 排队等待，Leader 切换
- **THEN** B 的等待向新 Leader 重新排队并最终在 A 释放后获授；通知-重发闭环在新 Leader 上生效

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

### Requirement: 客户端可选监控指标

客户端 SHALL 支持可选的自身请求观测指标，经构建器注入外部度量注册表启用：`openlatch.client.requests.total{type,status}`（按请求消息类型与结果计数，结果区分成功应答、业务拒绝、超时、发送失败）、`openlatch.client.request.duration`（请求发出至响应完成/失败的耗时）、`openlatch.client.reconnect.total`（重连发起次数）、`openlatch.client.locks.lost.total`（锁丢失判定次数）。未注入注册表时 MUST 默认关闭：零计数、零额外分配路径，客户端行为与不引入该特性时完全一致。启用指标 MUST NOT 改变任何既有行为契约（超时值、重试与同 id 重发、看门狗节奏、失锁判定）。对宿主应用依赖的度量库 MUST NOT 成为客户端的强制传递依赖（宿主不用则不引入）。

#### Scenario: 默认关闭零开销

- **WHEN** 未注入注册表构建客户端并执行获取/释放
- **THEN** 无任何指标记录路径被触发，行为与现状一致

#### Scenario: 注入后计数准确

- **WHEN** 注入注册表后执行固定脚本（成功获取若干、业务拒绝若干、一次超时）
- **THEN** `requests.total` 各 `{type,status}` 序列计数与脚本逐项吻合，duration 样本数与请求总数一致

#### Scenario: 断线与失锁计数

- **WHEN** 持锁连接断开且租约在重连窗口内到期触发锁丢失
- **THEN** `reconnect.total` 随重连尝试递增，`locks.lost.total` 恰好 +1

### Requirement: OAtomicLong 族 API

客户端 SHALL 提供 `OpenLatchClient.newAtomicLong(String key)`、`newAtomicInteger(String key)`、`newAtomicBoolean(String key)` 工厂与携带非零初值主张的对应重载（`newAtomicLong(key, initial)`——首建生效、与既有条目定型初值冲突时抛 `OpenLatchException`，语义映射服务端初值断言），分别返回公开接口 `OAtomicLong`、`OAtomicInteger`、`OAtomicBoolean` 实例。`OAtomicLong` 面：`long get()`、`void set(long)`、`long getAndSet(long)`、`long incrementAndGet()`、`long addAndGet(long delta)`、`boolean compareAndSet(long expected, long update)`（值 CAS，ABA 风险由契约声明）、`getStamped()`（返回接口内嵌 `Stamped` 读数记录，字段为值与版本戳）、`long getVersion()`、`boolean compareAndSetStamped(long expectedValue, long expectedVersion, long update)`（ABA-free 形态）、`long accumulateAndGet(long identity, LongBinaryOperator accumulatorFunction)`（客户端携带版本戳 CAS 循环，与 JDK 内部实现同构）。`OAtomicInteger`/`OAtomicBoolean` 为对应标量形态的同构面（int32 域 wrap / {true,false} 值域）。同步方法 SHALL 声明 `throws InterruptedException`（本地等待可中断），失败以非受检 `OpenLatchException`/`OpenLatchTimeoutException` 表达。全部操作 MUST NOT 依赖看门狗、租约与 `LockLost` 通知（无持有语义）；`get` 与写操作均为请求-应答即时路径，MUST NOT 进入等待-通知-重发闭环。可重试失败（`NOT_LEADER`、断连快速失败、超时）时写操作 SDK MUST 以**同一 `op_seq`** 自动重发至请求总超时界限——去重槽保证重发不重复生效；界限内仍不可判定则抛 `OpenLatchTimeoutException`，此时契约声明该操作效果不确定（值可能已变也可能未变，`getStamped` 可复核）。会话或进程死亡 MUST NOT 回滚本会话写入的值。

#### Scenario: 六操作与 stamped 读数往返

- **WHEN** 客户端对同一 key 依次执行 set、get、incrementAndGet、addAndGet、getAndSet、compareAndSet（命中与不命中各一次）、getStamped
- **THEN** 各返回值与串行语义一致；失败的 CAS 不推进版本戳；getStamped 读数等于服务端 (value, version)

#### Scenario: 换主窗口写操作自动重发不双加

- **WHEN** `incrementAndGet` 首发落入 Leader 切换窗口收到 `NOT_LEADER`，SDK 经重发现向新 Leader 以同 `op_seq` 重发
- **THEN** 方法正常返回、值恰 +1（新值 = 旧值 + 1，版本恰 +1）

#### Scenario: accumulateAndGet 争用有界重试

- **WHEN** 两客户端对同 key 并发执行 `accumulateAndGet(0, (a,b) -> a+b)` 各 100 次
- **THEN** 全部成功返回且终值为两端增量之和（CAS 循环在界限内收敛）；循环超出重试界限时抛 `OpenLatchException` 而非静默丢更新

#### Scenario: 会话死亡值存续

- **WHEN** 会话 A 写入值后关闭，会话 B 读取同 key
- **THEN** B 读到 A 写入后的值与推进后的版本戳，无任何回滚

### Requirement: OBarrier API

客户端 SHALL 提供循环屏障 `OBarrier` 与工厂 `OpenLatchClient.newBarrier(String key, int parties)`（创建者句柄，每次请求携带 `parties` 非零定型主张）、`newBarrier(String key, int parties, Runnable barrierAction)`（同前并声明该句柄到场合拢时携带动作）与 `newBarrier(String key)`（纯加入句柄，不主张初值，对从未定型的 barrier 被拒）。方法面：`void await() throws InterruptedException`——阻塞至所属世代的了结，受等待兜底超时约束；`boolean await(long timeout, TimeUnit unit) throws InterruptedException`——限时等待，返回是否以 TRIPPED 了结（`false` = 超时，且超时本身即破障，见下）；`void breakBarrier() throws OpenLatchException`；`boolean isBroken()`；`int getParties()`。异常面遵循库内非受检纪律：破障了结抛 `OBrokenBarrierException`（新增公开异常，继承 `OpenLatchException`；替代 JDK 受检 `BrokenBarrierException`，差异 MUST 在契约 Javadoc 声明）、兜底超时抛 `OpenLatchTimeoutException`、JUC 受检 `TimeoutException` 以 `await(timeout)` 返回 `false` 的 boolean 形态承载（JDK 差异声明同上）。等待编排复用等待-通知-重发闭环：`QUEUED` 后挂起等 `AWAIT_NOTIFY`，通知到达以同 `request_id` 重发按世代了结记录幂等了结；执行者形态（应答 `executor=true`）在**本调用栈内**执行 `barrierAction`，完成（正常返回或抛异常）后以 `BARRIER_ACTION_DONE` 终结本等待——动作正常完成 ⇒ 本方 await 正常返回且同世代他方随后放行；动作抛异常 ⇒ SDK 改发 `BARRIER_LEAVE`（离场即破障），本方以该异常终结且同世代全体收 `OBrokenBarrierException`（对齐 JDK"动作异常使屏障破障"）。超时与中断语义：**离场即破障**——`await(timeout)` 超时、`await()` 兜底超时与本地中断均触发客户端 `BARRIER_LEAVE`，当前世代即时 BROKEN，全体在队他方收 `OBrokenBarrierException`（对齐 JDK 超时/中断破障）；`breakBarrier()` 以 `await_request_id=0` 的 `BARRIER_LEAVE` 表达，无在队身份亦破当前世代（条目不存在时无操作幂等）。**到场是有副作用的请求**：在途 `BARRIER_AWAIT` 遇传输失败/断连/换会话 MUST NOT 自动重发（至多一次纪律，判例 `countDown`），本方 await 以 `OpenLatchException` 裁决；同时该会话旧世代的等待项由服务端会话清理连带破障，他方以 `OBrokenBarrierException` 感知（相对 JDK"线程死亡静默挂起"的增强 MUST 显式声明）。`isBroken()` 返回句柄本地最近一次所见裁决，MUST NOT 发起网络查询（声明非实时跨进程一致）。等待者 MUST NOT 持有租约、MUST NOT 产生续租流量。

#### Scenario: 三方合拢与世代复用

- **WHEN** 三个客户端句柄以同 key 同 parties=3 各自 await，先后到达
- **THEN** 三方 await 均在到场数达标后正常返回；其后同一批句柄再次 await 进入新世代并再次合拢（世代回卷复用）

#### Scenario: 动作由最后到场者执行且先于放行

- **WHEN** parties=3 携动作，第 3 位到场者被指定执行者
- **THEN** 其在本 `await()` 调用栈内执行动作；其余两方仅在其 `BARRIER_ACTION_DONE` 提交后才收到放行通知；动作完成前无任何他方从 await 返回

#### Scenario: 动作异常破障

- **WHEN** 执行者的 `barrierAction` 抛出运行时异常
- **THEN** 执行者的 await 以该异常终结，同世代其余在队方收 `OBrokenBarrierException`，世代号已推进

#### Scenario: 参与者超时连带破障

- **WHEN** parties=3 的世代已有 2 方在队，其中一方 `await(1, SECONDS)` 超时
- **THEN** 超时方得 `false`，另一在队方收 `OBrokenBarrierException`，该世代不再可能合拢

#### Scenario: 参与者进程死亡他方即时感知

- **WHEN** 世代内一方进程被杀，服务端经会话清理摘除其在队项
- **THEN** 其余在队方在通知-重发路径内收到 `OBrokenBarrierException`，不无限挂起；随后新到场的 await 进入新世代正常合拢

#### Scenario: await 无看门狗流量

- **WHEN** 客户端长时间 await（超过一个看门狗周期）
- **THEN** 该会话不因 await 发出任何 LEASE_RENEW 请求

#### Scenario: 换会话不自动重放到场

- **WHEN** 在途 `BARRIER_AWAIT` 遭遇连接闪断致会话切换
- **THEN** SDK 不以新会话自动重放该到场，本方 await 抛 `OpenLatchException`；旧会话在旧世代的在队项由服务端清理连带破障

