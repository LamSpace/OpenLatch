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

排队等待的获取请求在收到服务端的队首通知后，MUST 以原请求标识重发获取请求（服务端幂等，不会二次排队）；重发被授予则完成等待，仍排队则继续挂起等待下一次通知。等待项在等待总时限到达时 MUST 以超时失败；此后到达的通知 MUST 被忽略（不重发）。已超时/已失败的等待若在途重发随后被授予，客户端 MUST 归还该锁（发送释放），不得静默泄漏。重发请求本身超时（无响应）时，MUST 仅结束该次重发、保持挂起等待下一次通知，直到等待总时限到达才整体失败。同一等待收到两次通知而产生的重复授予，MUST 以首个授予为准，重复授予归还。携带 `condition` 字段的折叠形态获取请求（条件等待，见 OCondition API 条款）MUST 以完全相同的身份参与本闭环：收到唤醒通知后的重发信封 MUST 清除 `condition` 字段而保持 request_id 不变（唤醒即终结 await 阶段、转入普通排队获取语义——服务端两拓扑均按既有获取纪律授予），未获通知而自愈的重发保持原信封（含 `condition`）幂等重登记，重复通知与在途授予的终止竞争纪律逐项适用；其**总时限到期的收束例外于普通获取的"超时失败"路径**——MUST 先经 LEAVE 摘除登记再转入常规阻塞获取重新入锁后方返回/抛出（JDK"返回时持锁"保真），MUST NOT 在未持有锁的状态下结束一次 await。

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

#### Scenario: 条件等待复用闭环且超时收束不同

- **WHEN** 折叠 await 收到队首通知；另一场景中同型等待的本地超时先于通知到期
- **THEN** 通知场景：重发信封携带原 request_id 且 **`condition` 字段已清除**，被授予后以持有 1 级重入的状态返回调用方；超时场景：先 LEAVE 摘除（fire-and-forget），再经常规获取重新入锁后才返回 `false`——两条路径均以"持有锁"收束，无一泄漏等待位

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
7. **驻留窗周期核对**：集群形态下，home 会话激活且未改道期间，客户端 MUST 按**有界周期**经该会话查询集群视图（`CLUSTER_VIEW`）核对当值 Leader；核对所得 Leader 与本地路由不一致时 MUST 沿既有改道路径收敛——建立/更换获取车道，或提示指回 home 时回落 home，使**后续**非获取类请求（队列、原子、屏障、闩、订阅/发布、相位器、定时器）经当值 Leader 承载。核对周期 MUST 有界且显著小于等待总超时。核对 MUST NOT 接线在途等待与重发环（不得改动等待/重发语义），其本身的失败或无提示 MUST NOT 计为业务请求失败。本条款只收敛**路由**：各原语对 `NOT_LEADER` 是重发还是显式失败仍循其既有契约（等待类原语的终态抛出不变），且重发类原语的重发环 MUST 在每轮重发前重取路由——否则改道就位后请求仍钉在旧路由上耗尽预算。

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

#### Scenario: home 驻留非权威节点时非获取请求改道

- **WHEN** 客户端 home 连接驻留在已知 Follower 且保持连通，而集群 Leader 已迁至第三节点
- **THEN** 客户端在核对周期内把后续队列/原子/屏障/闩/订阅/相位器/定时器请求发往当值 Leader（可在该节点侧观察到请求到达），不再持续发往已非权威的 home；其中重发类原语（队列/原子）的请求在该窗内于既有预算中完成，不因 home 未断连而耗尽预算显式超时

#### Scenario: 启动窗提示滞后经周期核对改道

- **WHEN** 客户端握手所得 Leader 提示无效（选举中），而随后任一节点已可答出当值 Leader
- **THEN** 客户端在核对周期内把后续非获取请求改道至该 Leader；提示滞后期间发出的请求不因路由陈旧而在后续重发中持续落空

#### Scenario: 核对不打断在途请求也不制造伪成功

- **WHEN** 核对触发改道时其会话上存在在途的非获取请求
- **THEN** 在途请求不被核对面终止或改写语义，改道收敛后其后续重发不受影响；任何未被受理的请求仍以显式失败收场，MUST NOT 出现错值伪交付（空串/0/乱序）

#### Scenario: 单机形态不激活核对

- **WHEN** 客户端连接单机服务端（无 v2 提示能力）
- **THEN** 周期核对不发起，行为与既有单机语义一致

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

客户端 SHALL 提供 `OCountDownLatch`：`await()` 阻塞至屏障归零（受等待兜底超时约束）、`await(timeout)` 限时等待、`countDown()` 与 `countDown(n)` 扣减计数。`newCountDownLatch(key, count)` 创建的创建者句柄在每次请求携带 `total = count` 断言（首次触达即定型，`countDown(0)` 为纯初始化）；`newCountDownLatch(key)` 纯加入句柄不主张初值，对从未定型的屏障被拒。await 等待者 SHALL NOT 持有任何租约、MUST NOT 产生续租流量；断线重连后 SHALL 自动重发 await（幂等），屏障已归零时立即通过。**`NOT_LEADER` 应答状态为零生效**（服务端已答复、本次未受理）：`await` MUST 摘除本机等待登记、重取路由，并**仅当路由会话确已变化**时以新会话重发；该重道 MUST 受独立短预算约束（上限 = 2 个 Leader 核对周期），与等待总预算无关；预算耗尽或路由始终未变化时 MUST 以 `NOT_LEADER` 显式抛出。该重道 MUST NOT 使 `await` 假绿返回 `true`，MUST NOT 悬挂至等待总预算。**不确定窗**（传输失败/读界超时/断连）后的既有严格纪律不变。

#### Scenario: 跨进程倒计数放行

- **WHEN** 进程 A 以 count=2 建立屏障并 await，进程 B、C 各 `countDown()`
- **THEN** A 的 await 在计数归零后返回，B、C 的 countDown 各自立即应答剩余计数

#### Scenario: await 无看门狗流量

- **WHEN** 客户端长时间 await（超过一个看门狗周期）
- **THEN** 该会话不因 await 发出任何 LEASE_RENEW 请求

#### Scenario: 陈旧路由但存在真主时 await 重道放行

- **WHEN** `await` 首发落在已非权威的旧路由上并收到 `NOT_LEADER`，而集群在重道预算内已给出当值 Leader
- **THEN** 客户端重取路由、以新会话重发该 await，并在预算内按归零放行返回 `true`（不假绿、不因路由陈旧而失败）

#### Scenario: 无真主时 await 有界失败且不假绿

- **WHEN** 路由始终陈旧且集群在重道预算内未给出当值 Leader，`NOT_LEADER` 持续返回
- **THEN** 客户端在重道预算耗尽后以 `NOT_LEADER` 显式抛出，MUST NOT 返回 `true`、MUST NOT 悬挂至等待总预算

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

客户端 SHALL 提供循环屏障 `OBarrier` 与工厂 `OpenLatchClient.newBarrier(String key, int parties)`（创建者句柄，每次请求携带 `parties` 非零定型主张）、`newBarrier(String key, int parties, Runnable barrierAction)`（同前并声明该句柄到场合拢时携带动作）与 `newBarrier(String key)`（纯加入句柄，不主张初值，对从未定型的 barrier 被拒）。方法面：`void await() throws InterruptedException`——阻塞至所属世代的了结，受等待兜底超时约束；`boolean await(long timeout, TimeUnit unit) throws InterruptedException`——限时等待，返回是否以 TRIPPED 了结（`false` = 超时，且超时本身即破障，见下）；`void breakBarrier()`；`boolean isBroken()`；`long getParties()`。异常面遵循库内非受检纪律：破障了结抛 `OBrokenBarrierException`（新增公开异常，继承 `OpenLatchException`；替代 JDK 受检 `BrokenBarrierException`，差异 MUST 在契约 Javadoc 声明）、兜底超时抛 `OpenLatchTimeoutException`、JUC 受检 `TimeoutException` 以 `await(timeout)` 返回 `false` 的 boolean 形态承载（JDK 差异声明同上）。等待编排复用等待-通知-重发闭环：`QUEUED` 后挂起等 `AWAIT_NOTIFY`，通知到达以同 `request_id` 重发按世代了结记录幂等了结；执行者形态（应答 `executor=true`）在**本调用栈内**执行 `barrierAction`，完成（正常返回或抛异常）后以 `BARRIER_ACTION_DONE` 终结本等待——动作正常完成 ⇒ 本方 await 正常返回且同世代他方随后放行；动作抛异常 ⇒ SDK 改发 `BARRIER_LEAVE`（离场即破障），本方以该异常终结且同世代全体收 `OBrokenBarrierException`（对齐 JDK"动作异常使屏障破障"）。超时与中断语义：**离场即破障**——`await(timeout)` 超时、`await()` 兜底超时与本地中断均触发客户端 `BARRIER_LEAVE`，当前世代即时 BROKEN，全体在队他方收 `OBrokenBarrierException`（对齐 JDK 超时/中断破障）；且离场为**同步确认**：超时方 `await` 返回前 `BARRIER_LEAVE` 已经服务端生效（"返回即已破"次序——紧随其后的新到场 MUST NOT 落入被本次超时打破的世代；网络故障时降级为尽力而为，由服务端清扫与会话清理兜底）；`breakBarrier()` 以 `await_request_id=0` 的 `BARRIER_LEAVE` 表达，无在队身份亦破当前世代（条目不存在时无操作幂等）。**到场是有副作用的请求**：在途 `BARRIER_AWAIT` 遇传输失败/断连/换会话 MUST NOT 自动重发（至多一次纪律，判例 `countDown`），本方 await 以 `OpenLatchException` 裁决；同时该会话旧世代的等待项由服务端会话清理连带破障，他方以 `OBrokenBarrierException` 感知（相对 JDK"线程死亡静默挂起"的增强 MUST 显式声明）。**`NOT_LEADER` 应答状态为零生效**（服务端已答复、本次未登记到场）：`await` MUST 摘除本机等待登记、重取路由，并**仅当路由会话确已变化**时以新会话重发，受独立短预算约束（上限 = 2 个 Leader 核对周期）；预算耗尽或路由始终未变化时 MUST 以 `NOT_LEADER` 显式抛出，且 MUST NOT 悬挂至等待总预算。该形态 MUST NOT 与上一段"在途 `BARRIER_AWAIT` 遇传输失败/断连/换会话 MUST NOT 自动重发"的纪律混淆：后者约束**结果不确定**的形态（可能已到场，故不得跨会话重放），前者是服务端**已答复未受理**、无到场事实可重放。`isBroken()` 返回句柄本地最近一次所见裁决，MUST NOT 发起网络查询（声明非实时跨进程一致）。等待者 MUST NOT 持有租约、MUST NOT 产生续租流量。

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

#### Scenario: 超时返回即破生效次序保证

- **WHEN** 一方 `await(timeout)` 超时返回 false 后，另一方立即对同 key 到场
- **THEN** 后到方的到场项属新开启的世代（MUST NOT 收到前一世的 `BARRIER_BROKEN` 裁决）；两方各在自己的世代内独立收场

#### Scenario: 参与者进程死亡他方即时感知

- **WHEN** 世代内一方进程被杀，服务端经会话清理摘除其在队项
- **THEN** 其余在队方在通知-重发路径内收到 `OBrokenBarrierException`，不无限挂起；随后新到场的 await 进入新世代正常合拢

#### Scenario: await 无看门狗流量

- **WHEN** 客户端长时间 await（超过一个看门狗周期）
- **THEN** 该会话不因 await 发出任何 LEASE_RENEW 请求

#### Scenario: 换会话不自动重放到场

- **WHEN** 在途 `BARRIER_AWAIT` 遭遇连接闪断致会话切换
- **THEN** SDK 不以新会话自动重放该到场，本方 await 抛 `OpenLatchException`；旧会话在旧世代的在队项由服务端清理连带破障

#### Scenario: 陈旧路由但存在真主时到场重道合拢

- **WHEN** `await` 首发落在已非权威的旧路由上并收到 `NOT_LEADER`，而集群在重道预算内已给出当值 Leader
- **THEN** 客户端重取路由、以新会话重发该到场，并在预算内按世代合拢正常返回；不因路由陈旧而使该方被拒

#### Scenario: 到场未受理的重道不违反至多一次纪律

- **WHEN** 一次 `BARRIER_AWAIT` 收到 `NOT_LEADER` 后重道至新会话并被服务端受理
- **THEN** 该方在服务端恰有一条到场记录（原路由的答复证明未登记，无重复到场、无双计世代）

### Requirement: OAtomicReference API

客户端 SHALL 提供 `OpenLatchClient.newAtomicReference(String key)` 工厂与携带初值主张的对应重载（`byte[]` 形态与 UTF-8 `String` 便利形态各一；主张值为 null 等价于不主张——条目初值即为 null，零长度数组为"空串初值主张"），返回公开接口 `OAtomicReference` 实例。`byte[]` 为主形态、`String` 便利形态按 UTF-8 编解码且两形态语义逐项等价。接口面：`byte[] get()`（可返回 null）、`String getAsString()`、`Stamped getStamped()`（内嵌读数记录：`byte[] value()` 可为 null、`long version()`，附 `String valueAsString()` 便利）、`long getVersion()`、`void set(byte[] value)`/`set(String value)`（null=清空）、`byte[] getAndSet(byte[] value)`（+String 形态）、`boolean compareAndSet(byte[] expected, byte[] update)`（字节内容相等判定，null 为合法期望态；ABA 风险由契约声明）、`boolean compareAndSetStamped(byte[] expectedValue, long expectedVersion, byte[] update)`（ABA-free 形态；另各配 String 便利重载）。接口 MUST NOT 提供 `ADD`/`accumulateAndGet` 类算术面（相对 JDK `AtomicReference` 无降级的缺省——JDK 本无此面）。载荷为不透明字节串：SDK 与服务端 MUST NOT 对载荷做解释或对象反序列化。同步方法 SHALL 声明 `throws InterruptedException`，失败以非受检 `OpenLatchException`/`OpenLatchTimeoutException` 表达；服务端以 `INVALID_REQUEST` 拒绝超限载荷时 SDK MUST 抛 `OpenLatchException` 并透传拒绝语义，MUST NOT 静默截断或本地重试。可重试失败（`NOT_LEADER`、断连快速失败、超时）时写操作 MUST 以**同一 `op_seq`** 自动重发至请求总超时界限（沿用 `OAtomicLong` 族裁决的现成车道，去重槽保证重发不重复生效）；界限内不可判定抛 `OpenLatchTimeoutException` 并声明效果不确定。会话或进程死亡 MUST NOT 回滚本会话写入的值。全部操作 MUST NOT 依赖看门狗/租约/`LockLost` 通知，MUST NOT 进入等待-通知-重发闭环。有值引用形态 MUST 以 v6 握手使用（SDK 握手版本声明升至 6）；对握版本 <6 的服务端，引用形态请求收到 `INVALID_REQUEST` 时 SDK MUST 以显式异常表达协议不支持，MUST NOT 以同序号重发。契约 Javadoc MUST 显式声明降级/增强清单：每操作一次网络往返、超时不确定窗口、ABA 仅 stamped 消除、null 与空串可区分、载荷上限由服务端权威钳制（默认 4KB、随部署配置）、条目常驻不回收（载荷持久驻留）。

#### Scenario: 三形态载荷往返保真

- **WHEN** 对同一 key 依次 `set(字节串)`、`set(String)`、`set(null)` 并各以 `get`/`getStamped` 读取，随后 `compareAndSet(null, "x")` 与 `compareAndSet(new byte[0], "y")`
- **THEN** 各读数与写入字节逐一一致（String 形态按 UTF-8 往返）；`set(null)` 后读数为 null；期望 null 的 CAS 命中、期望空串的 CAS 不命中（两态可区分）

#### Scenario: 超限载荷显式失败零生效

- **WHEN** 客户端 `set` 携带超过服务端 `maxValueBytes` 的载荷
- **THEN** 抛 `OpenLatchException`（拒绝语义、非超时），条目值与版本戳不变，且无同序号重发发生

#### Scenario: 换主窗口写操作自动重发不双写

- **WHEN** `getAndSet(4KB 字节串)` 首发落入 Leader 切换窗口收到 `NOT_LEADER`，SDK 经重发现向新 Leader 以同 `op_seq` 重发
- **THEN** 方法正常返回旧值，服务端该 key 版本戳恰 +1，无重复落值

#### Scenario: 低版本服务端显式拒绝不重试

- **WHEN** v6 SDK 对握版本上限 <6 的服务端调用引用形态 `get`
- **THEN** SDK 以显式异常表达协议不支持（源于 `INVALID_REQUEST` 消息级拒绝），MUST NOT 自动重发或静默降级

#### Scenario: 会话死亡值存续

- **WHEN** 会话 A 写入有值引用后关闭，会话 B 读取同 key
- **THEN** B 读到 A 写入后的值与推进后的版本戳，无任何回滚

### Requirement: OBlockingQueue API

客户端 SHALL 提供 `OpenLatchClient.newBlockingQueue(String key, long capacity)` 与 `newDelayQueue(String key, long capacity)` 工厂（capacity ≤0 MUST 本地快速拒绝 `IllegalArgumentException`——协调面无无界队列；容量作为定型主张随每次写携带，首次 PUT 完成条目定型），返回公开接口 `OBlockingQueue`（延时工厂返回其子接口 `ODelayQueue`）。`byte[]` 为主元素形态、`String` 便利形态按 UTF-8 编解码且两形态语义逐项等价。`OBlockingQueue` 接口面：`void put(byte[] e)`/`put(String e)`（阻塞至有容量，可中断）、`boolean offer(byte[] e)`（立即式，满回 false）、`boolean offer(byte[] e, long timeout, TimeUnit unit)`（挂起式带客户端本地超时，判例 `wait_ms > 0` 客户端计时）、`byte[] take()`/`String takeAsString()`（阻塞至有可消费元素，可中断）、`byte[] poll()`（立即式，无回 null）、`byte[] poll(long timeout, TimeUnit unit)`（本地超时）、`int drainTo(Collection<? super byte[]> out)` / `int drainTo(Collection<? super byte[]> out, int maxElements)`（返回实际摘取数）、`byte[] peek()`（队首读数，空/未到期回 null）、`int size()`（驻留元素数，延时形态含未到期项）、`int remainingCapacity()`（`capacity − size`，一次 RTT）、`long capacity()`（定型回显）。`ODelayQueue` 追加 `boolean offerDelayed(byte[] e, long delay, TimeUnit unit)` / `offerDelayed(String e, …)`（延时注入——**不照抄 JDK `DelayQueue.offer(e, timeout, unit)` 签名**：该形态在 JDK 表"延迟后才可见"，与本地超时语义同形异义，显式改名以消除混读；契约 Javadoc MUST 声明该差异）。全部同步阻塞方法 SHALL 声明 `throws InterruptedException`，失败以非受检 `OpenLatchException`/`OpenLatchTimeoutException` 表达；服务端以 `INVALID_REQUEST` 拒绝（超限/形状/低版本/容量断言冲突）时 SDK MUST 抛显式异常并透传拒绝语义，MUST NOT 静默截断、本地改写参数或以同序号重发。阻塞 put/take 走等待-通知-重发闭环：`QUEUED` → 挂起 → `AWAIT_NOTIFY` → 同 `requestId` 重发 → `DENIED`（应用点竞态回弹）继续挂起 → `OK` 终态；重发超时纪律沿用既有等待车道裁决。写操作 MUST 以**同一 `op_seq`** 在可重试失败（`NOT_LEADER`、断连快速失败、超时）时自动重发至请求总超时界限（继承 `RemoteAtomicBase` 同 key 在途写互斥纪律，服务端每会话去重槽保证 PUT 不双插、TAKE 重放交付同一份字节）；界限内不可判定抛 `OpenLatchTimeoutException` 并声明效果不确定窗。队列操作 MUST NOT 依赖看门狗/租约/`LockLost` 通知。队列 MUST 以 v7 握手使用（SDK 握手版本声明升至 7）；对握版本上限 <7 的服务端，`QUEUE_OP` 收到 `INVALID_REQUEST` 时 SDK MUST 以显式异常表达协议不支持，MUST NOT 重发。元素为不透明字节：SDK 与服务端 MUST NOT 解释或反序列化。契约 Javadoc MUST 显式声明降级/增强/差异清单：每操作一次网络往返（`drainTo` 一次摘多）、超时不确定窗口（幂等由槽兜底但可重试窗内结果延迟）、元素绑定 key 不绑定会话——**投递者进程死亡不吞元素**（强于/异于 JDK 同进程堆消散语义）、`DELAY_QUEUE` 同到期时刻 FIFO（增强，JDK 不承诺）、未到期元素对 `PEEK/TAKE` 不可见但计入 `SIZE`、无 `iterator/contains/remove(Object)/spliterator` 面、元素不可为 null（与 `OAtomicReference` 的 null 语义刻意不同）、容量服务端上限权威钳制、条目常驻不回收（驻留成本与清理为消费/运维职责）、延时到点唤醒精度为服务端 tick 级。

应答形态裁决（MUST）：队列车道收到应答信封后 MUST 校验其携带 `queue_op_response` 载荷——`Envelope.type` 回显 `QUEUE_OP` 但载荷 oneof 缺该成员（异型/空载荷码形违例，如未含 `QUEUE_OP` 分支的拒绝组装路径产物）MUST 按瞬态失败裁决（与 `NOT_LEADER`/断连同车道：退避、重取路由、重发），MUST NOT 读取 protobuf 默认实例并据此裁决（默认实例 `status` 为枚举零值 `OK`，会被误判为"成功空应答"致 `take` 交付零长度元素、`size` 读回 0）。`TAKE` 终态收到 `OK` 而应答缺 `element_bytes` presence 属协议违例（交付契约要求 OK-TAKE 必携元素，显式 presence 使"零长度空串元素"与"缺省"可辨），MUST 以显式 `OpenLatchException` 抛出、MUST NOT 交付 `byte[0]`；`PEEK`/`SIZE` 的 OK-缺省载荷为合法空读数形态，不受本条约束。

#### Scenario: 生产消费 FIFO 闭环

- **WHEN** 会话 A 对容量 8 的队列依次 `put("a")、put("b")、put("c")`，会话 B 依次 `take()` 三次，随后 `offer("d")` 与 `poll()`
- **THEN** take 依序返回 "a"、"b"、"c"（UTF-8 往返字节级一致）；offer 回 true、poll 返回 "d"；`poll()` 于空队回 null、`offer` 于满队回 false

#### Scenario: 阻塞 put 腾位唤醒闭环

- **WHEN** 满容量队列上一个 `put` 挂起（服务端 QUEUED），另一会话消费一个元素，随后挂起者收到推送
- **THEN** 挂起的 `put` 经"通知→同 requestId 重发→OK"正常返回，总深度不超过容量，元素不双插（同 op_seq 槽保证）

#### Scenario: 中断与本地超时

- **WHEN** 挂起的 `take` 被线程中断；另一 `poll(50ms)` 于持续空队到期返回
- **THEN** 前者抛 `InterruptedException` 且服务端等待项经离场路径摘除（不吞元素）；后者回 null（超时为客户端本地计时，等待项由其超时臂自行了结）

#### Scenario: 换主窗口写重发不双插不偷吃

- **WHEN** `put(x)` 首发落入 Leader 切换窗口收到 `NOT_LEADER`，SDK 经重发向新 Leader 以同 `op_seq` 重发；另一场景 `take()` 交付后应答丢失重发
- **THEN** 队列中 x 恰出现一次；take 场景 SDK 收到与服务端首交付**字节相同**的元素，且未多摘一个

#### Scenario: 投递者进程死亡元素存续

- **WHEN** 会话 A `put` 若干元素后进程被杀，会话 B `size`/`take`
- **THEN** B 见 A 投递的全部元素并按序消费——无因 A 死亡产生的元素丢失（契约声明的可执行化）

#### Scenario: 延时元素按到期可见

- **WHEN** `offerDelayed("late", 300, MILLISECONDS)` 后立即 `peek`，等待到期后再 `take`
- **THEN** 到期前 `peek` 回 null 而 `size` 计 1；到期后 `take` 返回 "late"

#### Scenario: 超限与低版本显式失败零生效

- **WHEN** `put` 携带超过服务端 `maxValueBytes` 的元素；或 v7 SDK 对握版本上限 6 的服务端调用 `put`
- **THEN** 均抛 `OpenLatchException`（拒绝语义、非超时），队列零变化，且无同序号重发发生

#### Scenario: 换主窗非 Leader 拒绝不得伪成功（安全性质）

- **WHEN** 元素已提交后 Leader 被杀，客户端队列请求的路由落在非当值 Leader 节点
- **THEN** 该节点以同型 `queue_op_response` 的 `NOT_LEADER` 拒绝、SDK 按瞬态退避重取路由重发；全程 MUST NOT 出现"`take` 交付空串/错误元素"或"`size` 读 0"的伪成功形态——改道收敛时依序交付真实元素（透明），路由未能在请求预算内重建的驻留窗以显式异常收场（超时/会话换代口径，客户端改道机制的完整收敛兑现归"Leader 发现与故障转移"能力承载）

#### Scenario: 缺码形应答不得读作成功

- **WHEN** 队列车道收到 `Envelope.type = QUEUE_OP`、`request_id` 匹配、但载荷为异型（如仅 `acquire_response`）或空载荷的应答（脚本化服务面注入）
- **THEN** 车道按瞬态失败重取路由重发（不交付元素、不上报读数），至请求时限耗尽以 `OpenLatchTimeoutException`/会话族异常失败；`TAKE` 的真 `OK` 且缺 `element_bytes` 注入形态以显式协议违例异常失败、MUST NOT 交付 `byte[0]`

### Requirement: OTopic API

`OpenLatchClient` SHALL 提供 `newTopic(String key)` 工厂返回 `OTopic`，公开契约为：`publish(byte[])`/`publish(String)`（UTF-8 便利族）同步返回受理结果、`publishAsync` 异步对偶、`subscribe(OTopicMessageHandler)` 返回 `OTopicSubscription` 句柄、`unsubscribe()` 幂等退订该键本会话全部订阅。`OTopicSubscription` 暴露 `droppedCount()`（本地观察值）、`isActive()`、`close()`（幂等，等价 UNSUBSCRIBE）。消息体为不透明字节（服务端不解释、无反序列化）；PUBLISH 必携非 null 消息体（零长度合法），与队列元素纪律同判例、与引用形态 null 语义刻意不同。

交付语义与相对 JDK `Flow.Publisher`/`SubmissionPublisher` 的差异 MUST 以接口级 Javadoc 全量声明：

- **至多一次**：`publish` 返回 `OK` 仅表示服务端已受理并入 fan-out，不承诺任何订阅者收到；订阅者可能因缓冲满/断线/换主窗而丢消息，服务端不重投、不通知单条丢失。
- **弱背压 drop-newest 两级**：服务端每订阅缓冲满丢最新（既有缓冲照常交付），SDK 本地缓冲同策略；Publisher 不被慢消费者阻塞，慢消费者不被断开（显式不采纳 JDK `onOverflow → close` 判例——断连即会话死亡、连带全部持锁释放）。
- **顺序承诺仅限单订阅内**：同一订阅收到的 `topic_seq` 在**同 Leader term 内**严格升序；`droppedCount()` = 本地缓冲溢出计数 + 同 term 内 seq gap 推断，换 term（重连重订阅后）基线重置不跨 term 累计；跨 Publisher、跨订阅无全局序承诺。
- **去重仅同 Leader**：PUBLISH 沿用同 key 在途互斥 + 同 `op_seq` 重发（判例 `RemoteAtomicBase` 写车道），受理节点去重槽保证同 term 不双扇出；跨换主重试可能双投——消费侧幂等为应用义务，契约显式声明。
- **订阅绑定会话**：订阅者会话死亡即退订（服务端三路回收），与队列"死亡不吞元素"刻意相反；换主后订阅关系由 SDK 自动重订阅维持（应用句柄与语义不变，`subscription_id` 内部重映射），重挂前的窗口内消息不可追回。
- **监听器线程模型**：`OTopicMessageHandler` 单订阅内串行回调，执行于 SDK dispatcher 线程（MUST NOT 占用 Netty EventLoop）；回调异常被吞并记录，不中断后续交付。
- **服务端权威钳制**：`maxValueBytes`/`max-subscribers-per-key`/`max-subscription-buffer` 由服务端入口裁决，超限/上限拒绝以对应状态码同型送达（`INVALID_REQUEST`/`REJECT_SUBSCRIBERS`）；订阅存在有常驻登记表与缓冲成本。
- **版本门**：需 v8 握手（客户端三处常量升 8）；对 v7 服务端连接按既有区间外握手拒绝纪律失败——升级序先服务端后客户端（判例 v3–v7）。

`newTopic` 对同 key 可多实例；同会话同键服务端唯一登记，SDK 层后到 SUBSCRIBE 覆盖在前句柄的交付路由时 MUST 在句柄 `isActive()` 与回调行为上如实呈现覆盖语义（不静默双路由）。

#### Scenario: 发布-订阅闭环

- **WHEN** 两客户端分别 SUBSCRIBE 后，第三方对同 key PUBLISH 多条消息
- **THEN** 两订阅者各自收到全部消息的字节级副本（payload 无损、`topic_seq` 升序、`publisher_sid` 与 `publish_ts_ms` 可读），回执 `OK` 携带 `topic_seq`

#### Scenario: 退订停止交付

- **WHEN** 订阅者 `close()` 其后该 key 再 PUBLISH
- **THEN** 该订阅者不再收到任何推送；对已退订键重复 `close()`/`unsubscribe()` 不报错（幂等）

#### Scenario: 换主自动重订阅续收

- **WHEN** Leader 被 kill，订阅客户端重连至新 Leader 后第三方继续 PUBLISH
- **THEN** 应用句柄无感（未重调 subscribe）持续收到新 term 消息；换主窗内发布的消息不再出现（无重投），`droppedCount()` 不因 term 切换跨窗累计 gap

#### Scenario: 慢监听器本地丢弃不反压

- **WHEN** 监听器长时间阻塞致 SDK 本地缓冲溢出，同时该连接上其他原语（锁/队列）正常操作
- **THEN** 仅 topic 交付链丢弃并计数（`droppedCount()` 增长），其他原语请求不受影响，连接不断开，Publisher 侧无感知

#### Scenario: 发布重发同序号不双投

- **WHEN** PUBLISH 应答在途丢失，SDK 以同 `op_seq` 自动重发（未换主）
- **THEN** 订阅者各收到一份该消息（去重槽命中重放回执，无第二份）

### Requirement: OCondition API

`OLock` SHALL 提供 `newCondition(String name)` 返回 `OCondition`，公开契约为：`await() throws InterruptedException`、`await(long timeout, TimeUnit unit) throws InterruptedException → boolean`、`signal()`、`signalAll()`。条件身份为 (lock key, name) **命名寻址**：同名重复 `newCondition`、同 name 多进程句柄均绑定服务端同一等待集（相对 JDK 句柄身份的本质适配——跨进程可寻址是能力面，差异显式声明）。`OReadWriteLock` 所得读/写 `OLock` 上调用 `newCondition` MUST 抛 `UnsupportedOperationException`（v1 支持面仅 REENTRANT/FAIR/SIMPLE，指南与 Javadoc 随行声明）。句柄无状态、无需关闭（等待位由服务端三路回收，不存在 SDK 侧泄漏面）。

相对 JDK `Condition` 的差异与保真 MUST 以接口级 Javadoc 全量声明：

- **虚假唤醒允许且不承诺杜绝**（保真面：JDK 同契约）——来源清单：队首超时清扫后的推进促醒、换主重挂窗中已丢失的 signal 对应谓词变化、LEAVE/SIGNAL 竞态；**guard loop 谓词复查是调用方义务**，指南给出标准惯用法。
- **signal 权限双层**：本地先行——当前线程未持有本锁即抛 `IllegalMonitorStateException`；服务端权威——线路拒绝码 `NOT_HELD` 同样映射为 `IllegalMonitorStateException` 抛出（JDK 异常类型保真，与既有 `unlock()` 误用同型）。持有者对空集/无名条件 signal = 无操作正常返回（JDK 对齐）；键不存在/无人持有属权限不匹配，同样映射 `IllegalMonitorStateException`。
- **await 权限服务端降级**：本地仍检查持有着（误用即抛）；服务端不查 await 登记权限（跨换主重挂必需）——降级面与 ghost 有界性（合并深度护栏、三路回收）显式声明。
- **返回时持锁保真**：`await()`/`await(timeout)` 无论被唤醒、超时还是中断，返回（或抛出）前 MUST 已重新获取锁，重入计数从 **1** 起（与 await 前 N 级无关——JDK 同款算术，外层 unlock 次数相应减 N-1，惯用法示例钉死）；`await(timeout)` 返回值仅表示"是否以被 signal 路径收束"，超时后唤醒恰到达时返回值按收束先者如实呈现（best-effort 语义声明）。`awaitNanos`/`awaitUntil`/`awaitUninterruptibly` 与异步对偶不提供。
- **租约交互**：await 受理即全量释放——该 key 看门狗随持有簿记消失自动停摆，唤醒重获取签发**新租约凭证**并自动重启续租，应用无感；持有者进程死亡/租约到期时 sweep 释放锁并唤醒**入队者**，但**不代为唤醒条件等待者**（无人 signal 则永睡，与 JDK 对齐；推荐 `await(timeout)` 形态自救）。等待者自身无租约（Latch awaiter 判例）。
- **换主分层语义**：等待是承诺（折叠条目重放使释放确定生效、等待项随 ACQUIRE 车道迁移自动重挂——对应用透明）；**signal 是事件**（不重放、不补偿，换主窗内发出的 signal 对旧集合的搬运随换主灭失，等待项至多重挂后等待下一次 signal 或超时）；W11 观察面不覆盖 await（其走 ACQUIRE 车道），signal 家族直发车道为 W11 新成员。
- **成本模型**：folded await 至少 1 次 RTT（提交）+ 唤醒后 1 次重发 RTT + 被授予 RTT；signal/signalAll/LEAVE 各 1 RTT 即时回执；等待无长连接专属资源（服务端仅登记条目）。
- **版本门**：需 v9 握手（客户端三处常量升 9）；对 v8 及以下服务端连接按既有区间外握手拒绝纪律失败——升级序先服务端后客户端（判例 v3–v8）。

#### Scenario: 生产-消费守卫闭环

- **WHEN** 消费者持锁循环 `while (!ready) cond.await()`，生产者持锁置位后 `cond.signal()` 再 `unlock()`
- **THEN** 消费者在 signal 后（或虚假/清扫促醒的谓词复查后）以持有 1 级重入从 await 返回，谓词为真退出守卫；全链对应用仅暴露 JDK 惯用法

#### Scenario: 非持有 signal 双层同型异常

- **WHEN** 未持锁线程本地调用 `signal()`；以及本地检查通过但服务端裁决时持有已丢（如锁丢失窗）的 `signal()` 送达 `NOT_HELD`
- **THEN** 两路径均抛 `IllegalMonitorStateException`，无静默吞弃；连接与会话不受影响

#### Scenario: 超时 await 返回时持锁

- **WHEN** `await(500, MILLIS)` 到期而无人 signal
- **THEN** SDK 发 LEAVE 后转常规获取；返回 `false` 时调用线程已持有锁（重入 1 级），可安全复查谓词并决策

#### Scenario: 换主自动重挂续醒

- **WHEN** await 挂起期间 Leader 更替，其后持有者在新 Leader 上 signal
- **THEN** 等待项随车道迁移自动重挂（应用无感、同 request_id 幂等登记），新 signal 到达时照常唤醒重获取；换主窗内旧 signal 不补偿（有界窗，超时自救面）

#### Scenario: 命名寻址跨句柄等价

- **WHEN** 两个进程/两处代码分别对同 key 调用 `newCondition("x")`，其一 await、其二持锁 signal("x")
- **THEN** 唤醒跨进程生效（同 name 同集合）；调用 `newCondition("y")` 的等待项不受该 signal 扰动（集合隔离）

#### Scenario: 读写锁条件不支持显式化

- **WHEN** 对 `OReadWriteLock.writeLock()` 返回的 `OLock` 调用 `newCondition`
- **THEN** 立即抛 `UnsupportedOperationException`（本地裁决，不产生任何请求）

### Requirement: OPhaser API

客户端 SDK SHALL 提供 `OPhaser` 公开契约与 `OClient.newPhaser(String key)` / `newPhaser(String key, int initialParties)` 工厂：句柄构造零网络；`initialParties > 0` 时首次业务操作前同步提交一次 `REGISTER(count = initialParties)` 归属本会话（JDK `new Phaser(n)` 的注册语义对偶——构造后 n 方已注册未到场；注册失败按抛出形态传播，同句柄不重复提交）。API 面：`int register()` / `void bulkRegister(int parties)`（返回/回显到场相位——注册者进入当前相位应到集合）、`int arrive()`、`int arriveAndDeregister()`、`int arriveAndAwaitAdvance()`、`int awaitAdvance(int phase)`（无限挂起形态）、`int awaitAdvanceInterruptibly(int phase, long timeout, TimeUnit unit)`、`long getPhase()` / `int getRegisteredParties()` / `int getArrivedParties()` / `int getUnarrivedParties()`（JDK 返回值形态对齐：相位 long、计数 int）。**不提供** `onAdvance` 覆写钩子、`isTerminated()`/`forceTerminated()` 与父子分层构造——三砍为显式 Non-Goal，与 JDK `Phaser` 的这组差异 MUST 在接口级 Javadoc 降级清单逐条声明并给出应用侧替代惯用法（钩子→以 `arriveAndAwaitAdvance()` 返回的到场相位号判别后本地执行；终止→键级生命周期治理；分层→独立 key）。

车道与闭环：`PHASER_OP` 为直发请求-应答车道（判例 QUEUE/TOPIC/CONDITION——无 ACQUIRE 在途互斥，phaser 无持有概念；同请求超时重发以同 request_id 幂等重演，到场类去重槽、等待登记幂等与了结记录承载重放安全）。变异操作（register/arrive/arriveAndDeregister/arriveAndAwaitAdvance 的到场半程）经提交路径，客户端对 `NOT_LEADER` 执行既有退避改道；等待操作（awaitAdvance/arriveAndAwaitAdvance 的等待半程）收 `QUEUED` 后进入挂起环——按 request_id 过滤 `AWAIT_NOTIFY`（判例队列读车道与 v3 推送桥）、原 request_id 重发取数了结（`OK{相位}`）；`awaitAdvanceInterruptibly` 超时到期 MUST 发 `CANCEL` fire-and-forget 后抛 `TimeoutException`（JDK 对偶签名；`awaitAdvance(int)` 无限形态由等待总超时兜底——超时纪律条款延伸，MUST NOT 提供事实上永挂而无界收口的路径）；被中断（interrupt）同超时路径收束并抛 `InterruptedException`。换主/会话重建后 SDK MUST 以双通道自动重挂全部活跃等待项（车道激活事件 + 周期保活，判例 v8 topic 重挂；谓词在复制态——重挂即刻了结或续挂、**无损耗**，该增强与 v9 条件"换主窗 signal 丢失"的对照差异 MUST 在 Javadoc 与指南声明防混读）。

契约三清单（接口级 Javadoc 全量承载）：**保真面**——动态注册/离场、到场与等待解耦、按已见相位等待、相位号单调、arrive 族返回到场相位、`awaitAdvance` 返回当前相位、`getUnarrivedParties` 口径、旁观者 awaitAdvance 无需配额；**降级面**——每次调用至少一次 RTT（JDK 本地计数器的 `getPhase`/`arrive` 为纳秒级——网络成本模型显式声明，观察类计数建议低频读取或应用侧缓存句柄本地最近所见）、查询为 Leader 本地读数不保证线性化（返回即刻过期是契约）、无 onAdvance 的"动作先于全体放行"排序（应用侧钩子与他人的醒转无先后承诺）、无终止态（JDK 终止语义不映射）、`arriveAndDeregister`/`bulkRegister` 的配额严格归属（JDK 匿名 party 面收敛为会话记账，跨会话代扣不存在）、等待超时的 CANCEL 为尽力撤销（ghost 有界且受护栏钳制——与 v9 AWAIT 权限降级同型的宽容面声明）；**增强面**——跨进程参与者同相合拢（JDK 句柄进程内私有）、等待谓词随复制态跨换主自愈、管理面全集群配额/到场/等待读数可见。句柄无状态：同 key 重复 `newPhaser` 等价绑定同一服务端账簿（判例 v9 命名寻址跨进程等价的同型声明）；SDK MUST NOT 在本地伪造计数（读数恒来自服务端应答回显）。

指标与诊断：`ClientMetrics.statusCodeOf` 扩 `PHASER_OP` 归线；握手常量升 10；`FairOrderingSuite` 类公平性套件对 PHASER 不适用（合拢是广播事件、到场先后由 Raft apply 序承载——判例 v5 D6 BARRIER/ATOMIC 豁免的正文同款，豁免登记随实现提交）。

#### Scenario: 多方会合与返回值保真

- **WHEN** 两个进程各 `newPhaser(key, 2)`、各自 `register()` 使 registered=4，随后四方（跨两会话）依次 `arriveAndAwaitAdvance()`
- **THEN** 前三次到场返回到场相位号且调用挂起/排队，最后一次到场触发合拢；全部四方以 `OK` 收束，`arriveAndAwaitAdvance` 均返回同一到场相位号（JDK 返回值语义）；`getPhase()` 推进 1、`getArrivedParties()` 归零

#### Scenario: 超时不泄漏等待位

- **WHEN** 单参与者 `awaitAdvanceInterruptibly(0, 50ms)` 而无人使其相位推进
- **THEN** 到期抛 `TimeoutException`，SDK 已发出 `CANCEL`；该 request_id 的后续迟到唤醒通知被挂起环按既有终止竞争纪律忽略；服务端等待集经三路回收不残留（ghost 至多存续至清扫/会话收口，且全程计入合并护栏）

#### Scenario: 换主窗等待自愈

- **WHEN** 等待方 `expected_phase=5` 挂起期间集群换主，新相位 6 的变异条目已提交，旧 Leader 的通知随进程灭失
- **THEN** SDK 经车道激活/保活通道重挂 `AWAIT_ADVANCE(5)`，新 Leader 即刻回 `OK{phase=6}`——等待无损耗醒转；该差异（对照条件 signal 丢失窗）在 Javadoc 声明且 E2E 钉住

#### Scenario: 配额严格归属与构造注册

- **WHEN** 会话 A `newPhaser(key, 3)` 后，未注册的会话 B 对其 key 调用 `arriveAndDeregister()`；随后会话 A 第三次 `arriveAndDeregister()` 后又追加一次
- **THEN** B 被 `INVALID_REQUEST`（零配额拒绝，账簿零扰动）；A 的第三次离场正常（registered 归 0、空转）、第四次的配额透支同样被拒——JDK 匿名 party 的未定义行为在本 SDK 显式化为拒绝

### Requirement: OTimer API

客户端 SDK SHALL 提供 `OTimer` 公开契约与 `OClient.newTimer(key)` 工厂，对应"延时触发（定时单次标记）"原语（JDK `java.util.Timer` 的协调面可判定子集，协议 v11）。契约要求：

- **构造零网络**：句柄创建不发起任何请求（判例 `newPhaser`/`newTopic`）；无初始装载参数——装载恒为显式 `schedule` 操作（对照 `newPhaser(initialParties)`：timer 无"构造即预装"的语义来源）。
- **API 面**：`long schedule(long delay, TimeUnit unit)`（装载/重装载，返回本代代次回显；`delay < 0` 或超 horizon 由服务端拒绝映射异常，`delay == 0` 即立即可共见）；`void disarm()`（撤销装载，幂等；无条目 `INVALID_REQUEST` 映射异常）；`void await()`（等待当代标记；当代被撤销时以 `OpenLatchException`（`DENIED` 终态映射）异常收束——"等待不可满足"为异常终态而非布尔混读，契约成文）；`boolean await(long timeout, TimeUnit unit)`（`true`=见到当代标记；`false`=超时；当代被撤销 → 同 `await()` 的异常收束）；`boolean isFired()` / `boolean isArmed()` / `long getRemainingMillis()`（`QUERY` 读数映射，advisory——即刻过期是契约，判例 `getPhase()` 语言事实句；`getRemainingMillis()` 为客户端本地差值）。不暴露 `cancel(await)` 面（超时/中断自救内部发 `CANCEL` fire-and-forget，判例 v9/v10）。
- **周期与任务为显式不提供**：无 `scheduleAt(Instant)`（客户端墙钟偏差注入即污染谓词判据——仅收相对延迟，绝对到期恒由服务端应用点折算，v7 `offerDelayed` 同轴）；无周期重挂参数（服务端周期自触=常驻重发变异，恰与"到期零条目"裁决相反；客户端循环 `schedule` 即等价，差异声明入降级清单）；无任务载荷/回调（服务端不执行用户代码——v10 D4 onAdvance 同轴论证）。
- **单次与粘滞共见**：一次装载恰一发——到期后标记对一切到达者共见（含 fire 之后新到的 `await`/`isFired` 即刻通过），直至 `disarm`（代终结、此后等待者收 `DENIED` 终态/异常）或再 `schedule`（换代清钟、开新一轮）。相对 JDK `TimerTask` 的"跑完即弃"差异成文：本原语的"用完"判据是观察面的代次推进，非消费动作。
- **改期以最新代为准**：`schedule` 重装载换代清钟——已挂起的 `await` 以最新代到期时刻为准（推后多睡/提前先响皆合法形态，无"旧代承诺"补偿）；推荐 timed await 自救（判例条件"无人 signal 则永睡"运维提示同轴）。
- **死亡不撤钟（增强声明）**：装载方进程/会话死亡不影响触发——钟照响、标记照共见（账簿绑定 key；与 Barrier 破障、Phaser 摘除并列为死亡语义第三形态，接口级 Javadoc 对照呈现）；等待方自身死亡仅失其订阅位。
- **等待闭环与自动重挂**：`AWAIT` 收 `QUEUED` 后按 request_id 过滤 `AWAIT_NOTIFY` 挂起、以**原 request_id** 重发取终态（`OK`/`DENIED`）；活跃等待项经双通道自动重挂（车道激活事件+周期保活，判例 v8/v10）——谓词在复制账簿，**换主/断连重挂无损：已到期即刻了结、未到期续挂**（与 v9 条件 signal 丢失窗的对照防混读句随行）；重挂 MUST NOT 误换 request_id（v10 过程真缺陷教训的本原语复现钉死——装载去重槽面防假象双换代）。超时/中断 → `CANCEL` fire-and-forget → 抛对应异常（`TimeoutException`/`InterruptedException` 形态沿既有 SDK 纪律）。
- **直发车道与读数口径**：`TIMER_OP` 不走 ACQUIRE 车道（同 key 无在途互斥——装载无持有概念，判例 QUEUE/TOPIC/CONDITION/PHASER 直发族）；`NOT_LEADER` 既有退避改道（无提示字段判例）；`isFired` 等读数的"已响"判定随判定节点时钟，跨节点可见偏差 ≤ 节点间时钟偏移（显式降级声明——管理面原始 `fire_at_ms` 呈现无歧义读数，接口 Javadoc 同步）。
- 接口级 Javadoc 全量降级/增强/保真三清单（每次调用至少一次 RTT 的成本模型、`schedule` 回执代次即观察序、`await` 无持有语义、无 `awaitUninterruptibly`/`awaitUntil` 对偶为 Non-Goal）；握手常量升 11；`ClientMetrics.statusCodeOf` 扩 `TIMER_OP` 归线。

#### Scenario: 装载-到期-共见闭环

- **WHEN** 会话 A `schedule(50ms)` 后进程照常运行，会话 B 与 C 并发 `await(2s)`，D 在约 60ms 时新到 `isFired()`
- **THEN** B/C 于服务端到期后经 `QUEUED→AWAIT_NOTIFY→原 rid 重发` 链路先后返回 `true`（tick 级滞后为声明精度），D 即刻 `true`（粘滞共见不经等待）；A 再次 `schedule` 后新到的 `await` 落入新一轮阻塞

#### Scenario: 撤销等待者的异常收束

- **WHEN** 等待者挂起于未到期当代，另一会话 `disarm()`
- **THEN** 等待者经唤醒重发收 `DENIED`：`await()`/`await(timeout)` 均以 `OpenLatchException` 收束（非 `false`——超时与撤销两形态不得混读）；`isFired()` 转 `false`、`isArmed()` 转 `false` 如实呈现代终结

#### Scenario: 重连重挂同 request_id 不假象双换代

- **WHEN** 等待项挂起期间其连接重建（会话切换放弃旧挂起），SDK 对新句柄等待项以新 request_id 登记；对照同句柄保活重挂路径
- **THEN** 保活重挂恒以原 request_id 重发（命中服务端等待幂等登记、不二次入集）；装载幂等重发命中去重槽回放声（代次不前进）——构造任何"重挂即重装载"的形态被回归用例钉死（v10 真缺陷教训的 SDK 侧护栏）

#### Scenario: 低版本服务端与回滚口径

- **WHEN** v11 句柄对 v≤10 服务端使用
- **THEN** 区间外握手即拒（既有纪律）；不存在旧服务端误执行 timer 语义的窗口；升级序服务端先行的部署声明随指南

### Requirement: 虚拟线程亲和

客户端 SDK 的阻塞调用面（`OLock`/`OSemaphore`/`OCountDownLatch`/`OBarrier`/`OCondition`/`OPhaser`/`OTimer`/`OBlockingQueue`/`ODelayQueue`/`OAtomic*` 族的全部同步方法与 JUC 风格同步包装）SHALL 可被调用方在虚拟线程上安全且高效地执行：桥接形态（future 限时等待 + 客户端本地计时轮询 + 非阻塞写出）MUST NOT 存在钉扎虚拟线程载体线程的阻塞源，等待期间的调用方虚拟线程 MUST NOT 占用平台载体线程。运行时基线 JDK ≥24（本项目为 25）已消除 "monitored 区内阻塞钉扎"，客户端 MUST NOT 在调用方线程路径引入新的 native 帧阻塞源。

线程归属键（`Thread.currentThread().threadId()` 承载的重入计数、`isHeldByCurrentThread`、许可持有计数、条件 signal 持有归属查询）在平台线程与虚拟线程两种调用方形态下 MUST 行为一致；虚拟线程 id 进程内全局唯一且终止后不复用，本地持锁登记 MUST NOT 因调用方线程形态产生归属漂移。中断语义 MUST 与形态无关：阻塞中的调用方被中断时，既有"恢复中断标志/抛 `InterruptedException`/离场摘除服务端等待项"裁决逐项原样成立。

`OTopic` 每订阅的交付派发线程 SHALL 为虚拟线程：单订阅内串行回调、回调不占用 Netty EventLoop、回调异常吞并记日志的既有承诺 MUST 逐项不变；同进程并发订阅数增长时，平台线程计数 MUST NOT 随订阅数线性增长（维持客户端内部基建的常数形态）。

形态保持承诺（显式非目标锚定）：Netty EventLoop 线程、驱动全部超时面（看门狗续租节奏、请求超时、保活、重发）的共享定时器线程、锁丢失回调专用单线程执行器 SHALL 保持平台线程形态与既有数量承诺；本要求 MUST NOT 被解读为改动看门狗线程模型（现形态无随持锁/等待者规模增长的线程消耗）或改为失锁回调的串行保证。

#### Scenario: 万级虚拟线程等待者闭环通过

- **WHEN** 一万个虚拟线程（同规模平台线程形态不可启动）跨多 key 并发阻塞在 `lock()`/`acquire()`/`put()` 类等待上
- **THEN** 全部等待者经受"等待-通知-重发"闭环在等待总超时预算内正常了结，客户端进程内存平稳、无泄漏；等待者数量不产生续租流量（等待不持租约，既有"await 无看门狗流量"承诺在虚拟线程形态下依旧成立）

#### Scenario: 钉扎事件零断言

- **WHEN** 在 JFR `jdk.VirtualThreadPinned` 事件观测窗内，以虚拟线程执行锁/信号量/屏障/条件的阻塞等待、队列 `put`/`take` 的轮询路径与原子族 CAS 循环
- **THEN** 观测窗内钉扎事件计数为 0（该断言常驻为回归绊线，未来任何改动重新引入钉扎即测试失败）

#### Scenario: 虚拟线程归属矩阵

- **WHEN** 虚拟线程对可重入锁连续获取两次后调用两次 `unlock()`；另一虚拟线程查询 `isHeldByCurrentThread()`；非持锁线程调用 `unlock()`
- **THEN** 重入计数、归属查询与 `IllegalMonitorStateException` 拒绝行为与平台线程形态逐项一致，服务端仅见计数归零后的恰一次释放生效

#### Scenario: 虚拟线程中断与平台线程判例一致

- **WHEN** 虚拟线程调用方阻塞在 `await()`/`take()`/`acquire()` 时被中断
- **THEN** 抛 `InterruptedException`（或既有的 boolean/标志恢复裁决形态）、服务端等待项经离场路径摘除、后续操作不受损——与平台线程中断判例逐项等价

#### Scenario: OTopic 扇出下平台线程数恒定

- **WHEN** 同一客户端进程并发建立 500 个订阅并持续收到消息交付
- **THEN** 每订阅回调仍串行执行、交付内容与顺序承诺不变；进程平台线程计数相对订阅建立前基线保持恒定区间（派发线程虚拟形态化，不随订阅数线性增长）

#### Scenario: 基建线程形态保持

- **WHEN** 检视构建后客户端的线程清单与关停行为
- **THEN** EventLoop 线程、共享定时器线程、锁丢失回调执行器线程均为平台线程且数量维持既有承诺；仅 OTopic 派发线程呈虚拟线程形态；`shutdown()` 对全部内部线程的收敛行为不变
