# Spec Delta

## MODIFIED Requirements

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

## ADDED Requirements

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
