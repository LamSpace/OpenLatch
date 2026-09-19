## ADDED Requirements

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
