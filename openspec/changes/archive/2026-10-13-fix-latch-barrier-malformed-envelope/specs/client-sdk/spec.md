# Spec Delta: client-sdk

## MODIFIED Requirements

### Requirement: OCountDownLatch API

客户端 SHALL 提供 `OCountDownLatch`：`await()` 阻塞至屏障归零（受等待兜底超时约束）、`await(timeout)` 限时等待、`countDown()` 与 `countDown(n)` 扣减计数。`newCountDownLatch(key, count)` 创建的创建者句柄在每次请求携带 `total = count` 断言（首次触达即定型，`countDown(0)` 为纯初始化）；`newCountDownLatch(key)` 纯加入句柄不主张初值，对从未定型的屏障被拒。await 等待者 SHALL NOT 持有任何租约、MUST NOT 产生续租流量；断线重连后 SHALL 自动重发 await（幂等），屏障已归零时立即通过。**应答缺码形 MUST NOT 读作成功/读作放行**：SDK 收到应答信封后 MUST 校验其携带本请求的载荷——`Envelope.type` 回显 `LATCH_AWAIT` 而载荷 oneof 缺 `latch_await_response`（异型/空载荷码形违例）时，`await` MUST NOT 读取 protobuf 默认实例（其 `status` 为枚举零值 `OK`）并据此**假绿返回"已放行"**，MUST 按可重试失败并入等待-通知-重发闭环（以同 `request_id` 重发，判例 `RemotePhaser`/`RemoteTimer`），请求预算内仍不可判定则以显式失败收场（`await(timeout)` 返回 `false`、`await()` 抛兜底超时异常）；`countDown`/`init` 为**至多一次**请求（无 `op_seq` 去重槽），其应答缺 `latch_count_down_response` 时 MUST NOT 读默认实例（`status=OK`/`remaining=0`）静默返回剩余计数，MUST 以显式失败收场且 MUST NOT 自动重发（重发不安全，判例同 `countDown` 至多一次纪律）。

#### Scenario: 跨进程倒计数放行

- **WHEN** 进程 A 以 count=2 建立屏障并 await，进程 B、C 各 `countDown()`
- **THEN** A 的 await 在计数归零后返回，B、C 的 countDown 各自立即应答剩余计数

#### Scenario: await 无看门狗流量

- **WHEN** 客户端长时间 await（超过一个看门狗周期）
- **THEN** 该会话不因 await 发出任何 LEASE_RENEW 请求

#### Scenario: 缺码形 await 应答不得假绿放行

- **WHEN** `LATCH_AWAIT` 的应答 `type` 回显 `LATCH_AWAIT` 而载荷 oneof 缺 `latch_await_response`（脚本化服务面注入）
- **THEN** SDK 不读取默认实例、MUST NOT 断言"已放行"返回 `true`；按可重试失败以同 `request_id` 重发（`countType(LATCH_AWAIT) > 1`），请求预算内仍不可判定则以显式失败收场

#### Scenario: countDown 缺码形不得静默返回剩余计数

- **WHEN** `LATCH_COUNT_DOWN` 的应答缺 `latch_count_down_response` 载荷
- **THEN** SDK 不读取默认实例、MUST NOT 静默返回 `remaining=0` 伪成功；按显式失败收场且不自动重发（`countType(LATCH_COUNT_DOWN)` 不因本次失败增长）

### Requirement: OBarrier API

客户端 SHALL 提供循环屏障 `OBarrier` 与工厂 `OpenLatchClient.newBarrier(String key, int parties)`（创建者句柄，每次请求携带 `parties` 非零定型主张）、`newBarrier(String key, int parties, Runnable barrierAction)`（同前并声明该句柄到场合拢时携带动作）与 `newBarrier(String key)`（纯加入句柄，不主张初值，对从未定型的 barrier 被拒）。方法面：`void await() throws InterruptedException`——阻塞至所属世代的了结，受等待兜底超时约束；`boolean await(long timeout, TimeUnit unit) throws InterruptedException`——限时等待，返回是否以 TRIPPED 了结（`false` = 超时，且超时本身即破障，见下）；`void breakBarrier()`；`boolean isBroken()`；`long getParties()`。异常面遵循库内非受检纪律：破障了结抛 `OBrokenBarrierException`（新增公开异常，继承 `OpenLatchException`；替代 JDK 受检 `BrokenBarrierException`，差异 MUST 在契约 Javadoc 声明）、兜底超时抛 `OpenLatchTimeoutException`、JUC 受检 `TimeoutException` 以 `await(timeout)` 返回 `false` 的 boolean 形态承载（JDK 差异声明同上）。等待编排复用等待-通知-重发闭环：`QUEUED` 后挂起等 `AWAIT_NOTIFY`，通知到达以同 `request_id` 重发按世代了结记录幂等了结；执行者形态（应答 `executor=true`）在**本调用栈内**执行 `barrierAction`，完成（正常返回或抛异常）后以 `BARRIER_ACTION_DONE` 终结本等待——动作正常完成 ⇒ 本方 await 正常返回且同世代他方随后放行；动作抛异常 ⇒ SDK 改发 `BARRIER_LEAVE`（离场即破障），本方以该异常终结且同世代全体收 `OBrokenBarrierException`（对齐 JDK"动作异常使屏障破障"）。超时与中断语义：**离场即破障**——`await(timeout)` 超时、`await()` 兜底超时与本地中断均触发客户端 `BARRIER_LEAVE`，当前世代即时 BROKEN，全体在队他方收 `OBrokenBarrierException`（对齐 JDK 超时/中断破障）；且离场为**同步确认**：超时方 `await` 返回前 `BARRIER_LEAVE` 已经服务端生效（"返回即已破"次序——紧随其后的新到场 MUST NOT 落入被本次超时打破的世代；网络故障时降级为尽力而为，由服务端清扫与会话清理兜底）；`breakBarrier()` 以 `await_request_id=0` 的 `BARRIER_LEAVE` 表达，无在队身份亦破当前世代（条目不存在时无操作幂等）。**到场是有副作用的请求**：在途 `BARRIER_AWAIT` 遇传输失败/断连/换会话 MUST NOT 自动重发（至多一次纪律，判例 `countDown`），本方 await 以 `OpenLatchException` 裁决；同时该会话旧世代的等待项由服务端会话清理连带破障，他方以 `OBrokenBarrierException` 感知（相对 JDK"线程死亡静默挂起"的增强 MUST 显式声明）。`isBroken()` 返回句柄本地最近一次所见裁决，MUST NOT 发起网络查询（声明非实时跨进程一致）。等待者 MUST NOT 持有租约、MUST NOT 产生续租流量。**应答缺码形 MUST NOT 读作成功/读作放行**：SDK 收到应答信封后 MUST 校验其携带本请求的载荷——`Envelope.type` 回显而载荷 oneof 缺 `barrier_await_response`/`barrier_action_done_response`/`barrier_leave_response`（异型/空载荷码形违例）时 MUST NOT 读取 protobuf 默认实例（其 `status` 为枚举零值 `OK`）并据此裁决。等待路径（`BARRIER_AWAIT`）缺码形 MUST 并入等待-通知-重发闭环：**同会话**以同 `request_id` 重发，会话已换则走既有"到场不跨会话重放"裁决（抛 `OpenLatchException`），MUST NOT 假绿断言世代已合拢；执行者收尾路径（`BARRIER_ACTION_DONE`）缺码形按 `(会话, 世代)` 幂等重发（判例同其既有重发车道）；`breakBarrier` 与离场确认（`leaveAndAwait`）缺码形 MUST NOT 被读作 `OK`/确认成立——`breakBarrier` MUST 抛显式异常，离场确认 MUST 以"不确定"（`false`）收场，MUST NOT 据此虚报破障已生效（破坏"返回即已破"次序）或使 `isBroken()`/`getParties()` 读作默认实例。

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

#### Scenario: 缺码形 await 应答不得假绿放行

- **WHEN** `BARRIER_AWAIT` 的应答 `type` 回显而载荷 oneof 缺 `barrier_await_response`（脚本化服务面注入）
- **THEN** SDK 不读取默认实例、MUST NOT 断言世代已合拢返回 `true`；同会话以同 `request_id` 重发，请求预算内仍不可判定则以显式失败收场（超时口径），MUST NOT 误判 `generation`/`parties` 为默认值

#### Scenario: action-done 缺码形按世代幂等重发

- **WHEN** `BARRIER_ACTION_DONE` 的应答缺 `barrier_action_done_response` 载荷
- **THEN** SDK 不读取默认实例；按 `(会话, 世代)` 幂等以同 id 重发，预算内不可判定则抛 `OpenLatchTimeoutException`，MUST NOT 静默断言动作已了结

#### Scenario: breakBarrier 与离场确认缺码形不得读作确认

- **WHEN** `BARRIER_LEAVE`（`breakBarrier` 直发或超时/中断离场确认）的应答缺 `barrier_leave_response` 载荷
- **THEN** SDK 不读取默认实例、MUST NOT 断言离场已生效；`breakBarrier` 抛显式异常，离场确认按"不确定"（`false`）收场，`isBroken()` 不得据此虚报
