# Proposal

## Why

`RemoteCountDownLatch` 与 `RemoteBarrier` 读取应答时**均不判 presence**——等待路径直接读
`resp.getLatchAwaitResponse().getStatus()` / `resp.getBarrierAwaitResponse().getStatus()`。
缺码形应答（`Envelope.type` 回显 `LATCH_AWAIT` / `BARRIER_AWAIT` 而载荷 oneof 缺对应成员）
会被读作 protobuf 默认实例，其 `status` 为枚举零值 `OK`。**等待族后果最重**：`await`
**假绿返回"已放行"`（服务端从未归零/合拢），`countDown` 静默回 `remaining=0`。

这正是 `fix-atomic-malformed-envelope` 消灭的"静默错值"形态，而该 change 当时把这三族
（含本例的 latch / barrier）明列为 non-goal 搁置（W17）。本例把同一护栏扩到等待族两族——
静默错值严格劣于显式失败，且缺码形可由脚本化夹具**确定性复现**（不依赖负载敏感的竞态）。

## What Changes

- `RemoteCountDownLatch.doAwait`：await 应答读取前增 `hasLatchAwaitResponse()` 判定；缺码形
  并入既有等待-通知-重发闭环（同 `request_id` 重发），预算内仍不可判定则以显式失败收场——
  **MUST NOT 假绿返回 `true`**。
- `RemoteCountDownLatch.countDownOp`：`countDown`/`init` 为**至多一次**（无去重槽，重发不安全）；
  缺码形 MUST NOT 读默认实例返回 `remaining=0` 伪成功，按显式失败收场，且**不自动重发**。
- `RemoteBarrier.doAwait`：await 应答读取前增 `hasBarrierAwaitResponse()` 判定；缺码形**同会话**
  按同 `request_id` 重发，会话已换则走既有"到场不跨会话重放"裁决（抛 `OpenLatchException`）。
- `RemoteBarrier.barrierActionDone`：同款判定，缺码形按 `(会话, 世代)` 幂等重发。
- `RemoteBarrier.breakBarrier` 与 `leaveAndAwait`：缺码形 MUST NOT 被读作 `OK`/确认成立
  （`breakBarrier` 空成功、`isBroken` 错判、破坏"返回即已破"次序的静默错值）。
- **不新增机制**：语义上只是把"缺码形"并入各族既有失败车道（等待族重发 / 至多一次族显式失败）。
- **非目标**：不动 `RemoteTopic`（同型缺口，另立 change）；不触碰 v9 对 latch await `NOT_LEADER`
  的终态裁决；不放宽跨会话到场守卫。

## Capabilities

### New Capabilities

（无——既有原语的应答裁决修正。）

### Modified Capabilities

- `client-sdk`: `OCountDownLatch API` 与 `OBarrier API` 增「应答缺码形 MUST NOT 读作成功」条款
  及其场景；两条既有 Requirement 的条款与既有场景逐字保留。

## Impact

- **代码**：仅 `openlatch-client` 的 `RemoteCountDownLatch`（两处读点）与 `RemoteBarrier`（四处读点）。
- **测试**：扩展 `RemoteCountDownLatchScriptedTest` 并新增 `RemoteBarrierScriptedTest`——
  `ScriptedServer` 直驱注入缺码形应答（`type` 回显而载荷 oneof 缺成员），断言**不假绿、不静默值**，
  并区分"等待路径重发"与"至多一次路径不重发"。
- **兼容**：纯客户端行为修正。正常服务端从不产生缺码形应答，对既有部署**零影响**；变化的只是
  "异常输入下静默错值/假绿"→"重发，必要时显式失败"。
- **文档**：接口契约 Javadoc 声明该处置；双语排障指南增一条排查项（"latch/barrier await 疑似假绿"）。
