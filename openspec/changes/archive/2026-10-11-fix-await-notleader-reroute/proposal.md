# Proposal

## Why

上一轮（`fix-lane-reroute-convergence`，已归档）把非 ACQUIRE 车道的**路由收敛**修好了：home 驻留期按 1s 经 `CLUSTER_VIEW` 核对当值 Leader，队列/原子等重发类请求在换主窗与启动窗内收敛。

但**等待类原语只吃到一半红利**：闩与屏障的 `await` 对 `NOT_LEADER` **应答状态**仍是终态抛出（等待环的状态分派为 `OK`/`QUEUED` 之外一律 `throw`），且该分支不重取路由。于是"真主存在而本方路由陈旧"的那约 1s 窗口内发起的 `await` 仍会**立即失败**——尽管服务端的这次答复恰恰证明请求**从未被受理**。本次按 `DECISIONS.md` 2026-10-11 裁决把该形态改为**零生效重道**。

## What Changes

- 闩/屏障 `await` 收到 `NOT_LEADER` **应答状态**时：摘除本机等待登记 → 重取路由 → **仅当路由会话确已变化**时以新会话重发。
- 该重发受**独立短预算**约束（上限 = 2 个 Leader 核对周期，即 2s），与调用方的等待总预算无关；预算耗尽或路由在预算内始终未变 → 仍以 `NOT_LEADER` **显式抛出**。
- 保住 v9/D3 两条关切：**不假绿**（永不返回 `true`/不返回 0）与**不悬挂至等待总预算**（失败上界 2s）。
- **非目标**：不动相位器/定时器的同型等待面；不动变异类（发布/注册/装载/撤销）——其 `NOT_LEADER` 是"可能已受理、请换新请求重试"的**显式信号**，不得盲目重发；不动**不确定窗**（传输失败/读界超时/断连）后的既有严格纪律（闩的换代守卫、屏障的到场不跨会话重放）。

## Capabilities

### New Capabilities

（无——本次是既有等待原语在已声明拒绝码上的行为修正。）

### Modified Capabilities

- `client-sdk`：`OCountDownLatch API` 与 `OBarrier API` 两需求新增「`NOT_LEADER` 应答状态的零生效重道」条款及其场景。既有条款与全部既有场景逐字保留；环绕**不确定窗**的既有"至多一次 / 不跨会话重放"纪律原样保留，并把边界显式划清（已答复的 `NOT_LEADER` 不属不确定形态）。

## Impact

- **代码**：仅 `openlatch-client` 的两个等待环（`RemoteCountDownLatch.doAwait`、`RemoteBarrier.doAwait`）；重道预算常量取自 `OpenLatchClient` 的核对周期（单一事实源）。
- **测试**：新增"陈旧路由但存在真主"的确定性夹具（闩、屏障各一），同时锁定"预算内重道成功"与"预算耗尽仍显式失败且确有等待"两侧。既有 `RemoteCountDownLatchScriptedTest#sameShapeNotLeaderRejectOnAwaitThrowsNotFalseGreen` 的**断言不变**（显式异常 + 耗时 < 5s 在新行为下依然成立），仅其口径注释需同步。
- **兼容**：纯客户端行为增强。无协议变更、无服务端改动、无配置项。对**永久**返回 `NOT_LEADER` 的服务端/错配形态，失败由"立即"变为"约 2s 后"——有界退化，且异常仍携带可读的 `NOT_LEADER` 码。
