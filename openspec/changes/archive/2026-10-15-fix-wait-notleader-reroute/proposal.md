# Proposal

## Why

`fix-lane-route-refresh`（2026-10-14 合入）把非 ACQUIRE 车道的**路由收敛**修好了：惰性共享刷新把当值 Leader 发布为期望路由，各请求在自身"路由不可用"点上取用。

但**等待类原语只吃到一半红利**：闩与屏障的 `await` 对 `NOT_LEADER` **应答状态**仍是终态抛出（状态分派为 `OK`/`QUEUED` 之外一律 `throw`，见 `RemoteCountDownLatch:220`、`RemoteBarrier:305`）。于是"真主存在而本方路由陈旧"的那约 1s 窗口内发起的 `await` 仍会**立即失败**——尽管服务端的这次答复恰恰**证明请求从未被受理**（角色门在触达状态机前拒绝 = 零生效）。

该形态曾于 2026-10-11 修过（归档 `2026-10-11-fix-await-notleader-reroute`），随后**随整批回退**——回退理由不是该设计有误，而是其**使能前提（周期核对会改道）消失**，"保留只会变成无望窗内多等 2s"。**那个前提现在回来了**（`fix-lane-route-refresh` 提供触发器），故本 change 把该设计**重落**。

## What Changes

- 闩/屏障 `await` 收到 `NOT_LEADER` **应答状态**时：摘除本机等待登记 → **触发一次共享路由刷新**（本 change 相对回退版新增的一步：主动促成改道，而非被动轮询等待它发生）→ 仅在**本等待从未进入不确定窗**时以**新会话**重发续等。
- 重道受**独立短预算**约束（上限 2s，与调用方等待总预算无关）；预算耗尽或路由始终未变 → 仍以 `NOT_LEADER` **显式抛出**。
- 保住两条既有契约：**不假绿**（永不返回"已放行"/不返回 0）与**不悬挂**（失败上界 2s）。
- **非目标**：不动相位器/定时器的同型等待面（属另议）；不动**变异类**（发布/注册/装载/撤销——其 `NOT_LEADER` 是"可能已受理"的显式信号，不得盲目重发）；**不动不确定窗纪律**（闩的换代守卫、屏障的"到场不跨会话重放"原样保留）。

## Capabilities

### New Capabilities

（无——既有等待原语在已声明拒绝码上的行为修正。）

### Modified Capabilities

- `client-sdk`：`OCountDownLatch API` 与 `OBarrier API` 两需求新增「`NOT_LEADER` 应答状态的零生效重道」条款及场景（闩 2 条、屏障 1 条）。既有条款与**全部既有场景逐字保留**（闩 4、屏障 11）；不确定窗纪律的边界显式划清（已答复的 `NOT_LEADER` 零生效，不属不确定形态）。

## Impact

- **代码**：仅 `openlatch-client` 两个等待环（`RemoteCountDownLatch.doAwait`、`RemoteBarrier.doAwait`）；预算常量取 `OpenLatchClient` 既有刷新周期（单一事实源）。
- **测试**：新增"陈旧路由但真主存在"的确定性夹具（闩、屏障各一），锁定两侧（预算内重道成功 / 预算耗尽仍显式失败）；既有 `sameShapeNotLeaderRejectOnAwaitThrowsNotFalseGreen` 断言不变（显式异常 + 耗时上界在新行为下仍成立），仅口径注释同步。
- **兼容**：纯客户端增强，无协议/服务端/配置变更。对**永久**返回 `NOT_LEADER` 的形态，失败由"立即"变为"约 2s 后"——有界退化，异常仍携带 `NOT_LEADER` 码。
