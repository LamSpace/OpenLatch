# Proposal

## Why

`fix-lane-reroute-convergence`（#21）修好了非 ACQUIRE 车道的**路由收敛**；`fix-await-notleader-reroute`（#22）把**闩/屏障** `await` 的 `NOT_LEADER` 应答状态从"终态抛出"改为**零生效有界重道**。但 #22 刻意收了窄范围，于是这个族现在**不一致**：闩/屏障重道，**相位器/定时器的等待面仍终态抛出**——而支撑 #22 的判据（服务端已答复 `NOT_LEADER` ⇒ 本次未受理 ⇒ 跨会话重放安全）对它们**同样成立**，且更强：

- 两族等待环的跨会话重建块**本就**自述"纯等待无复制态副作用：跨会话重建信封安全（自愈通道）"；
- 两族的**瞬态**分支（传输失败/会话切换）**已经**放行跨会话重发（`route = null; env = null; continue`）——`NOT_LEADER` 的"未受理"证据比"瞬态"更明确。

也就是说，这不是"另一类原语的不同取舍"，而是**同一类里剩下的两个**。

## What Changes

- 相位器 `awaitAdvance` / `awaitAdvanceInterruptibly` 与定时器 `await()` / `await(timeout)` 的等待环，收到 `NOT_LEADER` **应答状态**时：摘除本机等待登记 → 在**独立短预算**（2 个 Leader 核对周期 = 2s）内等路由改道 → **仅当路由会话确已变化**时以新会话重发；预算耗尽或路由始终未变 → 仍以 `NOT_LEADER` **显式抛出**。
- 复用 #22 建立的共享机械：`OpenLatchClient#awaitReroutedRoute` 与同一预算常量（单一事实源），两族等待环各接入一处状态分派分支。
- **非目标**：不动变异类（相位器的变异车道、定时器的 `schedule`/`disarm`）——其 `NOT_LEADER` 语义与等待面不同，不在本次范围；不动不确定窗后的既有纪律；不改相位/代次语义与读数口径。

## Capabilities

### New Capabilities

（无——同一原语族的等待面行为对齐。）

### Modified Capabilities

- `client-sdk`：`OPhaser API` 与 `OTimer API` 两需求各新增「等待面 `NOT_LEADER` 应答状态的零生效重道」条款及其场景。既有条款与全部既有场景逐字保留；两族"同一会话以原 `request_id` 重发"的分片保活护栏原样保留，并把边界划清（跨会话重道必然换会话与 `request_id`，与该护栏约束的**同会话**保活重挂不冲突）。

## Impact

- **代码**：仅 `openlatch-client` 的两个等待环（`RemotePhaser.awaitAdvanceInterruptibly`、`RemoteTimer.awaitInterruptibly`）；共享助手与预算常量沿用 #22，不新增机制。
- **测试**：新增两条确定性夹具（相位器、定时器各一，复用 #22 的"陈旧路由 + 存在真主 / 无真主"两侧形态）。
- **兼容**：纯客户端行为增强。无协议变更、无服务端改动、无配置项。对**永久**返回 `NOT_LEADER` 的服务端/错配形态，失败由"立即"变为"约 2s 后"——与 #22 同款的有界退化。
- **文档**：双语指南 `09-troubleshooting` 的等待类条目由"相位器/定时器仍为终态"改为"全等待族一致"。
