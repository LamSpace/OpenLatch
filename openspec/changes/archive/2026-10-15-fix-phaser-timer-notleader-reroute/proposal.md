# Proposal

## Why

上一轮给闩/屏障的 `await` 落了"`NOT_LEADER` 应答状态的零生效重道"（缺口：真主存在而本方路由陈旧的那约 1s 窗内，等待会因一个**证明"本次从未被受理"**的答复而立即失败）。**等待面还剩两族没吃到同样的红利**：

- `RemotePhaser.awaitAdvanceInterruptibly`：状态分派只有 `OK`/`QUEUED` 可续，其余一律 `throw`——`NOT_LEADER` 因此是终态。
- `RemoteTimer.awaitInterruptibly`：同款（`OK`/`DENIED`/`QUEUED` 之外一律 `throw`）。

同一个洞，同一类不诚实的失败。而且这两个循环里**已经写着**"纯等待无复制态副作用：跨会话重建信封安全（自愈通道）"——也就是说，**换会话重发这件事对它们是既有语义**，本 change 不需要任何新增的安全论证，只是把"已答复的 `NOT_LEADER`"这条路径从"终态抛出"改成"有界重道"。

## What Changes

- `OPhaser API` 与 `OTimer API` 两需求新增「`NOT_LEADER` **应答状态的零生效重道**」条款（各附场景）：
  - **纯等待操作**（`awaitAdvance` / `awaitAdvanceInterruptibly` / `await()` / `await(timeout)`）收到 `NOT_LEADER` 应答状态时：摘本机等待登记 → **触发一次共享路由刷新** → 在**独立短预算**（上限 2s，与调用方等待总预算无关）内等路由改道 → **仅当路由会话确已变化**时以新会话重发续等；预算耗尽或路由始终未变 → 仍以 `NOT_LEADER` **显式抛出**。
  - 复用既有实现入口（`OpenLatchClient.awaitReroutedRoute` 与同一预算常量），两个等待环不各自造轮子。
  - 沿闩/屏障同款的**不确定窗护栏**：曾出现"结果不可判定"的失败（传输失败/读界超时/应答缺码形）的等待不再重道（对纯等待这里只是保守收窄，语义上跨会话重挂本就安全）。
- **非目标（本 change 明确不做）**：
  - `arriveAndAwaitAdvance` 的**到场半程**：它的 `NOT_LEADER` 不总能证明零生效——到场可能已在更早一次尝试里被计入，重道会把到场重复计数。要做须先立"到场是否已记账"的判据，属另议。
  - 变异操作（`register`/`arrive`/`arriveAndDeregister`、`schedule`/`disarm`）与读数的 `NOT_LEADER` 处置：它们是"变异车道显式超时面"那条独立残余的一部分，按既有约定须**逐族先立"未生效"判据**（不得盲目重发）。
  - 相位器/定时器的其它契约（到场语义、代终结、无损耗重挂）一律不动。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

- `client-sdk`：`OPhaser API` 与 `OTimer API` 两需求各新增一条「`NOT_LEADER` 应答状态的零生效重道」条款与场景；既有正文与**全部既有场景逐字保留**（相位器 4 条、定时器 4 条）。

## Impact

- **代码**：仅 `openlatch-client` 两个等待环（`RemotePhaser.awaitAdvanceInterruptibly`、`RemoteTimer.awaitInterruptibly`）与它们的契约注释（含公有 `OPhaser`/`OTimer` 接口）。**零 `src/main` 其它面改动**、零协议改动、零服务端改动。
- **测试**：新增"陈旧路由而真主可达"的确定性夹具（相位器、定时器各一枚）与"永久 `NOT_LEADER` 有界失败"两侧锁定（各一枚）。判据集沿用既有 W11 判据类，故**与门控退役同行后续自动进默认构建**。
- **兼容**：纯客户端增强。对**永久**返回 `NOT_LEADER` 的形态，失败由"立即"变为"约 2s 后"——有界退化，异常仍携带 `NOT_LEADER` 码。
