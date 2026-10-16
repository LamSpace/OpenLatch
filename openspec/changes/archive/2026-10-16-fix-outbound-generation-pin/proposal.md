# Proposal

## Why

台账 W22 的**未闭合残留**：调用方手持的 `LatchRoute` 是 `record(SessionContext session, RequestMultiplexer mux)`——**会话是创建时的快照，通道由 mux 活取**。全部 24 处出站都写成 `route.mux().sendWithId(env, ms)`：信封的 `requestId` 由**快照会话**分配，而写出落在 **mux 活取的那条通道**上。连接换代后（快照 S1 / 当前 S2），这条路径把"按 S1 编号的信封"写进 S2 的连接——服务端按**连接**判会话（`ClusterRequestHandler.handleAtomicOp` 取连接会话），该请求于是在 S2 的会话空间里当**新操作**落地：原子写与带 `op_seq` 的队列写的**去重槽按 `(session, op_seq)` 判定**，跨代不互相去重 ⇒ **静默双生效**的编排窗口（各族换代守卫按**路由对象**判会话，对这次换代不可见）。

上一 change（`2026-10-16-fix-outbound-pickup-same-generation`）已把**取口自身**的同代与"无会话零写出"修好；本 change 闭合**信封来源**这一侧的跨代配对。两者互补：前者保证"取口不混代"，后者保证"信封与其落点同代"。

## What Changes

- **出站口绑定同代基准**：`RequestMultiplexer` 新增 `sendPinned(envelope, pinned, timeoutMs)`——以 `pinned`（分配该信封 `requestId` 的会话）为同代基准，**仅当 `pinned` 仍是连接当前会话时写出**；否则瞬态 `ServerUnavailableException` 且**不写出**（调用方按可重试处理；写路径随后由各族换代守卫以显式 `SESSION_EXPIRED` 收场——静默错值严格劣于显式失败）。
- **路由即出站口**：`LatchRoute` 由 record 改为**不可绕过**的形态——不再暴露裸 `mux()`，只提供 `session()` 与 `send(envelope, timeoutMs)`（后者以本路由的会话为同代基准）；24 处 `route.mux().sendWithId(...)` 全部迁到 `route.send(...)`（参数表同形，机械替换）。
- **保留的入口与其语义**：`sendWithId(envelope, timeoutMs)` 保留给"信封由**当前**会话现取现发"的调用方（同代基准 = 调用当下的活动会话；`AwaitTracker` 同 id 重发、种子探针 `CLUSTER_VIEW`）。等待类在会话变化时是**失败或迁移**（不跨会话重发，台账 v9/v10 已钉），故其 2 参形态在本 change 内是忠实的；Javadoc 明写这条边界。
- **非目标**：不改"路由起手钉住"的**车道选取**语义（不因外部刷新重取全局路由）；不改各族重发/重放纪律；不改协议与服务端；不改 `sendHandshake` 的握手豁免。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

- `client-sdk`：`断连快速失败与重连` 需求的「出站取口 MUST 同代」条款**加强一条**——信封与其落点 MUST 同代：凡持有会话快照（路由）的调用方，其出站 MUST 以该快照为同代基准，快照已非当前会话时 MUST NOT 写出。既有条款与**全部既有场景逐字保留**（6 条），并新增 1 条场景（陈旧路由出站不得跨会话落点）。

## Impact

- **代码**：`openlatch-client` 的 `RequestMultiplexer`（`sendPinned`）、`OpenLatchClient.LatchRoute`（record → 不可绕过的出站口）、8 个 `Remote*` 原语（24 处机械迁移）。零协议/服务端/配置变更。
- **测试**：新增**确定性端到端夹具**（脚本化桩 + 真实 TCP + 真实重连：取路由 → 强制换代 → 经陈旧路由出站 ⇒ 服务端侧零出现、本端瞬态失败）+ 出站口边界锁定（陈旧 pin 拒绝 / 同代 pin 照常写出）。
- **风险面**：陈旧路由出站由"静默落到新会话"改为"显式瞬态失败"，调用方已有重取路由的路径（各族在"路由不可用"处重取）⇒ 对**读**是改道重试，对**写**是显式 `SESSION_EXPIRED`（契约本就声明"效果不确定，以 `getStamped()` 核对"）。
