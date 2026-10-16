# Design

## Context

动机见 `proposal.md`。塑造方案的现状与约束：

- **路由现状**：`LatchRoute` 是 `record(SessionContext session, RequestMultiplexer mux)`——会话取自 `latchRoute()` 创建时的**活读**（`lane.cm.session()` 或 `connectionManager.session()`），一旦创建即为**快照**；而 mux 的通道供应者是**活取**。24 处出站（8 个 `Remote*` 原语）一律 `route.mux().sendWithId(env, ms)`，信封的 `requestId` 由快照会话分配 ⇒ 跨代配对。
- **上一 change 已就位**：`sendWithId` 的取口判据（会话非空 → 通道可用 → 回读会话同对象）与 `sendHandshake` 的握手豁免口。本 change 在其上加"来源侧"的同代基准。
- **同代可判定的依据**：`SessionContext` **每连接新建**（对象身份即代次），且会话与通道在连接状态机内**成对更换**（`handleChannelInactive` 先清两者、`onHelloResult` 在 ACTIVE 绑定）⇒ "pin 仍为当前会话"可用一次身份比较判定，不需要协议字段。
- **去重槽按 (会话, op_seq)**：服务端 `AtomicEntry` 去重规则按 `(slotSession, slotOpSeq)` 命中重放；跨代会话不互相去重 ⇒ 跨代落点是**静默双生效**的编排窗口（而非"被拒"）。这正是必须由客户端堵死的理由：服务端无从分辨"这是一次跨代重发"。
- **既有纪律（不得放宽）**：各族"至多一次/不跨会话重放"、`LatchRoute` 的"路由起手钉住"（车道选取不因外部刷新重取）——本 change 只把**出站落点**与路由的会话绑死，不动车道选取。
- **等待类为何保留 2 参形态**：`AwaitTracker` 的挂起等待在会话变化时**失败或迁移**（断连 `failAll`、Leader 改道 `migrateWaitsTo` 以新会话重分配 `requestId`），其重发**只在同会话内**发生 ⇒ 以活会话为基准是忠实的；Javadoc 明写这条边界，不做无根据的强加。

## Goals / Non-Goals

**Goals:**

- 让"按某代会话编号的信封"**不可能**落在另一代会话的连接上（写路径不再存在"静默落到新会话"的编排）。
- 让这条保证**不可绕过**：持有路由的调用方拿不到裸出站口。
- 保证失败形态是**显式**的：瞬态"服务不可用"（调用方重取路由）或换代守卫的 `SESSION_EXPIRED`，绝不静默重复生效。

**Non-Goals:**

- 不改车道选取（"路由起手钉住"原样）、不改各族重发/重放纪律、不改协议/服务端。
- 不给 `AwaitTracker`/种子探针强加 pin（见上：其语义本就在同会话内）。
- 不为"跨代落点"造**人为可复现**的时序（见 D4）。

## Decisions

### D1 同代基准放在出站口，以**身份比较**判定

`sendPinned(envelope, pinned, timeoutMs)`：`pinned == null` ⇒ 瞬态失败；通道不可用 ⇒ 瞬态失败；`sessionSupplier.get() != pinned` ⇒ 瞬态失败且**不写出**。三项判负全部复用既有瞬态失败语义（`ServerUnavailableException`），不新增异常类型、不新增失败面。`sendWithId(envelope, timeoutMs)` 保留为 `sendPinned(envelope, 当前会话, timeoutMs)` 的便捷形态（语义：**现取现发**），并在 Javadoc 明写"持有快照的调用方 MUST 用带基准的形式"。

### D2 路由成为**唯一**出口，且必然带基准

`LatchRoute` 由 `record` 改为普通类：**不暴露 `mux()`**，只暴露 `session()` 与 `send(envelope, timeoutMs)`（后者 = `mux.sendPinned(envelope, session, timeoutMs)`）。

> 备选（保留 `mux()` 只做约定）被否：约定挡不住后来者——下一处 `route.mux().sendWithId(...)` 会重新打开同一窗口，且不会有任何测试发现。**结构上取消裸出口**才是闭合；代价是 24 处调用点的机械迁移（参数表同形，`route.mux().sendWithId(` → `route.send(`）。

### D3 拒绝**不**改写为"在新会话上重发"

陈旧路由出站一律瞬态失败，**不**由 mux 代调用方换会话重发。理由：写完收到的是"服务可用"的假象，而去重槽在换代后**不覆盖**——由 mux 换会话重发正是静默双生效本身。换会话重发只能是**调用方**的决定，且必须由各族既有的"换代守卫/幂等判据"背书（原子族：`SESSION_EXPIRED` 显式抛出并声明效果不确定、以 `getStamped()` 核对；队列族：`SessionContinuation` 单调不确定闩）。

### D4 夹具按"经路由出站"的**行为**锁定，不造人造时序

端到端夹具（脚本化桩 + 真实 TCP + 真实重连）：
1. 客户端连上桩、握手得会话 A；`client.latchRoute()` 取**陈旧路由**（快照 A）；
2. 关掉客户端通道 ⇒ 客户端重连、握手得会话 B（`session().sessionId() != A` 为换代判据）；
3. 经**该陈旧路由**出站 ⇒ 断言**服务端侧零出现该请求**且本端以瞬态失败收场。

**关于"红先"的诚实说明**：断言（服务端零出现 + 瞬态失败）自始未变；**调用形态**随本 change 的 seam 迁移（修前 `route.mux().sendWithId(env, ms)`、修后 `route.send(env, ms)`）——因为修法本身就是"取消裸出口"。修前该夹具实测**红**（请求确被写到桩侧），修后转绿。另加两枚边界锁定（非红先）：陈旧基准拒绝 / 同代基准照常写出。

### D5 观测与门禁不变

`sendPinned` 与 `sendHandshake` 共用既有私有 `dispatch`（登记、同 id 交叠、超时、指标收口单点），观测不双注册；既有单测夹具带非空会话者口径不变。

## Risks / Trade-offs

- [误伤正常路径] → 正常 ACTIVE 期 pin 与当前会话**同一对象**，恒通过；回归面＝客户端模块全部单测 + 8 个 `Remote*ScriptedTest` + 重连/集群 IT。
- [陈旧路由出站由"静默成功"变为"显式失败"，可能升高换主窗内写路径的显式失败率] → 这是**有意的**：静默错值（跨代双生效）严格劣于显式失败（台账 2026-10-11 回退轮同类裁决）；且调用方本就有"路由不可用即重取"的路径。此点在 W19 第 (2) 步的同负载复测中一并观察（换主窗内写路径的显式失败率）。
- [24 处机械迁移引入笔误] → 参数表同形替换、编译器兜底（`mux()` 不再存在，漏改即编译失败），并跑客户端模块全量 verify。

## Migration Plan

纯客户端变更：无协议/配置/状态迁移。回滚＝还原三处（`RequestMultiplexer` 便捷形态、`LatchRoute` 形态、24 处调用点），无残留状态。

## Open Questions

（无——不闭合面（车道选取语义、等待类跨会话纪律）均显式列为 Non-Goals。）
