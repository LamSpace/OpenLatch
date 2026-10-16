# Design

## Context

动机见 `proposal.md`。塑造方案的现状与约束：

- **两条发送路径**：`RequestMultiplexer.send(builder, timeout)`（业务主入口）先取会话、无会话即瞬态失败，再委托 `sendWithId`；`sendWithId(envelope, timeout)` 是"信封自带 requestId"的出站口，被 30 处调用（各族 `Remote*` 的重发/续发、`AwaitTracker` 同 id 重发、`ConnectionManager.sendHello`、`SeedDiscovery` 探针）。**它只查 TCP 活性，不查会话**——这正是"两条路径口径不一致"的所在。
- **通道语义错配**：`ConnectionManager.activeChannel()` 直返字段，`HELLO_SENT`（`channel=ch` 已置、`session` 仍为 `null`）期它交出的是**未握手通道**；而 `session()` 在 `ACTIVE` 之前恒为 `null`。两个供应者读的是同一个 `ConnectionManager`，但**准入判据不同表**。
- **换代的可观测性**：`session` 与 `channel` 在 `stateLock` 内成对更换（`handleChannelInactive` 先置 `channel=null; session=null`，`onHelloResult` 在 `ACTIVE` 时绑定 `session`），且 `SessionContext` 是**每连接新建**的实例 ⇒ **对象身份即代次**。这使"同代"可用一次回读判定，无需新协议字段。
- **既有纪律（不得放宽）**：各族"至多一次/不跨会话重放"等不确定窗纪律、`LatchRoute` 的"路由起手钉住"（不因外部刷新改取全局路由）——本 change 均不触碰。
- **既有夹具**：`ScriptedServer`（真实 TCP + 分帧 + 入站记录 + `handler` 返回 `null` 即不应答）已足以造"TCP 接受但 `HELLO` 延后/失败"的脚本化连接；`RequestMultiplexerTest` 的内存通道 + 供应者注入形态可直接复用为"取口期间换代"的确定性夹具。

## Goals / Non-Goals

**Goals:**

- 业务出站在"无会话/未握手"窗口**零写出**，并以瞬态"服务不可用"收场（调用方按可重试失败处理，重连后由其自身逻辑尝试续发）。
- 取口自身**不混代**：会话与通道必来自同一次取口。
- 握手豁免**路径级**化：`HELLO` 照旧先于会话发出，且该豁免不给业务请求留后门。

**Non-Goals:**

- 不闭合"调用方持有跨代 `LatchRoute` 快照"这一**残留**（见下 D4）。
- 不改各族重发/续发语义、不改协议、不改服务端、不改车道选取（"路由起手钉住"原样）。
- 不为既有 `sendWithId` 调用点新增"钉住会话"参数（30 处调用点的大改面不在本 change 判据内）。

## Decisions

### D1 判据取"会话非空 + 双读同一代会话"

`sendWithId` 的准入序列：**取会话**（`null` ⇒ 瞬态失败）→ **取通道**（`null` 或非 active ⇒ 瞬态失败）→ **回读会话并与首次取到的实例比身份**（不同 ⇒ 瞬态失败）。依据是 Context 里的"对象身份即代次"与"成对更换"：两次读到同一实例，则中间读到的通道必属该会话所绑连接；不同则说明取口期间已换代，宁可判负也不写出。判负一律走**已存在的瞬态失败路径**（`ServerUnavailableException`），不新增异常类型、不新增失败语义。

> 为什么不引入"取口快照记录"（把 `(channel, session)` 打成一条不可变记录由 `ConnectionManager` 在锁内产出）：那要改 `RequestMultiplexer` 的构造签名（两个供应者 → 一个），牵动 5 处测试装配与 2 处生产装配；双读以 3 行达成同等判据，且不动任何装配面。**取舍：以最小改面换判据等价**。

### D2 握手豁免是**路径级**的，不是旗标

新增 `sendHandshake(envelope, channel, timeout)`：以调用方给定的通道为出口，**不查会话**（`HELLO` 先于会话存在）。两个握手发起点各传自己的通道（连接状态机 `sendHello(ch)` 手上就有 `ch`；种子探针口有探针通道），业务路径**没有**入口能到达该豁免。备选（在 `sendWithId` 上加 `boolean handshake` 旗标）被否：旗标会把豁免暴露在同一条门上，"业务请求借道"从"不可能"降为"靠调用方自觉"。

### D3 状态门依约：`activeChannel()` 只在 `ACTIVE` 交出通道

`activeChannel()` 的 Javadoc 早已承诺"非活动状态返回 null"，实现直返字段属实现落后于契约。按契约修为"仅 `ACTIVE` 返回通道"，使"通道表"与"会话表"对同一状态机取同一判据——两条路径的准入口径由此**同表**。它同时是 D1 之外的第二道闸（未握手窗口连通道都取不到）。

### D4 残留：跨代 `LatchRoute` 快照**不**在本 change 内闭合（显式登记）

调用方（各族 `Remote*`）手持的 `LatchRoute` 存的是**创建时的 `SessionContext` 快照**，而 mux 活取会话：换代后可由"旧代编号的信封"落在"新一代通道"上。闭合它须让出站信封携带其钉住的会话（或让 `LatchRoute.session()` 改为活取），前者是 30 处调用点的改面，后者触及"路由起手钉住"的既有语义（2026-10-11 回退轮的判例域），**两者都不属本 change 的判据**。本 change 的判据只覆盖"取口自身不混代"与"无会话零写出"；台账 W22 行按此**收窄**（残留保留，写清其触发与闭合条件）。

> 之所以敢留：该残留的**可观测危害**需要"同一次逻辑写的旧代尝试已生效而应答丢失"与"换代落在守卫检查与写出之间"两件事同时成立，且各族在观察到任何失败时都会重取全局路由（重取后守卫按**当前**会话判定）——即危害窗远窄于"无会话仍写出"这一确定性形态。它是登记项，不是本轮的判据项。

### D5 既有调用点零签名变更

`sendWithId(Envelope, long)` 签名不动（30 处调用点不动），只在其内部加取口判据；`send()` 继续以它为唯一收口（观测不双注册）。种子探针的 `CLUSTER_VIEW` 走 `sendWithId`（其会话供应者恒非空且身份稳定），探针 `HELLO` 改走 `sendHandshake`。

## Risks / Trade-offs

- [把"未握手窗口"判负后，某些本可侥幸成功的请求变成显式失败] → 那正是契约要的：未握手连接上写出的业务请求必被服务端拒（`SESSION_EXPIRED`），本端先判负只是把**注定失败**提前且不再污染线路。失败是瞬态的，重连后由调用方下一次尝试续发。
- [判据过严，误伤正常路径] → 正常 `ACTIVE` 期两次回读同一实例，恒通过；回归面＝既有全部客户端用例（含 8 个 `Remote*ScriptedTest` 与重连/握手用例）不得变红。
- [豁免被业务请求借道] → D2 的路径级豁免：业务路径无入口到达。
- [残留被读成"已闭合"] → D4 + 台账行同步收窄，proposal 的 Impact 明写。

## Migration Plan

纯客户端变更：无协议/配置/状态迁移。回滚＝还原 `RequestMultiplexer`、`ConnectionManager`、`SeedDiscovery` 三处，无残留状态。

## Open Questions

- 跨代 `LatchRoute` 快照（D4 残留）是否值得以"信封携带钉住会话"闭合：取决于是否出现"旧代尝试已生效 + 应答丢失 + 换代落在守卫与写出之间"的现场或夹具。**本 change 不预判**。
