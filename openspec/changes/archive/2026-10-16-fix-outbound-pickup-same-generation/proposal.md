# Proposal

## Why

台账 W22 登记的是**客户端请求取口的会话/通道不同代**，代码面两处事实：

1. `ConnectionManager.activeChannel()`（`ConnectionManager.java:408`）直返字段，与同处 Javadoc（`:403-407`）承诺的"非活动状态返回 `null`"相悖——`HELLO_SENT` 期它仍指向 **TCP 已连通但尚未握手**的通道；
2. `RequestMultiplexer.sendWithId`（`:152`）只查 `channel.isActive()`（TCP 语义），而同文件 `send()`（`:129-133`）**查会话** ⇒ 两条发送路径的准入口径不一致：会话门只装在一条门上。

后果落在**重发/续发路径**上（`AwaitTracker` 的同 id 重发、各族 `Remote*` 持有的 `LatchRoute`）：在"当前无会话（握手未完成）"的窗口里，业务请求会被写出到线上——服务端按**连接**判会话（`ClusterRequestHandler.handleAtomicOp` 取连接会话），未握手连接收到的请求被 `validateEnvelope` 以 `hasSession(...)==false` 回 `SESSION_EXPIRED`，即客户端白写一条注定被拒的帧，且该帧还可能污染"这条逻辑请求已发出"的本地判断；跨代写出则是"信封按旧代编号落在新一代通道"的编排窗口（服务端每会话去重槽按 `(session, op_seq)` 判定，同名不同代的请求不互相去重）。

该形态已由确定性夹具坐实（2026-10-16，见台账 W22 行）：内存通道夹具下"无活动会话"与"路由会话 ≠ 连接当前会话"两种形态**2/2 红**（业务请求都被写出）。

## What Changes

- **出站取口同代**：`RequestMultiplexer` 的业务出站（`sendWithId`，`send` 经其收口）在取口时 MUST 有活动会话，且**会话与通道 MUST 同代**（取会话 → 取通道 → 回读会话，同一对象方得写出）；无会话、通道不可用或取口期间换代一律**不写出**，以瞬态 `ServerUnavailableException` 快速失败。
- **握手豁免走显式口**：新增**握手出站路径**（以调用方给定的、本次连接刚建立的通道为出口），`HELLO`（连接状态机与种子发现探针两处）改由它发出。豁免是**路径级**的，不因共用 `sendWithId` 而泄漏给业务请求。
- **状态门依约**：`ConnectionManager.activeChannel()` 按既有 Javadoc 只在 `ACTIVE` 交出通道（非 `ACTIVE` 返回 `null`）。

非目标：不动"路由起手钉住"（车道/mux 的选取语义）；不动各族等待-通知-重发闭环与不确定窗纪律；不动协议与服务端。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

- `client-sdk`：`断连快速失败与重连` 需求新增「出站取口 MUST 同代」条款与三条场景（握手未完成期业务请求零写出且瞬态失败 / 取口期间换代不得混代 / 握手豁免不适用于业务请求），既有条款与**全部既有场景逐字保留**（3 条）。

## Impact

- **代码**：`openlatch-client` 三处——`RequestMultiplexer`（取口 + 显式握手口）、`ConnectionManager`（`activeChannel()` 依约、握手改走豁免口）、`SeedDiscovery`（探针 `HELLO` 改走豁免口）。
- **测试**：新增脚本化连接夹具（`ScriptedServer`：TCP 接受、`HELLO` 不应答）锁定"未握手期零写出"；新增取口期间换代的内存夹具（脚本化会话供应者）锁定"不混代"；既有用 `sendWithId` 的单测（`RequestMultiplexerTest`、`ClientMetricsTest`）夹具已带非空会话，行为口径不变。
- **兼容**：纯客户端变更，无协议/服务端/配置变更，无状态迁移。回滚＝还原三处，无残留。
- **已登记残留（本 change 不闭合）**：调用方手持的 `LatchRoute` 会话**快照**与 mux **活取**会话之间的跨代配对（信封由旧代会话编号而通道已换代）。闭合它须让出站信封携带其钉住的会话（30 处调用点），属另一决策面；本 change 只保证**取口自身**不混代，台账 W22 行据此收窄。
