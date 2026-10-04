# Proposal

## Why

集群路径的 `NOT_LEADER` 拒绝应答对 `QUEUE_OP`、`LATCH_COUNT_DOWN`、`LATCH_AWAIT` 三种消息类型缺少同型载荷 case（`ClusterRequestHandler.notLeaderEnvelope` 落入 default 分支，以 `acquire_response` 码形应答），而客户端车道（`RemoteBlockingQueue` 等）对 oneof 缺省载荷盲读 protobuf 默认实例——`StatusCode.OK` 恰好是枚举零值，于是"非 Leader 拒绝"在客户端被成型为"成功空应答"：换主/选主窗内 `take` 交付 `""`、`size` 读回 `0`、Latch `await` 误判已破。这正是 `ClientClusterIT` 队列锚间歇红的根因（WATCHLIST W10，6 轮取证 3/6 复现；W10-DBG 实证两存活节点 `shadowDepth=2` 一致——复制面不丢，错在拒绝码形）。v7 轮已同步单机拒绝码形 `RequestDispatcher.errorResponse` 的 QUEUE_OP case，集群侧姊妹码形漏更；"拒绝状态码线路可见（客户端裁决依赖）"这一贯穿 v2–v7 的工程纪律从未成文入规格，缺一道防复发的门禁。

## What Changes

- **服务端病根**：`ClusterRequestHandler.notLeaderEnvelope` 补 `QUEUE_OP`/`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 三个 case——`NOT_LEADER` 状态码以与请求同型的响应载荷承载（沿既有 `ATOMIC_OP`/`BARRIER_*` 判例：同型载荷、无 leader 提示字段，改道由 mux 层重发现承载）；不动 proto 字段、不升协议版本。
- **客户端护栏（队列车道）**：`RemoteBlockingQueue` 的 `exchange`/`parkLoop` 判 `hasQueueOpResponse()`——缺码形按瞬态处理（退避重路由重发），MUST NOT 读默认实例；`TAKE` 收到 OK 且无 `element_bytes`（显式 presence，空元素与缺省可辨）判协议违例显式抛出，杜绝 `new byte[0]` 交付。
- **规格成文**：`wire-protocol` 新增不变式——拒绝应答的载荷码形 MUST 与请求消息类型同型（覆盖 `notLeaderEnvelope` 与 `errorResponse` 两处拒绝码形）；`client-sdk` 补队列车道对缺码形应答的裁决要求。
- **防复发门禁**：表驱动夹具枚举全部客户端接入车道 `MessageType`，断言两处拒绝码形对每一类型都产出同型载荷——未来新增消息类型不过门即红。
- **锚恢复与销账**：`ClientClusterIT` 队列锚摘除 `@Disabled`，锁定为**安全性质**语义（确定性复现夹具先行判红、修复转绿）——换主窗请求 MUST NOT 伪成功（空串/0 错值交付杜绝）；改道收敛轮全链路断言，未收敛轮以显式异常收场（实施期实测：改道收敛缺口的正确修复需重做非 ACQUIRE 车道路由语义，扩面试验判负回退，已登记 WATCHLIST **W11** 另行承载）；WATCHLIST W10 行按维护规约移除；`notLeaderEnvelope` 方法 Javadoc 的过时枚举同步修正。

不含：客户端 Latch/Barrier/Atomic 车道的缺码形盲读收敛（其服务端码形经本次修复后已正确，属既有判例加固范围外）；换主窗非 ACQUIRE 车道的改道**收敛**机制（W11）；proto 字段新增；W8 选举风暴形态 B 残余（另行承载）。

## Capabilities

### New Capabilities

（无——本 change 修复既有能力违例并成文隐含不变式。）

### Modified Capabilities

- `wire-protocol`：成文"拒绝应答载荷与请求类型同型"不变式，并补 `QUEUE_OP`/`LATCH_*` 被非 Leader 节点拒绝时以同型 `NOT_LEADER` 载荷应答的要求与场景。
- `client-sdk`：队列车道对缺码形/违例应答的裁决——缺码形按瞬态重发、`TAKE` 的 OK-缺元素判协议违例，MUST NOT 将默认实例误判为成功交付。

## Impact

- **服务端**：`openlatch-server` `ClusterRequestHandler.notLeaderEnvelope`（+3 case 与 Javadoc）；附带修正 `handleQueueOp` 提交失败路径的 `queue.total{status}` 指标此前把拒绝错记为 `ok` 的派生症状。
- **客户端**：`openlatch-client` `RemoteBlockingQueue`（`exchange`/`parkLoop` presence 护栏）；`OBlockingQueue`/`ODelayQueue` 公开契约与签名不变（修复使其"阻塞至交付"承诺回到言出必行）；队列车道未进入任何已发布版本（1.0.0 为 v6），无历史客户端兼容负担。
- **测试**：新增服务端拒绝码形单测（含 LATCH 两型，先判红）、客户端 Scripted 缺码形/违例形态单测、`ClusterHarness` 确定性"钉 follower 直连"复现夹具、门禁表驱动夹具；`ClientClusterIT:297` 摘 `@Disabled` 并连跑 ≥6 轮（对齐取证基线 3/6 红）。
- **文档**：WATCHLIST W10 行移除（本地未提交改动仅空行，一并处理），残余换主窗非 ACQUIRE 车道改道收敛缺口登记 W11（带判负试验数据与候选设计）；变更设计决策（缺码形裁决分级、LATCH 并入范围、沿 ATOMIC_OP 无提示判例、改道扩面判负记录 D6）落 design.md。
- **不受影响**：复制状态机、协议版本词表、用户双语指南与 README（v7 未发布，无面向用户的契约变化）。
