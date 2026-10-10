# Proposal

## Why

前一轮收口把「换主窗队列请求伪成功」（空串/0 错值交付）根治了，但当时批准兑现的承诺收窄为**安全性质**——不伪成功即达标的终态可以是显式失败。由此遗留一个**可用性缺口**：非 ACQUIRE 车道（队列/原子/barrier/latch/topic/phaser/timer）经 `latchRoute()` 取路由，改道只在 home 连接**断裂重连**时靠 HELLO 提示建立；home 活着停在非权威节点（Leader 已迁他处），或启动窗 seed 节点的 LeaderTracker 尚未收到当选事件（HELLO 提示为 -1）时，无断连即无新提示，车道永不重建，`NOT_LEADER`（无提示字段）被重发环消化至预算耗尽——任何队列/原子请求（含换主**前**的 put）都会 churn 满 30s 预算显式超时。该缺口已在 WATCHLIST W11 登记，并携带一次**判负**的扩面试验数据。

## What Changes

- 集群形态下，home 会话 ACTIVE 期间客户端按**有界周期**经 home 会话查询 `CLUSTER_VIEW`（既有只读、零日志、任意节点可答的管理消息）核对当前 Leader；发现与本地路由不一致时，沿**既有**改道路径收敛——建/换获取车道，或提示指回 home 时回落 home。
- 核对**不接线**在途等待与重发环。判负试验已实证：把车道 `NOT_LEADER` 计入共享阈值并触发强制发现，会让异步发现完成撞上重发环 mid-flight，锚点由 4/6 绿**回归至 1/6 绿**。本条按该数据设计，避免重走弯路。
- 新增**确定性红先夹具**覆盖两个根因窗（home 驻留非权威节点 / 启动窗提示滞后），作为本次修复的判据；既有概率锚保持常驻但不再充当根因探针。
- **非目标**：不改 wire 协议（`CLUSTER_VIEW` 与 v2 Leader 提示字段均已存在）；不改安全性质（换主窗 MUST NOT 伪成功照旧）；不改 ACQUIRE 车道既有语义与阈值路径；不动 `ClientClusterIT` 队列锚的显式失败豁免——收敛改由确定性夹具断言，该豁免降为安全网。

## Capabilities

### New Capabilities

（无——本次是既有客户端行为的修正，不引入新能力面。）

### Modified Capabilities

- `client-sdk`：修改「Leader 发现与故障转移」需求，新增「驻留窗周期核对」条款及其场景；既有六条条款与六个既有场景逐字保留。

## Impact

- **代码**：仅 `openlatch-client`——路由/改道路径（`latchRoute`、`followLeaderHint` 所在类）、复用 `SeedDiscovery` 的 `CLUSTER_VIEW` 编解码、`ConnectionManager` 会话生命周期。无 proto 变更、无服务端变更、无配置项新增。
- **测试**：新增客户端确定性夹具（`ScriptedServer` 注入提示滞后形态；集群夹具钉 home 于 follower 后迁移 Leader）；`ClientClusterIT` 队列锚继续常驻作安全性质回归底线。
- **兼容**：纯客户端行为增强。单机形态与服务端不支持 `CLUSTER_VIEW` 时该机制**静默不激活**（不得把探测失败计为业务失败）；旧客户端不受影响（服务端零改动）。回滚=还原客户端产物，服务端与协议无状态残留。
