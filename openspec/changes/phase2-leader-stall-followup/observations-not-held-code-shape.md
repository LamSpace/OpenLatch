# observations：孤立节点 RELEASE 道 `NOT_HELD` 码形成因（任务 4.1）

- 登记：2026-09-06，`phase2-leader-stall-followup` 任务 4.1。
- 触发事实：`docs/partition-drill-2026-09-06.md` 行「少数派 n3 RELEASE 道（多数派真持有凭证）→
  `OK:NOT_HELD`」，与 `openlatch.proto` 对 RELEASE/RENEW 应答的注释
  「`NOT_LEADER`(v2，仅转发失败路径)」码形预期不一致，疑被客户面误读为"锁已丢"。

## 门序现实现（`ClusterRequestHandler`，代码为准）

RELEASE/RENEW 属**转发车道**（`validateEnvelope(requireLeader=false)`，不设角色门），判定顺序：

1. **会话登记预检**（本地影子 `kernel.shadow().hasSession(sid)`，D12 语义）→ 未登记回
   `SESSION_EXPIRED`；载荷/键非法回 `INVALID_REQUEST`/`KEY_EMPTY`/`KEY_TOO_LONG`。
   ——本步无任何归属（持有者）判定，不产 `NOT_HELD`。
2. **提交转发**（`gateway.submit`）：够不到 Leader、降级在途终结、超时等一切失败
   → `RetryableCommitException` → `commitFailure` 映射 `NOT_LEADER` + 当时提示
   （`ClusterRequestHandler.java:291-299`）。——转发失败道**只产 `NOT_LEADER`**。
3. **归属判定**在**多数派提交后的应用点**（`mapRelease`/`mapRenew` 消费
   `ApplyResult`）：请求会话非持有者 → `NOT_HELD`；token 非法 → `INVALID_TOKEN`。
   `ApplyResult` 仅经 `ReplicationGateway.onApplied`（本副本应用回执，design D10）
   完成，**不存在先于提交/应用的本地归属判定分支**。

## 结论（成因陈述）

- `NOT_HELD` 在实现上**必须**以"条目获多数派提交并于应用点判归为空"为前提——
  即该观测本身反证当轮 n3 的转发道**可达** Leader（`docs/partition-drill-2026-09-06.md`
  同表 `OK:NOT_LEADER`×4 为 ACQUIRE 角色门快速道，不涉转发；两行合读，非 OK 判定与
  §11-3 结论均不受影响）。演练判据注记"转发道失败"为**归因误标**（当轮隔离未切断
  n3 内部客户端→Leader 的复制面；HELLO 能回 `OK` 仅可能出自 `onApplied` 道，
  `SessionCoordinator.handleHello` 无本地握手快速道）。
- design D5 需证实的客户面风险前提「归属判定先于转发失败可达」**不成立**：
  门序上转发失败恒先判、只产 `NOT_LEADER`；`NOT_HELD` 恒为权威应用点判定，
  对请求会话而言语义诚实（该会话确未持有该锁）。proto 注释与实现一致。
- **定夺（4.2 落点）**：不改实现；以文档对齐承载——proto RELEASE/RENEW 应答注释补
  "归属判定在应用点（多数派提交后）"的码形说明，部署文档故障语义节补 RELEASE/RENEW
  码形表；PartitionDrillIT 的"转发道失败"归因注记一并更正（判定语义不变）。

## 冻结面改动半径（`OpenlatchProtoContractFreezeTest`）

golden 契约行为 `kind name number` 三元组（枚举/字段名与号），**不消费** `//` 注释文本；
本 change 仅改 `openlatch.proto` 注释与文档，不改枚举值/字段号/名字——冻结面零触碰，
无需重录 golden。
