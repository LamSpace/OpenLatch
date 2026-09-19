## ADDED Requirements

### Requirement: BARRIER 命令复制边界与重放确定性

Barrier 的 `await`（到场）、`leave`（离场/破障）与 `actionDone`（动作了结）SHALL 分别以新增日志条目类型 `BARRIER_AWAIT_ENTRY`/`BARRIER_LEAVE_ENTRY`/`BARRIER_ACTION_DONE_ENTRY` 经复制日志多数派提交后应用（新增条目类型仅以新增枚举值表达，既有编号不变；条目载荷复用对应请求消息序列化附 sessionId，与 `LATCH_COUNT_DOWN_ENTRY`/`ATOMIC_OP_ENTRY` 模式一致）；未达多数派时 MUST NOT 产生到场计数、合拢、破障、世代回卷或了结记录变更。与 Latch `await` 零日志的边界差异 MUST 如此显式化：Barrier 到场改变的是复制状态（当前世代到场账簿、合拢判定、世代号、了结记录、执行者挂账），故 MUST 进日志；而**在队位次与通知时序**（哪个连接何时收到 `AWAIT_NOTIFY`、已通知等待者的响应超时窗口）仍是 Leader 内存表述，MUST NOT 进日志与快照。确定性契约：到场顺序与"最后到场者"判定、执行者指定、世代号推进与破障传播 MUST 由 apply 序承载并跨副本一致——同一命令序列在 Leader 与新副本上回放后，BARRIER 条目的 `(generation, 到场账簿, 动作挂账, 了结记录)` 逐项一致（状态摘要相等）；破障由 `SESSION_CLOSE` 条目应用触发时，其世代作用域与受影响等待者集合在任何副本上 MUST 一致。幂等重放：`(会话, 请求)` 到场去重与世代了结记录 MUST 是条目内复制状态而非 Leader 本地内存——Leader 切换后，客户端以同 `request_id` 重发的 `BARRIER_AWAIT` 在新 Leader 上 MUST 命中回放所得的去重/了结记录并得出与切换前一致的裁决，MUST NOT 重复计次。已通知但未重发的等待项其内存位次被超时摘除后，若其复制侧了结记录仍窗口内则重发仍可了结，窗口外重发按新到场计入当前世代（与锁/Latch 等待队列不随切换迁移同口径的显式竞态声明）。`SESSION_OPEN`/`SESSION_CLOSE` 既有条目对 BARRIER 的语义 MUST 仅为经引擎分派到 `BarrierEntry.removeSession` 的破障传播，MUST NOT 新增会话专用条目类型。

#### Scenario: 少数派分区不到场

- **WHEN** Leader 被隔离失去多数派时处理 `BARRIER_AWAIT`
- **THEN** 请求失败（无提交），全集群该 key 的到场账簿、世代号与合拢判定不变

#### Scenario: 合拢裁决跨副本一致

- **WHEN** parties=3 的三个到场命令提交后，在任一回放副本上查询该条目状态
- **THEN** 三副本的 `(generation, arrived=0, 了结记录=TRIPPED 定型)` 逐项一致，最后到场者判定一致

#### Scenario: 换主后旧请求重发不双计

- **WHEN** 到场命令提交成功但应答丢失，随后 Leader 切换；客户端向新 Leader 以同 `(会话, request_id)` 重发 `BARRIER_AWAIT`
- **THEN** 新 Leader 回放状态含该到场记录，重发命中去重、返回一致裁决，当前世代到场数不重复推进

#### Scenario: 执行者死亡破障经复制全副本一致

- **WHEN** 动作待决的 Leader 上执行者会话被关闭，`SESSION_CLOSE` 提交
- **THEN** 该世代在所有副本上经回放定型 BROKEN、世代号一致推进，无副本残留动作挂账

#### Scenario: 回放终态摘要相等

- **WHEN** 含交叠世代（合拢、回卷、破障交错）的同一命令序列分别在 Leader 与新建副本上全量回放
- **THEN** 两副本 BARRIER 条目状态摘要逐字节一致（含了结记录与动作挂账）
