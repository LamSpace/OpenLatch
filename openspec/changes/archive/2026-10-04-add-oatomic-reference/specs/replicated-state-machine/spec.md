## ADDED Requirements

### Requirement: 有值引用复制边界与载荷字节级重放确定

ATOMIC 家族的有值引用形态操作 SHALL 复用既有条目类型 `ATOMIC_OP_ENTRY` 承载（其 `command_payload` 为携带 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 与 `optional bytes` 载荷字段的 `AtomicOpRequest` 序列化，MUST NOT 新增 `RaftEntryType` 枚举值），复制边界与标量形态逐项一致：读写皆经多数派提交后应用、未达多数派 MUST NOT 产生值变更/版本戳推进/去重槽更新、`GET` 条目应用为零迁移、同 `(session, op_seq)` 重放在任何副本得出一致应答且不重复推进版本戳。`ApplyResult` SHALL 以纯增量字段承载载荷回执：`optional bytes atomic_old_value_bytes = 18`、`optional bytes atomic_value_bytes = 19`（有值引用形态 status=OK 时择用，显式 presence 口径与请求侧一致：缺省=null、零长度=""；标量形态与其余条目 MUST NOT 携带）。载荷的字节级比较与落值 MUST 跨副本确定：同一日志序列在任何副本、任何物理时刻重放，条目的值、版本戳与去重槽（含槽内载荷字节）逐字段一致，全量摘要可比对；快照序列化的载荷字节 MUST 确定性生成。状态机与条目侧 MUST NOT 以节点本地 `maxValueBytes` 复核条目载荷尺寸（超限命令在接入层即被拒绝、永不入日志，入日志者视为已钳制）——该纪律是回放确定性的前提：若 apply 按本地配置判定，跨节点配置漂移将导致同一日志一拒一受、副本分歧。

#### Scenario: 引用写条目经复制全副本一致

- **WHEN** `CAS_STAMPED`（载荷形态）成功提交后查询任一回放副本的条目状态
- **THEN** 各副本 `(value 字节, version, 去重槽含 old/value 字节)` 逐字段一致

#### Scenario: 引用 GET 重放零迁移

- **WHEN** 含有值引用 `GET` 条目的日志在新增副本上全量回放
- **THEN** 回放后该 key 状态与无 GET 条目时逐字节一致（摘要相等）

#### Scenario: 换主后引用写重发经去重槽不双写

- **WHEN** 载荷 `SET` 提交成功但应答丢失，Leader 切换后客户端向新 Leader 重发同 `(session, op_seq)` 请求
- **THEN** 新 Leader 命中已回放的去重槽，返回值字节级一致的应答，版本戳不重复推进

#### Scenario: 配置漂移不分歧

- **WHEN** 集群中一节点 `maxValueBytes` 配置低于产生载荷写入时的值，该节点回放含载荷写入的日志
- **THEN** 回放照常落值、与 Leader 终态逐字节一致（条目侧不复核），副本间无分歧
