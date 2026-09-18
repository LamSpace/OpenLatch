## ADDED Requirements

### Requirement: ATOMIC 操作复制边界与重放确定性

ATOMIC 家族的全部操作（含 `GET`）SHALL 以新增日志条目类型 `ATOMIC_OP_ENTRY` 经复制日志多数派提交后应用，与既有 `LOCK_ACQUIRE_ENTRY`/`LATCH_COUNT_DOWN_ENTRY` 条目模式一致（新增条目类型仅以新增枚举值表达，既有编号不变）；未达多数派时 MUST NOT 产生值变更、版本戳推进或去重槽更新，且任一回放副本对该 key 的观察 MUST 与提交前一致。`GET` 条目应用为**零迁移**：读取当前 `(value, version)` 作为回执，MUST NOT 触碰值、版本戳与去重槽——该条目在任何副本上的重放 MUST NOT 改变状态机可观察状态。写操作条目 MUST 携请求的 `(session, op_seq)`，应用侧以条目内状态（非 Leader 本地内存）做单槽去重：同槽重放在任何副本上 MUST 得出与原应用一致的应答且版本戳不重复推进。应答四元组 MUST 由 apply 结果导出并在 Leader 侧回执客户端；Follower 回放的应答不入任何客户端连接。Leader 切换时，未提交条目 MUST 以可重试错误完成（与既有写路径同口径），已提交条目经追赶回放后在任何新 Leader 上产生逐位一致的值与版本戳。

#### Scenario: 少数派分区不加值

- **WHEN** Leader 被隔离失去多数派时处理 `ADD(1)`
- **THEN** 请求失败（无提交），全集群该 key 的值与版本戳不变

#### Scenario: 写条目经复制全副本一致

- **WHEN** `CAS_STAMPED` 成功提交后查询任一回放副本的条目状态
- **THEN** 各副本 `(value, version, 去重槽)` 逐字段一致

#### Scenario: GET 重放零迁移

- **WHEN** 含高频 `GET` 条目的日志在新增副本上全量回放
- **THEN** 回放后该 key 状态与无 GET 条目时逐字节一致（摘要相等）

#### Scenario: 换主后重试经去重槽不双加

- **WHEN** 客户端 `ADD(1)` 提交成功但应答丢失，随后 Leader 切换，客户端向新 Leader 重发同 `(session, op_seq)` 请求
- **THEN** 新 Leader 状态机含已回放的去重槽，重发命中同槽、返回一致应答，值不双加
