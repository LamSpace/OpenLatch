# wire-protocol Delta Specification

## ADDED Requirements

### Requirement: 管理消息增量（v3）

协议 SHALL 以纯增量方式承载管理观察消息：`MessageType` 新增 `ADMIN_SUMMARY = 10`、`ADMIN_LIST_KEYS = 11`、`ADMIN_KEY_DETAIL = 12`、`ADMIN_LIST_SESSIONS = 13`；`Envelope.payload` oneof 新增八个消息、字段号续接 24–31：

- `AdminSummaryRequest { token }` / `AdminSummaryResponse { status, held_locks, held_semaphores, latch_entries, total_waiters, session_count, node_role, uptime_ms, version }`；
- `AdminListKeysRequest { token, page, page_size, prefix }` / `AdminListKeysResponse { status, total_matched, page, page_size, repeated AdminKeyInfo items }`，`AdminKeyInfo = { key, family, holders, remaining_lease_ms, waiter_count }`；
- `AdminKeyDetailRequest { token, key }` / `AdminKeyDetailResponse { status, family, holders, waiters, lease_expires_at_ms, remaining_lease_ms, permits_total, permits_available, latch_total, latch_remaining, wait_queue_leader_only }`（`status=NOT_HELD` 为明确的 key 未命中形态，MUST NOT 空壳成功），`holders = { session_id, thread_id, count, role }`、`waiters = { position, session_id, request_id, waited_ms, permits, notified }`；
- `AdminListSessionsRequest { token }` / `AdminListSessionsResponse { status, repeated AdminSessionInfo sessions }`，`AdminSessionInfo = { session_id, node_id, connected_at_ms, held_keys, waiting_keys }`。

`token` 为管理令牌字符串（校验语义由管理观察能力定义）。ADMIN 消息为 v3 专属语义：协商版本低于 3 的会话发送任意 `ADMIN_*` MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），v1/v2 客户端的既有行为逐项不变。既有字段编号与语义 MUST NOT 变更；`raft.proto` MUST NOT 引用任何 ADMIN 消息（管理协议不经复制日志）。

#### Scenario: 管理消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 13 个消息类型与 22 个 payload 消息，ADMIN 四组请求/响应成对存在、字段编号与本增量一致，Phase 1/v2/v3 既有基线编号零变更

#### Scenario: v2 会话发送 ADMIN 被拒

- **WHEN** 协商版本为 2 的会话发送 `ADMIN_SUMMARY`
- **THEN** 收到 `INVALID_REQUEST` 应答且连接保持，该会话后续业务请求照常服务

#### Scenario: raft.proto 零触碰

- **WHEN** 比对本次变更后 `raft.proto` 与 Phase 2/3-T1 基线
- **THEN** 字节级零差异，既有复制日志与快照格式不受影响

## MODIFIED Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW、LATCH_COUNT_DOWN、LATCH_AWAIT、ADMIN_SUMMARY、ADMIN_LIST_KEYS、ADMIN_KEY_DETAIL、ADMIN_LIST_SESSIONS），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及各阶段变更的增量定义逐项一致。v2/v3 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 13 个消息类型、7 种锁类型、12 个状态码与 22 个 payload 消息（Phase 1 的 9 个加 `ClusterView`、Latch 四个与 ADMIN 八个），且字段编号与增量定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew/LatchCountDown/LatchAwait/AdminSummary/AdminListKeys/AdminKeyDetail/AdminListSessions 十类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与定义一致

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号
