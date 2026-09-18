## MODIFIED Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW、LATCH_COUNT_DOWN、LATCH_AWAIT、ADMIN_SUMMARY、ADMIN_LIST_KEYS、ADMIN_KEY_DETAIL、ADMIN_LIST_SESSIONS、ATOMIC_OP），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及各阶段变更的增量定义逐项一致。v2/v3/v4 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 14 个消息类型、10 种锁类型、12 个状态码与 24 个 payload 消息（Phase 1 的 9 个加 `ClusterView`、Latch 四个、ADMIN 八个与 AtomicOp 两个），且字段编号与增量定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew/LatchCountDown/LatchAwait/AdminSummary/AdminListKeys/AdminKeyDetail/AdminListSessions/AtomicOp 十一类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与定义一致

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号

### Requirement: 协议版本

`protocol_version` 当前版本为 **4**。服务端 SHALL 同时接受 `client_protocol_version ∈ {1, 2, 3, 4}` 的握手；收到 `client_protocol_version` 不在该集合内的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。握手响应与后续应答的协议版本 MUST 等于客户端握手的请求版本（v1 客户端得到逐字段与 Phase 1 一致的响应，新增字段对其表现为可忽略的未知字段）；`server_protocol_version` 回显服务端自身版本 4。v3 专属语义（`LOCK_TYPE_FAIR`/`LOCK_TYPE_SEMAPHORE`/`LOCK_TYPE_LATCH` 的 ACQUIRE、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息）MUST 仅对握手中声明 v3 及以上版本的会话开放；v4 专属语义（`ATOMIC_OP` 消息）MUST 仅对握手中声明 v4 的会话开放：握版本低于其专属阈值的会话发送对应请求 MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），其既有版本行为逐项不变。

#### Scenario: v4 握手成功

- **WHEN** 客户端以 `client_protocol_version = 4` 发起握手
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 4` 且 `server_protocol_version = 4`

#### Scenario: v3 握手回归不变

- **WHEN** 客户端以 `client_protocol_version = 3` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 v3 全部握手与业务响应行为不变

#### Scenario: v2 握手回归不变

- **WHEN** 客户端以 `client_protocol_version = 2` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 Phase 2 全部握手与业务响应行为不变

#### Scenario: v1 握手回归不变

- **WHEN** v1 客户端以 `client_protocol_version = 1` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 Phase 1 全部握手与业务响应行为，v1 客户端无需理解任何新增字段即可工作

#### Scenario: 未知版本拒绝

- **WHEN** 客户端以 `client_protocol_version = 5`（或其他未知值）发起握手
- **THEN** 服务端回 `INVALID_REQUEST` 并断开连接

#### Scenario: 低版本客户端使用新类型被拒

- **WHEN** v3 会话发送 `ATOMIC_OP` 消息，或 v2 会话发送 `lock_type = LOCK_TYPE_SEMAPHORE` 的 ACQUIRE，或 v1 会话发送 LATCH_AWAIT 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝该请求且不断开连接，该会话的既有版本请求照常服务

## ADDED Requirements

### Requirement: v4 消息与字段增量

协议 SHALL 以纯增量方式扩展 v4：`MessageType` 新增 `ATOMIC_OP = 14`；`LockType` 新增 `LOCK_TYPE_ATOMIC_LONG = 7`、`LOCK_TYPE_ATOMIC_INTEGER = 8`、`LOCK_TYPE_ATOMIC_BOOLEAN = 9`——三值仅作为 `AtomicOpRequest.lock_type` 的形态判别，MUST NOT 出现在 ACQUIRE 的 `lock_type` 中（ACQUIRE 携带三值属消息合法性违例，MUST 以 `INVALID_REQUEST` 消息级拒绝、不断连，与 `LOCK_TYPE_LATCH` 同规则）；`Envelope.payload` 新增 `atomic_op_request = 32` / `atomic_op_response = 33`。管理面消息同步纯增量：`AdminSummaryResponse` 新增 `atomic_entries = 10`（int32，无持有语义单列计数，判例 `latch_entries`）；`AdminKeyInfo` 新增 `atomic_kind = 6`（string，词表 `long`/`integer`/`boolean`，非 ATOMIC 恒空）与 `atomic_value = 7`（sint64）；`AdminKeyDetailResponse` 新增 `atomic_kind = 12`（string）、`atomic_initial = 13`（int64）、`atomic_value = 14`（sint64）、`atomic_version = 15`（int64）——非 ATOMIC 条目这些字段恒缺省零值/空串。新增枚举 `AtomicOp { ATOMIC_GET = 0; ATOMIC_SET = 1; ATOMIC_GET_AND_SET = 2; ATOMIC_ADD = 3; ATOMIC_CAS = 4; ATOMIC_CAS_STAMPED = 5; }`。新增消息：`AtomicOpRequest { string key = 1; AtomicOp op = 2; LockType lock_type = 3; sint64 operand = 4; sint64 expected = 5; sint64 expected_version = 6; sint64 initial_value = 7; uint64 op_seq = 8; }`、`AtomicOpResponse { StatusCode status = 1; AtomicOp op = 2; bool applied = 3; sint64 old_value = 4; sint64 value = 5; sint64 version = 6; }`。字段语义：`operand` 为 SET 新值/GET_AND_SET 新值/ADD 增量/CAS 与 CAS_STAMPED 更新值；`expected` 为 CAS/CAS_STAMPED 期望值；`expected_version` 仅在 ADD 与 CAS_STAMPED 上有非零意义（>0 为版本断言、0 为不主张）；`initial_value` 非零表示建条目初值主张（条目不存在则以此值创建、存在则与当前态断言规则一致），0 为不主张；`op_seq` 为会话内单调递增的去重序号（写操作必填 ≥1，GET 填 0 表示不参与去重）。`applied` 对 CAS/CAS_STAMPED 表示是否落值，对其余写操作恒 true，对 GET 恒 false；`old_value`/`value`/`version` 为操作前后读数。状态码 MUST NOT 新增（拒绝复用 `INVALID_REQUEST`，会话失效复用 `SESSION_EXPIRED`）。既有字段编号与语义 MUST NOT 变更。

#### Scenario: ATOMIC 消息对存在且编号正确

- **WHEN** 构建协议模块并生成代码，检查 `AtomicOpRequest`/`AtomicOpResponse`
- **THEN** 两消息字段编号、类型与本增量逐项一致，`Envelope.payload` 新增 32/33 两槽，既有 10–31 号槽零变更

#### Scenario: ACQUIRE 携带原子类型被拒

- **WHEN** 任意版本会话发送 `lock_type ∈ {ATOMIC_LONG, ATOMIC_INTEGER, ATOMIC_BOOLEAN}` 的 ACQUIRE
- **THEN** 服务端以 `INVALID_REQUEST` 消息级拒绝且不断开连接

#### Scenario: v3 会话使用原子消息被拒

- **WHEN** 握手版本为 3 的会话发送 `ATOMIC_OP` 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝且不断开连接，该会话既有 v3 能力照常服务
