## MODIFIED Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW、LATCH_COUNT_DOWN、LATCH_AWAIT、ADMIN_SUMMARY、ADMIN_LIST_KEYS、ADMIN_KEY_DETAIL、ADMIN_LIST_SESSIONS、ATOMIC_OP、BARRIER_AWAIT、BARRIER_LEAVE、BARRIER_ACTION_DONE），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及各阶段变更的增量定义逐项一致。v2/v3/v4/v5/v6 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 17 个消息类型、12 种锁类型、13 个状态码与 30 个 payload 消息（Phase 1 的 9 个加 `ClusterView`、Latch 四个、ADMIN 八个、AtomicOp 两个与 Barrier 六个），且字段编号与增量定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew/LatchCountDown/LatchAwait/AdminSummary/AdminListKeys/AdminKeyDetail/AdminListSessions/AtomicOp/BarrierAwait/BarrierLeave/BarrierActionDone 十四类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与定义一致

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号

### Requirement: 协议版本

`protocol_version` 当前版本为 **6**。服务端 SHALL 同时接受 `client_protocol_version ∈ {1, 2, 3, 4, 5, 6}` 的握手；收到 `client_protocol_version` 不在该集合内的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。握手响应与后续应答的协议版本 MUST 等于客户端握手的请求版本（v1 客户端得到逐字段与 Phase 1 一致的响应，新增字段对其表现为可忽略的未知字段）；`server_protocol_version` 回显服务端自身版本 6。v3 专属语义（`LOCK_TYPE_FAIR`/`LOCK_TYPE_SEMAPHORE`/`LOCK_TYPE_LATCH` 的 ACQUIRE、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息）MUST 仅对握手中声明 v3 及以上版本的会话开放；v4 专属语义（`ATOMIC_OP` 消息）MUST 仅对握手中声明 v4 及以上的会话开放；v5 专属语义（`BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` 消息）MUST 仅对握手中声明 v5 及以上的会话开放；v6 专属语义（`ATOMIC_OP` 消息携带 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的有值引用请求）MUST 仅对握手中声明 v6 的会话开放：握版本低于其专属阈值的会话发送对应请求 MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），其既有版本行为逐项不变。

#### Scenario: v6 握手成功

- **WHEN** 客户端以 `client_protocol_version = 6` 发起握手
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 6` 且 `server_protocol_version = 6`

#### Scenario: v5 握手成功

- **WHEN** 客户端以 `client_protocol_version = 5` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 5`（回显请求版本）且 `server_protocol_version = 6`，v5 全部握手与业务响应行为逐项不变

#### Scenario: v4 握手成功

- **WHEN** 客户端以 `client_protocol_version = 4` 发起握手并继续业务请求
- **THEN** 服务端接受握手并回显请求版本 4，v4 全部握手与业务响应行为逐项保持不变

#### Scenario: v3 握手回归不变

- **WHEN** 客户端以 `client_protocol_version = 3` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 v3 全部握手与业务响应行为不变

#### Scenario: v2 握手回归不变

- **WHEN** 客户端以 `client_protocol_version = 2` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 Phase 2 全部握手与业务响应行为不变

#### Scenario: v1 握手回归不变

- **WHEN** v1 客户端以 `client_protocol_version = 1` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 Phase 1 全部握手与业务行为，v1 客户端无需理解任何新增字段即可工作

#### Scenario: 未知版本拒绝

- **WHEN** 客户端以 `client_protocol_version = 7`（或其他未知值）发起握手
- **THEN** 服务端回 `INVALID_REQUEST` 并断开连接

#### Scenario: 低版本客户端使用新类型被拒

- **WHEN** v5 会话发送 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的 `ATOMIC_OP` 消息，或 v4 会话发送 `BARRIER_AWAIT` 消息，或 v3 会话发送 `ATOMIC_OP` 消息，或 v2 会话发送 `lock_type = LOCK_TYPE_SEMAPHORE` 的 ACQUIRE，或 v1 会话发送 LATCH_AWAIT 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝该请求且不断开连接，该会话的既有版本请求照常服务

## ADDED Requirements

### Requirement: v6 消息与字段增量

协议 SHALL 以纯增量方式扩展 v6，且 MUST NOT 新增 `MessageType`、`Envelope.payload` 槽位或 `StatusCode`——v6 全部客户端可见增量落在既有 ATOMIC_OP 消息对与管理面消息的字段扩展上：`LockType` 新增 `LOCK_TYPE_ATOMIC_REFERENCE = 11`——仅作 `AtomicOpRequest.lock_type` 的有值引用形态判别，MUST NOT 出现在 ACQUIRE 的 `lock_type` 中（ACQUIRE 携带该值属消息合法性违例，MUST 以 `INVALID_REQUEST` 消息级拒绝、不断连，与 `LOCK_TYPE_LATCH`、三标量原子类型与 `LOCK_TYPE_BARRIER` 同规则）。`AtomicOpRequest` 新增 `optional bytes operand_bytes = 9`、`optional bytes expected_bytes = 10`、`optional bytes initial_bytes = 11`；`AtomicOpResponse` 新增 `optional bytes old_value_bytes = 7`、`optional bytes value_bytes = 8`。字段语义：有值引用形态（`lock_type = 11`）的操作数以 `optional bytes` 三字段承载并遵循显式 presence 口径——`operand_bytes` 为 SET/GET_AND_SET/CAS/CAS_STAMPED 的落值（缺省=null，空字节串=""，两形态可区分）、`expected_bytes` 为 CAS/CAS_STAMPED 的期望值（缺省=期望 null）、`initial_bytes` 为建条目初值主张（缺省=不主张；存在即主张，含零长度主张空串）；`expected_version`、`op_seq`、`op` 复用标量形态既有语义，其中 `ATOMIC_ADD` 对有值引用形态为值域外操作（MUST 以 `INVALID_REQUEST` 映射拒绝）。形态与字段互斥矩阵 MUST 在消息合法性层裁决：有值引用形态请求的 `operand`/`expected`/`initial_value` 标量字段 MUST 为 0（非零携带以 `INVALID_REQUEST` 消息级拒绝）；非有值引用形态请求 MUST NOT 携带任一 `optional bytes` 字段（presence 即形状违例，同样拒绝）。应答的 `old_value`/`value` 标量字段对有值引用形态恒为 0，载荷读数由 bytes 字段承载；拒绝态 bytes 字段 MUST 全部缺省。管理面消息同步纯增量：`AdminKeyInfo` 新增 `atomic_payload_size = 11`（int32，有值引用条目当前值字节数，其余家族恒 0）与 `atomic_payload_preview = 12`（string，服务端截断转义预览，其余家族恒空串）；`AdminKeyDetailResponse` 新增 `atomic_payload_size = 21`（int32）与 `atomic_payload_preview = 22`（string）；`AdminKeyInfo.atomic_kind` 与 `AdminKeyDetailResponse.atomic_kind` 词表扩充 `reference`（取值 `long`/`integer`/`boolean`/`reference`）。既有字段编号与语义 MUST NOT 变更。

#### Scenario: v6 字段编号纯增量

- **WHEN** 构建协议模块并生成代码，检查 `AtomicOpRequest`/`AtomicOpResponse`/`AdminKeyInfo`/`AdminKeyDetailResponse`
- **THEN** 新增字段编号与类型与本增量逐项一致，既有字段编号与语义零变更，`MessageType` 与 `Envelope.payload` 无新增

#### Scenario: ACQUIRE 携带有值引用类型被拒

- **WHEN** 任意版本会话发送 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的 ACQUIRE
- **THEN** 服务端以 `INVALID_REQUEST` 消息级拒绝且不断开连接

#### Scenario: 标量槽位与载荷字段互斥

- **WHEN** 有值引用形态请求携带非零 `operand` 标量字段，或标量原子形态请求携带任一 `optional bytes` 字段
- **THEN** 服务端以 `INVALID_REQUEST` 消息级拒绝且不断连，不产生任何状态变更

#### Scenario: null 与空字节串显式 presence 往返保真

- **WHEN** 对 `operand_bytes` 缺省、`operand_bytes` 为零长度、`operand_bytes` 为非空字节串三种请求做编解码往返
- **THEN** 三形态在生成代码中可判别（`hasOperandBytes()` 逐一区分），序列化字节可无损还原
