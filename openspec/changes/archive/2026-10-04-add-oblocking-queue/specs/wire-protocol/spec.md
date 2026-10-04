## MODIFIED Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW、LATCH_COUNT_DOWN、LATCH_AWAIT、ADMIN_SUMMARY、ADMIN_LIST_KEYS、ADMIN_KEY_DETAIL、ADMIN_LIST_SESSIONS、ATOMIC_OP、BARRIER_AWAIT、BARRIER_LEAVE、BARRIER_ACTION_DONE、QUEUE_OP），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及各阶段变更的增量定义逐项一致。v2/v3/v4/v5/v6/v7 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 18 个消息类型、14 种锁类型、13 个状态码与 32 个 payload 消息（Phase 1 的 9 个加 `ClusterView`、Latch 四个、ADMIN 八个、AtomicOp 两个、Barrier 六个与 QueueOp 两个），且字段编号与增量定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew/LatchCountDown/LatchAwait/AdminSummary/AdminListKeys/AdminKeyDetail/AdminListSessions/AtomicOp/BarrierAwait/BarrierLeave/BarrierActionDone/QueueOp 十五类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与定义一致

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号

### Requirement: 协议版本

`protocol_version` 当前版本为 **7**。服务端 SHALL 同时接受 `client_protocol_version ∈ {1, 2, 3, 4, 5, 6, 7}` 的握手；收到 `client_protocol_version` 不在该集合内的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。握手响应与后续应答的协议版本 MUST 等于客户端握手的请求版本（v1 客户端得到逐字段与 Phase 1 一致的响应，新增字段对其表现为可忽略的未知字段）；`server_protocol_version` 回显服务端自身版本 7。v3 专属语义（`LOCK_TYPE_FAIR`/`LOCK_TYPE_SEMAPHORE`/`LOCK_TYPE_LATCH` 的 ACQUIRE、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息）MUST 仅对握手中声明 v3 及以上版本的会话开放；v4 专属语义（`ATOMIC_OP` 消息）MUST 仅对握手中声明 v4 及以上的会话开放；v5 专属语义（`BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` 消息）MUST 仅对握手中声明 v5 及以上的会话开放；v6 专属语义（`ATOMIC_OP` 消息携带 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的有值引用请求）MUST 仅对握手中声明 v6 及以上的会话开放；v7 专属语义（`QUEUE_OP` 消息）MUST 仅对握手中声明 v7 的会话开放：握版本低于其专属阈值的会话发送对应请求 MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），其既有版本行为逐项不变。

#### Scenario: v7 握手成功

- **WHEN** 客户端以 `client_protocol_version = 7` 发起握手
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 7` 且 `server_protocol_version = 7`

#### Scenario: v6 握手成功

- **WHEN** 客户端以 `client_protocol_version = 6` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 6`（回显请求版本）且 `server_protocol_version = 7`，v6 全部握手与业务响应行为逐项不变

#### Scenario: v5 握手成功

- **WHEN** 客户端以 `client_protocol_version = 5` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 v5 全部握手与业务响应行为不变

#### Scenario: v4 握手成功

- **WHEN** 客户端以 `client_protocol_version = 4` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 v4 全部握手与业务响应行为不变

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

- **WHEN** 客户端以 `client_protocol_version = 8`（或其他未知值）发起握手
- **THEN** 服务端回 `INVALID_REQUEST` 并断开连接

#### Scenario: 低版本客户端使用新类型被拒

- **WHEN** v6 会话发送 `QUEUE_OP` 消息，或 v5 会话发送 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的 `ATOMIC_OP` 消息，或 v4 会话发送 `BARRIER_AWAIT` 消息，或 v3 会话发送 `ATOMIC_OP` 消息，或 v2 会话发送 `lock_type = LOCK_TYPE_SEMAPHORE` 的 ACQUIRE，或 v1 会话发送 LATCH_AWAIT 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝该请求且不断开连接，该会话的既有版本请求照常服务

## ADDED Requirements

### Requirement: v7 消息与字段增量

协议 SHALL 以纯增量方式扩展 v7：`MessageType` 新增 `QUEUE_OP = 18`；`Envelope.payload` oneof 新增 `QueueOpRequest queue_op_request = 40` 与 `QueueOpResponse queue_op_response = 41`；`LockType` 新增 `LOCK_TYPE_QUEUE = 12` 与 `LOCK_TYPE_DELAY_QUEUE = 13`（仅作 `QueueOpRequest.lock_type` 的队列形态判别，MUST NOT 出现在 ACQUIRE 的 `lock_type` 中——ACQUIRE 携带 12/13 属消息合法性违例，以 `INVALID_REQUEST` 消息级拒绝、不断连，与 LATCH/BARRIER/原子形态同规则）；新增枚举 `QueueOp { QUEUE_OP_PUT = 0, QUEUE_OP_TAKE = 1, QUEUE_OP_DRAIN = 2, QUEUE_OP_PEEK = 3, QUEUE_OP_SIZE = 4 }`。

`QueueOpRequest { key=1, op=2, lock_type=3, blocking=4(bool), capacity=5(sint64), element_bytes=6(optional bytes), delay_ms=7(sint64), max_elements=8(int32), op_seq=9(uint64) }`。字段语义：`op` 决定操作（写=PUT/TAKE/DRAIN，读=PEEK/SIZE，读写皆全量经提交，判例 v4 GET）；`blocking` 仅 PUT/TAKE 有意义（true=不可满足时挂起回 `QUEUED`，false=不可满足时立即回 `DENIED`；其余 op 携带非零为形状违例）；`capacity` 为定型断言（非零主张判例 `permits_total`/`parties`，0=不主张）；`element_bytes` 为 PUT 的元素（**PUT 必须携带——缺省 presence 即形状违例**，元素不可为 null，零长度空字节串为合法元素；非 PUT 携带为违例）；`delay_ms` 仅 `LOCK_TYPE_DELAY_QUEUE` 形态的 PUT 有效（≥0 的相对延迟，绝对到期时刻由应用点折算；其余情形携带非零为违例）；`max_elements` 仅 DRAIN 有效（>0 提取上限，0=取服务端钳制上限；其余 op 携带非零为违例）；`op_seq` 为会话内单调写序号（PEEK/SIZE 填 0 不参与去重，GET 判例）。`lock_type` MUST 取 12/13 之一（其余值携 QUEUE_OP 为违例）。

`QueueOpResponse { status=1, op=2, queue_position=3(int32), element_bytes=4(optional bytes), drained_bytes=5(repeated bytes), size=6(sint64), capacity=7(sint64) }`。`status` 取 `OK/QUEUED/DENIED/OVERLOADED` 与错误码（**双"满"语义**：元素队列满的立即式回 `DENIED`、等待挂起深度超限回 `OVERLOADED`——`StatusCode` 词表零新增，两语义以注释钉死）；`queue_position` 仅 `QUEUED` 有效（1 起）；`element_bytes` 为 TAKE 成功交付/PEEK 读数（显式 presence：缺省=无元素/空队，含零长度=空串元素）；`drained_bytes` 为 DRAIN 按出队序的交付列表（空列表为合法结果，DRAIN 恒立即式不挂起）；`size` 仅 SIZE 有效（当前元素数，延时形态含未到期项）；`capacity` 为条目定型容量回显（未定型/拒绝路径为 0）。`AwaitNotify.request_id_ref` 口径延伸至原 `QUEUE_OP` 的 `request_id`（无新字段）。

管理面同步纯增量：`AdminKeyWaiterInfo` 新增 `queue_track = 7`（int32，队列挂起等待者的轨道判别：0=非队列等待者（无意义）、1=等容量（put-waiter）、2=等元素（take-waiter））；`AdminSummaryResponse` 新增 `queue_entries = 12`（int32，队列条目数，无持有语义单列）；`AdminKeyInfo` 新增 `queue_capacity = 13`(sint64)/`queue_depth = 14`(int32)/`queue_head_payload_size = 15`(int32)/`queue_head_payload_preview = 16`(string)；`AdminKeyDetailResponse` 新增 `queue_capacity = 23`(sint64)/`queue_depth = 24`(int32)/`queue_head_expiry_ms = 25`(int64)/`queue_total_payload_bytes = 26`(int64)/`queue_head_payload_size = 27`(int32)/`queue_head_payload_preview = 28`(string)；`AdminKeyInfo.family` 词表增 `queue`。队列字段对非队列家族恒零值/空串；预览沿用 v6 恒定长度截断转义纪律，全量元素 MUST NOT 出现在管理应答。既有字段编号与语义 MUST NOT 变更。

#### Scenario: v7 字段编号纯增量

- **WHEN** 构建协议模块并生成代码，检查 `MessageType`/`Envelope.payload`/`LockType`/`QueueOp`/`QueueOpRequest`/`QueueOpResponse`/管理面消息
- **THEN** 新增枚举值与字段编号与本增量逐项一致，既有字段编号与语义零变更，`StatusCode` 无新增取值

#### Scenario: ACQUIRE 携带队列类型被拒

- **WHEN** 任意版本会话发送 `lock_type = LOCK_TYPE_QUEUE` 或 `LOCK_TYPE_DELAY_QUEUE` 的 ACQUIRE
- **THEN** 服务端以 `INVALID_REQUEST` 消息级拒绝且不断开连接

#### Scenario: 元素缺省 presence 即违例

- **WHEN** v7 会话发送不携带 `element_bytes` 的 PUT 请求
- **THEN** 服务端以 `INVALID_REQUEST` 消息级拒绝且不断连、不产生日志条目（元素不可为 null 与 v6 引用形态"null 为一等公民"刻意不同，契约注释 MUST 显式声明该差异）

#### Scenario: 形状互斥矩阵入口拒绝

- **WHEN** QUEUE_OP 请求出现形态外组合（非 PUT 携 `element_bytes`、非 DELAY_QUEUE 形态携非零 `delay_ms`、非 DRAIN 携非零 `max_elements`、PEEK/SIZE 携非零 `op_seq`、非 PUT/TAKE 携 `blocking`、`lock_type` 非 12/13）
- **THEN** 服务端以 `INVALID_REQUEST` 消息级拒绝且不断连，状态零扰动

#### Scenario: 元素字节往返保真与两态可辨

- **WHEN** 对 `element_bytes` 缺省、零长度、非空字节串三种请求做编解码往返，并对 TAKE 应答的"无元素"与"空串元素"两态编码解码
- **THEN** presence 在生成代码中可判别（`hasElementBytes()` 逐一区分），字节内容无损；恰限 4KB 元素二进制往返字节级保真
