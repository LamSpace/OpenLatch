# wire-protocol Specification

## Purpose
定义 OpenLatch 客户端与服务器之间的线路协议：基于 Protobuf 的消息模式、分帧格式、请求关联规则与版本约定，为 server / client / starter 各模块提供统一的交互契约。
## Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW、LATCH_COUNT_DOWN、LATCH_AWAIT、ADMIN_SUMMARY、ADMIN_LIST_KEYS、ADMIN_KEY_DETAIL、ADMIN_LIST_SESSIONS、ATOMIC_OP、BARRIER_AWAIT、BARRIER_LEAVE、BARRIER_ACTION_DONE、QUEUE_OP、TOPIC_OP、TOPIC_MESSAGE、CONDITION_OP），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及各阶段变更的增量定义逐项一致。v2/v3/v4/v5/v6/v7/v8/v9 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 21 个消息类型、14 种锁类型、14 个状态码与 37 个 payload 消息（Phase 1 的 9 个加 `ClusterView`、Latch 四个、ADMIN 八个、AtomicOp 两个、Barrier 六个、QueueOp 两个、TopicOp 两个及 `TopicMessage` 与 ConditionOp 两个），且字段编号与增量定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew/LatchCountDown/LatchAwait/AdminSummary/AdminListKeys/AdminKeyDetail/AdminListSessions/AtomicOp/BarrierAwait/BarrierLeave/BarrierActionDone/QueueOp/TopicOp/ConditionOp 十七类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与定义一致（`TOPIC_MESSAGE` 为服务端主动推送，不属于配对交互，按推送关联规则约束；v9 条件等待不设独立等待消息——await 形态为 `ACQUIRE` 消息携带 `condition` 字段的折叠形态，见 v9 消息与字段增量）

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号

### Requirement: 管理消息增量（v3）

协议 SHALL 以纯增量方式承载管理观察消息：`MessageType` 新增 `ADMIN_SUMMARY = 10`、`ADMIN_LIST_KEYS = 11`、`ADMIN_KEY_DETAIL = 12`、`ADMIN_LIST_SESSIONS = 13`；`Envelope.payload` oneof 新增八个消息、字段号续接 24–31：

- `AdminSummaryRequest { token }` / `AdminSummaryResponse { status, held_locks, held_semaphores, latch_entries, total_waiters, session_count, node_role, uptime_ms, version }`；
- `AdminListKeysRequest { token, page, page_size, prefix }` / `AdminListKeysResponse { status, total_matched, page, page_size, repeated AdminKeyInfo items }`，`AdminKeyInfo = { key, family, holders, remaining_lease_ms, waiter_count }`；
- `AdminKeyDetailRequest { token, key }` / `AdminKeyDetailResponse { status, family, holders, waiters, lease_expires_at_ms, remaining_lease_ms, permits_total, permits_available, latch_total, latch_remaining, wait_queue_leader_only }`（`status=NOT_HELD` 为明确的 key 未命中形态，MUST NOT 空壳成功），`holders = { session_id, thread_id, count, role }`、`waiters = { position, session_id, request_id, waited_ms, permits, notified }`；
- `AdminListSessionsRequest { token }` / `AdminListSessionsResponse { status, repeated AdminSessionInfo sessions }`，`AdminSessionInfo = { session_id, node_id, connected_at_ms, held_keys, waiting_keys }`。

`token` 为管理令牌字符串（校验语义由管理观察能力定义）。ADMIN 消息为 v3 专属语义：协商版本低于 3 的会话发送任意 `ADMIN_*` MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），v1/v2 客户端的既有行为逐项不变。既有字段编号与语义 MUST NOT 变更；`raft.proto` MUST NOT 引用任何 ADMIN 消息（管理协议不经复制日志）。

#### Scenario: 管理消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** v3 基线的 13 个消息类型与 22 个 payload 消息齐备（v4 面为既有项的超集、编号零变更），ADMIN 四组请求/响应成对存在、字段编号与本增量一致，Phase 1/v2/v3 既有基线编号零变更

#### Scenario: v2 会话发送 ADMIN 被拒

- **WHEN** 协商版本为 2 的会话发送 `ADMIN_SUMMARY`
- **THEN** 收到 `INVALID_REQUEST` 应答且连接保持，该会话后续业务请求照常服务

#### Scenario: raft.proto 零触碰

- **WHEN** 比对本次变更后 `raft.proto` 与 Phase 2/3-T1 基线
- **THEN** 字节级零差异，既有复制日志与快照格式不受影响

### Requirement: 分帧格式

线路帧 SHALL 为 4 字节大端长度前缀（不含自身）加一个序列化的 `Envelope`。单帧最大长度 MUST 为 1 MiB，超限的连接 MUST 被断开。

#### Scenario: 长度前缀编码

- **WHEN** 发送方序列化一个 `Envelope` 并写上线路
- **THEN** 帧头为该 Envelope 序列化字节数的 4 字节大端表示，其后紧跟序列化字节

### Requirement: 请求标识与关联

`request_id` MUST 在单条连接内唯一；响应消息 MUST 回显请求的 `request_id`。服务端推送的 `Envelope.request_id` MUST 为 0：`AWAIT_NOTIFY` 通过 `request_id_ref` 指向原请求（`ACQUIRE`/`LATCH_AWAIT`/`BARRIER_AWAIT`/`QUEUE_OP` 的 `request_id`）；`TOPIC_MESSAGE` 为广播交付推送，MUST NOT 携带 `request_id_ref` 或指向任何原请求——其关联键为 `TopicMessage.subscription_id`（SUBSCRIBE 回执分配）。

#### Scenario: 响应关联

- **WHEN** 客户端以 `request_id = r` 发送任意业务请求
- **THEN** 对应响应的 `Envelope.request_id` 等于 `r`

#### Scenario: 推送关联

- **WHEN** 服务端就某个挂起的获取请求（`request_id = r`）发出可重试通知
- **THEN** 推送的 `Envelope.request_id` 为 0 且 `AwaitNotify.request_id_ref` 等于 `r`

#### Scenario: 推送关联（广播型）

- **WHEN** 服务端向订阅会话推送 `TOPIC_MESSAGE`
- **THEN** `Envelope.request_id` 为 0，`TopicMessage.subscription_id` 等于该订阅 SUBSCRIBE 应答分配的 id，消息不携带任何 `request_id_ref` 语义

### Requirement: 编解码往返保真

所有消息类型经序列化再反序列化后，MUST 与原消息逐字段相等。

#### Scenario: 全消息类型 round-trip

- **WHEN** 对每一类携带 payload 的 `Envelope`（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY 及其请求/响应变体）执行序列化—反序列化
- **THEN** 反序列化结果与原消息的所有字段值相同

### Requirement: 未知字段容忍

收发双方 MUST 不因收到携带未知字段的 Protobuf 消息而报错，且未知字段 MUST 按 proto3 默认行为在解码侧保留。

#### Scenario: 携带未知字段的消息可解码

- **WHEN** 一方收到包含本方模式未知字段的 `Envelope`
- **THEN** 解码成功、已知字段正确可读，且未知字段在重新序列化后仍被保留

### Requirement: 协议版本

`protocol_version` 当前版本为 **9**。服务端 SHALL 同时接受 `client_protocol_version ∈ {1, 2, 3, 4, 5, 6, 7, 8, 9}` 的握手；收到 `client_protocol_version` 不在该集合内的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。握手响应与后续应答的协议版本 MUST 等于客户端握手的请求版本（v1 客户端得到逐字段与 Phase 1 一致的响应，新增字段对其表现为可忽略的未知字段）；`server_protocol_version` 回显服务端自身版本 9。v3 专属语义（`LOCK_TYPE_FAIR`/`LOCK_TYPE_SEMAPHORE`/`LOCK_TYPE_LATCH` 的 ACQUIRE、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息）MUST 仅对握手中声明 v3 及以上版本的会话开放；v4 专属语义（`ATOMIC_OP` 消息）MUST 仅对握手中声明 v4 及以上的会话开放；v5 专属语义（`BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` 消息）MUST 仅对握手中声明 v5 及以上的会话开放；v6 专属语义（`ATOMIC_OP` 消息携带 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的有值引用请求）MUST 仅对握手中声明 v6 及以上的会话开放；v7 专属语义（`QUEUE_OP` 消息）MUST 仅对握手中声明 v7 及以上的会话开放；v8 专属语义（`TOPIC_OP` 消息）MUST 仅对握手中声明 v8 及以上的会话开放；v9 专属语义（`CONDITION_OP` 消息，以及 `ACQUIRE` 消息携带 `condition` 字段的条件等待折叠形态）MUST 仅对握手中声明 v9 的会话开放：握版本低于其专属阈值的会话发送对应请求 MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），其既有版本行为逐项不变。`TOPIC_MESSAGE` 为服务端→客户端推送，仅出现在 v8 会话连接上（服务端 MUST NOT 向协商版本 <8 的会话推送该类型）；条件等待的唤醒推送复用 `AWAIT_NOTIFY`（其 ref 恒指向携带 `condition` 字段的 `ACQUIRE` 的 request_id），v9 MUST NOT 新增任何推送消息类型。

#### Scenario: v9 握手成功

- **WHEN** 客户端以 `client_protocol_version = 9` 发起握手
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 9` 且 `server_protocol_version = 9`

#### Scenario: v8 握手成功

- **WHEN** 客户端以 `client_protocol_version = 8` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 8`（回显请求版本）且 `server_protocol_version = 9`，v8 全部握手与业务响应行为（含 topic 操作）逐项不变

#### Scenario: v7 握手成功

- **WHEN** 客户端以 `client_protocol_version = 7` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 7`（回显请求版本）且 `server_protocol_version = 9`，v7 全部握手与业务响应行为逐项不变

#### Scenario: v6 握手成功

- **WHEN** 客户端以 `client_protocol_version = 6` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 6`（回显请求版本）且 `server_protocol_version = 9`，v6 全部握手与业务响应行为逐项不变

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

- **WHEN** 客户端以 `client_protocol_version = 10`（或其他未知值）发起握手
- **THEN** 服务端回 `INVALID_REQUEST` 并断开连接

#### Scenario: 低版本客户端使用新类型被拒

- **WHEN** v8 会话发送 `CONDITION_OP` 消息或发送携带 `condition` 字段的 `ACQUIRE` 消息，或 v7 会话发送 `TOPIC_OP` 消息，或 v6 会话发送 `QUEUE_OP` 消息，或 v5 会话发送 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的 `ATOMIC_OP` 消息，或 v4 会话发送 `BARRIER_AWAIT` 消息，或 v3 会话发送 `ATOMIC_OP` 消息，或 v2 会话发送 `lock_type = LOCK_TYPE_SEMAPHORE` 的 ACQUIRE，或 v1 会话发送 LATCH_AWAIT 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝该请求且不断开连接，该会话的既有版本请求照常服务

### Requirement: Leader 提示字段（v2）

`HelloResponse` SHALL 复用既有预留字段 `leader_hint`（field 5，int64）承载当前 Leader 的 `nodeId`，并新增 `leader_address`（field 6，string）承载 Leader 的接入地址（`host:port`）；`AcquireResponse`、`ReleaseResponse`、`LeaseRenewResponse` SHALL 各新增 `leader_node_id`（int64）与 `leader_address`（string）字段。`NOT_LEADER`（StatusCode=10，Phase 1 预留码）在 v2 中启用：非当值 Leader 的节点对写请求 MUST 以该码应答，且 MUST 随附 `leader_node_id`——已知 Leader 时为真实 nodeId，应答节点尚无法给出 Leader 身份（启动后未收到 Leadership 事件、或收到显式无主通知）时为 `-1`；旧 Leader 死亡的过渡窗内提示 MAY 仍为最后已知 nodeId（陈旧性由客户端"改连失败 + 强制重发现"兜底，见 design D3）；`leader_address` 在服务端未配置地址映射时 MUST 为空字符串。提示字段仅在相关应答中出现：OK 应答 MUST NOT 填充 leader 字段（保持 proto3 默认缺省），v1 客户端收到的应答中这些字段作为未知字段被容忍。`CLUSTER_VIEW`（`MessageType = 7`，请求无 payload）的响应 MUST 为 `ClusterView { repeated NodeInfo nodes = 1; StatusCode status = 2 }`（`status` 自述结果：成功 `OK`+成员表，失败错误码+空表，v2 未发布前增补以保证拒绝状态码线路可见），`NodeInfo = { node_id, address, is_leader }`，由任意节点依据本地视图作答。

#### Scenario: HELLO 返回 Leader 提示

- **WHEN** v2 客户端向任意集群节点（含 Follower）发送 HELLO
- **THEN** 响应携带当前 Leader 的 `leader_hint`（nodeId）与 `leader_address`；组内暂无 Leader 时 `leader_hint = -1`

#### Scenario: NOT_LEADER 随附提示

- **WHEN** v2 客户端向 Follower 发送 ACQUIRE
- **THEN** 收到 `status = NOT_LEADER` 的应答，其 `leader_node_id` 为当前 Leader 的 nodeId、`leader_address` 为其接入地址（若服务端已配置地址映射）

#### Scenario: 未知 Leader 提示为 -1

- **WHEN** 应答节点尚未取得任何 Leadership 事件（如集群初始化窗口）即拒绝写请求
- **THEN** `NOT_LEADER` 应答的 `leader_node_id = -1`；过渡窗内的陈旧提示不视为违约（客户端兜底）

#### Scenario: CLUSTER_VIEW 查询集群视图

- **WHEN** v2 客户端向任意集群节点发送 `CLUSTER_VIEW`
- **THEN** 收到 `ClusterView` 响应，`nodes` 覆盖全部成员且恰有一个 `is_leader = true`（选举中可为零个），不产生任何日志条目

#### Scenario: 单机模式不响应 CLUSTER_VIEW

- **WHEN** 客户端向单机模式（`enabled=false`）的服务端发送 `CLUSTER_VIEW`
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝，HELLO/PING 与业务路径不受影响

### Requirement: v3 消息与字段增量

协议 SHALL 以纯增量方式扩展 v3：`LockType` 新增 `LOCK_TYPE_FAIR = 4`、`LOCK_TYPE_SEMAPHORE = 5`、`LOCK_TYPE_LATCH = 6`；`AcquireRequest` 新增 `permits`（field 6，请求许可数，仅 SEMAPHORE 有效，缺省语义为 1）与 `permits_total`（field 7，总许可数，仅 SEMAPHORE 建条目时要求）；`ReleaseRequest` 新增 `permits`（field 4，归还许可数，缺省语义为 1）；新增 `MessageType` `LATCH_COUNT_DOWN = 8`、`LATCH_AWAIT = 9` 与四个 payload 消息：`LatchCountDownRequest { key, count, total }` / `LatchCountDownResponse { status, remaining }`、`LatchAwaitRequest { key, total }` / `LatchAwaitResponse { status }`。`count` 为本次扣减量；`total` 为屏障初始计数的定型断言——非零时条目不存在则创建、存在则须与定型值一致，`0` 为不主张（`countDown` 不主张且屏障不存在、`await` 不主张且屏障不存在均被拒绝；`count = 0` 携带非零 `total` 即纯初始化调用）。初始计数经 countDown 与 await 双通道定型，杜绝"worker 先于创建者 await 到达即丢失扣减"的初始化竞态。既有字段编号与语义 MUST NOT 变更。

#### Scenario: 许可字段合法性

- **WHEN** 非 SEMAPHORE 类型的 ACQUIRE 携带 `permits > 1` 或 `permits_total > 0`，或 SEMAPHORE 的 RELEASE 携带 `permits ≤ 0`
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝该请求，会话与既有锁状态不受影响

#### Scenario: 首次定型与不符拒绝

- **WHEN** SEMAPHORE 首次 ACQUIRE 未携带 `permits_total`，或既有条目上的请求携带与其总许可数不符的非零 `permits_total`；或 LATCH 请求（LATCH_COUNT_DOWN / LATCH_AWAIT）对不存在的屏障未携带非零 `total`，或对既有屏障携带与其定型值不符的非零 `total`
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝，条目状态零扰动

#### Scenario: 非存在屏障的纯加入被拒

- **WHEN** 对从未定型的 LATCH key 发送 `total = 0` 的 LATCH_AWAIT 或 LATCH_COUNT_DOWN
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝（纯加入与无断言扣减 MUST NOT 隐式创建屏障）

#### Scenario: 纯初始化调用

- **WHEN** 对不存在的屏障发送 `count = 0, total = n` 的 LATCH_COUNT_DOWN
- **THEN** 以 `n` 创建屏障并返回 `remaining = n`；对已存在且定型值相符的屏障，同一调用为空操作返回当前剩余（屏障条目自定型起存续至节点重启，不随归零或参与者散尽回收）

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

### Requirement: v5 消息与字段增量

协议 SHALL 以纯增量方式扩展 v5：`MessageType` 新增 `BARRIER_AWAIT = 15`、`BARRIER_LEAVE = 16`、`BARRIER_ACTION_DONE = 17`；`LockType` 新增 `LOCK_TYPE_BARRIER = 10`——仅作 BARRIER 家族 key 定型判别，由三组 BARRIER 消息的 `key` 通道承载，MUST NOT 出现在 ACQUIRE 的 `lock_type` 中（ACQUIRE 携带该值属消息合法性违例，MUST 以 `INVALID_REQUEST` 消息级拒绝、不断连，与 `LOCK_TYPE_LATCH` 与三原子类型同规则）；`StatusCode` 新增 `BARRIER_BROKEN = 12`（在带世代裁决，非请求错误：等待重发命中已破障世代、或动作了结回报时世代已破，应答回该码）；`Envelope.payload` 新增 `barrier_await_request = 34`、`barrier_await_response = 35`、`barrier_leave_request = 36`、`barrier_leave_response = 37`、`barrier_action_done_request = 38`、`barrier_action_done_response = 39`。新增消息：`BarrierAwaitRequest { string key = 1; int64 parties = 2; bool carries_action = 3; }`、`BarrierAwaitResponse { StatusCode status = 1; int32 queue_position = 2; int64 generation = 3; bool executor = 4; int64 parties = 5; }`、`BarrierLeaveRequest { string key = 1; int64 await_request_id = 2; }`、`BarrierLeaveResponse { StatusCode status = 1; }`、`BarrierActionDoneRequest { string key = 1; int64 generation = 2; }`、`BarrierActionDoneResponse { StatusCode status = 1; }`。字段语义：`parties` 为定型断言（≥0，0 为不主张）；`carries_action` 标记调用方携带 barrierAction（最后到场者应答 `executor=true` 时据此裁决需否回报动作了结）；`queue_position` 在 `status=QUEUED` 时有效（1 起）；`generation` 为本等待项加入的世代号（自 1 起、每 key 单调递增，应答方定型）；`executor=true` 且 `status=QUEUED` 表示"你是最后到场者且当前世代处于动作待决态，执行动作后以 `BARRIER_ACTION_DONE` 回报，本等待以该应答终结"；`parties` 回显条目定型值（0=未定型，仅拒绝路径可能）；`await_request_id` 指向本会话原 `BARRIER_AWAIT` 信封的 `request_id`（0 = 无在队等待项的纯破障主张，即 `breakBarrier()` 路径）；`BARRIER_ACTION_DONE.generation` 为执行者持有的世代号。等待者放行通知 MUST 复用 `AWAIT_NOTIFY`（key + `request_id_ref` 指向原 `BARRIER_AWAIT` 信封 `request_id`），MUST NOT 新增推送类型。管理面消息同步纯增量：`AdminSummaryResponse` 新增 `barrier_entries = 11`（int32，无持有语义单列计数，判例 `latch_entries`/`atomic_entries`）；`AdminKeyInfo` 新增 `barrier_parties = 8`（int64）、`barrier_generation = 9`（int64）、`barrier_arrived = 10`（int32）——非 BARRIER 条目恒缺省零值；`AdminKeyDetailResponse` 新增 `barrier_parties = 16`（int64）、`barrier_generation = 17`（int64）、`barrier_arrived = 18`（int32）、`barrier_action_pending = 19`（bool）、`barrier_last_final = 20`（string，词表 `tripped`/`broken`/`none`，最近完结世代的了结形态）——非 BARRIER 条目这些字段恒缺省零值/空串；`AdminKeyInfo.family` 词表扩充 `barrier`。既有字段编号与语义 MUST NOT 变更。

#### Scenario: BARRIER 消息对存在且编号正确

- **WHEN** 构建协议模块并生成代码，检查三组 BARRIER 请求/响应消息
- **THEN** 字段编号、类型与本增量逐项一致，`Envelope.payload` 新增 34–39 六槽，既有 10–33 号槽零变更

#### Scenario: ACQUIRE 携带屏障类型被拒

- **WHEN** 任意版本会话发送 `lock_type = LOCK_TYPE_BARRIER` 的 ACQUIRE
- **THEN** 服务端以 `INVALID_REQUEST` 消息级拒绝且不断开连接

#### Scenario: v4 会话使用屏障消息被拒

- **WHEN** 握手版本为 4 的会话发送 `BARRIER_AWAIT` 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝且不断开连接，该会话既有 v4 能力照常服务

#### Scenario: 破障世代重发回在带裁决码

- **WHEN** 等待者以原 `request_id` 重发 `BARRIER_AWAIT`，其所属世代已被破障
- **THEN** 应答 `status = BARRIER_BROKEN`（非 `INVALID_REQUEST`、非 `SESSION_EXPIRED`），连接保持

#### Scenario: 管理面屏障字段纯增量

- **WHEN** 检查 `AdminSummaryResponse`/`AdminKeyInfo`/`AdminKeyDetailResponse` 生成代码
- **THEN** 屏障新增字段编号与本增量一致，既有字段编号与语义零变更，非 BARRIER 条目的屏障字段为缺省值

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

### Requirement: 拒绝应答码形同型

客户端接入车道可见的拒绝应答 SHALL 与请求消息类型同型：`Envelope.type` 回显请求类型，且 `payload` MUST 携带该类型对应的 Response 消息并以对应状态码自述拒绝结果——MUST NOT 以其他类型的 Response 载荷或空载荷承载拒绝。本协议将既有工程纪律"拒绝状态码线路可见（客户端裁决依赖）"成文为不变式，覆盖接入层门控拒绝（版本/形状/钳制）、集群路径角色门拒绝（`NOT_LEADER`）与提交失败拒绝全部拒绝码形。`leader_node_id`/`leader_address` 提示字段仍仅由 v2 定义的三类应答（Acquire/Release/LeaseRenew）承载——`ATOMIC_OP`/`BARRIER_*`/`LATCH_*`/`QUEUE_OP`/`TOPIC_OP` 的同型拒绝沿无提示字段判例，改道由客户端 Leader 发现与故障转移机制兜底。`TOPIC_MESSAGE` 为推送非应答，MUST NOT 承载任何拒绝形态。`StatusCode.OK` 为枚举零值的事实 MUST NOT 被任何拒绝路径间接利用：缺载荷应答在收发任一侧按 oneof 缺省解读时呈现 protobuf 默认实例即"成功空应答"，属码形违例。

#### Scenario: TOPIC_OP 被非 Leader 节点拒绝码形同型

- **WHEN** v8 会话向非当值 Leader 的集群节点发送任意 `TOPIC_OP`（含 SUBSCRIBE/PUBLISH）
- **THEN** 应答信封 payload 为 `topic_op_response`、`status = NOT_LEADER` 且 `op` 回显请求操作，不产生任何订阅/发布副作用；客户端对同型拒绝码执行既有退避改道

#### Scenario: QUEUE_OP 被非 Leader 节点拒绝码形同型

- **WHEN** v7 会话向非当值 Leader 的集群节点发送任意 `QUEUE_OP`（含只读 op：SIZE/PEEK），该节点角色门拒绝或提交以在途可重试原因失败
- **THEN** 应答信封 payload 为 `queue_op_response`、`status = NOT_LEADER` 且 `op` 回显请求操作，不产生日志条目、连接保持；客户端对同型拒绝码执行既有退避改道

#### Scenario: LATCH 拒绝码形同型杜绝假绿

- **WHEN** `LATCH_COUNT_DOWN` 或 `LATCH_AWAIT` 被集群路径拒绝（角色门或提交失败）
- **THEN** 应答分别携带 `latch_count_down_response` / `latch_await_response` 且 `status = NOT_LEADER`——客户端 MUST NOT 可能读到默认实例形态的 `OK`（`remaining=0`/"屏障已破"假绿形态在协议层不可达）

#### Scenario: 全类型拒绝码形表驱动门禁

- **WHEN** 构建门禁枚举客户端接入车道全部请求 `MessageType`（含 `TOPIC_OP`），逐一构造被各拒绝码形路径拒绝的应答并检查 payload
- **THEN** 每一类型的拒绝应答都携带与请求同型的 Response 载荷；任何新增消息类型未通过本门禁（无同型拒绝 case）即构建红

### Requirement: v8 消息与字段增量

协议 SHALL 以纯增量方式扩展 v8：`MessageType` 新增 `TOPIC_OP = 19` 与 `TOPIC_MESSAGE = 20`；`Envelope.payload` oneof 新增 `TopicOpRequest topic_op_request = 42`、`TopicOpResponse topic_op_response = 43`、`TopicMessage topic_message = 44`；新增枚举 `TopicOp { TOPIC_OP_SUBSCRIBE = 0, TOPIC_OP_UNSUBSCRIBE = 1, TOPIC_OP_PUBLISH = 2 }`。`LockType` MUST NOT 新增取值（topic 无 core 条目形态判别）；`StatusCode` 新增 `REJECT_SUBSCRIBERS = 13`（第 14 值）。既有字段编号与语义 MUST NOT 变更。

`TopicOpRequest { key=1, op=2, payload_bytes=3(optional bytes), op_seq=4(uint64) }`。字段语义：`op` 决定操作；`payload_bytes` 为 PUBLISH 的消息体（**PUBLISH 必须携带——缺省 presence 即形状违例**，消息体不可为 null，零长度空字节串为合法消息体，判例 v7 元素纪律；非 PUBLISH 携带为违例）；`op_seq` 仅 PUBLISH 有效（会话内单调写序号，供受理节点去重；SUBSCRIBE/UNSUBSCRIBE 填 0，非零为违例）。`key` 与既有长度纪律一致（超限拒绝）。

`TopicOpResponse { status=1, op=2, subscription_id=3(uint64), topic_seq=4(uint64) }`。`status` 取 `OK/REJECT_SUBSCRIBERS/NOT_LEADER/INVALID_REQUEST/INTERNAL_ERROR/SESSION_EXPIRED`（形状违例与撞 key 一律 `INVALID_REQUEST`，判例引擎形状拒绝的线路映射；`QUEUED`/`DENIED`/`OVERLOADED` 对 topic 恒不可达）；`subscription_id` 仅 SUBSCRIBE 的 `OK` 有效（本订阅路由键，0=无；同会话同键重复 SUBSCRIBE 幂等返回同一登记语义），UNSUBSCRIBE 恒 0；`topic_seq` 仅 PUBLISH 的 `OK` 有效（本次发布的受理序号，命中去重槽的重发返回同一份），其余恒 0。UNSUBSCRIBE 幂等：未存在的订阅亦回 `OK`（无错误态）。

`TopicMessage { key=1, subscription_id=2(uint64), topic_seq=3(uint64), publisher_sid=4(uint64), publish_ts_ms=5(int64), payload_bytes=6(bytes) }` 为服务端→订阅会话的广播交付推送：`topic_seq` 为 Leader term 内按 key 单调的发布序号（换主后重新起算，非持久状态）；`publisher_sid` 为发布方逻辑会话 id（0=不可知）；`publish_ts_ms` 为受理节点时钟的受理时刻；`payload_bytes` 恒存在（presence 纪律的交付侧对偶）。

管理面同步纯增量：`AdminSummaryResponse` 新增 `topic_entries`（Leader 本地订阅登记表按 key 计数）；`AdminKeyInfo` 新增 `topic_subscribers`（int32）；`AdminKeyDetailResponse` 新增 `topic_subscribers`（int32）与 `repeated AdminTopicSubscriberInfo`（`{ session_id, subscription_id, subscribed_at_ms }`）；`AdminKeyInfo.family` 词表增 `topic`。topic 字段对非 topic 键恒零值/空列表；**已交付消息内容与全量消息字节 MUST NOT 出现在任何管理应答中**（订阅维仅计数与会话/序号/时刻）。既有字段编号与语义 MUST NOT 变更。

#### Scenario: v8 字段编号纯增量

- **WHEN** 构建协议模块并生成代码，检查 `MessageType`/`Envelope.payload`/`TopicOp`/`TopicOpRequest`/`TopicOpResponse`/`TopicMessage`/`StatusCode`/管理面消息
- **THEN** 新增枚举值与字段编号与本增量逐项一致（19/20、42/43/44、状态码 13），既有字段编号与语义零变更，`LockType` 无新增取值，`RaftEntryType` 与 `raft.proto` 零差异

#### Scenario: 消息缺省 presence 即违例

- **WHEN** v8 会话发送不携带 `payload_bytes` 的 PUBLISH 请求
- **THEN** 服务端以 `INVALID_REQUEST` 同型消息级拒绝且不断连、零 fan-out（消息体不可为 null，与 v6 引用形态"null 为一等公民"刻意不同，与 v7 元素纪律同判例）

#### Scenario: 形状互斥矩阵入口拒绝

- **WHEN** TOPIC_OP 请求出现形态外组合（非 PUBLISH 携 `payload_bytes`、SUBSCRIBE/UNSUBSCRIBE 携非零 `op_seq`、PUBLISH 携零 `op_seq`）
- **THEN** 服务端以 `INVALID_REQUEST` 同型消息级拒绝且不断连，订阅登记零扰动

#### Scenario: 消息字节往返保真

- **WHEN** 对 PUBLISH 请求 `payload_bytes` 缺省、零长度、非空、恰限 `maxValueBytes` 四种形态与 `TopicMessage` 全字段做编解码往返
- **THEN** presence 在生成代码中可判别（`hasPayloadBytes()` 逐一区分），字节内容无损；恰限 4KB 消息体二进制往返字节级保真

#### Scenario: 订阅维管理字段齐备

- **WHEN** 预置含多订阅的 topic key 后请求 SUMMARY/LIST_KEYS/KEY_DETAIL
- **THEN** `topic_entries`、`topic_subscribers` 计数与订阅者明细列表（会话 id、订阅 id、订阅时刻）逐项可读，已交付消息内容零外发；非 topic 键的 topic 字段恒零值/空列表

### Requirement: v9 消息与字段增量

协议 SHALL 以纯增量方式扩展 v9：`MessageType` 新增 `CONDITION_OP = 21`；`Envelope.payload` oneof 新增 `ConditionOpRequest condition_op_request = 45`、`ConditionOpResponse condition_op_response = 46`；`AcquireRequest` 新增 `optional string condition = 8`；新增枚举 `ConditionOp { CONDITION_OP_SIGNAL = 0, CONDITION_OP_SIGNAL_ALL = 1, CONDITION_OP_LEAVE = 2 }`。`LockType`、`StatusCode`、`RaftEntryType` MUST NOT 新增取值（条件不是 key 形态、拒绝语义全部由既有码承载、await 复用既有 `LOCK_ACQUIRE_ENTRY`——三词表现值上界即本条款的编号证据）；`raft.proto` 与 `SnapshotLock` MUST 零差异/零扩展（折叠标记随 `AcquirePayload` 内 `AcquireRequest.condition` 字段透传入日志，无需条目层新字段）。既有字段编号与语义 MUST NOT 变更。

`AcquireRequest.condition` 的 presence 即**条件等待折叠形态**：该 ACQUIRE 的经复制裁决语义为"若 (会话,线程) 恰为锁持有归属则重入计数一步清零并清除租约（释放折叠），否则释放零操作"；登记入等待集为受理节点的本地裁决（单机在条目关键区内随折叠命令原子完成；集群在 Leader 受理预检点先登记、释放半程随后经提交生效——两拓扑均保证"登记先于释放可见"，signal 不可能穿过未登记的已释放态）。被唤醒后的重发信封 MUST 清除 `condition` 字段而保持 `request_id` 不变（唤醒终结 await 阶段、转入普通排队获取语义——单机经队首重发规则命中授予，集群经普通获取通道授予；客户端凭通知到达切换信封形态，服务端两形行为均由既有获取/折叠纪律完整定义）。缺省 presence 时既有 ACQUIRE 语义逐项不变。折叠形态形状纪律：`condition` MUST 非空且 UTF-8 字节数 ≤ `maxKeyLength`（空串/超长 `INVALID_REQUEST`）；`wait_ms = 0`（立即式）携带 condition 为违例（等待恒为挂起形态，`>0` 的本地计时窗与 `-1` 对服务端同判）；`lock_type ∈ {READ, WRITE, SEMAPHORE, LATCH, BARRIER, ATOMIC_*}` 携带 condition 为违例（v1 支持面仅 REENTRANT/SIMPLE/FAIR 三互斥形态）；`lease_ms`/`thread_id` 沿用既有纪律（重新获取时的生效租约与归属由重发的无条件字段信封按既有获取语义承载）。非 LOCK 家族既有条目 key 上携带 condition → 既有家族不匹配拒绝的线路映射 `INVALID_REQUEST`。

`ConditionOpRequest { key=1, op=2, condition=3(string), thread_id=4(int64), await_request_id=5(int64) }`。`op` 决定操作；`condition` 恒必携非空（寻址目标等待集，超长违例）；形状互斥矩阵：SIGNAL/SIGNAL_ALL 必携 `thread_id`（权限检查的归属线程）、`await_request_id` MUST 为 0；LEAVE 必携 `await_request_id`（指向原折叠 ACQUIRE 的 request_id，按 (会话, request_id) 摘除登记）、`thread_id` MUST 为 0；矩阵外组合一律同型 `INVALID_REQUEST`、等待集零扰动。

`ConditionOpResponse { status=1, op=2 }`。`status` 取 `OK/NOT_HELD/NOT_LEADER/INVALID_REQUEST/SESSION_EXPIRED/INTERNAL_ERROR`（`QUEUED`/`DENIED`/`OVERLOADED` 对 CONDITION_OP 恒不可达——signal 家族恒即时回执，await 折叠侧的等待满超限属 `AcquireResponse` 线）；SIGNAL/SIGNAL_ALL 的 `OK` 不承诺唤醒效果（搬运后授予经等待-通知-重发闭环另行完成），权限先行：调用者恰为持有归属时，目标集空/无此条件名回 `OK` 无操作；条目不存在或无人持有恒 `NOT_HELD`（JDK"不持锁无从 signal"同型）；LEAVE 恒 `OK`（幂等，未存在的登记无错误态）。await 折叠形态的应答即 `AcquireResponse` 既有词表：`QUEUED`（已登记，`queue_position` 为本 key 等待项合计口径位次）、`OVERLOADED`（等待项超限）、`NOT_LEADER`（随附 v2 leader 提示字段）、错误码同既有映射——`AcquireResponse` 消息 MUST NOT 新增字段。

管理面同步纯增量：`AdminKeyInfo` 新增 `condition_waiters`（int32）；`AdminKeyDetailResponse` 新增 `condition_waiters`（int32）与 `repeated AdminConditionWaiterInfo condition_waiters_info`（`{ condition=1, session_id=2, request_id=3, thread_id=4, registered_at_ms=5 }`）；`family` 词表零新增（条件骑 LOCK 家族）。条件字段对非 LOCK 键恒零值/空列表；`AdminSummaryResponse` 零新增计数（等待者总数口径已含条件等待者，由 metrics 能力钉定）。既有字段编号与语义 MUST NOT 变更。

#### Scenario: v9 字段编号纯增量

- **WHEN** 构建协议模块并生成代码，检查 `MessageType`/`Envelope.payload`/`ConditionOp`/`ConditionOpRequest`/`ConditionOpResponse`/`AcquireRequest` 与管理面消息
- **THEN** 新增枚举值与字段编号与本增量逐项一致（21、45/46、condition=8），既有字段编号与语义零变更，`LockType`/`StatusCode` 无新增取值，`RaftEntryType` 与 `raft.proto` 字节零差异、`SnapshotLock` 零扩展

#### Scenario: 折叠形态 presence 可判别与往返保真

- **WHEN** 对 `AcquireRequest` 的 `condition` 缺省、空串、正常短名、恰限 `maxKeyLength` 四形态做编解码往返，并对 `ConditionOpRequest` 三操作全形状（含矩阵违例形）往返
- **THEN** presence 在生成代码中可判别（`hasCondition()` 逐一区分），恰限字节串无损；违例形不改变编解码保真（形状裁决在接入层，协议层只载字节）

#### Scenario: 条件唤醒推送零新类型

- **WHEN** 检查 v9 全部新增/变更后消息与 `AWAIT_NOTIFY` 定义
- **THEN** `AWAIT_NOTIFY` 与 `AwaitNotify` 消息字节级零变更，条件等待的唤醒通知 `request_id_ref` 指向携带 `condition` 的 `ACQUIRE` 的 request_id（折叠形态复用 ref 型推送纪律）

#### Scenario: CONDITION_OP 被非 Leader 节点拒绝码形同型

- **WHEN** v9 会话向非当值 Leader 的集群节点发送 SIGNAL/SIGNAL_ALL/LEAVE 任一 `CONDITION_OP`
- **THEN** 应答信封 payload 为 `condition_op_response`、`status = NOT_LEADER` 且 `op` 回显请求操作，不产生任何搬运/摘除副作用；客户端对同型拒绝码执行既有退避改道（无 leader 提示字段，判例 QUEUE/TOPIC 直发车道）

#### Scenario: 条件维管理字段齐备

- **WHEN** 预置含多条件多等待者的锁 key 后请求 LIST_KEYS/KEY_DETAIL
- **THEN** `condition_waiters` 计数与条件等待明细五字段列表逐项可读；非 LOCK 键与 Follower 视角条件字段恒零值/空列表
