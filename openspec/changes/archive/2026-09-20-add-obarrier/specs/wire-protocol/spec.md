## MODIFIED Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW、LATCH_COUNT_DOWN、LATCH_AWAIT、ADMIN_SUMMARY、ADMIN_LIST_KEYS、ADMIN_KEY_DETAIL、ADMIN_LIST_SESSIONS、ATOMIC_OP、BARRIER_AWAIT、BARRIER_LEAVE、BARRIER_ACTION_DONE），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及各阶段变更的增量定义逐项一致。v2/v3/v4/v5 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 17 个消息类型、11 种锁类型、13 个状态码与 30 个 payload 消息（Phase 1 的 9 个加 `ClusterView`、Latch 四个、ADMIN 八个、AtomicOp 两个与 Barrier 六个），且字段编号与增量定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew/LatchCountDown/LatchAwait/AdminSummary/AdminListKeys/AdminKeyDetail/AdminListSessions/AtomicOp/BarrierAwait/BarrierLeave/BarrierActionDone 十四类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与定义一致

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号

### Requirement: 协议版本

`protocol_version` 当前版本为 **5**。服务端 SHALL 同时接受 `client_protocol_version ∈ {1, 2, 3, 4, 5}` 的握手；收到 `client_protocol_version` 不在该集合内的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。握手响应与后续应答的协议版本 MUST 等于客户端握手的请求版本（v1 客户端得到逐字段与 Phase 1 一致的响应，新增字段对其表现为可忽略的未知字段）；`server_protocol_version` 回显服务端自身版本 5。v3 专属语义（`LOCK_TYPE_FAIR`/`LOCK_TYPE_SEMAPHORE`/`LOCK_TYPE_LATCH` 的 ACQUIRE、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息）MUST 仅对握手中声明 v3 及以上版本的会话开放；v4 专属语义（`ATOMIC_OP` 消息）MUST 仅对握手中声明 v4 及以上的会话开放；v5 专属语义（`BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` 消息）MUST 仅对握手中声明 v5 的会话开放：握版本低于其专属阈值的会话发送对应请求 MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），其既有版本行为逐项不变。

#### Scenario: v5 握手成功

- **WHEN** 客户端以 `client_protocol_version = 5` 发起握手
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 5` 且 `server_protocol_version = 5`

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

- **WHEN** 客户端以 `client_protocol_version = 6`（或其他未知值）发起握手
- **THEN** 服务端回 `INVALID_REQUEST` 并断开连接

#### Scenario: 低版本客户端使用新类型被拒

- **WHEN** v4 会话发送 `BARRIER_AWAIT` 消息，或 v3 会话发送 `ATOMIC_OP` 消息，或 v2 会话发送 `lock_type = LOCK_TYPE_SEMAPHORE` 的 ACQUIRE，或 v1 会话发送 LATCH_AWAIT 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝该请求且不断开连接，该会话的既有版本请求照常服务

## ADDED Requirements

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
