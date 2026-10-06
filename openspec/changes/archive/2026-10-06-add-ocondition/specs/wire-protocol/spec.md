# Spec Delta

## MODIFIED Requirements

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

## ADDED Requirements

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
