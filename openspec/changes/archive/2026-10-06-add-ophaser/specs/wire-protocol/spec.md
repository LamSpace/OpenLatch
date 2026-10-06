# Spec Delta

## MODIFIED Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW、LATCH_COUNT_DOWN、LATCH_AWAIT、ADMIN_SUMMARY、ADMIN_LIST_KEYS、ADMIN_KEY_DETAIL、ADMIN_LIST_SESSIONS、ATOMIC_OP、BARRIER_AWAIT、BARRIER_LEAVE、BARRIER_ACTION_DONE、QUEUE_OP、TOPIC_OP、TOPIC_MESSAGE、CONDITION_OP、PHASER_OP），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及各阶段变更的增量定义逐项一致。v2/v3/v4/v5/v6/v7/v8/v9/v10 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 22 个消息类型、15 种锁类型、14 个状态码与 39 个 payload 消息（Phase 1 的 9 个加 `ClusterView`、Latch 四个、ADMIN 八个、AtomicOp 两个、Barrier 六个、QueueOp 两个、TopicOp 两个及 `TopicMessage`、ConditionOp 两个与 PhaserOp 两个），且字段编号与增量定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew/LatchCountDown/LatchAwait/AdminSummary/AdminListKeys/AdminKeyDetail/AdminListSessions/AtomicOp/BarrierAwait/BarrierLeave/BarrierActionDone/QueueOp/TopicOp/ConditionOp/PhaserOp 十八类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与定义一致（`TOPIC_MESSAGE` 为服务端主动推送，不属于配对交互，按推送关联规则约束；v9 条件等待与 v10 phaser 等待均不设独立等待/推送消息——条件 await 为 `ACQUIRE` 携带 `condition` 字段的折叠形态、phaser 等待为 `PHASER_OP` 消息对内 `AWAIT_ADVANCE`/阻塞位 `ARRIVE_AND_AWAIT` 操作词承载，唤醒推送统一复用 `AWAIT_NOTIFY`）

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号

### Requirement: 协议版本

`protocol_version` 当前版本为 **10**。服务端 SHALL 同时接受 `client_protocol_version ∈ {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}` 的握手；收到 `client_protocol_version` 不在该集合内的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。握手响应与后续应答的协议版本 MUST 等于客户端握手的请求版本（v1 客户端得到逐字段与 Phase 1 一致的响应，新增字段对其表现为可忽略的未知字段）；`server_protocol_version` 回显服务端自身版本 10。v3 专属语义（`LOCK_TYPE_FAIR`/`LOCK_TYPE_SEMAPHORE`/`LOCK_TYPE_LATCH` 的 ACQUIRE、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息）MUST 仅对握手中声明 v3 及以上版本的会话开放；v4 专属语义（`ATOMIC_OP` 消息）MUST 仅对握手中声明 v4 及以上的会话开放；v5 专属语义（`BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` 消息）MUST 仅对握手中声明 v5 及以上的会话开放；v6 专属语义（`ATOMIC_OP` 消息携带 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的有值引用请求）MUST 仅对握手中声明 v6 及以上的会话开放；v7 专属语义（`QUEUE_OP` 消息）MUST 仅对握手中声明 v7 及以上的会话开放；v8 专属语义（`TOPIC_OP` 消息）MUST 仅对握手中声明 v8 及以上的会话开放；v9 专属语义（`CONDITION_OP` 消息，以及 `ACQUIRE` 消息携带 `condition` 字段的条件等待折叠形态）MUST 仅对握手中声明 v9 及以上的会话开放；v10 专属语义（`PHASER_OP` 消息，含全部七个操作词与 `LOCK_TYPE_PHASER` 判别）MUST 仅对握手中声明 v10 的会话开放：握版本低于其专属阈值的会话发送对应请求 MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），其既有版本行为逐项不变。`TOPIC_MESSAGE` 为服务端→客户端推送，仅出现在 v8 会话连接上（服务端 MUST NOT 向协商版本 <8 的会话推送该类型）；条件等待的唤醒推送复用 `AWAIT_NOTIFY`（其 ref 恒指向携带 `condition` 字段的 `ACQUIRE` 的 request_id）；phaser 等待项的唤醒推送同样复用 `AWAIT_NOTIFY`（其 ref 恒指向等待方 `PHASER_OP` 请求的 request_id），v9/v10 MUST NOT 新增任何推送消息类型。

#### Scenario: v10 握手成功

- **WHEN** 客户端以 `client_protocol_version = 10` 发起握手
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 10` 且 `server_protocol_version = 10`

#### Scenario: v9 握手成功

- **WHEN** 客户端以 `client_protocol_version = 9` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 9`（回显请求版本）且 `server_protocol_version = 10`，v9 全部握手与业务响应行为（含条件等待与 topic 操作）逐项不变

#### Scenario: v8 握手成功

- **WHEN** 客户端以 `client_protocol_version = 8` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 8`（回显请求版本）且 `server_protocol_version = 10`，v8 全部握手与业务响应行为（含 topic 操作）逐项不变

#### Scenario: v7 握手成功

- **WHEN** 客户端以 `client_protocol_version = 7` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 7`（回显请求版本）且 `server_protocol_version = 10`，v7 全部握手与业务响应行为逐项不变

#### Scenario: v6 握手成功

- **WHEN** 客户端以 `client_protocol_version = 6` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 6`（回显请求版本）且 `server_protocol_version = 10`，v6 全部握手与业务响应行为逐项不变

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

- **WHEN** 客户端以 `client_protocol_version = 11`（或其他未知值）发起握手
- **THEN** 服务端回 `INVALID_REQUEST` 并断开连接

#### Scenario: 低版本客户端使用新类型被拒

- **WHEN** v9 会话发送 `PHASER_OP` 消息，或 v8 会话发送 `CONDITION_OP` 消息或发送携带 `condition` 字段的 `ACQUIRE` 消息，或 v7 会话发送 `TOPIC_OP` 消息，或 v6 会话发送 `QUEUE_OP` 消息，或 v5 会话发送 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的 `ATOMIC_OP` 消息，或 v4 会话发送 `BARRIER_AWAIT` 消息，或 v3 会话发送 `ATOMIC_OP` 消息，或 v2 会话发送 `lock_type = LOCK_TYPE_SEMAPHORE` 的 ACQUIRE，或 v1 会话发送 LATCH_AWAIT 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝该请求且不断开连接，该会话的既有版本请求照常服务

## ADDED Requirements

### Requirement: v10 消息与字段增量

协议 SHALL 以纯增量方式扩展 v10：`MessageType` 新增 `PHASER_OP = 22`；`Envelope.payload` oneof 新增 `PhaserOpRequest phaser_op_request = 47`、`PhaserOpResponse phaser_op_response = 48`；`LockType` 新增 `LOCK_TYPE_PHASER = 14`（key 家族定型判别，仅 `PHASER_OP` 消息携带；`ACQUIRE` 等其余车道携带属违例）；新增枚举 `PhaserOp { PHASER_OP_REGISTER = 0, PHASER_OP_ARRIVE = 1, PHASER_OP_ARRIVE_AND_AWAIT = 2, PHASER_OP_ARRIVE_AND_DEREGISTER = 3, PHASER_OP_AWAIT_ADVANCE = 4, PHASER_OP_CANCEL = 5, PHASER_OP_QUERY = 6 }`（七操作：`bulkRegister` 折叠为 REGISTER 携带 `parties > 1`，阻塞位折叠判例 v7 `put/offer` 与 `take/poll`——到场与等待合为 `ARRIVE_AND_AWAIT` 单操作词）。`StatusCode` MUST NOT 新增取值（parties 上限与等待满均骑既有 `OVERLOADED` 资源护栏码——v7"等待满"轨判例；形状违例与无条目拒绝对号 `INVALID_REQUEST`；v10 起三词表编号证据更新为：`StatusCode` 止于 13、`LockType` 止于 14、`RaftEntryType` 止于 14——`PHASER_OP_ENTRY = 14` 即 v10 的条目层编号证据，既有守卫"RaftEntryType 止于 13"基线随本提案更替为 14）。`raft.proto` 仅两处增量：`RaftEntryType.PHASER_OP_ENTRY = 14` 与 `SnapshotLock` phaser 字段续接；`SessionPayload`/`AcquirePayload` 等既有载荷消息零变更。既有字段编号与语义 MUST NOT 变更。

`PhaserOpRequest { key=1, op=2, parties=3(int32), expected_phase=4(sint64), await_request_id=5(int64) }`。形状互斥矩阵：REGISTER 必携 `parties ∈ [1, max-parties-per-phaser]`、其余字段 0；ARRIVE/ARRIVE_AND_AWAIT/ARRIVE_AND_DEREGISTER/QUERY `parties` MUST 为 0、`expected_phase` MUST 缺省（哨兵 `sint64` 语义：未主张恒 0、`await_advance` 必携非零主张——`AWAIT_ADVANCE` 必携 `expected_phase ≥ 0`（等待谓词为"当前相位 > expected_phase"，JDK `awaitAdvance(phase)` 的入相判别对偶）、其余操作该字段 MUST 为未主张 0；`CANCEL` 必携 `await_request_id`（指向被撤销等待项的原 `PHASER_OP` request_id）、`AWAIT_ADVANCE` 与到场/注册/查询类该字段 MUST 为 0；矩阵外组合一律同型 `INVALID_REQUEST`、条目账簿与等待集零扰动（判例 v8 payload/op_seq 矩阵、v9 thread_id/await_request_id 矩阵）。key 校验、`maxKeyLength`、会话有效性纪律沿用既有入口裁决；`lock_type = LOCK_TYPE_PHASER` 仅在 PHASER_OP 车道合法，`ACQUIRE`/`BARRIER_*`/其余家族消息对已定型 phaser key 的命中走既有家族不匹配拒绝线路映射 `INVALID_REQUEST`。

`PhaserOpResponse { status=1, op=2, phase=3(sint64), registered=4(int32), arrived=5(int32) }`。`status` 取 `OK/QUEUED/OVERLOADED/NOT_LEADER/INVALID_REQUEST/SESSION_EXPIRED/INTERNAL_ERROR`。`phase` 为相位回显：REGISTER/ARRIVE/ARRIVE_AND_AWAIT/ARRIVE_AND_DEREGISTER 携带**到场相位号**（JDK `arrive()` 返回值语义——该次注册/到场发生时条目所处相位；若该操作恰触发合拢，回显仍为到场时的相位，推进经后续查询或唤醒可见）；`AWAIT_ADVANCE` 受理回执 `QUEUED` 时 `phase` 为受理时刻当前相位（观察值），了结重发命中回执 `OK` 时 `phase` 为**了结时刻的当前相位**（JDK `awaitAdvance` 返回"下一到场相位号"语义：新相位号 > 入参）；`ARRIVE_AND_AWAIT` 的到场恰触发合拢时直接 `OK`（不发生等待），`phase` 回显到场相位；`QUERY` 回 `OK` 且 `phase`/`registered`/`arrived` 三计数为 Leader 本地账簿的抓取读数（`unarrived` 由客户端按差值导出，线路不重复承载）。收到唤醒通知后的重发恒以 `OK` 终态了结（等待即终结，不再有二次 `QUEUED`——对照锁/Latch/Barrier 重发的"仍排队"分支：phaser 唤醒谓词"相位已推进"在通知时刻已成立，无回炉语义；亦无破相概念，终态仅 `OK`）。等待位次字段（`queue_position`）对 phaser `QUEUED` 不承载语义（恒 0——phaser 等待是广播谓词等待非队首批授，判例 topic 订阅回执无位次；对照 v9 折叠 await 的位次为合并口径）。

管理面同步纯增量：`AdminKeyInfo` 新增 `phaser_phase`（sint64）、`phaser_registered`（int32）、`phaser_arrived`（int32）；`AdminKeyDetailResponse` 新增 `phaser_phase`/`phaser_registered`/`phaser_arrived` 与 `repeated AdminPhaserWaiterInfo phaser_waiters_info`（`{ session_id=1, request_id=2, expected_phase=3(sint64), registered_at_ms=4 }`）、`repeated AdminPhaserPartyInfo phaser_parties_info`（`{ session_id=1, parties=2(int32) }`——每会话注册配额）；`family` 词表新增 `phaser`（新家族行，与 `topic` 行并列 Leader 口径）；`AdminSummaryResponse` 新增 `phaser_entries`（Leader 视角条目数——phaser 为复制态家族，各节点读数经镜像收敛，与 topic 本地表口径分轨）。phaser 字段对非 phaser 键恒零值/空列表；`AdminKeyDetailResponse` 的等待区段复用 `wait_queue_leader_only` 同源口径（等待集 Leader 易失、Follower 如实零）。既有字段编号与语义 MUST NOT 变更。

#### Scenario: v10 字段编号纯增量与证据线更替

- **WHEN** 构建协议模块并生成代码，检查 `MessageType`/`Envelope.payload`/`PhaserOp`/`PhaserOpRequest`/`PhaserOpResponse`/`LockType`/`RaftEntryType` 与管理面消息
- **THEN** 新增枚举值与字段编号与本增量逐项一致（22、47/48、LockType 14、`PHASER_OP_ENTRY` 14），既有字段编号与语义零变更，`StatusCode` 无新增取值；编号证据基线断言由"RaftEntryType 止于 13"更替为"止于 14、14 为 phaser 专用、1–13 既有编号语义逐项不变"且常驻守卫通过

#### Scenario: PhaserOp 全形状往返保真

- **WHEN** 对 `PhaserOpRequest` 七操作全形状（含矩阵违例形：REGISTER 携 `expected_phase`、`AWAIT_ADVANCE` 携 `parties`、`CANCEL` 携 `expected_phase` 等）做编解码往返
- **THEN** 各形状无损编码、`expected_phase` 的未主张与 0 主张经 `sint64`+presence 纪律可判别，违例形不改变编解码保真（形状裁决在接入层，协议层只载字节）

#### Scenario: 相位唤醒推送零新类型

- **WHEN** 检查 v10 全部新增/变更后消息与 `AWAIT_NOTIFY` 定义
- **THEN** `AWAIT_NOTIFY` 与 `AwaitNotify` 消息字节级零变更，phaser 等待项的唤醒通知 `request_id_ref` 指向等待方 `PHASER_OP`（`AWAIT_ADVANCE` 或 `ARRIVE_AND_AWAIT`）的 request_id

#### Scenario: PHASER_OP 被非 Leader 节点拒绝码形同型

- **WHEN** v10 会话向非当值 Leader 的集群节点发送任意 `PHASER_OP`（含只读 `QUERY` 与本地类 `AWAIT_ADVANCE`）
- **THEN** 应答信封 payload 为 `phaser_op_response`、`status = NOT_LEADER` 且 `op` 回显请求操作，不产生任何账簿/等待集副作用；客户端对同型拒绝码执行既有退避改道（无 leader 提示字段，判例 QUEUE/TOPIC/CONDITION 直发车道）

#### Scenario: 条件维 phaser 管理字段齐备

- **WHEN** 预置含多会话配额与等待者的 phaser key 后请求 LIST_KEYS/KEY_DETAIL
- **THEN** `phaser_phase`/`phaser_registered`/`phaser_arrived` 计数与等待明细、配额明细逐项可读；非 phaser 键与 Follower 视角对应字段恒零值/空列表；SUMMARY 的 `phaser_entries` 计数与列表行数自洽
