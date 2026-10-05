# Spec Delta

## MODIFIED Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW、LATCH_COUNT_DOWN、LATCH_AWAIT、ADMIN_SUMMARY、ADMIN_LIST_KEYS、ADMIN_KEY_DETAIL、ADMIN_LIST_SESSIONS、ATOMIC_OP、BARRIER_AWAIT、BARRIER_LEAVE、BARRIER_ACTION_DONE、QUEUE_OP、TOPIC_OP、TOPIC_MESSAGE），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及各阶段变更的增量定义逐项一致。v2/v3/v4/v5/v6/v7/v8 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 20 个消息类型、14 种锁类型、14 个状态码与 35 个 payload 消息（Phase 1 的 9 个加 `ClusterView`、Latch 四个、ADMIN 八个、AtomicOp 两个、Barrier 六个、QueueOp 两个与 TopicOp 两个及 `TopicMessage`），且字段编号与增量定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew/LatchCountDown/LatchAwait/AdminSummary/AdminListKeys/AdminKeyDetail/AdminListSessions/AtomicOp/BarrierAwait/BarrierLeave/BarrierActionDone/QueueOp/TopicOp 十六类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与定义一致（`TOPIC_MESSAGE` 为服务端主动推送，不属于配对交互，按推送关联规则约束）

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号

### Requirement: 协议版本

`protocol_version` 当前版本为 **8**。服务端 SHALL 同时接受 `client_protocol_version ∈ {1, 2, 3, 4, 5, 6, 7, 8}` 的握手；收到 `client_protocol_version` 不在该集合内的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。握手响应与后续应答的协议版本 MUST 等于客户端握手的请求版本（v1 客户端得到逐字段与 Phase 1 一致的响应，新增字段对其表现为可忽略的未知字段）；`server_protocol_version` 回显服务端自身版本 8。v3 专属语义（`LOCK_TYPE_FAIR`/`LOCK_TYPE_SEMAPHORE`/`LOCK_TYPE_LATCH` 的 ACQUIRE、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息）MUST 仅对握手中声明 v3 及以上版本的会话开放；v4 专属语义（`ATOMIC_OP` 消息）MUST 仅对握手中声明 v4 及以上的会话开放；v5 专属语义（`BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` 消息）MUST 仅对握手中声明 v5 及以上的会话开放；v6 专属语义（`ATOMIC_OP` 消息携带 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的有值引用请求）MUST 仅对握手中声明 v6 及以上的会话开放；v7 专属语义（`QUEUE_OP` 消息）MUST 仅对握手中声明 v7 及以上的会话开放；v8 专属语义（`TOPIC_OP` 消息）MUST 仅对握手中声明 v8 的会话开放：握版本低于其专属阈值的会话发送对应请求 MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），其既有版本行为逐项不变。`TOPIC_MESSAGE` 为服务端→客户端推送，仅出现在 v8 会话连接上（服务端 MUST NOT 向协商版本 <8 的会话推送该类型）。

#### Scenario: v8 握手成功

- **WHEN** 客户端以 `client_protocol_version = 8` 发起握手
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 8` 且 `server_protocol_version = 8`

#### Scenario: v7 握手成功

- **WHEN** 客户端以 `client_protocol_version = 7` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 7`（回显请求版本）且 `server_protocol_version = 8`，v7 全部握手与业务响应行为（含队列操作）逐项不变

#### Scenario: v6 握手成功

- **WHEN** 客户端以 `client_protocol_version = 6` 发起握手并继续业务请求
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 6`（回显请求版本）且 `server_protocol_version = 8`，v6 全部握手与业务响应行为逐项不变

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

- **WHEN** 客户端以 `client_protocol_version = 9`（或其他未知值）发起握手
- **THEN** 服务端回 `INVALID_REQUEST` 并断开连接

#### Scenario: 低版本客户端使用新类型被拒

- **WHEN** v7 会话发送 `TOPIC_OP` 消息，或 v6 会话发送 `QUEUE_OP` 消息，或 v5 会话发送 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的 `ATOMIC_OP` 消息，或 v4 会话发送 `BARRIER_AWAIT` 消息，或 v3 会话发送 `ATOMIC_OP` 消息，或 v2 会话发送 `lock_type = LOCK_TYPE_SEMAPHORE` 的 ACQUIRE，或 v1 会话发送 LATCH_AWAIT 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝该请求且不断开连接，该会话的既有版本请求照常服务

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

## ADDED Requirements

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
