# wire-protocol Specification

## Purpose
定义 OpenLatch 客户端与服务器之间的线路协议：基于 Protobuf 的消息模式、分帧格式、请求关联规则与版本约定，为 server / client / starter 各模块提供统一的交互契约。
## Requirements
### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW、LATCH_COUNT_DOWN、LATCH_AWAIT），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及各阶段变更的增量定义逐项一致。v2/v3 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 9 个消息类型、7 种锁类型、12 个状态码与 14 个 payload 消息（Phase 1 的 9 个加 `ClusterView` 与 Latch 四个），且字段编号与增量定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew/LatchCountDown/LatchAwait 六类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与定义一致

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号

### Requirement: 分帧格式

线路帧 SHALL 为 4 字节大端长度前缀（不含自身）加一个序列化的 `Envelope`。单帧最大长度 MUST 为 1 MiB，超限的连接 MUST 被断开。

#### Scenario: 长度前缀编码

- **WHEN** 发送方序列化一个 `Envelope` 并写上线路
- **THEN** 帧头为该 Envelope 序列化字节数的 4 字节大端表示，其后紧跟序列化字节

### Requirement: 请求标识与关联

`request_id` MUST 在单条连接内唯一；响应消息 MUST 回显请求的 `request_id`。服务端推送（`AWAIT_NOTIFY`）的 `Envelope.request_id` MUST 为 0，并通过 `request_id_ref` 指向原获取请求的 `request_id`。

#### Scenario: 响应关联

- **WHEN** 客户端以 `request_id = r` 发送任意业务请求
- **THEN** 对应响应的 `Envelope.request_id` 等于 `r`

#### Scenario: 推送关联

- **WHEN** 服务端就某个挂起的获取请求（`request_id = r`）发出可重试通知
- **THEN** 推送的 `Envelope.request_id` 为 0 且 `AwaitNotify.request_id_ref` 等于 `r`

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

`protocol_version` 当前版本为 **3**。服务端 SHALL 同时接受 `client_protocol_version ∈ {1, 2, 3}` 的握手；收到 `client_protocol_version` 不在该集合内的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。握手响应与后续应答的协议版本 MUST 等于客户端握手的请求版本（v1 客户端得到逐字段与 Phase 1 一致的响应，新增字段对其表现为可忽略的未知字段）；`server_protocol_version` 回显服务端自身版本 3。v3 专属语义（`LOCK_TYPE_FAIR`/`LOCK_TYPE_SEMAPHORE`/`LOCK_TYPE_LATCH` 的 ACQUIRE、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息）MUST 仅对握手中声明 v3 的会话开放：v1/v2 会话发送上述请求 MUST 以 `INVALID_REQUEST` 消息级拒绝（MUST NOT 断开连接），其既有 v1/v2 行为逐项不变。

#### Scenario: v3 握手成功

- **WHEN** 客户端以 `client_protocol_version = 3` 发起握手
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 3` 且 `server_protocol_version = 3`

#### Scenario: v2 握手回归不变

- **WHEN** 客户端以 `client_protocol_version = 2` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 Phase 2 全部握手与业务响应行为不变

#### Scenario: v1 握手回归不变

- **WHEN** v1 客户端以 `client_protocol_version = 1` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 Phase 1 全部握手与业务响应行为，v1 客户端无需理解任何新增字段即可工作

#### Scenario: 未知版本拒绝

- **WHEN** 客户端以 `client_protocol_version = 4`（或其他未知值）发起握手
- **THEN** 服务端回 `INVALID_REQUEST` 并断开连接

#### Scenario: 低版本客户端使用新类型被拒

- **WHEN** v2 会话发送 `lock_type = LOCK_TYPE_SEMAPHORE` 的 ACQUIRE，或 v1 会话发送 LATCH_AWAIT 消息
- **THEN** 服务端以 `INVALID_REQUEST` 拒绝该请求且不断开连接，该会话的既有锁类型请求照常服务

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
