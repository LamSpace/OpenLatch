# wire-protocol delta — s3-leader-discovery-failover

## MODIFIED Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY、CLUSTER_VIEW），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2/§6.2 及本变更的增量定义逐项一致。v2 新增 MUST 仅以新增枚举值/字段/消息的形式表达：Phase 1 已发布的字段编号与字段语义 MUST NOT 变更、复用或删除。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 7 个消息类型、4 种锁类型、12 个状态码与 10 个 payload 消息（Phase 1 的 9 个加 `ClusterView`），且字段编号与设计说明书的定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew 四类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与设计说明书 §3.2.1–§3.2.3 一致

#### Scenario: v1 基线冻结

- **WHEN** 对本变更生成的 `openlatch.proto` 与 Phase 1 基线逐项比对字段编号与枚举值
- **THEN** Phase 1 全部已发布编号零变更，新增项不占用既有编号

### Requirement: 协议版本

`protocol_version` 当前版本为 **2**。服务端 SHALL 同时接受 `client_protocol_version ∈ {1, 2}` 的握手；收到 `client_protocol_version` 既非 1 也非 2 的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。握手响应与后续应答的协议版本 MUST 等于客户端握手的请求版本（v1 客户端得到逐字段与 Phase 1 一致的响应，新增字段对其表现为可忽略的未知字段）；`server_protocol_version` 回显服务端自身版本 2。

#### Scenario: v2 握手成功

- **WHEN** 客户端以 `client_protocol_version = 2` 发起握手
- **THEN** 服务端接受握手，响应 `Envelope.protocol_version = 2` 且 `server_protocol_version = 2`

#### Scenario: v1 握手回归不变

- **WHEN** v1 客户端以 `client_protocol_version = 1` 发起握手并继续业务请求
- **THEN** 服务端接受并保持 Phase 1 全部握手与业务响应行为，v1 客户端无需理解任何新增字段即可工作

#### Scenario: 未知版本拒绝

- **WHEN** 客户端以 `client_protocol_version = 3`（或其他未知值）发起握手
- **THEN** 服务端回 `INVALID_REQUEST` 并断开连接

## ADDED Requirements

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
