# Delta Spec: wire-protocol

## Purpose

定义 OpenLatch 客户端与服务器之间的线路协议：基于 Protobuf 的消息模式、分帧格式、请求关联规则与版本约定，为 server / client / starter 各模块提供统一的交互契约。

## ADDED Requirements

### Requirement: 协议消息模式

协议 SHALL 以单一 `Envelope` 消息封装所有交互，`type` 字段取值于 `MessageType` 枚举（HELLO、LOCK_ACQUIRE、LOCK_RELEASE、LEASE_RENEW、PING、AWAIT_NOTIFY），`payload` 为 oneof 承载对应的请求/响应消息。全部枚举（`MessageType`、`LockType`、`StatusCode`）与消息的字段编号、字段语义 MUST 与设计说明书 §3.2 逐项一致。

#### Scenario: 消息定义完备

- **WHEN** 构建协议模块并生成代码
- **THEN** 存在全部 6 个消息类型、4 种锁类型、12 个状态码与 9 个 payload 消息，且字段编号与设计说明书 §3.2 的定义一致

#### Scenario: 请求与响应消息配对

- **WHEN** 检查 Hello/Acquire/Release/LeaseRenew 四类交互
- **THEN** 每类交互均有成对的 Request 与 Response 消息，字段与设计说明书 §3.2.1–§3.2.3 一致

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

Phase 1 的 `protocol_version` MUST 固定为 1。服务端在收到 `client_protocol_version != 1` 的握手时 MUST 回 `INVALID_REQUEST` 并断开连接，不做隐式兼容。

#### Scenario: 版本一致握手成功

- **WHEN** 客户端以 `client_protocol_version = 1` 发起握手
- **THEN** 服务端接受握手并在响应中回 `server_protocol_version = 1`
