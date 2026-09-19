## MODIFIED Requirements

### Requirement: 会话握手

连接建立后的第一条消息 MUST 是 HELLO。握手成功前到达的任何业务请求 MUST 被回以 `INVALID_REQUEST`（不断连）。HELLO 的 `auth_token` 判定按业务令牌认证开关门控（语义见 transport-security"业务令牌认证与默认兼容守卫"）：认证关闭（默认）时维持 Phase 1 兼容守卫——HELLO 携带非空 `auth_token` MUST 回以 `INVALID_REQUEST` 并断连；认证开启时——HELLO 的 `auth_token` MUST 命中服务端配置的业务令牌之一，否则（缺失/为空/不符/未配置）统一回以 `INVALID_REQUEST` 并断连，不泄露原因。HELLO 的协议版本号不在支持区间 [1,5] 时 MUST 回以 `INVALID_REQUEST` 并断连（不做隐式兼容）。认证与版本判定 MUST 发生在会话分配、注册表登记或集群 `SESSION_OPEN` 复制之前（单机与集群同一门闩），被拒 HELLO 不得产生可观察状态副作用。握手成功时服务器 MUST 分配在连接生命周期内唯一的 `session_id`，并回以包含 `session_id`、服务端协议版本与默认租约时长的响应。同一连接上的重复 HELLO MUST 被回以 `INVALID_REQUEST`。

#### Scenario: 握手前业务请求被拒

- **WHEN** 连接建立后未发 HELLO 即发送获取锁请求
- **THEN** 服务器回以 `INVALID_REQUEST`，连接保持但请求不被处理

#### Scenario: 协议版本越界断连

- **WHEN** HELLO 携带不在支持区间 [1,5] 内的协议版本号
- **THEN** 服务器回以 `INVALID_REQUEST` 并断开连接

#### Scenario: 认证关闭时非空令牌被拒（兼容守卫）

- **WHEN** 业务认证关闭（默认），HELLO 携带非空 `auth_token`
- **THEN** 服务器回以 `INVALID_REQUEST` 并断开连接，行为与 Phase 1 规则一致

#### Scenario: 认证开启时令牌不符断连

- **WHEN** 业务认证开启，HELLO 携带缺失/为空/错误的 `auth_token`
- **THEN** 服务器回以 `INVALID_REQUEST` 并断开连接，应答不区分失败原因

#### Scenario: 正常握手建立会话

- **WHEN** 客户端发送合法 HELLO（认证开启时 `auth_token` 命中配置令牌之一）
- **THEN** 服务器回以成功响应，包含新分配的 `session_id` 与默认租约时长

#### Scenario: 重复 HELLO 被拒

- **WHEN** 客户端在握手成功后再次发送 HELLO
- **THEN** 服务器回以 `INVALID_REQUEST`，原会话不受影响

## ADDED Requirements

### Requirement: v5 屏障门控与结果映射

服务器 SHALL 对 `BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` 三类消息实施 v5 门控：握版本 <5 的会话发送任一 BARRIER 消息 MUST 以 `INVALID_REQUEST` 消息级拒绝且不断连（判例：LATCH 消息对 v3 门、ATOMIC 消息对 v4 门），该会话既有版本能力照常服务。核心结果到协议状态码的映射 MUST 遵循：parties 断言不成立（`REJECT_BARRIER_PARTIES`）与家族/形态不匹配（`REJECT_TYPE_MISMATCH`）映射 `INVALID_REQUEST`；会话不存在映射 `SESSION_EXPIRED`；等待队列深度超限沿用 LATCH await 对 `REJECT_QUEUE_FULL` 的现行映射（不新增映射）；等待项所属世代已破障映射 `BARRIER_BROKEN`；动作了结的非指定执行者回报、或携带未知世代号映射 `INVALID_REQUEST`。屏障操作 MUST NOT 进入 ACQUIRE/RELEASE/LEASE_RENEW 通道，`AWAIT_NOTIFY` 推送通道复用不新建。

#### Scenario: v4 会话发屏障消息被拒不不断连

- **WHEN** 握版本 4 的会话发送 `BARRIER_AWAIT`
- **THEN** 收到 `INVALID_REQUEST` 应答且连接保持，该会话的锁/Semaphore/Latch/原子操作全部照常可用

#### Scenario: ACQUIRE 携带屏障定型值被拒

- **WHEN** 会话发送 `lock_type = LOCK_TYPE_BARRIER` 的 ACQUIRE 请求
- **THEN** 以 `INVALID_REQUEST` 消息级拒绝且不断连，不产生日志条目（与三原子类型与 `LOCK_TYPE_LATCH` 同规则）

#### Scenario: 破障等待项重发得在带裁决

- **WHEN** 等待者以原 `request_id` 重发 `BARRIER_AWAIT`，服务端判定其世代已破障
- **THEN** 应答 `status = BARRIER_BROKEN`，连接与会话不受影响
