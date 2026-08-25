# lock-server Specification

## Purpose

提供 OpenLatch 单节点锁服务器：将锁语义核心经线路协议暴露为 TCP 长连接服务，负责会话握手、请求分发与错误码映射、队首通知推送、租约到期扫描驱动、断连与空闲会话清理、自我保护限额，以及可执行交付形态。

## Requirements

### Requirement: 服务启动与配置加载

服务器 MUST 从 `-Dopenlatch.config=<path>` 指定的 Java Properties 文件加载配置；未指定时 MUST 使用内置默认值启动。配置项 MUST 覆盖：监听端口（默认 9410）、Worker 线程数（默认 2×CPU）、空闲断连时限、默认租约与租约钳制区间、扫描周期、队首响应时限、key 长度上限、单 key 队列深度上限、单连接未完成请求上限。启动成功后 MUST 监听配置端口，并在启动日志中打印端口、协议版本与关键限额配置。

#### Scenario: 默认配置启动

- **WHEN** 未提供配置文件直接启动服务器
- **THEN** 服务器以内置默认值启动并监听 9410 端口，启动日志包含端口、协议版本与关键限额

#### Scenario: 指定配置文件启动

- **WHEN** 通过 `-Dopenlatch.config=<path>` 提供包含自定义端口的配置文件
- **THEN** 服务器以配置值启动并监听自定义端口

#### Scenario: 端口被占用启动失败

- **WHEN** 配置端口已被其他进程占用
- **THEN** 服务器启动失败，进程退出并给出明确错误信息，不进入半启动状态

### Requirement: 关停序列

收到关停信号（JVM 退出钩子）时，服务器 MUST 依次执行：停止租约扫描调度 → 关闭全部连接 → 等待资源终止回收。关停过程中 MUST 不再产生新的队首通知推送。

#### Scenario: 关停顺序无通知竞态

- **WHEN** 服务器在持有连接与挂起等待者的状态下收到关停信号
- **THEN** 扫描调度先于连接关闭停止，关停全程无新的通知推送写出，进程在有限时间内退出

### Requirement: 会话握手

连接建立后的第一条消息 MUST 是 HELLO。握手成功前到达的任何业务请求 MUST 被回以 `INVALID_REQUEST`（不断连）。HELLO 的协议版本号不为 1 或 `auth_token` 非空时，MUST 回以 `INVALID_REQUEST` 并断连。握手成功时服务器 MUST 分配在连接生命周期内唯一的 `session_id`，并回以包含 `session_id`、服务端协议版本与默认租约时长的响应。同一连接上的重复 HELLO MUST 被回以 `INVALID_REQUEST`。

#### Scenario: 握手前业务请求被拒

- **WHEN** 连接建立后未发 HELLO 即发送获取锁请求
- **THEN** 服务器回以 `INVALID_REQUEST`，连接保持但请求不被处理

#### Scenario: 协议版本不匹配断连

- **WHEN** HELLO 携带不等于 1 的协议版本号
- **THEN** 服务器回以 `INVALID_REQUEST` 并断开连接

#### Scenario: 非空 auth_token 被拒

- **WHEN** HELLO 携带非空 `auth_token`（Phase 1 要求为空）
- **THEN** 服务器回以 `INVALID_REQUEST` 并断开连接

#### Scenario: 正常握手建立会话

- **WHEN** 客户端发送合法 HELLO
- **THEN** 服务器回以成功响应，包含新分配的 `session_id` 与默认租约时长

#### Scenario: 重复 HELLO 被拒

- **WHEN** 客户端在握手成功后再次发送 HELLO
- **THEN** 服务器回以 `INVALID_REQUEST`，原会话不受影响

### Requirement: 请求分发与错误码映射

对已握手连接，服务器 MUST 处理 `LOCK_ACQUIRE`/`LOCK_RELEASE`/`LEASE_RENEW` 请求：交由锁语义引擎裁决，并将裁决结果映射为协议状态码——授予/排队/立即拒绝/`INVALID_TOKEN`/`NOT_HELD`/`SESSION_EXPIRED`/`OVERLOADED`/`KEY_EMPTY`/`KEY_TOO_LONG` 一一对应，不得吞错或自造状态。响应 MUST 回显请求的 `request_id`。授予响应 MUST 携带租约凭证与实际生效租约；排队响应 MUST 携带队列位次。`PING` MUST 不回任何响应。

#### Scenario: 授予响应携带凭证

- **WHEN** 客户端以合法参数获取一个空闲锁键
- **THEN** 响应状态为 `OK`，携带租约凭证、实际生效租约时长，且 `request_id` 与请求一致

#### Scenario: 排队响应携带位次

- **WHEN** 锁键已被其他会话持有，客户端以可排队方式请求
- **THEN** 响应状态为 `QUEUED`，携带队列位次，且 `request_id` 与请求一致

#### Scenario: 立即式获取被拒

- **WHEN** 锁键已被占用，客户端以 `wait_ms = 0` 请求
- **THEN** 响应状态为 `DENIED`，不进入等待队列

#### Scenario: 凭证不匹配的释放被拒

- **WHEN** 客户端以错误租约凭证释放锁
- **THEN** 响应状态为 `INVALID_TOKEN`，锁的持有状态不受影响

### Requirement: 消息合法性校验

信封的消息类型与 payload 不匹配（如 `LOCK_ACQUIRE` 携带释放 payload）或缺失 payload 时，服务器 MUST 回以 `INVALID_REQUEST` 且 MUST NOT 断连。无法解码的帧（协议解析失败）MUST 记录日志并断连。

#### Scenario: 类型与 payload 不匹配

- **WHEN** 客户端发送 `LOCK_ACQUIRE` 类型但携带 `ReleaseRequest` payload 的信封
- **THEN** 服务器回以 `INVALID_REQUEST`，连接保持，后续合法请求仍被正常处理

#### Scenario: 不可解码帧断连

- **WHEN** 连接上收到无法按协议解析的字节序列
- **THEN** 服务器记录日志并断开该连接

### Requirement: 队首通知推送

锁语义引擎发出队首通知事件时，服务器 MUST 向该队首等待者所属连接推送 `AWAIT_NOTIFY`；推送信封的 `request_id` MUST 为 0，`request_id_ref` MUST 指向原获取请求的 `request_id`。若目标连接已不存在，推送 MUST 被静默丢弃，不影响其他连接与服务稳定性（队列位置由引擎的队首响应超时机制兜底回收）。

#### Scenario: 释放触发队首通知

- **WHEN** 持有者完全释放锁，且队列中有等待者
- **THEN** 队首等待者的连接收到 `AWAIT_NOTIFY`，其 `request_id_ref` 等于该等待者原获取请求的 `request_id`

#### Scenario: 通知时连接已断开

- **WHEN** 引擎对某会话发出队首通知事件，但该会话的连接已不存在
- **THEN** 推送被静默丢弃，服务无异常，其余连接不受影响

### Requirement: 租约到期扫描驱动

服务器 MUST 以固定周期（默认 500ms）驱动锁语义引擎的租约到期扫描与队首响应超时清扫，二者由单一调度线程串行执行。到期未续租的锁 MUST 被自动释放，释放 MUST 触发对新队首的通知。

#### Scenario: 未续租锁到期释放

- **WHEN** 客户端获取锁后既不续租也不释放，时间超过锁的租约期
- **THEN** 锁在一个扫描周期内被自动释放，队列中的下一等待者收到通知并可获取该锁

### Requirement: 断连会话清理与空闲检测

连接断开（含客户端主动断开、空闲断连、网络故障检测）时，服务器 MUST 立即清理该会话的全部持锁与等待项：持有的锁被强制释放并通知新队首，等待项被摘除。连接在空闲时限（默认 60 秒）内无任何读入时，服务器 MUST 主动断开该连接并执行同一清理路径。

#### Scenario: 持锁断连即时释放

- **WHEN** 持有锁的客户端连接断开（未发送释放）
- **THEN** 锁立即被释放，其他客户端可即刻获取，无需等待租约到期

#### Scenario: 等待中断连摘除

- **WHEN** 排队等待中的客户端连接断开
- **THEN** 其等待项被摘除，后续授予顺序不包含该等待者

#### Scenario: 空闲连接被断开

- **WHEN** 一条连接在空闲时限内无任何读入（且无 PING）
- **THEN** 服务器主动断开该连接并清理其会话

### Requirement: 帧长限制与自我保护限额

单个帧的载荷超过 1 MiB 时，服务器 MUST 断开该连接。单连接未完成请求数超过限额（默认 1024）时，超限请求 MUST 被回以 `OVERLOADED`。

#### Scenario: 超帧长断连

- **WHEN** 连接上收到载荷超过 1 MiB 的帧
- **THEN** 服务器断开该连接

#### Scenario: 未完成请求超限

- **WHEN** 单连接的未完成请求数达到限额后仍有新请求到达
- **THEN** 超限请求被回以 `OVERLOADED`

### Requirement: 可执行交付形态

构建产物 MUST 包含可经 `java -jar` 直接启动的可执行 jar，主类为服务器入口。进程启动即监听服务端口，并注册 JVM 退出钩子执行关停序列。

#### Scenario: 独立启动与冒烟

- **WHEN** 以 `java -jar` 启动可执行 jar，随后依次执行 HELLO、获取、续租、释放的完整请求序列
- **THEN** 各响应状态正确（授予携带凭证、续租成功、完全释放），服务全程无异常
