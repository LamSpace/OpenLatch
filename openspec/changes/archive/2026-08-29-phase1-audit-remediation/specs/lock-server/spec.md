## MODIFIED Requirements

### Requirement: 消息合法性校验

信封的消息类型与 payload 不匹配（如 `LOCK_ACQUIRE` 携带释放 payload）或缺失 payload 时，服务器 MUST 回以 `INVALID_REQUEST` 且 MUST NOT 断连。`type` 为协议未定义数值（枚举越界）时，服务器 MUST 回以 `INVALID_REQUEST`（响应无法回显未知类型时 MUST 以 `MESSAGE_TYPE_UNKNOWN` 占位并照常回显 `request_id`）且 MUST NOT 断连、MUST NOT 因回包构造失败而使请求静默悬挂。无法解码的帧（协议解析失败）MUST 记录日志并断连。

#### Scenario: 类型与 payload 不匹配

- **WHEN** 客户端发送 `LOCK_ACQUIRE` 类型但携带 `ReleaseRequest` payload 的信封
- **THEN** 服务器回以 `INVALID_REQUEST`，连接保持，后续合法请求仍被正常处理

#### Scenario: 未知消息类型数值

- **WHEN** 客户端发送 `type` 为协议未定义数值（如 99）且携带任意合法 payload 的信封
- **THEN** 服务器回以 `INVALID_REQUEST` 并回显 `request_id`，连接保持；该连接后续请求仍被正常处理

#### Scenario: 不可解码帧断连

- **WHEN** 连接上收到无法按协议解析的字节序列
- **THEN** 服务器记录日志并断开该连接

### Requirement: 服务启动与配置加载

服务器 MUST 从 `-Dopenlatch.config=<path>` 指定的 Java Properties 文件加载配置；未指定时 MUST 使用内置默认值启动。配置项 MUST 覆盖：监听端口（默认 9410，允许 0 表示由操作系统分配临时端口）、Worker 线程数（默认 2×CPU）、空闲断连时限、默认租约与租约钳制区间、扫描周期、队首响应时限、key 长度上限、单 key 队列深度上限、单连接未完成请求上限。非法配置值 MUST 在启动时快速失败并给出明确错误信息。启动成功后 MUST 监听配置端口（port=0 时监听实际分配端口），并在启动日志中打印端口、协议版本与关键限额配置。

#### Scenario: 默认配置启动

- **WHEN** 未提供配置文件直接启动服务器
- **THEN** 服务器以内置默认值启动并监听 9410 端口，启动日志包含端口、协议版本与关键限额

#### Scenario: 指定配置文件启动

- **WHEN** 通过 `-Dopenlatch.config=<path>` 提供包含自定义端口的配置文件
- **THEN** 服务器以配置值启动并监听自定义端口

#### Scenario: 端口 0 配置启动

- **WHEN** 通过配置文件将监听端口设为 0
- **THEN** 校验通过，服务器监听操作系统分配的临时端口，启动日志打印实际端口号

#### Scenario: 端口被占用启动失败

- **WHEN** 配置端口已被其他进程占用
- **THEN** 服务器启动失败，进程退出并给出明确错误信息，不进入半启动状态
