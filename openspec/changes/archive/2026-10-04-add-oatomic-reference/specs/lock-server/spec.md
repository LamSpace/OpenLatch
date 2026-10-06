## MODIFIED Requirements

### Requirement: 服务启动与配置加载

服务器 MUST 从 `-Dopenlatch.config=<path>` 指定的 Java Properties 文件加载配置；未指定时 MUST 使用内置默认值启动。配置项 MUST 覆盖：监听端口（默认 9410，允许 0 表示由操作系统分配临时端口）、Worker 线程数（默认 2×CPU）、空闲断连时限、默认租约与租约钳制区间、扫描周期、队首响应时限、key 长度上限、单 key 队列深度上限、单连接未完成请求上限、有值引用载荷字节上限 `maxValueBytes`（`openlatch.server.limit.max-value-bytes`，默认 4096，MUST ≥1 且 MUST ≤512KiB——为 1MiB 帧上限留信封编解码安全边际）。非法配置值 MUST 在启动时快速失败并给出明确错误信息。启动成功后 MUST 监听配置端口（port=0 时监听实际分配端口），并在启动日志中打印端口、协议版本与关键限额配置（含 `maxValueBytes`）。

#### Scenario: 默认配置启动

- **WHEN** 未提供配置文件直接启动服务器
- **THEN** 服务器以内置默认值启动并监听 9410 端口，启动日志包含端口、协议版本与关键限额（含载荷上限 4096）

#### Scenario: 指定配置文件启动

- **WHEN** 通过 `-Dopenlatch.config=<path>` 提供包含自定义端口的配置文件
- **THEN** 服务器以配置值启动并监听自定义端口

#### Scenario: 端口 0 配置启动

- **WHEN** 通过配置文件将监听端口设为 0
- **THEN** 校验通过，服务器监听操作系统分配的临时端口，启动日志打印实际端口号

#### Scenario: 载荷上限越界配置快速失败

- **WHEN** 配置 `openlatch.server.limit.max-value-bytes` 为 0、负数或超过 512KiB
- **THEN** 启动失败并给出明确错误信息，不进入半启动状态

#### Scenario: 端口被占用启动失败

- **WHEN** 配置端口已被其他进程占用
- **THEN** 服务器启动失败，进程退出并给出明确错误信息，不进入半启动状态

## ADDED Requirements

### Requirement: v6 有值引用门控与载荷入口钳制

服务器 SHALL 对有值引用形态实施 v6 门控：握版本 <6 的会话发送 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的 `ATOMIC_OP` MUST 以 `INVALID_REQUEST` 消息级拒绝且不断连（判例：LATCH 对 v3 门、ATOMIC 对 v4 门、BARRIER 对 v5 门），该会话既有版本能力（含标量原子形态）照常服务。载荷尺寸钳制 SHALL 由接入层专属执行：单机分发与集群 Leader 提交路径在处理 `ATOMIC_OP`（有值引用形态）时，MUST 于任何状态变更前（集群路径为 `gateway.submit` 之前、单机路径为引擎调用之前）判定请求携带的 `operand_bytes`/`expected_bytes`/`initial_bytes` 字节长度是否超过本节点 `maxValueBytes`，超限 MUST 以 `INVALID_REQUEST` 消息级拒绝且 MUST NOT 产生日志条目/引擎调用（超限命令永不入复制面）。核心结果到协议状态码的映射 SHALL 沿既有原子通道口径延伸：`REJECT_TYPE_MISMATCH`（含同 key 跨形态）、`REJECT_ATOMIC_INIT`、`REJECT_ATOMIC_RANGE`（含引用形态携带 ADD）映射 `INVALID_REQUEST`；会话不存在映射 `SESSION_EXPIRED`；CAS 家族成败由 `applied` 承载（`OK + applied=false`，不占错误码）。形态-字段互斥违例（引用形态携非零标量槽位、标量形态携 bytes 字段、引用形态携 ADD）属消息合法性违例，MUST 在入口以 `INVALID_REQUEST` 拒绝且不断连。状态机与条目侧 MUST NOT 复核载荷尺寸（见 replicated-state-machine 相应条款）。Follower 侧非本车道消息与转发口径沿用既有 ATOMIC 路径，MUST NOT 新建通道。

#### Scenario: v5 会话发引用形态被拒不不断连

- **WHEN** 握版本 5 的会话发送 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 的 `ATOMIC_OP`
- **THEN** 收到 `INVALID_REQUEST` 应答且连接保持，该会话的锁/Semaphore/Latch/屏障/标量原子操作全部照常可用

#### Scenario: 超限载荷拒绝且零日志产生

- **WHEN** v6 会话在集群 Leader 上提交携带 5KB 载荷的引用形态 `SET`（`maxValueBytes=4096`）
- **THEN** 收到 `INVALID_REQUEST` 应答，复制日志位点无任何推进（可执行断言：提交前后日志条目数不变），条目状态零扰动

#### Scenario: 恰限载荷放行

- **WHEN** 同会话提交携带恰为 4096 字节载荷的 `SET`
- **THEN** 请求经钳制判定放行、正常提交与落值

#### Scenario: 钳制下调存量照常可读

- **WHEN** `maxValueBytes` 由 4096 下调为 1024 后，v6 会话 `GET` 既有 4KB 值，随后尝试写入新的 2KB 值
- **THEN** 读取照常返回存量值不截断；新写入按新限拒绝（`INVALID_REQUEST`），条目未被变更

#### Scenario: 形态字段互斥违例入口拒绝

- **WHEN** 引用形态请求携带非零 `operand` 标量字段，或 `long` 形态请求携带 `operand_bytes`
- **THEN** 以 `INVALID_REQUEST` 消息级拒绝且不断连、不入日志
