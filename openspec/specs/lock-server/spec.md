# lock-server Specification

## Purpose

提供 OpenLatch 单节点锁服务器：将锁语义核心经线路协议暴露为 TCP 长连接服务，负责会话握手、请求分发与错误码映射、队首通知推送、租约到期扫描驱动、断连与空闲会话清理、自我保护限额，以及可执行交付形态。
## Requirements
### Requirement: 服务启动与配置加载

服务器 MUST 从 `-Dopenlatch.config=<path>` 指定的 Java Properties 文件加载配置；未指定时 MUST 使用内置默认值启动。配置项 MUST 覆盖：监听端口（默认 9410，允许 0 表示由操作系统分配临时端口）、Worker 线程数（默认 2×CPU）、空闲断连时限、默认租约与租约钳制区间、扫描周期、队首响应时限、key 长度上限、单 key 队列深度上限、单连接未完成请求上限、有值引用载荷字节上限 `maxValueBytes`（`openlatch.server.limit.max-value-bytes`，默认 4096，MUST ≥1 且 MUST ≤512KiB——为 1MiB 帧上限留信封编解码安全边际）、队列容量上限 `maxQueueCapacity`（`openlatch.server.limit.max-queue-capacity`，默认 1024，MUST ∈[1, 65536]——定型断言的入口钳制上限，配合每元素 `maxValueBytes` 构成单 key 驻留上界）、drain 应答字节预算 `maxDrainBytes`（`openlatch.server.limit.max-drain-bytes`，默认 262144（256KiB），MUST ∈[1, 512KiB]——`drainTo` 提取上限的派生预算）、队列就绪扫描周期 `queueReadyTickMs`（`openlatch.server.queue.ready-tick-ms`，默认 200，MUST ≥10——延时形态的唤醒精度）。非法配置值 MUST 在启动时快速失败并给出明确错误信息。启动成功后 MUST 监听配置端口（port=0 时监听实际分配端口），并在启动日志中打印端口、协议版本与关键限额配置（含 `maxValueBytes`、`maxQueueCapacity`、`maxDrainBytes`）。

#### Scenario: 默认配置启动

- **WHEN** 未提供配置文件直接启动服务器
- **THEN** 服务器以内置默认值启动并监听 9410 端口，启动日志包含端口、协议版本与关键限额（含载荷上限 4096、队列容量上限 1024、drain 预算 256KiB）

#### Scenario: 指定配置文件启动

- **WHEN** 通过 `-Dopenlatch.config=<path>` 提供包含自定义端口的配置文件
- **THEN** 服务器以配置值启动并监听自定义端口

#### Scenario: 端口 0 配置启动

- **WHEN** 通过配置文件将监听端口设为 0
- **THEN** 校验通过，服务器监听操作系统分配的临时端口，启动日志打印实际端口号

#### Scenario: 载荷上限越界配置快速失败

- **WHEN** 配置 `openlatch.server.limit.max-value-bytes` 为 0、负数或超过 512KiB
- **THEN** 启动失败并给出明确错误信息，不进入半启动状态

#### Scenario: 队列限额越界配置快速失败

- **WHEN** 配置 `max-queue-capacity` 为 0 或超 65536、`max-drain-bytes` 为 0 或超 512KiB、`ready-tick-ms` 低于 10
- **THEN** 启动失败并给出明确错误信息，不进入半启动状态

#### Scenario: 端口被占用启动失败

- **WHEN** 配置端口已被其他进程占用
- **THEN** 服务器启动失败，进程退出并给出明确错误信息，不进入半启动状态

### Requirement: 关停序列

收到关停信号（JVM 退出钩子）时，服务器 MUST 依次执行：停止租约扫描调度 → 关闭全部连接 → 等待资源终止回收。关停过程中 MUST 不再产生新的队首通知推送。

#### Scenario: 关停顺序无通知竞态

- **WHEN** 服务器在持有连接与挂起等待者的状态下收到关停信号
- **THEN** 扫描调度先于连接关闭停止，关停全程无新的通知推送写出，进程在有限时间内退出

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

### Requirement: v7 队列门控与入口钳制

服务器 SHALL 对队列操作实施 v7 门控：握版本 <7 的会话发送 `QUEUE_OP` 消息 MUST 以 `INVALID_REQUEST` 消息级拒绝且不断连（判例 LATCH 对 v3 门、ATOMIC 对 v4 门、BARRIER 对 v5 门、有值引用对 v6 门），该会话既有版本能力照常服务。入口钳制 SHALL 由接入层专属执行（单机分发与集群 Leader 提交路径同型，集群路径 MUST 于 `gateway.submit` 之前、单机路径 MUST 于引擎调用之前完成全部判定）：`PUT` 携带的 `element_bytes` 长度超过 `maxValueBytes` MUST 以 `INVALID_REQUEST` 拒绝且 MUST NOT 产生日志条目/引擎调用；`PUT` 的容量主张超过本节点 `maxQueueCapacity` MUST 同样拒绝且零日志；`DRAIN` 的提取上限 MUST 钳定为 `min(请求值(0=缺省上限), maxQueueCapacity, floor(maxDrainBytes / maxValueBytes))` 且**钳定后的 N 随请求入日志**（全副本应用一致，apply 不读配置）；形状互斥矩阵（非 PUT 携元素、非 DELAY_QUEUE 形态携 `delay_ms`、非 DRAIN 携 `max_elements`、读携 `op_seq`、ACQUIRE/其余消息携 `lock_type=12/13`）属消息合法性违例，MUST 在入口以 `INVALID_REQUEST` 拒绝且不断连。核心结果到协议状态码的映射 SHALL 沿既有通道口径延伸：`REJECT_QUEUE_CAPACITY`/`REJECT_TYPE_MISMATCH` 映射 `INVALID_REQUEST`；`REJECT_SESSION` 映射 `SESSION_EXPIRED`；`DENIED`（元素不可满足的立即式）与 `QUEUED`（挂起）与 `REJECT_QUEUE_FULL`（等待挂起深度超限，映射 `OVERLOADED`——沿用锁/Latch 等待深度护栏既有映射判例）为在带裁决、非请求错误。状态机与条目侧 MUST NOT 复核载荷尺寸/容量/批量预算（见 replicated-state-machine 相应条款）。Follower 侧队列消息转发沿用既有车道，MUST NOT 新建通道。

#### Scenario: v6 会话发队列操作被拒不不断连

- **WHEN** 握版本 6 的会话发送 `QUEUE_OP` PUT
- **THEN** 收到 `INVALID_REQUEST` 应答且连接保持，该会话的锁/Latch/屏障/原子（含引用形态）操作全部照常可用

#### Scenario: 超限元素拒绝且零日志产生

- **WHEN** v7 会话在集群 Leader 上提交携带 5KB 元素的 PUT（`maxValueBytes=4096`），或携带容量主张 2048 的 PUT（`maxQueueCapacity=1024`）
- **THEN** 均收到 `INVALID_REQUEST` 应答，复制日志位点无任何推进（可执行断言：提交前后日志条目数不变），条目状态零扰动

#### Scenario: 恰限元素与恰限容量放行

- **WHEN** 同会话提交携带恰为 4096 字节元素、容量主张恰为 1024 的 PUT
- **THEN** 请求经钳制判定放行、正常提交与入队

#### Scenario: drain 上限钳定入日志

- **WHEN** v7 会话提交 `DRAIN(max_elements=1000)`（默认配置：预算 256KiB / 元素上限 4KB → 派生上限 64）
- **THEN** Leader 在提交前将提取上限钳定为 64 并随条目入日志；任何副本回放至多摘取 64 项（apply 不读本地配置）

#### Scenario: 形状互斥违例入口拒绝

- **WHEN** TAKE 请求携带 `element_bytes`，或 `QUEUE` 形态请求携带非零 `delay_ms`，或 PEEK 携带非零 `op_seq`
- **THEN** 以 `INVALID_REQUEST` 消息级拒绝且不断连、不入日志
### Requirement: 队列双角色唤醒与就绪驱动

Leader 侧 `WaitQueue` SHALL 以双轨承载队列挂起：等待项角色由原请求 op 导出（put-waiter=等容量，take-waiter=等元素），队首推进 MUST 分轨——PUT 提交使元素入队仅唤醒该 key take 轨队首（DELAY 形态且队首可消费时）、TAKE/DRAIN 提交使容量释放仅唤醒 put 轨队首；既有锁/Semaphore/Latch/Barrier 的单轨行为 MUST NOT 受该改造扰动。等待深度护栏 `maxQueueDepthPerKey` 双轨共用（同 key 合计）。应用点 `DENIED` 回弹的阻塞式等待者 MUST 原位重新挂回对应轨道（位次保持、不向客户端推错误），等待其下一次唤醒。**队列就绪驱动** SHALL 以 Leader-only 周期扫描承载延时形态唤醒：每 `queueReadyTickMs` 遍历存在 take-waiter 的队列 key（由等待队列反查，非全表扫描条目），对 ShadowTable 队列镜像中"队首元素到期时刻不晚于当前时刻"的 key 推送 `AWAIT_NOTIFY`（`request_id_ref` 指向原 TAKE）——驱动 MUST NOT 提交任何日志条目、MUST NOT 改变复制状态（到期为可见性判定，消费仍经 TAKE 提交路径在应用点终判）；已通知标记防重复推送，通知丢失由既有队首响应超时清扫兜底推进；Leader 当选 MUST 立即首扫（换主窗口漏扫补偿，判例到期驱动）；FOLLOWER MUST NOT 运行该驱动。单机部署形态 SHALL 以内嵌调度同型驱动扫描（引擎时钟可手工推进为测试道），唤醒语义与集群一致。

#### Scenario: 入队唤醒取者、出队唤醒放者

- **WHEN** 满队列上挂有 put-waiter、空队列上挂有 take-waiter，随后一个 TAKE 与一个 PUT 分别提交成功
- **THEN** TAKE 的唤醒事件仅送达 put 轨队首、PUT 的仅送达 take 轨队首——不发生跨轨误唤

#### Scenario: 延时到点唤醒消费闭环

- **WHEN** take-waiter 挂起于队首为 +1s 到期元素的队列，无后续 PUT，扫描驱动按 tick 发现到期
- **THEN** 等待者收到 `AWAIT_NOTIFY`、同 `requestId` 重发 TAKE、应用点谓词命中、取回该元素——全程零"到期专用"日志条目（仅正常 TAKE 条目）

#### Scenario: 通知丢失由清扫兜底

- **WHEN** 就绪推送因连接半开未被重发兑现
- **THEN** 已通知队首超时清扫重新评估并再次推送（或撤销已通知标记待下一轮），等待不永久悬挂

#### Scenario: 回弹保位次

- **WHEN** 两个阻塞 TAKE 位次 1/2 挂起，被唤醒重发后元素被位次 1 取走、位次 2 应用点 DENIED 回弹
- **THEN** 位次 2 重新挂于队首（其后可见性事件先唤它），客户端未收到错误应答，持续等待至下一唤醒

#### Scenario: 换主后挂起重建与首扫唤醒

- **WHEN** take-waiter 挂起期间 Leader 切换（本地等待队列随任期清零、客户端重挂）
- **THEN** 新 Leader 当选首扫发现"队首已到期"并推送唤醒，消费照常闭环；旧任期的推送状态不影响正确性
