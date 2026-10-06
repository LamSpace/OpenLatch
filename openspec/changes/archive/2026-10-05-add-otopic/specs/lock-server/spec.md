# Spec Delta

## MODIFIED Requirements

### Requirement: 服务启动与配置加载

服务器 MUST 从 `-Dopenlatch.config=<path>` 指定的 Java Properties 文件加载配置；未指定时 MUST 使用内置默认值启动。配置项 MUST 覆盖：监听端口（默认 9410，允许 0 表示由操作系统分配临时端口）、Worker 线程数（默认 2×CPU）、空闲断连时限、默认租约与租约钳制区间、扫描周期、队首响应时限、key 长度上限、单 key 队列深度上限、单连接未完成请求上限、有值引用载荷字节上限 `maxValueBytes`（`openlatch.server.limit.max-value-bytes`，默认 4096，MUST ≥1 且 MUST ≤512KiB——为 1MiB 帧上限留信封编解码安全边际）、队列容量上限 `maxQueueCapacity`（`openlatch.server.limit.max-queue-capacity`，默认 1024，MUST ∈[1, 65536]——定型断言的入口钳制上限，配合每元素 `maxValueBytes` 构成单 key 驻留上界）、drain 应答字节预算 `maxDrainBytes`（`openlatch.server.limit.max-drain-bytes`，默认 262144（256KiB），MUST ∈[1, 512KiB]——`drainTo` 提取上限的派生预算）、队列就绪扫描周期 `queueReadyTickMs`（`openlatch.server.queue.ready-tick-ms`，默认 200，MUST ≥10——延时形态的唤醒精度）、单 key 订阅数上限 `maxSubscribersPerKey`（`openlatch.server.limit.max-subscribers-per-key`，默认 64，MUST ∈[1, 1024]——钳广播 fan-out 放大面）、每订阅在途缓冲上限 `maxSubscriptionBuffer`（`openlatch.server.limit.max-subscription-buffer`，默认 256，MUST ∈[1, 65536]——条数口径，钳每订阅服务端堆内存与写出队列，满则 drop-newest）。非法配置值 MUST 在启动时快速失败并给出明确错误信息。启动成功后 MUST 监听配置端口（port=0 时监听实际分配端口），并在启动日志中打印端口、协议版本与关键限额配置（含 `maxValueBytes`、`maxQueueCapacity`、`maxDrainBytes`、`maxSubscribersPerKey`、`maxSubscriptionBuffer`）。

#### Scenario: 默认配置启动

- **WHEN** 未提供配置文件直接启动服务器
- **THEN** 服务器以内置默认值启动并监听 9410 端口，启动日志包含端口、协议版本与关键限额（含载荷上限 4096、队列容量上限 1024、drain 预算 256KiB、订阅上限 64、订阅缓冲 256 条）

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

#### Scenario: topic 限额越界配置快速失败

- **WHEN** 配置 `max-subscribers-per-key` 为 0 或超 1024、`max-subscription-buffer` 为 0 或超 65536
- **THEN** 启动失败并给出明确错误信息，不进入半启动状态

#### Scenario: 端口被占用启动失败

- **WHEN** 配置端口已被其他进程占用
- **THEN** 服务器启动失败，进程退出并给出明确错误信息，不进入半启动状态

## ADDED Requirements

### Requirement: v8 topic 门控与入口分发

服务端 SHALL 在分发层对 `TOPIC_OP` 执行 v8 门：协商版本 <8 的会话发送 `TOPIC_OP` MUST 以同型 `INVALID_REQUEST` 消息级拒绝、不断连（判例 v3–v7 门）。门后入口裁决 MUST 依序完成：key 非空与长度校验（既有纪律）、请求形状校验（op 与 `payload_bytes`/`op_seq` 的携带矩阵，违例同型 `INVALID_REQUEST`、登记与状态零扰动）、PUBLISH 逐条 `maxValueBytes` 钳制（判定唯一在接入层，超限同型拒绝、零 fan-out、`REJECT_SUBSCRIBERS` 之外的订阅侧限额不在本层）。topic 操作 MUST NOT 进入状态机提交路径与 core 引擎（零日志边界由 replicated-state-machine 能力钉定）；单机形态由接入节点自身受理（等效常驻 Leader），集群形态仅 Leader 受理——Follower 对任意 `TOPIC_OP` MUST 以同型 `NOT_LEADER` 拒绝且零副作用。PUBLISH 的 `OK` 应答语义为**已受理并已入 fan-out**，MUST NOT 被表述或实现为"订阅者已收到"。

#### Scenario: v7 会话发 TOPIC_OP 被门控

- **WHEN** 协商版本为 7 的会话发送任意 `TOPIC_OP`
- **THEN** 收到 `topic_op_response` 且 `status = INVALID_REQUEST`、`op` 回显，连接保持，该会话队列/锁等既有请求照常服务

#### Scenario: 超限载荷 PUBLISH 零生效

- **WHEN** v8 会话发送 `payload_bytes` 超 `maxValueBytes` 的 PUBLISH
- **THEN** 同型 `INVALID_REQUEST` 拒绝，该消息未进入任何订阅缓冲（fan-out 零发生），订阅登记表与计数线之外状态零扰动

#### Scenario: Follower 拒绝零副作用

- **WHEN** 客户端向 Follower 节点发送 SUBSCRIBE 或 PUBLISH
- **THEN** 同型 `NOT_LEADER` 拒绝（无 leader 提示字段，改道由客户端故障转移兜底），Follower 上不存在任何订阅登记或推送

### Requirement: topic 订阅登记表生命周期与 fan-out

Leader（单机=常驻）SHALL 维护进程内订阅登记表：`(session, key)` 唯一登记（含 subscription_id、每订阅在途缓冲、丢弃计数、订阅时刻），同会话同键重复 SUBSCRIBE 为**幂等覆盖**（既有缓冲与计数存续，返回同一登记语义）。SUBSCRIBE 受理时 `maxSubscribersPerKey` 按 key 判定：已满则同型 `REJECT_SUBSCRIBERS` 拒绝且该 key 既有订阅零扰动。UNSUBSCRIBE MUST 摘除登记并释放其缓冲；对未存在订阅回 `OK`（幂等，无错误态）。会话关闭 MUST 经两条既有路径之一摘除该会话全部订阅、去重槽与缓冲：本节点断连传播的 `SESSION_CLOSE`、失联探针补发的 `SESSION_CLOSE`——摘除后该会话不再收到任何 `TOPIC_MESSAGE`（**死亡即退订**，与队列"死亡不吞元素"刻意相反，契约须随行声明）。换主时登记表随进程本地态清零，新 Leader 等待客户端重挂（无快照/日志来源，判例 `WaitQueue` 换主清零）。

PUBLISH 受理 MUST 经每会话去重槽：命中（同 `session`+`op_seq`）则重放回执（同一 `topic_seq`）且 MUST NOT 二次 fan-out。未命中则为 key 分配 term 内单调 `topic_seq`、记录槽、并按该 key 全部登记订阅逐一入队：每订阅独立缓冲、独立交付队列，入队满时 **drop-newest**（丢最新一条、丢弃计数 +1、既有缓冲照常交付），单订阅的慢或断线 MUST NOT 阻塞其他订阅交付或 Publisher 应答。交付序：单订阅内 `topic_seq` 严格升序。服务端 MUST NOT 为 topic 设任何定时器或扫描驱动（推送即时，无 tick 精度语义——与 v7 延时形态相反的空缺为有意设计）。撞 key 防护：SUBSCRIBE/PUBLISH MUST 在受理节点只读探测 LockTable，key 已被锁/Semaphore/Latch/Barrier/ATOMIC/QUEUE 家族条目占据时以同型 `INVALID_REQUEST` 拒绝（内部裁决 `REJECT_TYPE_MISMATCH`，线路映射判例家族误用同规则）；该探测为尽力而为（与并发建条目存在竞态窗，"一 key 一形态"的应用侧契约由 client-sdk 与 user-documentation 能力承载声明）。

#### Scenario: 订阅幂等与退订回收

- **WHEN** 同一会话对同 key 连续 SUBSCRIBE 两次后执行 UNSUBSCRIBE，再次 SUBSCRIBE
- **THEN** 前两次返回同一登记（`subscription_id` 一致、计数线仅一条订阅），退订后该键订阅数 -1 且缓冲释放，第三次建立新登记；全程无重复登记条目（防泄漏断言：registry 订阅总数与活跃会话×键的并集恒等）

#### Scenario: 订阅数上限拒绝

- **WHEN** 某 key 已有 `max-subscribers-per-key` 个订阅，新会话 SUBSCRIBE
- **THEN** 新订阅被同型 `REJECT_SUBSCRIBERS` 拒绝，既有订阅不受任何扰动（不被挤占、不被关闭）

#### Scenario: 会话死亡即退订

- **WHEN** 订阅者进程被 kill（失联探针补发 `SESSION_CLOSE`），随后该 key 有新 PUBLISH
- **THEN** 死亡会话不再收到任何推送（无悬挂登记），其余订阅者照常收到；登记表该会话条目已摘除

#### Scenario: 慢订阅者 drop-newest 不扩散

- **WHEN** 某订阅连接阻塞致其缓冲达上限，期间该 key 连续 PUBLISH
- **THEN** 仅该订阅的新消息被丢弃并计数（其后续收到的 `topic_seq` 出现 gap），其他订阅与 Publisher 的 `OK` 应答不受影响，Publisher 不被阻塞

#### Scenario: 重发不双扇出

- **WHEN** 同一会话以同 `op_seq` 重发已成功 PUBLISH（应答丢失重试）
- **THEN** 返回同一 `topic_seq` 回执，各订阅缓冲不出现第二条同序消息

#### Scenario: 换主后登记清零重挂

- **WHEN** Leader 更替后旧订阅连接迁移至新 Leader 并重发 SUBSCRIBE
- **THEN** 新 Leader 上仅存在重挂后的登记（旧 term 条目随旧进程清零、无残留），新 term 的 `topic_seq` 重新起算，换主窗内发布的消息不向任何订阅者重投

#### Scenario: 撞 key 只读探测拒绝

- **WHEN** 对已被队列条目占据的 key 发送 SUBSCRIBE
- **THEN** 同型 `INVALID_REQUEST` 拒绝（撞 key 裁决，判例家族误用映射），队列条目状态零扰动，不产生订阅登记
