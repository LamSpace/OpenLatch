## MODIFIED Requirements

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

## ADDED Requirements

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
