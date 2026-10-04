# Proposal

## Why

ROADMAP 二档（小载荷）在 v6 开辟载荷通道基建后的首个实质原语：`OBlockingQueue`（对应 JDK `BlockingQueue` + `DelayQueue` 变体），为"协调 + 小载荷"承载第一个真实场景（任务分发/工作交接/延时投递）。其所需的全部机制先例均已就位——载荷通道四件套（v6）、等待挂起与推送唤醒（Latch/Barrier）、服务端定时扫描（租约到期驱动）、每键去重（ATOMIC 槽）——且三档"延时触发"行的裁决即"由 `OBlockingQueue` 延时形态覆盖"。基本形态与延时形态一揽子落地：拆开则去重槽、双角色唤醒、快照膨胀夹具等难点全部落在第一步，第二步只剩增量，两轮回归成本吞掉拆分收益。

## What Changes

- 协议升至 **v7 门**：`MessageType` 新增 `QUEUE_OP = 18` 单一消息对（`Envelope.payload` 槽 40/41；op 面 `QueueOp = PUT / TAKE / DRAIN / PEEK / SIZE`，读写皆全量经提交，判例 ATOMIC v4 裁决）；`put`/`offer`、`take`/`poll` 折叠为同一 op 的**阻塞位**（挂起/立即两态，超时由客户端本地计时，判例 `AcquireRequest.wait_ms` 三态纪律），客户端 SDK 侧仍完整暴露 JDK 六操作面。握手接受区间升至 [1,7]（`OpenLatchServer.PROTOCOL_VERSION` 6→7，客户端三处常量同步）；v≤6 会话携带 QUEUE_* → `INVALID_REQUEST` 消息级拒绝、不断连（判例 v3/v4/v5/v6 门）。
- `LockType` 新增 `LOCK_TYPE_QUEUE = 12`、`LOCK_TYPE_DELAY_QUEUE = 13`，归新家族位 `KeyFamily.QUEUE`（同 key 跨家族/跨形态互拒沿用既有规则；QUEUE 与 DELAY_QUEUE 互斥——出队规则由形态定型：FIFO 序 vs 最早到期序，同到期时刻内保持 FIFO，属**语义增强**须契约显式声明）。容量 `capacity` 定型断言（判例 `permits_total`/`parties`：建条目非零主张、既有条目须匹配、0 不主张）。
- 载荷面沿用 v6 纪律：元素为不透明 `optional bytes`（服务端 MUST NOT 解释；元素**不可为 null**——缺省 presence 即形状违例 `INVALID_REQUEST`，零长度空字节串为合法元素，判例 JDK `BlockingQueue` rejectNull）；逐元素钳制复用 `maxValueBytes` 入口裁决，判定唯一在接入层、条目/apply 侧不复核（v6 D5 纪律原样延伸，防回放分歧）。
- 延时形态：`delay_ms` 于请求携带，**折算发生在应用点**（`expiresAt = 条目携带时刻 + delay_ms`，判例租约 `expires_at_ms` 经 `EntryClock` 确定化）；出队谓词 `expiresAt <= 应用时刻` 纯比较、零配置依赖。新增 Leader-only 唤醒驱动（复用 `LeaseExpiryDriver` 扫描骨架：角色短路、换任立即首扫、周期 tick）：到点仅对挂起 take 队首推送 `AWAIT_NOTIFY`，**MUST NOT 提交日志条目、MUST NOT 改变复制状态**（到期是可见性判定非状态迁移）；FOLLOWER 无定时器。
- core 新建 `QueueEntry`（复用机制而非扩类，判例 Barrier/AtomicRefEntry）：元素双端队列 + 形态/容量定型 + **每会话去重槽**（槽内 `(op_seq, 回执含元素字节)`）——客户端继承 `RemoteAtomicBase` 同 key 在途写互斥纪律，单会话至多一个在途写，应答丢失重发恒命中**本会话**槽位重放回执（不双插、不偷吃；强于 ATOMIC 单全局槽的理由：队列 PUT/TAKE 重放撞槽被夺即破坏性，SET 撞槽仅无害覆盖）。`SESSION_CLOSE`：摘除该会话挂起等待与去重槽、**元素零触碰**——元素生命周期绑定 key 而非会话，持有者/投递者死亡不吞元素（ROADMAP 契约声明项）。
- 复制路径：`RaftEntryType.QUEUE_OP_ENTRY = 13` + 载荷包裹（session/request 既有模式）；`ApplyResult`/`SnapshotLock` 纯增量字段（元素列表确定性序列化含到期时刻与去重槽回执；编号规划与设计细节由 design 钉定）。空队/满容量的**挂起侧不入日志**（判例 Latch await Leader 本地裁决），容量竞态由应用点终判兜底（不足回 DENIED 零迁移，take 侧回弹后重新挂队）。
- 等待队列双角色分轨：Leader 本地 `WaitQueue` 节点增角色位（由原请求 op 导出）；元素落地只唤 take 队首、容量腾出只唤 put 队首、延时到点唤 take 队首（`onKeyFreedPermits` 语义分轨）；等待深度沿用 `maxQueueDepthPerKey` 护栏（**"满"双语义分轨**：元素满 → 立即式回 `DENIED`；等待者满 → 既有映射 `REJECT_QUEUE_FULL→OVERLOADED`，判例锁/Latch 深度护栏；`StatusCode`/`ApplyStatus` 词表零新增）。
- 新增服务端配置：`openlatch.server.limit.max-queue-capacity`（默认 1024，启动校验）、`openlatch.server.limit.max-drain-bytes`（默认 256KiB，`drainTo` 应答信封入口钳制，钳定后的 `max_elements` 随条目进日志保回放一致；钳制下调不追溯存量注记沿用 v6）。
- 管理面/控制台/指标：`AdminKeyInfo`/`AdminKeyDetailResponse` 增队列字段（深度/容量/最早到期/首元素截断预览对——服务端构造、恒定长度、判例 v6 D8；**全量元素列表 MUST NOT 入任何管理应答**）；family 词表增 `queue`；SUMMARY 增 `queue_entries` 计数线（判例 latch/atomic/barrier 单列）；指标新增 `openlatch.server.queue.total{op,status}` 单一命名点（元素深度 gauge 命名避让既有等待队深 `openlatch.server.queue.depth.max` 口径，design 钉定）。
- 客户端 SDK：公开 `OBlockingQueue` 接口（`byte[]` 主形态 + UTF-8 `String` 便利族：`put/offer/offer(timeout)/take/poll/poll(timeout)/drainTo/size/peek/remainingCapacity`）、`RemoteBlockingQueue` 实现（op_seq 与同序号重发继承现成车道，阻塞操作经 QUEUED→推送→同 `requestId` 重发环，判例 Latch await 客户端环）、`OpenLatchClient.newBlockingQueue(key, capacity)` / `newDelayQueue(key, capacity)` 工厂；契约 Javadoc 按 Lock 接口级撰写语义降级/增强清单（RTT、超时不确定窗、死亡不吞元素、同到期 FIFO 增强、服务端权威钳制、条目常驻不回收、需 v7 握手）。
- 快照膨胀治理回归扩相：N 个队列 key × 满容量 × 恰限元素 + 去重槽回执 → 尺寸落界断言 + 安装后逐字段保真（元素字节/到期时刻/槽）+ 追赶回放正常；旧（v6）快照加载兼容回归。
- 双语用户指南扩章、契约 Javadoc、ROADMAP 二档行状态流转与同 key 同类型约定登记（新增 QUEUE 家族行）；三档"延时触发"行注记由本原语覆盖兑现；WATCHLIST 登记热点队列日志条目率膨胀观察（与 W6/W7 同型，触发→另立 change 评估读路径折案）。
- 纯增量发布，无 **BREAKING**：v1–v6 既有能力行为不变；回滚窗口注记同 v6（回滚前队列 key 先清或接受不可用，guide 兼容章）。

## Capabilities

### New Capabilities

（无——全部落在既有能力面上。）

### Modified Capabilities

- `wire-protocol`: v7 门与握手常量；`QUEUE_OP` 消息对与 `QueueOp` 词表、`LOCK_TYPE_QUEUE/DELAY_QUEUE` 判别、容量断言与阻塞位形状纪律、元素 presence（null 即违例）、`delay_ms` 应用点折算口径；`max-queue-capacity`/`max-drain-bytes` 入口钳制与超限零日志；已发布编号冻结纪律延伸。
- `core-lock-engine`: `QueueEntry` 状态机（形态/容量定型、FIFO/最早到期两序规则、每会话去重槽与回执重放、双"满"语义、`SESSION_CLOSE` 元素零触碰）；`CoreEngine` 队列门面与家族互拒；`KeyFamily.QUEUE` 家族位。
- `replicated-state-machine`: `QUEUE_OP_ENTRY` 命令与 apply 分派；`ApplyResult` 元素回执字段；回放确定性（到期折算、drain 计数入条目、去重回放一致、apply 不读配置）；挂起与唤醒不入日志。
- `snapshot-recovery`: `SnapshotLock` 队列字段（元素确定性序列化、到期时刻、每会话去重槽表）与旧快照兼容；`CoreStateRestore` 扩展与重建工厂；队列快照尺寸有界回归。
- `lock-server`: 单机/集群两路 QUEUE_OP 分发的 v7 门与入口钳制接线；`WaitQueue` 双角色队首推进分轨；Leader-only 延时唤醒驱动（不入日志）；结果→状态码映射延伸；ShadowTable 登记点扩展与到期清扫家族豁免。
- `client-sdk`: `OBlockingQueue` 公开契约（降级/增强清单含"死亡不吞元素"与"同到期 FIFO"两处显式声明）、工厂与 String 便利形态、握手升 7、去重槽纪律的同 key 在途写互斥与同序号重发幂等。
- `admin-console`: 队列行与详情呈现（family=queue、深度/容量/最早到期/首元素截断预览；全量元素零外发）。
- `admin-observability`: `AdminKeyInfo`/`AdminKeyDetailResponse` 队列字段契约（预览沿用恒定长度截断转义纪律）、family 词表与 SUMMARY `queue_entries` 口径。
- `metrics-observability`: `openlatch.server.queue.total` 新命名点与词表（单一命名点纪律）；元素深度 gauge 命名避让等待队深既有口径。
- `user-documentation`: 双语指南队列与延时形态用法、容量/载荷/快照治理运维注记与回滚窗口、v1–v7 兼容矩阵、术语表。

## Impact

- **openlatch-protocol**：`openlatch.proto`（`MessageType`/`LockType`/`QueueOp`/`QueueOpRequest`/`QueueOpResponse`/`Envelope` 槽/管理面字段）与 `raft.proto`（`RaftEntryType`/`QueueOpPayload`/`ApplyResult`/`SnapshotLock`）纯增量；两份契约冻结测试扩至 v7 面；`ProtocolCodecTest` 队列消息与元素字节往返。
- **openlatch-core**：`KeyFamily`/`LockType` 扩值、`QueueEntry` 新类、`CoreEngine` 队列门面、`CoreStateRestore` 扩展、`CoreInspection` 队列读数、`Outcome` 队列通道口径。
- **openlatch-server**：`OpenLatchServer.PROTOCOL_VERSION` 6→7 与 `startScheduler` 触达；`ServerConfig` 两个新限额项与校验；`RequestDispatcher`/`ClusterRequestHandler` 门控+钳制+Leader 本地挂起裁决；`LockStateMachineCore` apply 分派；`WaitQueue` 双角色改造与 `ReplicationGateway` 推进分轨；队列就绪唤醒驱动（新类，判例 `LeaseExpiryDriver`）；`ShadowTable`/`AdminRequestHandler`/`ServerMetrics` 扩展；快照生成/加载两侧。
- **openlatch-client**：`OBlockingQueue`/`RemoteBlockingQueue`/工厂；`ConnectionManager`/`RequestMultiplexer` 握手常量；等待重发环复用 `AwaitTracker` 车道。
- **openlatch-console**：队列呈现。
- **openlatch-spring-boot-starter**：不动（注解语义面向锁；队列无等待/持有语义可织入，判例 v6 Non-Goal）。
- **测试与基准**：core 单测/判定矩阵、门控与形状矩阵、确定性（含配置漂移与到期折算）、快照往返+膨胀夹具队列版、集群 E2E（并发 FIFO、双角色唤醒、延时到点、DENIED 回弹重挂）、kill 进程裁决（死亡不吞元素）、公平套件同款扩、客户端 IT、混沌对齐注记、`BenchmarkMain` 队列相；`-Pdrill` 全量复跑。
- **文档**：`docs/guide` zh/en 对称扩章（01/03/05/07/08/10/术语表）；ROADMAP 状态行与三档行注记；WATCHLIST 新观察行；契约 Javadoc（`check-source-citations.sh` 门禁）。
- **发布面**：protocol/client/starter 为 Maven Central 构件（1.0.0 已发布），本 change 仅协议与 SDK 纯增量、不动版本号与发布流程；无 **BREAKING** 变更。
