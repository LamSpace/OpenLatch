# Design

## Context

动机与范围见 proposal.md（Why / What Changes）。本设计面对的现状与约束：

- v6 载荷通道四件套已就位：`optional bytes` 显式 presence 口径、`maxValueBytes` 接入层钳制（引擎/条目不复核，防回放分歧）、快照膨胀回归夹具、控制台恒定截断预览——本 change 逐项沿用并扩展到"多元素"形态。
- 等待-推送闭环已有两判例：Latch/Barrier 的挂起不入日志（集群路径由 Leader 本地 `WaitQueue` 承载、`AcquirePayload` 注记"应用侧引擎调用恒不登记等待项"；单机路径由条目 `awaiters` + `fireNotify` 承载），提交后经 `AWAIT_NOTIFY` 推送、客户端同 `requestId` 重发幂等命中；已通知超时清扫（`sweepNotified`/`sweepWaitQueue`）为通知丢失兜底。
- 服务端定时器判例 `LeaseExpiryDriver`：单线程周期扫描、非 Leader 短路、换任 `onLeadershipGained` 立即首扫、驱动条目 seq 高位隔离、提交前查 `ShadowTable` 投影。
- 幂等判例两级：ATOMIC 每键单槽 `(session, op_seq, 应答四元组)`；Barrier `(会话,请求)` 到场账簿。客户端 `RemoteAtomicBase` 已裁决"同 key 在途写互斥 + 可重试失败同 `op_seq` 重发"。
- 约束：已发布 proto 编号冻结（golden 测试锁证）；回放确定性（apply 结果不得依赖节点本地配置/物理时钟——`EntryClock` 判例）；库产物禁 `--enable-preview`；对外注释禁引内部材料（`check-source-citations.sh`）；Netty 入站帧上限 1MiB。

## Goals / Non-Goals

**Goals:**

- `OBlockingQueue` 对 JDK `BlockingQueue` 语义保真到协调面允许的最大程度：有界 FIFO、`put/take` 阻塞可中断、`offer/poll` 立即式、`drainTo` 批量摘取；元素不丢失（复制面多数派提交）、不双投（应答丢失重发经去重槽重放）。
- `ODelayQueue` 对 JDK `DelayQueue` 保真"最早到期先出"；同到期时刻内 FIFO 为**语义增强**（JDK 不承诺），契约显式声明。
- 建立"元素生命周期绑定 key"契约：投递者/任一会话死亡不吞元素——与锁/Semaphore 的归属语义正交，与 ATOMIC"值不绑定会话"同型延伸。
- 沉淀双机制供 `OTopic` 复用：等待队列双角色分轨唤醒、每会话去重槽的"回执形状"。

**Non-Goals:**

- 无界队列形态（JDK `LinkedBlockingQueue` 默认无界）——协调面常驻治理不允许，容量恒必定型。
- `iterator`/`contains`/`remove(Object)`/ spliterator 族——O(n) 字节匹配与集合视图在 RTT 面失真，不入面。
- 元素级 TTL 自动清除——到期只影响"可见性"，未被消费的元素不随到期消失（契约声明，与 DelayQueue 本体一致）。
- 服务端等待超时裁决——`offer/poll(timeout)` 的超时为客户端本地计时（判例 `wait_ms > 0` 由客户端计时）；服务端挂起无期限，由等待深度限额与客户端总时限兜底。
- 多 key 原子事务、优先级队列、`remainingCapacity` 的服务端订阅推送。
- Starter/AOP 面不动（队列无锁式持有语义可织入，判例 v6）。

## Decisions

### D1 基本形态与延时形态一揽子、共用 v7 门与消息对

拆分（v7 纯 FIFO、v8 延时）被否决：去重槽回执、双角色唤醒、快照膨胀夹具、`drainTo` 信封钳制等全部难点属第一步，延时仅为"出队谓词 + 折算 + 扫描器"三处加法；拆两轮则回归全量各跑一次而收益甚微。且 ROADMAP 三档"延时触发"行的裁决即"由 `OBlockingQueue` 延时形态覆盖"，一揽子一次兑现。

### D2 单一 `QUEUE_OP` 消息对 + op 枚举 + 阻塞位

`MessageType.QUEUE_OP = 18`，`Envelope.payload` 槽 40/41；`QueueOp { PUT=0, TAKE=1, DRAIN=2, PEEK=3, SIZE=4 }`。`put/offer`、`take/poll` 折叠为同一 op 的 `blocking` 位（判例 `AcquireRequest.wait_ms` 三态：挂起/立即/客户端计时），SDK 面仍完整暴露 JDK 六操作。备选"每操作一对消息"（v5 三对判例）否决：v5 拆分因三种命令回执形状差异大（executor/generation/了结形态各一套），队列五种 op 回执同构（状态 + 元素槽/列表 + 位次数），拆对纯复制装配层。`PEEK/SIZE` 读写皆全量经提交（v4 GET 裁决延伸，零迁移）。

### D3 `KeyFamily.QUEUE` 一个家族位、`LOCK_TYPE_QUEUE = 12` / `LOCK_TYPE_DELAY_QUEUE = 13` 两形态

形态承载出队规则差异（到达序 vs 到期时刻序）与到期字段是否参与判定，定型后互斥（同 key 跨形态互拒，ATOMIC 四形态同族判例直用）。备选"独立两个 KeyFamily"否决：队列与延时队列本就互斥（同一 key 两种语义不可共存），无需第二家族位，独立家族会架空"同 key 同类型"约定的单一判定点。备选"单形态 + delay 字段"否决：DelayQueue 语义下 `PEEK/SIZE/drainTo` 全部要叠到期谓词，形态判别比字段判别干净（apply 分派与快照 presence 都随形态）。

### D4 元素非 null 且不可解释——与 v6 引用形态的 presence 语义**刻意不同**

`element_bytes` 为 `optional bytes`（presence 承载"携带与否"），但 PUT 缺省 presence = 形状违例 `INVALID_REQUEST`（判例 JDK `BlockingQueue.rejectNull`）；零长度空字节串是合法元素。应答侧 `element_bytes` 的缺省语义 = 空队列/无元素（PEEK 空队、TAKE 拒绝路径）。**契约注释必须显式声明此差异**，防止实现者按 v6"null 为一等公民"照抄。服务端全程不解释字节（v6 D1 原样）。

### D5 每会话去重槽（强于 ATOMIC 全局单槽），回执形状按 op 定型

ATOMIC 单全局槽的前提是"重试撞槽位被夺 = 无害覆盖同一值"；队列 PUT 被夺重放=双插、TAKE=偷吃，均破坏性。裁决：条目内 `Map<session, Slot{op_seq, op, 回执}>` **每会话单槽**——客户端继承 `RemoteAtomicBase` "同 key 在途写互斥"纪律，单会话任意时刻至多一个在途写，重发恒命中**本会话**槽（跨会话各占各槽，无遮蔽）。槽回执形状：PUT 槽无需载荷（重放 = OK 已插入）；TAKE 槽存已交付元素字节（重放必须同元素——应答丢失场景客户端从未见过该元素，重放新元素 = 丢件 + 偷吃双重破坏）；DRAIN 槽存已交付列表。`PEEK/SIZE` 读类 op `op_seq=0` 不参与去重（GET 判例）。`SESSION_CLOSE` 摘该会话槽与挂起等待、元素零触碰。备选"元素 uid 客户端生成 + LRU 账簿"否决：新客户端契约（uid 分配/碰撞面）与无界账簿治理成本均高于每会话单槽；备选"接受 ATOMIC 同款风险"否决（破坏性）。

### D6 挂起不入日志、唤醒只推送、应用点 DENIED 回弹继续挂

集群路径：Leader 入口对 `blocking=true` 且当前不可满足（满/空/未到期，查 `ShadowTable` 队列镜像）的请求**不提交条目**，直接 `WaitQueue.enqueue` 回 `QUEUED`（Latch `latchAwaitLocal` 判例）；可满足则提交 `QUEUE_OP_ENTRY`，`request_id` 进载荷供幂等。**回弹臂**（队列特有，锁/Latch 无判例）：预检后并发竞态使 apply 点不可满足（如两 take 抢一元素）→ ApplyStatus `DENIED` 零迁移、不写槽 → Leader 视应答码把该 `(session, request_id)` **重新挂回** WaitQueue 原位（不丢位、不推错误给客户端）；立即式（`blocking=false`）同路径直接回 `DENIED` 终结（offer false/poll null）。单机路径：条目 `awaiters` 承载挂起（判例 LatchEntry），唤醒经 `fireNotify → NotifyEventBridge` 同车道。客户端阻塞环收到 `DENIED` 重发下一轮 TAKE——阻塞方法的终态只有 OK/中断/超时。

### D7 到期折算在 apply、唤醒扫描 Leader-only 零日志

`delay_ms` 由请求携带；**绝对到期时刻 = 条目携带时刻 + delay_ms**，折算发生在 apply（`EntryClock` 注入条目时刻）——与租约 `expires_at_ms` 完全同构，回放恒等、跨副本确定（节点本地时钟/配置不参与）。到期时刻为复制状态（入元素、入快照）。出队谓词 `expires_at <= 应用/预检时刻` 纯比较。延时唤醒不需要日志条目（到期不改变复制状态，只改变可见性）：新增 `QueueReadyDriver`（复用 `LeaseExpiryDriver` 装配骨架：单线程 tick、非 Leader 短路、`onLeadershipGained` 首扫），每 tick 遍历"有 take 挂起的队列 key"集合（由 `WaitQueue` 反查，非全表扫描），对 ShadowTable 镜像队首到期已至者推送唤醒（`notified` 标记防重推，超时清扫 `sweepNotified` 为丢失兜底）——**MUST NOT 提交条目**。换主：挂起者随 `WaitQueue.clear()` 消亡、客户端重挂（既有换主重发道），新 Leader 首扫自然覆盖"挂起时已到期"的存量。FOLLOWER 无定时器。独立 tick 配置 `openlatch.server.queue.ready-tick-ms`（默认 200ms）：唤醒延迟 SLA 与租约扫描（秒级）不同暴露面，不合流。单机部署经既有 `startScheduler` 同型挂一段队列就绪扫描（`CoreEngine` 时钟可手工推进为测试道）。契约声明：到点唤醒精度 = tick 级（最坏 +200ms），消费正确性不依赖精度。

### D8 `WaitQueue` 双角色分轨

节点增 `role`（0=等容量/put-waiter，1=等元素/take-waiter；由原请求 op 导出，非新客户端字段）。队首推进分轨：DRAIN/TAKE 提交使元素出队 → `onCapacityFreed`（唤 put 轨队首）；PUT 提交使元素入队 → `onElementReady`（唤 take 轨队首，DELAY 形态需队首已到期才唤）；延时到期 → `onElementReady`。既有 `onKeyFreedPermits`/`broadcastKey`/`deferHead`/`sweepNotified` 语义按轨映射，锁/Semaphore/Latch/Barrier 行为零扰动（默认单轨同旧）。等待深度限额 `maxQueueDepthPerKey` 双轨共用（同 key 合计，护栏语义不变）。

### D9 `drainTo` 信封钳制与存量超限角

入口钳制（Leader/单机分发，提交前）：`max_elements ≤ min(请求值或不限时缺省值, capacity, floor(max-drain-bytes / maxValueBytes))`，钳定后的 N 随条目进日志（apply 出队 `min(N, 满足谓词的头连续段)`，全副本一致）。默认 4KB × 256KiB → 上限 64 元素，应答最坏 256KiB，半帧安全。**接受角**：`maxValueBytes` 下调后存量超大元素（钳制下调不追溯存量，v6 契约）被 drain 时实际应答字节可超预算（上界 = N × 历史最大元素 ≤ N × 512KiB 配置绝对上限），最坏撞 1MiB 帧致该连接断——guide 运维注记"队列存续期下调 `maxValueBytes` 前先清大元素"，不为此加读态尺寸镜像（复杂度不成比）。备选"apply 侧按元素累计字节截断"否决：apply 需要配置态参与判定，直接违反 D5(v6) 回放分歧红线。

### D10 容量钳制：`openlatch.server.limit.max-queue-capacity` 默认 1024、校验 [1, 65536]

PUT 建条目/定型断言承载 `capacity`（判例 `permits_total`/`parties`：非零主张、既有条目不符即拒、0 不主张）；入口层拒绝超本节点上限的主张（`INVALID_REQUEST`，配置校验走 `ServerConfig.validate` 既有快速失败通道）。单 key 最坏常驻 = 1024 × `maxValueBytes`（默认 4MB，512KiB 配置顶时为 512MB——治理注记入 guide：上限调大须与快照阈值联动评估）。不入 `CoreConfig`（v6 D5/D6 纪律：core 不感知限额）。

### D11 编号冻结表（新增面一次性钉定）

- `openlatch.proto`：`MessageType.QUEUE_OP = 18`；`Envelope.payload` `queue_op_request = 40`、`queue_op_response = 41`；`LockType.LOCK_TYPE_QUEUE = 12`、`LOCK_TYPE_DELAY_QUEUE = 13`；`enum QueueOp { QUEUE_OP_PUT = 0; QUEUE_OP_TAKE = 1; QUEUE_OP_DRAIN = 2; QUEUE_OP_PEEK = 3; QUEUE_OP_SIZE = 4; }`。
- `QueueOpRequest`：`key=1, op=2, lock_type=3, blocking=4, capacity=5(sint64), element_bytes=6(optional bytes), delay_ms=7(sint64), max_elements=8(int32), op_seq=9(uint64)`。
- `QueueOpResponse`：`status=1, op=2, queue_position=3, element_bytes=4(optional bytes), drained_bytes=5(repeated bytes), size=6(sint64), capacity=7(sint64)`。
- `AdminKeyInfo`：`queue_capacity=13, queue_depth=14, queue_head_payload_size=15, queue_head_payload_preview=16`；`family` 词表增 `queue`。
- `AdminKeyDetailResponse`：`queue_capacity=23, queue_depth=24, queue_head_expiry_ms=25, queue_total_payload_bytes=26, queue_head_payload_size=27, queue_head_payload_preview=28`。
- `raft.proto`：`RaftEntryType.QUEUE_OP_ENTRY = 13`；`QueueOpPayload{session_id=1, request_id=2, request=3}`；`ApplyResult`：`queue_element_bytes=20(optional bytes), queue_drained_bytes=21(repeated bytes), queue_size=22(sint64)`；`SnapshotLock`：`queue_capacity=32(sint64), queue_elements=33(repeated SnapshotQueueElement), queue_dedup_slots=34(repeated SnapshotQueueSlot)`；`SnapshotQueueElement{payload=1(bytes), expires_at_ms=2(int64)}`（队列序即列表序）；`SnapshotQueueSlot{session_id=1, op_seq=2, op=3(int32:0=PUT/1=TAKE/2=DRAIN), element_bytes=4, drained_bytes=5(repeated)}`。
- `StatusCode`/`ApplyStatus` 零新增（元素满立即式=`DENIED`，等待深度超限=`OVERLOADED`，双"满"分轨在 spec 注释钉死）。

### D12 指标与观察面命名避让

Counter `openlatch.server.queue.total{op, status}`（`op` 词表 `put/take/drain/peek/size`，单一命名点常量，Vocabulary 测试扩表；`QUEUED`/`DENIED`/`OVERLOADED` 各走 status 线）。元素深度 Gauge 命名 **`openlatch.server.elements.depth.max`**（抓取时刻单键最大元素数）——避让既有 `openlatch.server.queue.depth.max`（其口径是**等待队列**深度，重名即口径污染）。SUMMARY 增 `queue_entries = 12`（无持有语义单列，判例 latch/atomic/barrier）。管理面：首元素大小 + 恒定截断预览（v6 D8 纪律原样）+ `queue_total_payload_bytes` 驻留总量（忘删 key 的泄漏发现面）；**全量元素列表 MUST NOT 入任何管理应答**。

### D13 客户端 SDK 面与车道

公开 `OBlockingQueue`（byte[] 主形态 + String(UTF-8) 便利族：`put/offer/offer(timeout)/take/poll/poll(timeout)/peek/drainTo/size/remainingCapacity/capacity`）与子接口 `ODelayQueue extends OBlockingQueue`（`offerDelayed(byte[]|String, delay, unit)` 显式命名——JDK `DelayQueue.offer(e, timeout, unit)` 的 delay 注入签名与容量阻塞位重名易混，协调面改名并在契约声明该差异；JDK `DelayQueue` 无容量语义，此处容量必定型，属增强声明）。`RemoteBlockingQueue`/`RemoteDelayQueue` 实现挂 `OpenLatchClient.newBlockingQueue(key, capacity)` / `newDelayQueue(key, capacity)` 工厂（capacity ≤0 本地拒绝）。写侧 op_seq 分配与同序号重发继承 `RemoteAtomicBase` 现成纪律；阻塞环为队列专用跟踪（QUEUED→挂起→AWAIT_NOTIFY→同 `requestId` 重发→DENIED 回弹继续挂→OK/中断/超时终态），不扩 `AwaitTracker` 的 `AcquireSpec` 形状（锁车道零扰动）。超限/形状/低版本拒绝 = 不可重试显式异常（v6 判例）。握手声明 6→7。

### D14 ShadowTable 与恢复接线

`SLock` 增队列镜像字段（capacity、depth、head 到期时刻、总驻留字节、首元素 size+预览源数据），apply 与快照回灌两处登记；队列条目无租约——`expireUpTo` 家族豁免续列（判例 LATCH/BARRIER/ATOMIC），`heldIndex` 不收队列 key（无持有者面）。`CoreStateRestore` 增 `QueueState` 自包含记录（capacity/元素列表/槽表）与 `QueueEntry.restored(...)` 工厂；序列化确定性 = 元素序即语义序（与既有"序列化序即语义序"纪律同构）。旧（≤v6）快照无队列字段按缺省加载，兼容回归。

## Risks / Tradeoffs

- **每次变更一条日志条目**（读写皆经提交，v4 GET 裁决延伸）→ 热点队列 key 的日志/快照膨胀暴露面大于 W6（原子读放大）与 W7（屏障到场）→ 缓解：`drainTo` 提供批量摊薄 + WATCHLIST 登记 W9 观察行（触发→另立 change 评估读路径折案/合并提案），默认纪律不动。
- **DENIED 回弹臂无既有判例**，竞态时序复杂（预检-提交-回弹-重挂窗口内换主/断连）→ 缓解：`request_id` 幂等位既有、`WaitQueue.enqueue` 幂等命中保位次；专项 E2E（双 take 抢单元、换主窗回弹）钉行为；立即式与阻塞式在回弹分界上语义正交（前者终结后者续挂），契约注释逐条声明。
- **TAKE/DRAIN 槽回执携带元素字节 → 快照随活跃读写会话数膨胀**（每会话 ≤ 1 槽 × ≤ capacity×元素）→ 缓解：SESSION_CLOSE 清槽（会话死亡即收敛）、槽表规模 = 最近一写、恰限批量写回归夹具含槽维度断言。
- **延时唤醒精度 = tick 级**（默认 200ms 最坏延迟）且依赖 Leader 扫描 → 缓解：消费正确性不依赖唤醒精度（谓词在 apply/入口双点判定，唤醒只是提示）；`sweepNotified` 超时清扫与客户端重发为通知丢失兜底。
- **drain 大存量元素超帧角**（D9 接受角）→ 缓解：guide 运维注记 + 默认配置半帧安全；不引入读态尺寸镜像。
- **队列条目常驻不回收 + 容量默认 1024 = 泄漏驻留成本**（v6 "条目不回收"放大版）→ 缓解：控制台 `queue_total_payload_bytes` 列 + guide 清理手册段落（人工经 SDK 消费清空或管理面观测后运维处置；服务端不做自动回收——Non-Goal）。
- **回滚窗口**：v6 前二进制不解 `lock_type=12/13` 条目与队列快照字段（proto3 未知字段容忍但枚举/apply 行为不良）→ 与 v4/v5/v6 同型注记入 guide 兼容章（回滚前队列 key 清空或接受不可用）。

## Migration Plan

纯增量发布，无数据迁移：协议 v6→7（v1–v6 客户端行为零扰动，握手区间 [1,7]）；三个新配置项均有默认值（不配即 1024 / 256KiB / 200ms）；快照新增字段旧二进制不读不坏（未知字段容忍），回滚按兼容章注记执行（队列 key 先清或接受不可用）。部署序：服务端先升（v7 能力静默待命）→ 客户端按需升。

## Open Questions

（无——容量/延时精度/回弹时序等原候选歧义均已在 D1–D14 裁决；`remainingCapacity` 是否值得 SDK 便利方法属实现期口味，随 SDK 编写定夺。）
