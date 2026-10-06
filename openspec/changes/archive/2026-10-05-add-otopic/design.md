# Design

## Context

见 proposal.md - Why。当前状态约束：

- 协议门已至 v7（`QUEUE_OP=18`、`RaftEntryType` 止于 13、`StatusCode` 13 值、`LockType` 14 值、`Envelope.payload` oneof 已用至槽 41）。
- 客户端恒定跟随 home 节点（= Leader）：HELLO 应答携 `leader_hint`/`leader_address`，`ConnectionManager` 重连后经 `migrateWaitsTo`/`replayOnHome` 重挂等待。`pushAwaitNotify` 的"Leader 本地连接投递、跨接入节点不转发"由该拓扑成立——**OTopic 的 fan-out 继承同一拓扑，无需任何跨节点转发设计**。
- 会话 id 由接入节点按连接分配（`(nodeId<<32)|localSeq`），`SESSION_OPEN`/`SESSION_CLOSE` 是复制态；换主重连必得新 sid。
- W10 已销账：拒绝应答与请求同型成文为 `wire-protocol` 常驻条款；W11 在观察非 ACQUIRE 车道的换主窗改道收敛。
- v7 队列全套判例可直接援引：入口形状唯一裁决（v6 D5）、每会话去重槽（v7 D5）、阻塞/立即折叠（v7 D2）、管理面"零放大"纪律、`queue.total` 单一命名点。

## Goals / Non-Goals

**Goals:**
- 广播发布/订阅的至多一次交付契约，弱背压为显式声明（drop-newest，两级缓冲）。
- 项目首个零复制日志原语的完整论证与**常驻守卫条款**（防后续演进按"读写皆经提交"惯性误塞日志）。
- 订阅条目防泄漏：UNSUBSCRIBE/会话死亡/失联探针三路回收，可观测、可断言。
- 客户端订阅跨换主存续（自动重订阅，对应用透明）。

**Non-Goals:**
- 不做消息持久化/重放、不做请求门控强背压（`request(n)`）、不做多订阅者的消费组/负载均衡语义（那是队列的分工，ROADMAP 分工行）。
- 不动 `raft.proto`、不动 core 模块、不动 starter。
- 不新增定时器（topic 即时 fan-out，无 tick 唤醒需求——与 v7 `QueueReadyDriver` 相反的空缺是有意设计）。

## Decisions

### D1 零日志：topic 全操作不产生复制条目

SUBSCRIBE/UNSUBSCRIBE/PUBLISH 与 fan-out 一律不经 Raft 提交。`raft.proto` 零触碰、`ApplyResult`/`SnapshotLock` 零扩展、core 零改动（无 `TopicEntry`、无 `KeyFamily` 新家族位、无 `LockType` 新值）。论据三条（钉入 `replicated-state-machine` 规格）：

1. **回放即重投**：队列 PUT 入日志无害，因 apply 仅改状态、唤醒走 Leader 本地 `WaitQueue`；topic 的"apply 副作用"就是推送本身，日志重放会把陈旧消息二次 fan-out，且无可零迁移化（消息非状态）。
2. **写放大**：先多数派再 ×N fan-out 即 `log × N`，W9 观察项的恶性变体。
3. **语义诚实**：`Flow.Publisher` 本位至多一次协调面广播，ROADMAP"弱背压为显式契约"已承认丢消息。

备选（均否决）：SUBSCRIBE 入日志——换主后客户端必得新 sid，复制侧登记全成死指针，只换来需作废清理的陈旧态，零收益；PUBLISH 入日志——论据 1/2 直接击穿。**落地纪律第 2 条对本原语整体豁免，本 change 即判例。**

### D2 订阅登记表：server 侧新类 `TopicRegistry`（Leader 本地态）

判例 `WaitQueue` 的装配位与生命周期：单机=常驻权威；集群=仅 Leader 持有并 fan-out，换主清零（客户端重挂）。登记内容：`(session, key) → { subscription_id, 缓冲队列, dropped 计数, subscribed_at_ms }`、`key → topic_seq 生成器`、`session → PUBLISH 去重槽`。回收三路（防泄漏验收点）：本节点 `channelInactive` → `SESSION_CLOSE` 应用点 purge；失联探针补发 `SESSION_CLOSE` 的 purge（复用 gateway `SESSION_CLOSE` 既有钩子位）；UNSUBSCRIBE 显式摘除。备选：进 `ShadowTable`（否决——无复制来源可镜像）、进 core 引擎（与 D1 矛盾）。Follower 无 registry，TOPIC_OP 一律同型 `NOT_LEADER` 拒绝。

### D3 单一 `TOPIC_OP = 19` 消息对 + 三操作词表

`TopicOp { SUBSCRIBE=0, UNSUBSCRIBE=1, PUBLISH=2 }`，判例 v7 单消息对（而非 Barrier 三消息）：三操作载荷字段集高度重叠、门控与拒绝同型处理共用一套 op 回显。无阻塞位——订阅恒立即回执、交付经推送。`Envelope.payload` 新增槽 `TopicOpRequest=42`/`TopicOpResponse=43`。

`TopicOpRequest { key=1, op=2, payload_bytes=3(optional bytes), op_seq=4(uint64) }`：`payload_bytes` **PUBLISH 必携**（缺省 presence 即形状违例 `INVALID_REQUEST`，判例 v7 D4；非 PUBLISH 携带为违例）、逐条 ≤ `maxValueBytes` 入口钳制（判定唯一在接入层，判例 v6 D5）；`op_seq` 仅 PUBLISH 有效（会话内单调写序号，SUBSCRIBE/UNSUBSCRIBE 填 0）。

`TopicOpResponse { status=1, op=2, subscription_id=3(uint64), topic_seq=4(uint64) }`：`subscription_id` 仅 SUBSCRIBE 成功有效（0=无）；`topic_seq` 仅 PUBLISH 有效（受理序号，命中去重槽的重发返回同一份）；`status` 取既有词表 + 新增 `REJECT_SUBSCRIBERS`（订阅数达上限，key 既有订阅不受影响）；撞 key 的裁决为内部 Outcome `REJECT_TYPE_MISMATCH`，**线路映射沿用 v3+ 形状拒绝判例 = `INVALID_REQUEST`**（`StatusCode` 词表无该值，既有全部家族误用同样以 `INVALID_REQUEST` 送达）。

### D4 新推送类型 `TOPIC_MESSAGE = 20` + `subscription_id` 关联

不复用 `AWAIT_NOTIFY`（广播交付无"原请求"可 ref，塞 ref 会把订阅伪装成一次性请求）。`TopicMessage { key=1, subscription_id=2(uint64), topic_seq=3(uint64), publisher_sid=4(uint64), publish_ts_ms=5(int64), payload_bytes=6(bytes) }`，Envelope 槽 44。推送纪律延伸"请求标识与关联"：`Envelope.request_id MUST 0`；`TOPIC_MESSAGE` 是首个**无 ref** 推送——路由键 `subscription_id`（Leader 在 SUBSCRIBE 回执分配，registry 本地单调；重订阅得新 id，SDK 句柄内部重映射）。（session, key）唯一 → 同会话同键重复 SUBSCRIBE = 幂等覆盖登记、返回同一 `subscription_id` 形态（SDK 自动重订阅路径的实现基础）。

### D5 PUBLISH 每会话去重槽，受理语义 = 已入 fan-out

registry 内 `session → { op_seq, topic_seq }`（恒最近一次，判例 v7 D5 每会话槽；不双扇出即达成）。应答 `OK` 语义 = **已受理并已入 fan-out**，不承诺任何订阅者已收到；重发命中槽不二次 fan-out（即便首 fan-out 已部分丢失——至多一次的自洽面）。跨换主重试可能双投（槽不存续）：契约声明消费侧幂等。无 `max_fanout` 类应答字段（扇出数是运行态非请求语义）。

### D6 弱背压 = drop-newest，两级缓冲

服务端每订阅应用层有界队列（`max-subscription-buffer`，默认 256 条）：入队即 fan-out 完成点，出队侧 `writeAndFlush` 至该订阅连接；队满**丢最新一条**并计数（既有缓冲照常交付）。SDK 本地二级缓冲同策略。**不阻塞 Publisher、不断开订阅**——JDK `SubmissionPublisher` overflow-close 判例显式不采纳：本拓扑订阅者 multiplex 在会话连接上，断连 = 会话死亡 = 该会话全部持锁释放，灾难性耦合（这是与 JDK 场景的本质差异，契约声明）。备选：drop-oldest（否决：破坏"已交付前缀连续"的 gap 推断面）、按 `channel.isWritable` 反压 Publisher（否决：一个慢订阅者拖停全键广播，广播面被最慢者钳制）。

### D7 交付序 = 同 term `topic_seq`，gap 即丢弃推断

`topic_seq` 由 Leader registry 按 key 单调分配——**仅 term 内有序**：换主后 term 重置、seq 重新起算（不持久化，D1 推论；持久化需日志，与 D1 矛盾且 gap 精确性不值得一条目）。单订阅内交付序 = seq 升序（每订阅独立队列）；跨 Publisher/跨订阅无全局序。客户端 `droppedCount()` = 本地缓冲溢出计数 + **同 term 内 seq gap 推断**（跨 term 基线重置，不累计）；契约声明"有损不通知，仅计数可查"。

### D8 撞 key：Leader 本地只读探测，契约声明"一 key 一形态"

SUBSCRIBE/PUBLISH 在受理节点只读探测 LockTable（`key` 已被锁/Semaphore/Latch/Barrier/ATOMIC/QUEUE 家族占据 → 内部裁决 `REJECT_TYPE_MISMATCH`，线路送达 `INVALID_REQUEST`，判例引擎形状拒绝映射）。尽力而为：探测与既有家族的建条目路径无原子性（竞态窗声明入契约），反向（先 topic 后队列）无法拒绝——队列建条目不知 registry 存在（registry 不在 core）。故"一 key 一形态"对 topic 从机制互斥**降级为应用契约**，与既有家族互拒机制的差异 MUST 在规格、契约 Javadoc、指南三处显式声明。备选：registry 入 core 供 LockTable 反查（与 D1/core 零改动矛盾，否决）；完全正交不探测（否决：撞名时观察面与语义双重混乱，排查成本高，探测成本仅一次本地 map 读）。

### D9 编号冻结表（新增面一次性钉定）

| 面 | 新增 | 编号 |
|---|---|---|
| `MessageType` | `TOPIC_OP` | 19 |
| `MessageType` | `TOPIC_MESSAGE` | 20 |
| `Envelope.payload` | `topic_op_request` / `topic_op_response` / `topic_message` | 42 / 43 / 44 |
| `StatusCode` | `REJECT_SUBSCRIBERS` | 13（第 14 值） |
| `LockType` / `QueueOp` 同族枚举 | 无新值 / 新增 `TopicOp{SUBSCRIBE,UNSUBSCRIBE,PUBLISH}` 0–2 | — |
| `RaftEntryType` / `raft.proto` | **零新增**（止于 13） | D1 |
| `SnapshotLock` | **零扩展** | D1 |
| 管理面 | `AdminSummaryResponse.topic_entries`、`AdminKeyInfo.topic_subscribers`、`AdminKeyDetailResponse.topic_subscribers` + `repeated AdminTopicSubscriberInfo topic_subscribers_info`（`{session_id, subscription_id, subscribed_at_ms}`）各续接所属消息下一空闲字段号；`family` 词表增 `topic` | 实现期按现表续接 |

既有编号与语义零变更；握手三处常量 7→8（`OpenLatchServer.PROTOCOL_VERSION`、客户端 `ConnectionManager`/`RequestMultiplexer`）。

### D10 配置与指标命名

新增 `openlatch.server.limit.max-subscribers-per-key`（默认 64，校验 [1,1024]，钳 fan-out 放大面）与 `openlatch.server.limit.max-subscription-buffer`（默认 256，校验 [1,65536]，钳每订阅服务端堆与写出队列）；启动日志打印项扩展。指标三个新命名点：`openlatch.server.topic.total{op,status}`（status 词表含 `REJECT_SUBSCRIBERS`；`QUEUED`/`DENIED` 线对 topic 不可达）、`openlatch.server.topic.dropped.total`、`openlatch.server.topic.subscribers.max`（抓取时刻单键订阅数峰值——与 `queue.depth.max`（等待者）/`elements.depth.max`（元素）三口径注释互引防混读）；`waiters` Gauge 不含 topic（无等待语义）。

### D11 客户端 SDK 面

`OpenLatchClient.newTopic(key)` → `OTopic { publish(byte[])/publish(String)/publishAsync、subscribe(handler) → OTopicSubscription、unsubscribe() }`；`OTopicSubscription { droppedCount(), isActive(), close() }`（close 幂等 = UNSUBSCRIBE）。PUBLISH 继承 `RemoteAtomicBase` 同 key 在途互斥 + 同 op_seq 重发车道（判例 v7 D13）；SUBSCRIBE/UNSUBSCRIBE 直发请求-应答车道（无挂起环）。handler **单订阅内串行回调**、SDK dispatcher 线程执行（绝不占 Netty EventLoop），异常吞并记日志不断交付链。**活跃订阅登记表挂 home 迁移钩子**：重连成功按（session,key）自动重发 SUBSCRIBE、内部重映射 `subscription_id`，对应用透明；该新非 ACQUIRE 车道的换主窗改道行为纳入 W11 观察面。

### D12 守卫测试两族（常驻）

`ReplicationGateway`/状态机路径的 topic 零条目断言（digest 一致 + 日志长度零增长 + follower 重启无重投）入 `StateMachineTopicTest`；`RejectCodecTableTest` 扩 `TOPIC_OP` 同型拒绝行（W10 判例常驻——NOT_LEADER 等拒绝 MUST NOT 呈"成功空应答"形态）；快照零增量夹具入 `SnapshotFamilyRoundTripTest` 同族。

## Risks / Trade-offs

- [fan-out 放大压 Leader IO（高频 publish × 多订阅者）] → 两级限额（D9/D10）钳定 + drop 计数可观测 + WATCHLIST 新行登记触发式评估（批量帧/请求门控为后手，另立 change）。
- [订阅泄漏（应用忘 close）] → 会话死亡三路回收是机制兜底；`topic.subscribers.max` gauge + `REJECT_SUBSCRIBERS` 拒绝线使泄漏可观测可钳制；E2E 断言回收后 registry 归零。
- [至多一次被误读为"至少一次"的现场歧义] → 契约声明区集中（Javadoc/指南 03/08/09），`droppedCount()` 与告警口径给出排查抓手；"死亡即退订"与队列"死亡不吞元素"刻意相反，防混读对照句随行标注。
- [撞 key 竞态窗] → 尽力而为声明入契约；观察面（LIST_KEYS 的 topic 行 vs LockTable 家族行）互斥呈现可事后判别。
- [换主窗丢量无界（选举慢时）] → E2E 断言"续收 + 丢量有界于选举窗 × publish 率"；指南 09 给判别路径（先查 home 重连日志再查 SUBSCRIBE 重放）。
- [drop-newest 与 JDK Flow 的 onOverflow 直觉冲突] → 契约显式声明不采纳 close-on-overflow 及其会话耦合理由。

## Migration Plan

1. 纯增量发布：服务端先升 v8（v≤7 客户端连接与行为逐项不变），随后 SDK 升 8；新客户端连旧 v7 服务端按既有"区间外握手拒绝"纪律失败——回滚窗注记与升级序沿 v3–v7 判例入指南 05/10。
2. 回滚至 v7 二进制：topic 无任何持久态（registry/缓冲/去重槽随进程灭），**窗口天然干净**（无队列式"回滚前清 key"要求）——该运维差异在指南 05 与回滚演练注记中显式记录。
3. 演练：`RollingRestartDrillIT`/`LeaderKillDrillIT` 负载段扩 topic 相（自动重订阅 + 无陈消息重投断言），`-Pdrill` 全量复跑由用户在非沙箱执行。

## Open Questions

- `ADMIN_LIST_SESSIONS` 是否增"订阅 topic 数"字段（与持锁/等待并列）——观察增益小，留实现期定，不阻塞任何工件。
- 控制台 topic 呈现复用锁列表行还是独立页——复用行足够，若需独立页后立 change。
