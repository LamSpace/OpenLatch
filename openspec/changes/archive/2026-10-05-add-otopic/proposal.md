# Proposal

## Why

ROADMAP 二档（小载荷）的收口项：`OTopic`（对应 JDK `Flow.Publisher`/`SubmissionPublisher` 的广播发布/订阅），载荷通道基建（v6）、挂起与推送唤醒、每会话去重槽（v7）、home 迁移与重挂车道（`migrateWaitsTo`/`replayOnHome`）全部先例就位，本原语不再开辟任何新基建，只需一条广播 fan-out 通道。它同时是项目首个**零复制日志**的原语——这一点与既有全部落地判例相反，裁决论据必须钉入规格（见下），否则后续演进会按"读写皆经提交"的惯性把它错误地塞进日志。

## What Changes

- 协议升至 **v8 门**：`MessageType` 新增 `TOPIC_OP = 19` 单一消息对（`Envelope.payload` 槽续接；op 词表 `TopicOp = SUBSCRIBE / UNSUBSCRIBE / PUBLISH` 三操作，无阻塞位——订阅恒立即回执）与 `TOPIC_MESSAGE = 20` **新服务端推送类型**（`request_id = 0`，载荷 `{key, topic_seq, publisher_sid, publish_ts_ms, optional bytes payload}`；不复用 `AWAIT_NOTIFY`——广播交付无"原请求"可 ref）。握手接受区间升至 [1,8]（`OpenLatchServer.PROTOCOL_VERSION` 7→8，客户端 `ConnectionManager`/`RequestMultiplexer` 常量同步）；v≤7 会话携带 TOPIC_* → `INVALID_REQUEST` 消息级拒绝、不断连（判例 v3–v7 门）。新增状态码**仅一个**：`REJECT_SUBSCRIBERS`（key 订阅数达上限时拒绝 SUBSCRIBE，消息级拒绝）；其余词表零新增——慢消费者"缓冲满"**不入拒绝面**，仅丢弃并计数。
- **零日志裁决（本提案的核心分岔，design D1 承载论据）**：SUBSCRIBE/UNSUBSCRIBE/PUBLISH 全部不产生复制日志条目——`raft.proto` 零触碰（`RaftEntryType` 止于 13）、`ApplyResult`/`SnapshotLock` 零扩展、**core 模块零改动**（无 `TopicEntry`、无 `KeyFamily` 新家族位、无 `LockType` 新值）。论据三条：① 回放即重投——topic 的"apply 副作用"就是推送本身，日志重放会把陈旧消息二次 fan-out（队列 PUT 进日志无害是因 apply 只改状态、唤醒走 Leader 本地态，此处无对应折中）；② 写放大——每条 publish 先经多数派再 ×N fan-out 即 `log × N`，W9 观察项以更恶性形态复现；③ 语义诚实——`Flow.Publisher` 本位是至多一次的协调面广播。落地纪律第 2 条（core 条目状态机 + Raft 命令路径）对本原语整体不适用，以本 change 显式豁免并登记判例。
- **订阅登记表为 Leader 本地态**（判例 `WaitQueue`：换主清零、客户端重挂）：server 侧新建 `TopicRegistry`——登记表、key 级 `topic_seq` 生成器、PUBLISH 每会话去重槽、缓冲水位与丢弃计数、`SESSION_CLOSE`（本节点断连与失联探针补发两条路径）与 UNSUBSCRIBE 的条目摘除（**防泄漏验收点**）。会话 id 按连接分配（换主重连必换 sid），日志化登记表在新 Leader 上全是死指针，零收益——此论证随 D1 一并钉入。
- **交付语义契约**（弱背压为显式契约，本提案裁决落地）：**至多一次**；慢消费者策略取 **drop-newest**——每订阅服务端在途缓冲（满则丢最新一条并计数，既有缓冲照常交付），SDK 本地二级缓冲同策略；**不阻塞 Publisher、不断开订阅**（JDK `SubmissionPublisher` overflow-close 判例显式不采纳：本拓扑里订阅者 multiplex 在会话连接上，断连 = 会话死亡 = 该会话全部持锁释放，灾难性耦合）。单订阅内交付序 = Leader `topic_seq` 升序；跨 Publisher 无全局序承诺。Publisher 重发在**同一 Leader 内**经去重槽恰好一次（判例 v7 D5 每会话槽）；跨换主重试可能双投（至多一次承诺降级处，契约声明消费侧幂等）。换主窗陈旧消息**不重放**，订阅存续由 SDK 自动重订阅保证。
- 载荷面沿用既有纪律：payload 为不透明 `bytes` 且 **PUBLISH 必携**（缺省 presence 即形状违例 `INVALID_REQUEST`，判例 v7 D4 rejectNull）；逐条钳制复用 `max-value-bytes` 入口裁决（判定唯一在接入层，v6 D5 防回放分歧——零日志下更无复核点）。
- **撞 key 裁决**：SUBSCRIBE/PUBLISH 由 Leader 本地**只读探测** LockTable，key 已被锁/信号量/屏障/原子/队列家族占据 → 内部裁决 `REJECT_TYPE_MISMATCH`、线路送达 `INVALID_REQUEST`（判例引擎形状拒绝映射；尽力而为、不入日志、无竞态保证）；"一 key 一形态"对 topic 从机制互斥**降级为契约声明**（应用侧保持），与既有家族互拒机制的差异 MUST 在规格与契约 Javadoc 显式声明。
- 新增服务端配置：`openlatch.server.limit.max-subscribers-per-key`（默认 **64**，校验 [1,1024]，钳 fan-out 放大面）、`openlatch.server.limit.max-subscription-buffer`（默认 **256** 条，校验 [1,65536]，钳每订阅服务端堆内存与写出队列）。
- 管理面/控制台/指标：key 详情增订阅读数（订阅数、订阅会话列表——Leader 本地读数，挂 `leader_only` 标志，判例 `wait_queue_leader_only`）；family 词表增 `topic`（纯观察口径，不指 core 家族位）；SUMMARY 增 `topic_entries` 计数线；控制台呈现订阅维（**已交付消息内容与全量订阅会话零外发**，防放大纪律沿 v7）。指标新增 `openlatch.server.topic.total{op,status}`、`openlatch.server.topic.dropped.total`、`openlatch.server.topic.subscribers.max` 单一命名点（与 `queue.total`/`elements.depth.max` 口径注释互引防混读）。
- 客户端 SDK：公开 `OTopic`/`OTopicSubscription` 契约（`publish(byte[]/String)`、`subscribe(handler)` 返回带 `droppedCount()`/`isActive()`/`close()` 的句柄、`unsubscribe()` 幂等）、`OpenLatchClient.newTopic(key)` 工厂；PUBLISH 继承 `RemoteAtomicBase` 同 key 在途互斥 + 同 op_seq 重发车道（判例 v7 D13 写车道）；监听器**单订阅内串行回调**、SDK dispatcher 线程执行（绝不占 Netty EventLoop）；**活跃订阅登记表挂 home 迁移钩子，重连成功自动重发 SUBSCRIBE**——新增一条非 ACQUIRE 车道，纳入 W11 收敛观察面；接口级 Javadoc 全量降级/增强清单（至多一次、drop-newest 两级缓冲、换主窗不重放、去重仅同 Leader、跨换主重试可双投、tick 无关的即时推送、常驻订阅的登记表成本）。
- **守卫回归两族（常驻条款，防未来误塞日志）**：零日志条目断言（topic 流量下跨副本 digest 逐字节一致、日志条目数零增长、follower 重启不收陈消息）入 `replicated-state-machine`；零快照增量断言（N topic × M 订阅快照尺寸与基线零 delta）入 `snapshot-recovery`；`RejectCodecTableTest` 扩 topic 拒绝形状行（W10 修复判例常驻——NOT_LEADER 等拒绝不得被成型为伪成功）。
- 测试：`TopicGatingTest`（v7 会话消息级拒绝）；`TopicRegistryTest` 判定矩阵（重复 SUBSCRIBE 幂等、UNSUBSCRIBE 回收、会话死亡 purge、缓冲满 drop-newest 计数、订阅上限拒绝、seq 单调、去重槽重放）；`ClusterTopicTest` E2E（N 订阅 fan-out 完整性与每订阅 seq 序、并发 Publisher、中途退订回收、kill 订阅者进程无泄漏且余者不受扰、慢消费者 drop 计数、换主自动重订阅续收且丢量有界）；`ClientTopicIT` + Scripted；协议冻结扩 v8 面与 presence 三形态往返；治理/配置/词表夹具；指标与控制台断言扩维；演练负载段扩 topic 相（RollingRestart/LeaderKill）、`BenchmarkMain` 扩 topic 相基线。
- 文档：双语用户指南 zh/en 对称扩章（01 概念+速查行 / 03 topic 章含监听器线程模型与两级丢弃 / 05 两配置行 + 回滚窗口注记——**topic 无持久态，回滚 v7 无残留状态，窗口天然干净** / 07 呈现 / 08 指标与告警（drop 率、订阅水位）/ 09 排查（换主后收不到消息的判别路径）/ 10 v1–v8 矩阵 / 术语表）；ROADMAP 二档行状态流转与"一 key 一形态"契约声明登记（并修正落地纪律节"下一个门为 v6"过期注记至 v8 起顺延表述）；WATCHLIST 登记新观察行（高频 publish × 多订阅者的 fan-out 放大：drop 率与 Leader 写出水位，触发 → 另立 change 评估批量帧/请求门控强背压），与 W11 交叉引用。
- 纯增量发布，无 **BREAKING**：v1–v7 既有能力行为不变；starter 不动（注解语义面向锁，判例 v6/v7 Non-Goal）；发布面沿用"仅客户端 SDK 链上 Central"裁决。

## Capabilities

### New Capabilities

（无——全部落在既有能力面上；core-lock-engine 亦零 delta，零日志裁决使其无需触及引擎契约，此点本身由 replicated-state-machine 条款承载。）

### Modified Capabilities

- `wire-protocol`: v8 门与握手常量；`TOPIC_OP` 消息对与 `TopicOp` 词表、`TOPIC_MESSAGE` 推送类型（`request_id=0`、无 ref 纪律）、`REJECT_SUBSCRIBERS` 状态码增量、payload 必携与 presence 违例形状、`max-value-bytes`/两新限额入口钳制与超限零投递；已发布编号冻结纪律延伸。
- `lock-server`: 单机/集群两路 TOPIC_OP 分发与 v8 门接线；`TopicRegistry` Leader 本地生命周期（订阅幂等、UNSUBSCRIBE/`SESSION_CLOSE`/失联探针摘除防泄漏、换主清零）；fan-out 本地连接投递与 drop-newest 缓冲裁决；撞 key 只读探测拒绝；结果→状态码映射延伸。
- `replicated-state-machine`: **零日志条款**——topic 全操作 MUST NOT 产生日志条目；topic 流量下跨副本 digest 一致；follower/重启回放对 topic 无感（守卫回归常驻）。
- `snapshot-recovery`: topic 状态 MUST NOT 入快照（`SnapshotLock` 零扩展）；N topic × M 订阅快照尺寸零增量守卫；旧快照兼容不受扰。
- `client-sdk`: `OTopic`/`OTopicSubscription` 公开契约（降级/增强清单：至多一次、drop-newest 两级、换主不重放、去重仅同 Leader、跨换主重试可双投、监听器串行与线程模型）、`newTopic` 工厂、PUBLISH 写车道幂等、home 迁移自动重订阅、握手升 8。
- `admin-observability`: key 详情订阅读数（订阅数/订阅会话列表，`leader_only` 标志口径）、family 词表 `topic` 观察口径、SUMMARY `topic_entries` 计数线。
- `admin-console`: topic 行与详情呈现（订阅维；已交付内容与全量订阅会话零外发）。
- `metrics-observability`: `topic.total`/`topic.dropped.total`/`topic.subscribers.max` 新命名点与词表（单一命名点纪律；与队列两口径命名避让注释互引）。
- `user-documentation`: 双语指南 topic 章、弱背压/丢弃运维口径、回滚窗口注记、v1–v8 兼容矩阵、术语表。

## Impact

- **openlatch-protocol**：`openlatch.proto`（`MessageType` 19/20、`TopicOp`/`TopicOpRequest`/`TopicOpResponse`/`TopicMessage`、`Envelope` 槽续接、`StatusCode` 一值）纯增量；`raft.proto` **零触碰**；契约冻结测试扩 v8 面；`ProtocolCodecTest` topic 消息与字节往返。
- **openlatch-core**：**零改动**（本提案的结构性声明；无 TopicEntry、无家族位、无 LockType 值）。
- **openlatch-server**：`OpenLatchServer.PROTOCOL_VERSION` 7→8；`ServerConfig` 两新限额与启动校验；`RequestDispatcher`/`ClusterRequestHandler` 门控+钳制+撞 key 探测+本地 fan-out；新类 `TopicRegistry`（判例 `WaitQueue` 装配位与生命周期钩子）；`ReplicationGateway` 会话关闭钩子接入 purge（既有 `SESSION_CLOSE` 应用路径）；`AdminRequestHandler`/`ServerMetrics`/控制台断言扩展。**无状态机 apply 改动、无快照改动、无新增驱动定时器**（topic 即时推送，无需 tick 唤醒）。
- **openlatch-client**：`OTopic`/`OTopicSubscription`/实现/工厂；`ConnectionManager`/`RequestMultiplexer` 握手常量 8；`TOPIC_MESSAGE` 入站分发与监听器 dispatcher；home 迁移重订阅登记表；PUBLISH 复用 `RemoteAtomicBase` 车道。
- **openlatch-console**：订阅维呈现。
- **openlatch-spring-boot-starter**：不动。
- **测试与基准**：上列测试矩阵全族；`-Pdrill` 全量复跑（含 topic 相）；`BenchmarkMain` topic 相基线入 `target/benchmark/`。
- **文档**：`docs/guide` zh/en 对称扩章；ROADMAP 状态行+纪律节过期注记修正；WATCHLIST 新观察行与 W11 交叉引用；契约 Javadoc（`check-source-citations.sh` 门禁）。
- **发布面**：protocol/client 构件纯增量、不动版本号与发布流程；无 **BREAKING**。
