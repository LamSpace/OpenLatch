# Proposal

## Why

ROADMAP 三档候选原语 `OCondition`（JDK `Condition` 对应，"锁 key 下的服务端等待集，虚假唤醒承诺、signal 权限、与租约到期交互需专章设计"）经用户裁决提前立项（2026-10-05，需求信号未至、以预研立项完善契约设计）。技术上该原语的全部机制先例已就位——锁等待队列与队首重发（v1）、Latch/BARRIER 的"等待不入日志"（v3/v5）、队列双轨与就绪驱动（v7）、topic 零日志豁免与客户端双通道重挂（v8）——且它是二档以来第一个**不开辟新基建**的协调原语：await 折叠进 ACQUIRE 车道即继承全套幂等重发/换主迁移/提示改道纪律，signal 家族是纯 Leader 本地队列手术。立项先行还可验证"等待不入日志"判例族在"等待跨越复制面"形态下的成立边界，为三档另一候选（OPhaser）探路。

## What Changes

- 协议升至 **v9 门**：`MessageType` 新增 `CONDITION_OP = 21` 单一消息对（`ConditionOp = SIGNAL / SIGNAL_ALL / LEAVE` 三操作，判例 `TopicOp` 三操作词表；**AWAIT 不在词表内**）；`AcquireRequest` 新增 `optional string condition = 8`——**presence 即 await 模式**，本原语的核心结构裁决：等待挂起折叠进 ACQUIRE 车道而非独立消息（design D1 承载论据：复用等待-通知-重发闭环、`migrateWaitsTo` 车道迁移重挂、`NOT_LEADER` 提示字段、规则 7 队首重发授予、深度护栏，独立消息对则需复制整套车道语义）。握手接受区间升至 [1,9]（`OpenLatchServer.PROTOCOL_VERSION` 8→9，客户端 `ConnectionManager`/`RequestMultiplexer` 常量同步）；v≤8 会话携带 `condition` 字段的 ACQUIRE 或任何 CONDITION_OP → `INVALID_REQUEST` 消息级拒绝、不断连（判例 v3–v8 门）。**`LockType`、`StatusCode`、`RaftEntryType` 三词表零新增**——`raft.proto` 零触碰（`RaftEntryType` 止于 13 的证据链延伸：await 复用既有 `LOCK_ACQUIRE_ENTRY = 3`，command_payload 为请求消息序列化故新字段天然流通）。
- **半入日志裁决（本提案的核心分岔，design D1/D2 承载论据）**：await 经日志（释放锁是复制态变更，必须多数派；"释放+登记"同一条目临界区原子执行，消灭丢唤醒窗——若把释放拆成客户端逐步 RELEASE 则跨换主窗账簿撕裂）；**SIGNAL/SIGNAL_ALL/LEAVE 零日志**（纯 Leader 本地等待集→等待队列的位次搬运与推送，无复制态变更；v8"落地纪律第 2 条"豁免判例扩展为"signal 是事件不是状态"类目）。由此推导的换主分层语义钉死为契约：**等待是承诺**（await 条目重放即全量释放照常生效、登记表空由客户端重挂补登记——ACQUIRE 车道迁移机制免费兑现），**signal 是事件**（换主窗内不重放、无补偿，等待者以 timed await 自救）。
- **core 新建等待集形态**（`LockEntry` 扩展，非新条目类、非新家族——条件不是 key 形态，判例 topic 零 `LockType`）：`condition_name → 到达序等待队列` 的进程内等待集，Leader 易失、不入日志不入快照（`LatchEntry.awaiters`/BARRIER 在队位次同判例生命周期）。await 应用语义：**条件持有者全量释放**（当前 (会话,线程) 恰为持有归属则重入计数一步清零、触发既有队首通知纪律；非持有则释放零操作、登记照常——服务端宽容面，理由见下条权限裁决）+ `(session, request_id)` 幂等登记（同键重复到达=重挂非双登记）；等待人数与锁等待队列**合并计数**受 `max-queue-depth-per-key` 护栏钳制（超限 `OVERLOADED`，判例队列"等待满"分轨）。SIGNAL 取该集到达序队首一人搬运入既有 `waiters` 队列（候选人排除调用归属自身）、SIGNAL_ALL 全员按序搬运——signal 权限论证使搬运恒发生于锁被持时点，唤醒由后续释放/到期/会话关闭应用点的队首通知接力（事件驱动零新定时器，与 topic 即时性同相、对比 v7 延时 tick）；被搬运等待项由既有队首重发环授予（拿到**新租约凭证、重入一级**，与 JDK"唤醒返回时持锁一级"精确对齐）。清理三路：LEAVE 显式摘除（幂等）、会话死亡摘除（`SESSION_CLOSE` 双路，"死亡即退订"口径与 topic 同判例、"死亡不吞锁"——等待者无租约、死亡时不持锁，无账可碰）、换主 `clear()`（判例 `WaitQueue.clear()`）。
- **契约三专章**（ROADMAP 验收点逐一给结论）：①**虚假唤醒允许且不承诺杜绝**——来源清单（队首超时清扫促醒、换主重挂后 signal 已丢、LEAVE/SIGNAL 竞态）入接口级 Javadoc，guard loop 是调用方义务（与 JDK `Condition` 文档同型，此条是**语义保真**而非降级）；②**signal 权限服务端权威**：SIGNAL/SIGNAL_ALL 要求 (会话,线程) 恰为持有归属，否则 `NOT_HELD`（SDK 抛 `IllegalMonitorStateException`，JDK 同型异常）；**AWAIT 权限降级为 SDK 本地检查**（服务端宽容登记——为车道迁移跨会话重挂所必需，恶意/误用 ghost 受合并深度护栏与三路清理钳制，降级面显式声明）；空集/不存在条件的 signal = `OK` 无操作（JDK 对齐）。③**租约交互四分支**：await 受理即全量释放→看门狗对该 key 自然停摆（复用解锁簿记）；重新获取=新 token 重启看门狗；持有者进程死亡/租约到期→sweep 释放锁并唤醒**入队者**但**不代为唤醒条件等待者**（signal 仍是唯一通道，与 JDK"无人 signal 则永睡"对齐，推荐 timed await）；等待者自身无租约（LatchAwait 判例）。
- **支持面 v1 收窄**：REENTRANT/FAIR/SIMPLE 三种互斥归属形态可挂条件；READ/WRITE 携带 `condition` 字段或 SEMAPHORE/LATCH/BARRIER/ATOMIC/QUEUE 家族 key 上的 CONDITION_OP → `INVALID_REQUEST` 形状违例（JDK `ReadWriteLock` 条件在读者群重获取的微妙分支不值得第一版吃下，登记后续评估位）。FAIR 锁的位次承诺延伸声明：被搬运等待项按搬运时刻入队（唤醒 RTT 窗内后来者不越位——既有队首纪律天然保证），**不承诺** signal 的目标线程优先于其他入队者。
- 管理面/控制台/指标纯增量：LIST_KEYS/KEY_DETAIL 增 `condition_waiters` 计数与条件等待明细（`{condition, session_id, request_id, thread_id, registered_at_ms}`，Leader-only 口径挂 `wait_queue_leader_only` 同源标志；Follower 如实零读）；SUMMARY 不新增条目计数（等待者总数 Gauge 口径**计入**条件等待者——它们是等待，对照 topic 订阅者不计入的既有分轨）；family 词表**零新增**（条件骑 LOCK 家族，对照 topic 需新观察词表的差异）；控制台锁详情页增条件等待区段。指标新命名点 `openlatch.server.condition.total{op,status}`（op ∈ signal/signal_all/leave；await 计数落 `acquire.total` 既有线——它就是 ACQUIRE 生命周期）与 `openlatch.server.condition.waiters.max`（抓取时刻单键峰值；**四口径注释互引**：等待队深 `queue.depth.max`/元素 `elements.depth.max`/订阅 `topic.subscribers.max`/条件等待 `condition.waiters.max`）。
- 客户端 SDK：`OLock.newCondition(String name)` → `OCondition { await() throws InterruptedException、await(timeout, unit) → boolean、signal()、signalAll() }`；**命名寻址替代 JDK 句柄身份**（`newCondition("x")` 重复调用绑定同一服务端等待集——跨进程可寻址是能力面而非缺陷，差异显式声明）；await 闭环实现 = 折叠 ACQUIRE 信封 + `AwaitTracker` 既有等待-通知-重发状态机原样复用（唤醒通知后以同 request_id 重发**清除 condition 字段的信封**转入普通获取、未获通知的自愈重发保持折叠信封幂等重登记、终止竞争补偿释放与重复通知以首个为准——全部既有纪律）；超时/中断统一 **JDK"返回时持锁"保真**：LEAVE(R) fire-and-forget + 常规获取重新入锁后才返回 false/抛 `InterruptedException`；`awaitNanos`/`awaitUntil`/`awaitUninterruptibly` 不提供（差异清单声明）。
- **守卫回归两族（常驻条款）**：复制边界断言入 `replicated-state-machine`（await 条目恰一条既有类型条目、signal 家族零条目、跨副本 digest 一致、重放全量释放幂等、登记表随 term 清零无 ghost 重放）；快照零增量断言入 `snapshot-recovery`（N 等待集 × M 条件等待者负载下快照字节与同状态无负载基线逐字节相等——等待集不在快照面，`SnapshotLock` v9 零扩展即编号证据）。`RejectCodecTableTest` 扩 CONDITION_OP 同型拒绝行（W10 判例常驻）。
- 测试：`ConditionGatingTest`（v8 会话两类新形消息级拒绝不断连 + 形状互斥矩阵 + 家族违例）；core 判定矩阵（登记幂等/合并护栏/搬运 FIFO/空集无操作/LEAVE 三路清理/重放全量释放/条件持有者一步清零/ghost 登记钳制）；`ClusterConditionTest` E2E（生产-消费闭环含谓词守卫、signalAll 全员醒来互斥串行、跨条件隔离、FAIR 位次、换主重挂+signal 不重放契约的有界断言、kill 持有者进程等待者不受扰、会话死亡 registry 归零）；客户端 Scripted + `ClientConditionIT`（超时返回持锁、中断、虚假唤醒 guard loop 惯用、双进程 condition_name 寻址）；协议冻结扩 v9 编号表与 presence 往返；`StateMachineConditionTest`；管理/词表/控制台/指标夹具扩条件维；演练负载段扩 condition 相（RollingRestart/LeaderKill）、`BenchmarkMain` 扩 condition 相基线。
- 文档：ROADMAP **决策记录补三档立项裁决行**（档位规则变更先行记账，维护规约）+ 三档行状态→进行中；双语指南 zh/en 对称扩章（01 概念：条件等待集与命名寻址、signal 是事件/等待是承诺分层；03 SDK condition 章：guard loop 惯用法、超时返回持锁、与 JDK 差异清单；05 部署：零新配置行注记 + 回滚窗注记（v9 客户端连 v8 二进制服务端按既有区间外握手拒绝纪律**直接不可连**——升级序服务端先行；回滚至 v8 前在途 v9 会话重连即拒，条件等待随会话终结fail-fast，无持久态残留、无需清 key，对照队列回滚注记呈现）+ 09 排查（"await 不醒"三分法：signal 权限被拒/换主窗 signal 丢失/缺 guard loop）；07/08/10（v1–v9 矩阵）/术语表扩维；WATCHLIST 登记 W13（换主窗 signal 丢失率与条件等待集水位观察），与 W11 交叉引用（signal 家族为非 ACQUIRE 车道新成员）。
- 纯增量发布，无 **BREAKING**：v1–v8 既有能力行为不变；starter 不动（注解语义面向锁获取，判例 v6/v7/v8 Non-Goal）；发布面沿用"仅客户端 SDK 链上 Central"裁决。

## Capabilities

### New Capabilities

（无——全部落在既有能力面上。）

### Modified Capabilities

- `wire-protocol`: v9 门与握手常量；`CONDITION_OP` 消息对与三操作词表、`AcquireRequest.condition` presence 折叠、形状互斥矩阵与家族违例、`AWAIT_NOTIFY` ref 名单经 ACQUIRE 身份天然覆盖（零变更声明）、`StatusCode`/`LockType`/`RaftEntryType` 三零新增的编号证据条款、管理面条件字段。
- `core-lock-engine`: 条件等待集与 await 释放折叠——临界区原子性、条件持有者全量释放幂等、`(session, request_id)` 重挂幂等登记、合并深度护栏、SIGNAL/SIGNAL_ALL 搬运（通知由释放/到期/会话关闭接力）、LEAVE/死亡/换主三路清理、FAIR 位次延伸。
- `lock-server`: v9 门控（ACQUIRE 带字段与 CONDITION_OP 两形）、signal 家族 Leader 直受理与 Follower 同型 `NOT_LEADER` 零副作用、await 走 ACQUIRE 车道（提交通道与 NOT_LEADER 提示沿用）、结果→状态码映射延伸（NOT_HELD 权限线、OVERLOADED 合并护栏线）。
- `replicated-state-machine`: 条件复制边界——await 恰一既有类型条目、释放+登记应用语义与重放幂等、signal 家族零日志、digest 跨副本一致、登记表 Leader 易失随 term 清零。
- `snapshot-recovery`: 快照内容完整性延伸（条件等待集不入快照、`SnapshotLock` v9 零扩展证据条款、恢复后等待集为空客户端重挂照常服务）；载荷快照尺寸条款扩条件维零增量守卫。
- `client-sdk`: `OCondition` 公开契约（降级/增强/保真三清单：虚假唤醒 guard loop 义务、AWAIT 权限本地化降级、SIGNAL 权限服务端权威、超时/中断返回持锁保真、命名寻址替代句柄身份、换主窗 signal 无承诺、await 至少一次 RTT 成本）、`OLock.newCondition(name)`、等待-通知-重发闭环条款延伸承载条件等待、握手升 9。
- `admin-observability`: KEY_DETAIL/LIST_KEYS 条件等待读数（计数+明细，Leader-only 口径、Follower 如实零读）、`waiters` 合计口径计入条件等待者、观察零扰动延伸。
- `admin-console`: 锁详情页条件等待区段呈现（等待位次与会程；无内容外发面——条件名即寻址文本）。
- `metrics-observability`: `condition.total{op,status}` 与 `condition.waiters.max` 单一命名点、四口径命名避让注释互引、`waiters` Gauge 计入条件等待者口径。
- `user-documentation`: 双语指南 condition 章、三专章契约表述、排查三分法、v1–v9 矩阵与升级序、W13 运维口径、术语表。

## Impact

- **openlatch-protocol**：`openlatch.proto`（`MessageType` 21、`ConditionOp`、`ConditionOpRequest/Response`、`Envelope.payload` 槽 45/46、`AcquireRequest.condition = 8`、管理面条件字段）纯增量；`raft.proto` **零触碰**（既有 `LOCK_ACQUIRE_ENTRY` 载荷透传新字段）；契约冻结测试扩 v9 面；`ProtocolCodecTest` 扩 presence 往返。
- **openlatch-core**：`LockEntry` 增条件等待集与 await 折叠/搬运/摘除方法（条目锁内临界区）；`AcquireCommand` 增 `condition` 分量；新命令/结果记录（`ConditionOpCommand`/`ConditionOpResult` 等）；`CoreEngine` 门面 `acquire` 折叠分支与 `conditionOp`；明细观察面/`waiterCount` 口径扩；恢复路径零改动（等待集不入快照、无定型账簿跨 term）。
- **openlatch-server**：`OpenLatchServer.PROTOCOL_VERSION` 8→9；`RequestDispatcher`/`ClusterRequestHandler` v9 门与 CONDITION_OP 分发（Leader 本地直受理、Follower 同型拒绝）；`ReplicationGateway` 会话关闭钩子扩等待集摘除；`QueueReadyDriver` **零改动**（唤醒全为事件驱动，无新定时器）；`AdminRequestHandler`/`ServerMetrics`/控制台断言扩展；**无新增配置项、无新增驱动定时器**；`ShadowTable` 序列化面零扩展（等待集非镜像对象——无复制来源可镜像；其折叠释放的收账走既有 apply 释放镜像通道，新增 `releaseFully` 归属整体摘除法仅为收账入口）。
- **openlatch-client**：`OCondition` 契约与 `RemoteCondition` 实现、`OLock.newCondition`；`RemoteLock` 内部暴露信封折叠构造点（await 模式 ACQUIRE 经既有 `AwaitTracker`）；握手常量 8→9；`ClientMetrics` 状态码归线扩 CONDITION_OP。
- **openlatch-console**：条件等待区段呈现。
- **openlatch-spring-boot-starter**：不动。
- **测试与基准**：上列测试矩阵全族；`-Pdrill` 全量复跑（含 condition 相）；`BenchmarkMain` condition 相基线入 `target/benchmark/`（signal 受理 ops/s、await 全闭环时延）。
- **文档**：`docs/guide` zh/en 对称扩章；ROADMAP 决策记录行+三档行状态；WATCHLIST W13 登记与 W11 交叉引用；契约 Javadoc（`check-source-citations.sh` 门禁）。
- **发布面**：protocol/client 构件纯增量；升级序服务端先于 SDK（判例 v3–v8：v9 客户端对 v8 服务端区间外握手即拒，不存在旧服务端误执行新语义的窗口）。
