# Design

## Context

见 proposal.md - Why。当前状态约束：

- 协议门已至 v8（`CONDITION_OP` 前最新 `TOPIC_MESSAGE = 20`、`StatusCode` 14 值、`LockType` 14 值、`RaftEntryType` 止于 13、`Envelope.payload` oneof 已用至槽 44）。
- 锁的获取/释放/续租/到期皆经 `LOCK_ACQUIRE_ENTRY`/`LOCK_RELEASE_ENTRY` 等既有复制条目（"经复制授予/排队裁决"语义）；**等待队列是 core 条目内的 Leader 本地态**（`LockEntry.waiters`、`LatchEntry.awaiters`），不入日志不入快照，换主随进程清零；`WaitQueue`（server 侧）承载深度护栏、通知时序与清扫。
- 客户端等待-通知-重发闭环（`AwaitTracker`）：QUEUED 应答挂起 → `AWAIT_NOTIFY(request_id_ref)` → **原信封同 request_id 原地重发** → 规则 7 队首重发命中授予；终止竞争/补偿释放/重复通知以首个为准全部成文。ACQUIRE 车道经 `migrateWaitsTo`/`replayOnHome` 跨换主/跨会话迁移重挂，`NOT_LEADER` 随附 leader 提示（v2 三应答字段）。
- 会话归属 `(session, threadId)` 为锁持有单位；重入计数 `writeCount` 一步一释；租约到期 sweep 释放并唤醒队首；看门狗按 `HeldLockRegistry` 条目存续自动续租。
- v8 已确立"落地纪律第 2 条（core 条目+Raft 命令路径）可整体豁免"的判例与编号证据纪律（`RaftEntryType` 止于 13）；v7 确立"等待不入日志、应用点回弹"判例族。
- W10 已销账（拒绝码形同型常驻门禁）；W11 在观察非 ACQUIRE 车道换主窗改道收敛——**本原语的 await 不新增车道**（折叠进 ACQUIRE），signal 家族为 topic 同型直发车道。

## Goals / Non-Goals

**Goals:**
- JDK `Condition` 的可判定子集保真落地：await 全量释放+重获取、signal 须持锁、返回时持锁（含超时/中断路径）、虚假唤醒允许。
- 丢唤醒窗在**同关键区**内消灭（释放+登记原子性），ghost 等待项有确定的三路收敛。
- 等待集零快照足迹、signal 家族零日志足迹——两族常驻守卫断言。
- 复用最大化：不新增客户端车道、不新增定时器、不新增配置、不新增 `StatusCode`/`LockType`/`RaftEntryType`。

**Non-Goals:**
- 不做 READ/WRITE 锁上的条件（读者群重获取分支另立评估位）；不提供 `awaitNanos`/`awaitUntil`/`awaitUninterruptibly`/异步 `awaitAsync` 族。
- 不做 signal 的可靠投递/补偿重发（signal 是事件不是状态——换主窗丢失即丢失，timed await 是应用侧自救面）。
- 不动 `raft.proto`、不动 starter、不动 `ShadowTable` 结构（await 的释放半程复用既有释放应用簿记，条件集本身无复制来源可镜像）。
- 不做条件等待者的持久化或跨 term 恢复（等待承诺由条目承载，唤醒服务由重挂承载）。

## Decisions

### D1 await 折叠进 ACQUIRE 车道，signal 家族独立 `CONDITION_OP = 21` 消息对

`AcquireRequest` 新增 `optional string condition = 8`——presence 即 await 模式；SIGNAL/SIGNAL_ALL/LEAVE 走新消息对 `CONDITION_OP`（`ConditionOp` 三值 0–2，判例 `TopicOp`/`QueueOp` 单消息对）。

论据（await 为何不另立 AWAIT 操作词）：await 的生命周期半程**就是**一次排队获取（QUEUED→通知→同 id 重发→队首授予），半程**就是**一次全量释放（复制态变更）——两半程的协议形态、幂等纪律、改道提示、深度护栏、簿记钩子在 ACQUIRE 车道全部现成。独立 AWAIT 消息意味着：要么重造客户端等待状态机（`AwaitTracker` 是锁专用栈），要么在复制面新增条目类型重造授予路径（凭证/租约/ShadowTable 镜像三处逻辑分叉，分叉即分歧风险）。备选（均否决）：AWAIT 入 CONDITION_OP 词表（否决——上述重复）；CONDITION_OP.AWAIT 仅做登记、释放由客户端先行 RELEASE×N（否决——跨换主窗账簿撕裂且释放-登记两步间存在真丢唤醒窗）。

**登记与释放的双拓扑落位（实现核对定稿）**：单机——折叠命令在条目关键区内原子执行 [若 (sid,tid) 为持有归属则重入一步清零+清租约+既有队首通知判定 → 按 condition_name 入等待集]，一体完成；集群——apply 点仅执行**释放半程**（引擎 release-only 应用方法，非持有零操作，恒经复制确定重放；`AcquirePayload` 内 `AcquireRequest.condition` 天然透传，`raft.proto` 零触碰），登记在 **Leader 受理预检点**先于提交完成（判例 v7 队列"预检登记、应用点回弹"）。两拓扑共享同一不变式：**登记先于释放可见**——预检登记时持有归属仍是 awaiter，第三方 SIGNAL 必被权限检查拒绝（NOT_HELD），signal 穿不过"已释放未登记"的窗。集群受理回执由 Leader 在应用点结果上改写为 QUEUED（提交后送达），客户端收到 await 回执即"登记已生效"。**唤醒重发协议纪律**：收到 `AWAIT_NOTIFY(ref=R)` 后重发信封 MUST 清除 `condition` 字段保持 request_id——await 阶段由 signal 终结，重发即普通排队获取（单机命中队首重发规则、集群走普通获取通道含快路径与提示）；未获通知的自愈重发保持原折叠信封走幂等重登记。SIGNAL 搬运候选人 MUST 排除调用归属 (sid,tid) 自身（集群预检窗内 awaiter 短暂"持有且在集"，自 signal 防御跳过；JDK 同款效果）。

### D2 半入日志裁决：await 恰一条既有类型条目，signal 家族零日志

await 必须经日志——释放是复制态变更（owner/writeCount/lease 变化，Follower 与快照必须可见），且"释放+等待"原子性只有单条目能承载。复用 `LOCK_ACQUIRE_ENTRY = 3`（`command_payload` 即请求消息序列化，新字段天然透传）→ **`raft.proto` 零触碰，`RaftEntryType` 止于 13 的证据链在 v9 继续成立**。

SIGNAL/SIGNAL_ALL/LEAVE 零日志：三操作只搬运 Leader 本地等待集→本地等待队列、发本地推送，不触碰任何复制态——v8"等待不入日志"（v3 起）与"受理与 fan-out 全在 Leader 本地"（v8）判例的合流，登记为"signal 是事件不是状态"类目。换主语义由此分层钉死：**等待是承诺**（条目重放=释放照常生效；集合清零由客户端 ACQUIRE 车道迁移重挂补登记，机制现成），**signal 是事件**（不重放、不补偿，丢失窗契约声明+timed await 自救）。

Follower 收到 CONDITION_OP：同型 `NOT_LEADER` 拒绝零副作用（无 leader 提示字段，判例 QUEUE/TOPIC 直发车道）；v≤8 会话的 CONDITION_OP 或带 `condition` 字段的 ACQUIRE：`INVALID_REQUEST` 消息级拒绝不断连（判例 v3–v8 门）。

### D3 权限分轨：SIGNAL 服务端权威、AWAIT SDK 本地权威

SIGNAL/SIGNAL_ALL 在受理点检查 (会话,线程) 恰为持有归属，否则 `NOT_HELD`（词表现成，判例释放校验）；SDK 对非持有调用抛 `IllegalMonitorStateException` 本地先行。空集/无此条件/锁条目不存在 = `OK` 无操作（JDK 对齐，ghost 无害面）。

AWAIT 的服务端裁决降级为"形状与会话合法性+合并深度护栏"，**不检查调用者持有**（否则跨换主重挂必挂：重挂时旧会话持有已随 `SESSION_CLOSE` 释放、新会话非持有者——重挂是登记半程的重演，不应要求权限）。误用/恶意 awaiter 的登记为 ghost（永无 signal 到达则随会话死亡/LEAVE/换主三路收口，人数受合并护栏钳制），且与"谓词竞态"不可区分本就是 JDK 并发常识面。降级理由与后果 MUST 三处声明（规格、Javadoc、指南）——判例"一 key 一形态对 topic 降级为应用契约"的显式降级同型。

丢唤醒窗消灭论证（对偶于权限检查）：唯一的 holder 即 awaiter，任何第三方在 await 提交前不可能通过 SIGNAL 权限检查（它不持有）；awaiter 自己 signal 自己所在的集属事件先序（JDK 同型）。故"先登记后释放"与"signal 权限"两条纪律合流后，同关键区原子性不再暴露竞态。

### D4 等待集住在 `LockEntry`（core），非 server 侧 registry

SIGNAL 的搬运终点是 `LockEntry.waiters`（core 侧裁决队列），跨 monitor 的搬运（server registry → core deque）需新同步协议且破坏条目锁"单关键区"不变式——故等待集为 `LockEntry` 字段：`LinkedHashMap<String, ArrayDeque<Waiter>>`（condition_name → 到达序队列；`Waiter` 复用现有记录）。回收三路全部收口在条目既有生命周期钩子：LEAVE（显式、幂等、按 (sid,R) 摘除——**同时顺带摘除同 (sid,R) 的已搬运歧义**：LEAVE 命中集合摘除，集合无则查 deque 未通知位？不查——LEAVE 后客户端必走常规重新获取，deque 残留由 `headReplyTimeoutMs` 清扫兜底，ghost 收敛）；`SESSION_CLOSE`/断连（`closeSession` 家族扫描沿用）；换主（进程本地态，无独立 clear——条目侧集合随 Leader 进程灭，同 awaiters 判例）。不进入快照序列化、不进入 `waiterCount` 既有口径？——进入 `waiterCount`（条件等待者是等待者，`waiters` Gauge 与 SUMMARY 等待者总数口径**计入**；对照 topic 订阅者不计入的既有分轨在规格侧随行对照）。Follower 的 apply 不登记（D1），故 Follower 集合恒空，管理面 Follower 读数如实为零——与 `wait_queue_leader_only` 口径同源，无需新标志字段。

备选：server 侧仿 `TopicRegistry`（否决——搬运跨模块同步复杂、`waiterCount`/明细观察面反而要新增 server→core 读钩子）；入 `ShadowTable`（否决——无复制来源可镜像，与 v7 队列镜像性质相反）。

### D5 多条件 = 命名寻址：`condition_name` 字段而非 key 后缀

条件身份 = (lock key, condition_name)。`condition` 字段（UTF-8 string，非空、≤ `maxKeyLength` 字节，超长/空串形状违例 `INVALID_REQUEST`）承载寻址；SIGNAL/SIGNAL_ALL/LEAVE 的 `ConditionOpRequest { key=1, op=2, condition=3, thread_id=4, await_request_id=5 }`——形状互斥矩阵：SIGNAL/SIGNAL_ALL 必携非零 `thread_id`、`await_request_id` 恒 0；LEAVE 必携非零 `await_request_id`（指向原 await 的 ACQUIRE request_id）、`thread_id` 恒 0；违例同型拒绝零扰动（判例 topic payload/op_seq 矩阵）。

否决备选 key 后缀（`key#cond`）：污染 key 空间与 LIST_KEYS 前缀过滤、撞"一 key 一形态"家族判别、管理面须反解析——命名寻址同时是跨进程能力面（JDK 句柄身份无法跨进程，我们的 `newCondition("x")` 重复调用/多进程绑定同一集合，契约显式声明此差异）。

### D6 超时/中断的 JDK 保真：返回时持锁

JDK 契约"await 无论被 signal、超时还是中断，返回（或抛出）前必须重新持有锁"整体保真：

- `await(timeout, unit)` 本地计时（判例 `wait_ms > 0` 客户端本地计时、`poll/offer(timeout)`）；超时 → `CONDITION_OP.LEAVE(R)` fire-and-forget（失败不致命，三路兜底）→ **常规阻塞获取**重新入锁 → 返回 `false`。
- 中断 → 同路径，重新入锁后抛 `InterruptedException`（不提供 awaitUninterruptibly 对偶，差异声明）。
- 竞态收敛表（LEAVE 与 SIGNAL 并发到达，条目锁内全序）：SIGNAL 先 → 已搬运，LEAVE no-op，通知/授予照常到 → await 以"被唤醒"收束或经清扫兜底；LEAVE 先 → 集合摘除，SIGNAL 空集无操作（事件丢失属契约面）。客户端侧"超时后通知到达/在途重发被授予"由 `AwaitTracker` 既有终止竞争+补偿释放纪律收口，**虚假唤醒面归零承诺、全部竞态收敛到 guard loop 义务**。

被唤醒的 await 重新获取到的是**新租约凭证、重入一级**（原 N 级重入在 await 时一步清零，返回后计数从 1 起——JDK 同款算术，指南示例钉死惯用法）；看门狗经既有解锁/加锁簿记钩子自动停摆/重启，SDK 无新状态。

### D7 深度护栏与计时：合并计数、零新配置、零新定时器

AWAIT 登记计入 `max-queue-depth-per-key`（"本 key 等待项"统一口径：等待队列 + 全部条件集；超限 `OVERLOADED`，判例队列等待满分轨）。SIGNAL 搬运恒发生于"调用者持有（锁必忙）"的时点（权限论证），唤醒由后续释放/到期/会话关闭应用点的队首通知接力——**事件驱动闭环，无需就绪扫描**（搬运后调用 `notifyHeadIfPossible` 保留为无害收口分支）。对照 v7 `QueueReadyDriver` 的 tick 需求：条件等待无"到期谓词"，就绪条件全部由状态迁移事件表达——零新定时器（与 topic 即时性同相）。`headReplyTimeoutMs` 清扫覆盖"通知未回重发"的 ghost 搬运项——既有纪律原样适用。

### D8 支持面：三种互斥归属形态

REENTRANT/FAIR/SIMPLE 的 key 可挂条件；READ/WRITE 请求携带 `condition` 字段 → `INVALID_REQUEST` 形状违例（v1 范围裁决，JDK 读者群语义差异登记后续评估位）；非 LOCK 家族 key 上的 CONDITION_OP 与带 `condition` 的 ACQUIRE → 既有家族不匹配拒绝的线路映射（`INVALID_REQUEST`）。FAIR 延伸声明：搬运项按搬运时刻入既有 FIFO（后来者不越位——既有队首纪律保证），**不承诺**被 signal 者优先于搬运前已入队者（JDK 非公平锁同款；公平锁的位次承诺只约束入队序）。

### D9 编号冻结表（新增面一次性钉定）

| 面 | 新增 | 编号 |
|---|---|---|
| `MessageType` | `CONDITION_OP` | 21 |
| `Envelope.payload` | `condition_op_request` / `condition_op_response` | 45 / 46 |
| `AcquireRequest` | `optional string condition` | 8 |
| 新枚举 | `ConditionOp { CONDITION_OP_SIGNAL = 0, CONDITION_OP_SIGNAL_ALL = 1, CONDITION_OP_LEAVE = 2 }` | — |
| `ConditionOpRequest` | `{ key=1, op=2, condition=3, thread_id=4, await_request_id=5 }` | — |
| `ConditionOpResponse` | `{ status=1, op=2 }`，status 取 `OK/NOT_HELD/NOT_LEADER/INVALID_REQUEST/SESSION_EXPIRED/OVERLOADED/INTERNAL_ERROR` | — |
| `StatusCode` / `LockType` / `RaftEntryType` / `raft.proto` / `SnapshotLock` | **零新增/零触碰/零扩展**（三词表止于现值即编号证据：21 类型/14 锁型/14 码/条目 13/快照 v9 无字段） | D1/D2 |
| 管理面 | `AdminKeyInfo.condition_waiters`(int32)、`AdminKeyDetailResponse.condition_waiters`(int32) + `repeated AdminConditionWaiterInfo condition_waiters_info`（`{ condition, session_id, request_id, thread_id, registered_at_ms }`）各续接所属消息下一空闲字段号；`family` 词表**零新增**（骑 LOCK 家族） | 实现期按现表续接 |

既有编号与语义零变更；握手三处常量 8→9（`OpenLatchServer.PROTOCOL_VERSION`、客户端 `ConnectionManager`/`RequestMultiplexer`）。`AWAIT_NOTIFY` 的 ref 名单无需变更——条件等待的 ref 恒指向 ACQUIRE request_id（D1 折叠的直接红利）。

### D10 观察面命名与口径

`openlatch.server.condition.total{op}`（`signal`/`signal_all`/`leave`）`{status}`（含 `NOT_HELD` 权限线与 `OVERLOADED` 护栏线；await 计数落 `acquire.total` 既有线，不另立）；`openlatch.server.condition.waiters.max`（抓取时刻单键条件等待峰值）——与 `queue.depth.max`（等待队深）/`elements.depth.max`（元素）/`topic.subscribers.max`（订阅）**四口径命名点注释互引防混读**（判例三口径互引条款扩为四）。`waiters` Gauge 与 SUMMARY 等待者总数**计入**条件等待者；管理面条件明细 Leader-only 口径、观察零扰动延伸（不得推进搬运/清扫时序）。

### D11 客户端 SDK 面

`OLock.newCondition(String name)` 返回 `OCondition`（接口纯增量；句柄无状态——服务端集合按名寻址，句柄可重复创建、跨进程等价）；`await()/await(timeout,unit)/signal()/signalAll()`，`IllegalMonitorStateException` 本地先行（对照 JDK）。实现 `RemoteCondition`：await = 经 `RemoteLock` 暴露的构造点生成带 `condition` 的 ACQUIRE 信封 → 入既有 `AwaitTracker`（同 R、同重发环、同终止竞争纪律）；signal 家族 = 直发请求-应答车道（无挂起环，判例 topic SUBSCRIBE 直发）。`ClientMetrics.statusCodeOf` 扩 CONDITION_OP 归线。握手常量升 9。异步对偶不提供（Non-Goal）。

### D12 守卫测试两族与既有回归锚关联（常驻）

`StateMachineConditionTest`：纯 await/signal 流量下三副本 digest 逐字节一致；每 await 恰贡献**一条既有类型**条目、signal 家族零条目；重启/快照追赶=释放已生效、集合为空、客户端重挂后 signal 恢复可达。`SnapshotFamilyRoundTripTest` 同族扩维：N 键 × M 条件等待者负载快照字节与无负载基线逐字节相等。`RejectCodecTableTest` 扩 CONDITION_OP 与带 `condition` ACQUIRE 的同型拒绝行（W10 判例常驻）。**W10 @Disabled 锚与 W11 关联声明**：条件等待复用 ACQUIRE 换主迁移机制=继承同一暴露面，E2E 换主断言一律用"有界总超时+eventually 续醒"形态，禁止裸时序断言（复跑纪律沿记忆档：单轮防压测伪证、红先保 drill-logs）。

## Risks / Trade-offs

- [换主窗 signal 丢失 → awaiter 永睡至超时] → 契约定层（等待是承诺/signal 是事件）+ 指南推荐 timed await + W13 登记观察（丢失率与水位）；不提供补偿重发（重发即状态化，与 D2 矛盾）。
- [AWAIT 权限服务端不查（D3 降级）被误读为"协议漏洞"] → 规格/Javadoc/指南三处显式声明降级与后果（ghost 有界、三路回收、护栏钳制）；SIGNAL 权威面不受影响。
- [LEAVE/SIGNAL 竞态吃信号（ghost 消耗一次 signal 事件）] → D6 竞态收敛表逐形钉死；观察面（`condition.total{NOT_HELD,OVERLOADED}` 线与明细水位）可事后判别；guard loop 是最终收口。
- [重入计数与"返回持 1 级"的算术误用]（应用外层 unlock 次数与 await 前不一致） → 与 JDK 完全同款非新增风险，指南示例钉死标准惯用法。
- [条件等待者计入 max-queue-depth 使纯等待大集群挤压正常获取位次] → 口径已在契约声明（统一"本 key 等待项"），超限显式 `OVERLOADED` 可观测；如需分轨限额后立 change。
- [FAIR 锁用户对"被 signal 者优先"的直觉] → 契约显式"位次=搬运时刻"，指南 03 章与 Javadoc 双呈现。
- [v9 客户端对回滚后的 v8 服务端区间外握手即拒] → 属既有纪律的显形而非新风险；指南 05/10 回滚窗注记写明"回滚前确认无活跃 v9 会话/应用先行降 SDK"。

## Migration Plan

1. 纯增量发布：服务端先升 v9（v≤8 客户端连接与行为逐项不变），随后 SDK 升 9；升级序与区间外握手拒绝纪律沿 v3–v8 判例入指南 05/10。
2. 回滚至 v8 二进制：等待集与搬运态均为进程易失、无快照足迹——窗口无残留状态可清（对照 topic"零持久态回滚干净"注记，条件另需会话面收口：活跃 v9 客户端重连即拒，fail-fast 显式）。
3. 演练：`RollingRestartDrillIT`/`LeaderKillDrillIT` 负载段扩 condition 相（重挂续醒 + 换主窗 signal 丢失的有界断言），`-Pdrill` 全量复跑由用户在非沙箱单轮执行。

## Open Questions

- `ADMIN_LIST_SESSIONS` 是否增"条件等待数"字段（与持锁/在队并列）——观察增益小，实现期定，不阻塞工件。
- 控制台条件区段是否需要按 condition 分组小计（当前明细列表已含 name 字段可分组呈现）——复用现呈现足够，如需后立 change。
