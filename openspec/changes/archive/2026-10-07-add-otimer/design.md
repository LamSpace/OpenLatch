# Design

## Context

见 proposal.md - Why（三档立项复刻裁决、覆盖矩阵核验、机制先例就位清单）。当前状态约束：

- 协议门已至 v10（`MessageType` 止于 `PHASER_OP = 22`、`StatusCode` 止于 13、`LockType` 止于 14、`RaftEntryType` 止于 14、`Envelope.payload` 已用至槽 48、`SnapshotLock` 已用至字段 41、`AdminKeyInfo` 至 21、`AdminKeyDetailResponse` 至 37、`AdminSummaryResponse` 至 14）。握手三处常量：服务端 `OpenLatchServer.PROTOCOL_VERSION = 10`、客户端 `ConnectionManager`/`RequestMultiplexer` 各一。
- 到期语义判例族两端已在正文：**租约到期是状态迁移故必须进日志**（v3 `LeaseExpiryDriver`：Leader 扫描提交 `LEASE_EXPIRE_ENTRY`、token 幂等、在途抑制）；**延时唤醒是可见性提示故零日志**（v7 `QueueReadyDriver` 类级 Javadoc 原文公式；消费终判恒在应用点，`ODelayQueue` 契约句"绝对到期时刻随复制日志确定化，Leader 切换与副本回放不改判"）。
- 等待/事件零日志判例族：v3/v5/v7"等待不入日志"、v8"signal 是事件不是状态"豁免类目、v9 双拓扑、v10"等待是订阅不是状态"+ 分片保活重挂"谓词在复制账簿跨换主无损自愈"。死亡语义已有两形态成文：Barrier"死亡即破障"（v5）、Phaser"死亡=隐式摘除配额且已到场事实不撤销"（v10）；队列形态为对偶第三句"死亡不吞元素"（v7）。
- 等待队列与推送桥（`WaitQueue`、`AWAIT_NOTIFY`、`AwaitTracker`/`queue_position`、`headReplyTimeoutMs` 清扫）、`ShadowTable` 镜像、管理四消息、指标框架、控制台、演练 harness 与基准 harness 均为现成装配位；v10 过程修复教训在案（分片重挂误换 request_id 致到场重复计数——同-rid 回归用例为必做项）。
- W10 已销账（拒绝码形同型常驻门禁 `RejectCodecTableTest`）；W11 观察非 ACQUIRE 车道换主窗改道收敛（v8/v9/v10 均有注记扩展先例）；W14 为 phaser 条目率观察项——timer 的"装载低频 + 到期零条目"与其构成对照证据。
- `FairOrderingSuite` 对 ATOMIC/BARRIER/TOPIC/PHASER 已有豁免正文判例。

## Goals / Non-Goals

**Goals:**
- JDK `Timer` 的可判定子集保真落地：定时单次触发、全体共见标记、到期前撤销、查询读数；死亡不撤钟作为一等契约成文（本原语相对进程本地 `java.util.Timer` 的核心增量）。
- 复制边界一句话可验：**每装载/撤销恰一条 `TIMER_OP_ENTRY`，等待/取消/查询零条目，到期恒零条目**——digest 一致与重放幂等由应用点折算 + 去重槽承载。
- 编号面一次性冻结（D10 表）：`StatusCode` 零新增、三词表证据线更替为常驻守卫、`SnapshotLock`/管理面纯续接。
- 复用最大化：零新推送类型（骑 `AWAIT_NOTIFY`）、单一新驱动类（判例 QueueReadyDriver 逐件同型）、直发车道零新语义、两枚新配置各司一职。

**Non-Goals:**
- 不做周期重挂（`schedule(task, delay, period)`）、不做任务载荷/回调执行、不做绝对时刻装载（三砍裁决见 D9，均登记差异声明）。
- 不做一 key 多钟并存/优先级闹钟堆（KeyedTimer 形态不采——一 key 一钟、代次互斥，与"一 key 一形态"家族纪律同构）。
- 不做 key 后缀寻址、多 timer 合一、载荷面（无 payload 字段，`AWAIT` 不携带任何内容）。
- 不动 starter、不改 `QueueReadyDriver`/`LeaseExpiryDriver` 本体、不动 `WaitQueue` 队首纪律本体（timer 等待非队首批授）。
- 不做到期投递的可靠补偿与线性化承诺（等待自愈靠重挂+谓词重评；读数 advisory、可见时刻随判定节点钟）。

## Decisions

### D1 到期=派生谓词零条目（半入日志），装载/撤销=变异入日志

账簿仅存 `{generation, armed, fire_at_ms}`；`marked = armed ∧ 判定时刻 ≥ fire_at_ms`。SCHEDULE/DISARM 每一变异恰一条 `TIMER_OP_ENTRY`（装载决定"未来某刻对谁可见"，是复制态——沿 v5/v10"迁移入日志"，落地纪律第 2 条不豁免）；**到期本身不产生任何条目**——它是复制数据与钟的纯函数，判例公式即 v7 `QueueReadyDriver` 正文"到期是可见性判定而非复制状态迁移"的类目化：**"到期不是迁移，是时间的兑现"**。与判例族两端的对照：`LeaseExpiryDriver` 不提交到期条目则锁永不释放（释放是破坏性迁移，必须复制）——timer 不提交到期条目则 nothing lost，因为没有任何位需要被置上，等待者重发时谓词对同一复制数据重评即自洽。备选（否决）：**到期入日志**（TimerExpiryDriver 提交 FIRE 位——否决三论据：①digest 面早已逐字节一致（账簿字节不含 marked），入日志买到的只是"判定时刻统一到 Leader"，代价是每钟一条目 + 换主窗漏提由新主再驱动（在途抑制/token 幂等整套复制）+ 到期延迟从 tick 级变 tick+复制链两级；②fire 与 SCHEDULE 的应用点折算给出同一时刻，两条目同义重复；③"时钟走过到期点日志恒零新增"成为反向守卫后，本原语的条目率面只剩显式用户操作——装载天然低频，与 W14 型热点条目形态绝缘）。查询/等待/取消零日志沿 v10 类目直接延伸。

### D2 单消息对五操作词表，AWAIT 为阻塞操作词

`TIMER_OP` 单消息对、`TimerOp` 五词（SCHEDULE/DISARM/AWAIT/CANCEL/QUERY），AWAIT 即等待操作词承载阻塞位（判例 v7 `take` 与 v10 `AWAIT_ADVANCE`——无"立即式 await"需求，JDK `Timer` 本无等待面，等待是我们补的订阅通道）。形状互斥矩阵钉在 `TimerOpRequest`：SCHEDULE 必携 `delay_ms ∈ [0, max-timer-horizon-ms]`、其余四操作恒 0；`CANCEL` 必携 `await_request_id`（指向被撤销等待项的原 request_id）、其余操作恒 0；矩阵外组合同型 `INVALID_REQUEST`、账簿与等待集零扰动（判例 v8/v9/v10 矩阵族）。备选（否决）：SCHEDULE 与 AWAIT 合词（否决——装载者与等待者常非同一会话，合词制造"装载即等待"的伪耦合；对照 phaser `ARRIVE_AND_AWAIT` 合词成立恰因到场与等待同主体）。

### D3 代次粘滞标记：round 语义由"粘滞 + 换代清零"承载，AWAIT 无代次入参

`generation` 自 1 起每次 SCHEDULE 严格 +1（单调、不取模、重启/换主不回退——观察值，非等待谓词输入）。`DISARM` 为**代终结**粘滞态：当代永不再可 marked，直至下一次 SCHEDULE 开新代。AWAIT 谓词 = "当代 `armed ∧ now ≥ fire_at_ms`"，**不携带已见代次**：粘滞使"fire 之后到达的等待者即刻共见"、换代清零使"重装载后的等待自然落入新一轮"——单次标记的 round 语义无需任何"已见"历史（对照 v10 `awaitAdvance(expected_phase)` 必须带参：phaser 相位是连续推进史，"已见 5 未见 6"不可由当值重建；timer 是二值标记，谓词当下重评即完备）。改期=换代清钟：在等旁观者以最新代为准——原等 T1 者可能因改期至 T2>T1 而多睡（契约声明句 + timed await 自救推荐，判例条件"无人 signal 则永睡"运维提示同轴）；因改期至 T2<T1 而提前醒（无害方向）。了结无需了结记录：AWAIT 重发谓词幂等重评（OK/DENIED/续 QUEUED 三态自决），比 phaser 的有界了结记录再省一格——登记为"等待是订阅"类目里最薄的一档。

### D4 死亡不撤钟：触发绑定 key，与 Barrier/Phaser 成三形态对照表

`SESSION_CLOSE` 对 timer 账簿零扰动——创建者死、钟照响（本原语的核心存在价值："定时"承诺的是**与发起方生命无关的**未来事件；队列判例"死亡不吞元素"同轴：元素/标记一旦装载即脱离会话归属）。等待位按 (会话) 摘除（订阅层恒随会话灭，与 v10 同点）。对照表成文（规格/Javadoc/指南三处）：Barrier——死亡即破障（定型应到集合不可逆，缺席饿死全场，破局）；Phaser——死亡隐式摘除配额（动态应到集合，摘除即完备）；Timer——死亡零扰动（无应到集合概念，触发不依赖任何在场者）。备选（否决）：死亡撤钟（否决——把本原语降级为"带 RTT 的客户端 sleep"，创建者一死全体等待者永睡，恰是需求要消除的形态）；死亡后等待者随账簿存续续等（等待位已摘除，SDK 重挂由会话生命周期裁决——不引入"会话灭而订阅存"）。

### D5 唤醒双源：到期靠 tick 扫描（零日志），撤销靠 apply 事件（即时）

到期唤醒：新类 `TimerReadyDriver`，判例 `QueueReadyDriver` 逐件同型——Leader 角色短路（判定在 gateway 内再核一道防时序窄窗）、单守护线程固定周期 `timer-ready-tick-ms`、**当选立即首扫**（换任窗漏扫补偿——挂起者随换主清零重挂，其"挂起时已到期"存量需首扫；判例两驱动同句）、只对有挂起等待者且当代 `PENDING ∧ now ≥ fire_at_ms` 的 key 全员推 `AWAIT_NOTIFY`、MUST NOT 提交条目/MUST NOT 改变复制状态（误唤醒由回执重评自愈）。撤销唤醒：DISARM 的 apply 点事件驱动即时唤醒全体在等者（不等 tick——撤销是已提交变异，等待者的 DENIED 终态无钟依赖；唤醒源与账簿迁移同关键区，判例 v9 D4"账簿迁移→唤醒筛选单锁域"论证复用）。两类唤醒共用 `AWAIT_NOTIFY`（ref=等待项 request_id），零新推送类型。**到期不广播、无等待者不扫描**：fire 的"发生"无需通知任何不在场者（谓词恒可重评），唤醒面仅服务于在等订阅。

### D6 装载去重槽：每会话单槽、覆盖式，判例 v7 交付槽

`(session) → {request_id, op, 回执回声}` 单槽：同 (会话, request_id) 重发命中即回放回声（同代次、不双换代、不双改态）；新装载覆盖旧槽——旧 rid 的迟到重发在覆盖后按新变异执行（声明竞态，判例 v7 队列槽覆盖语义与 v5 D7 窗口口径同族）。DISARM 亦经槽（幂等：已 DISARMED 再 DISARM → OK 回声零迁移，v9 LEAVE 判例延伸）。备选（否决）：(会话, request_id) 窗口集（否决——装载低频且单会话在途装载本应至多一笔，单槽即完备；phaser 到场需集因 party 并发到场，timer 无对应形态）。

### D7 StatusCode 零新增：`DENIED` 首次作 timer 终态可达

horizon 越界/形状违例/无条目 → `INVALID_REQUEST`（判例 lease 越界、v10 `REJECT_NO_ENTRY` 线路映射）；等待深度超限 → `OVERLOADED`（骑 `max-queue-depth-per-key`，v7/v9/v10 轨）；到期了结 → `OK`；撤销终态了结 → `DENIED`（既有码值复用——队列"元素不可满足的立即式"与 timer"当代标记不可再满足"同构语义："本次等待不可能成功"，不另立枚；`Outcome.REJECT_TIMER_NO_ENTRY`/`REJECT_TIMER_DELAY_OVER` 为 core 内部细分码）。新增枚举值的词汇扩张成本判例（v10 D8 存档句）沿用：`DENIED` 在 timer 线上可达、`QUEUED` 经 AWAIT 可达、`BARRIER_BROKEN`/`REJECT_SUBSCRIBERS`/`NOT_HELD` 恒不可达（可达面钉入 metrics 条款）。

### D8 读数 clock-relative 声明与呈现面分轨

`QUERY`/AWAIT 终态的 `marked` 由 Leader 判定时刻折算；**管理面不做折算**——各节点呈现原始 `{generation, armed, fire_at_ms}`（复制态逐字节一致，无时钟歧义），等待明细 Leader-only 如实零（双速呈现延伸，与 phaser 账簿/等待分轨同型）。跨节点"何时看见已响"偏差 ≤ 节点间时钟偏移：显式降级声明（Javadoc/规格/指南三处，纪律第 4 条），10-compatibility 与 09 排查各一句（"两台机器对同一钟的『响没响』可能在毫秒窗内不一致，判据以装载时刻与 horizon 为契约面"）。`getRemainingMillis()` 客户端本地差值（advisory，即刻过期是契约——判例 v10 `getPhase()` 语言事实句）。

### D9 三砍 Non-Goals：周期、任务、绝对时刻

①周期重挂不做：服务端周期自触 = 常驻重发变异，正是"到期零条目"裁决的反面（每条周期一次 `TIMER_OP_ENTRY`，W14 治理最惧形态坐实）；客户端 `while` + `schedule` 即等价（装载显式、条目率应用可控），差异声明入清单。②任务载荷/回调不做：服务端不执行用户代码（v10 D4 onAdvance 同轴论证——本处更简单：连"回报两阶段"的先例都不需要援引）；本原语只承载"标记位 + 唤醒"。③绝对时刻装载不收：客户端墙钟偏差注入账簿即污染谓词判据（相对延迟在应用点折算使偏差恒被服务端时钟吸收——v7 `offerDelayed` 收相对延迟不收绝对 `expiresAt` 的同轴理由，此处显式化）。另拒：一 key 多钟堆（与家族"一 key 一形态"纪律同构的多形态即多状态机，收益稀薄）。

### D10 编号冻结表（新增面一次性钉定）

| 面 | 新增 | 编号/依据 |
|---|---|---|
| `MessageType` | `TIMER_OP` | 23 |
| `Envelope.payload` | `timer_op_request` / `timer_op_response` | 49 / 50 |
| `LockType` | `LOCK_TYPE_TIMER` | 15（proto 与 core/client 枚举同步；仅 TIMER_OP 携带，ACQUIRE/其余车道携带属违例） |
| `KeyFamily` | `TIMER` | 新家族、新条目类 `TimerEntry` |
| `RaftEntryType` | `TIMER_OP_ENTRY` | 15——**v10"止于 14"编号证据更替为"止于 15"**，守卫基线同轮改（既有"止于 14、14 为 phaser 专用"断言改版本相对口径，见 wire-protocol/replicated-state-machine 增量） |
| `StatusCode` | **零新增** | `INVALID_REQUEST`/`OVERLOADED`/`QUEUED`/`DENIED`/`NOT_LEADER` 复用（D7） |
| 新枚举 | `TimerOp` 五值（SCHEDULE=0 / DISARM=1 / AWAIT=2 / CANCEL=3 / QUERY=4） | — |
| `TimerOpRequest` | `{ key=1, op=2, delay_ms=3(int64), await_request_id=4(int64) }` | 形状互斥矩阵（D2） |
| `TimerOpResponse` | `{ status=1, op=2, generation=3(sint64), armed=4(bool), fire_at_ms=5(sint64), marked=6(bool) }` | `remaining` 客户端差值导出 |
| `ApplyResult` | timer 回执字段续接 | 27 起：`timer_generation`/`timer_fire_at_ms`/`timer_armed`——`fire_at_ms` 应用点折算、经回执取回（判例 phaser 回执 23–26）；无 marked 位 |
| `SnapshotLock` | timer 字段续接 | 42 起：generation/armed/fire_at_ms/每会话去重槽（新消息 `SnapshotTimerSlot {session_id, request_id, op, echo_generation, echo_fire_at_ms}`——armed 由槽内 op 推导不另设位） |
| 管理面 | `AdminKeyInfo` 22–24（`timer_generation`/`timer_armed`/`timer_fire_at_ms`）；`AdminKeyDetailResponse` 自 38 续接（同三计数 + `repeated AdminTimerWaiterInfo {session_id, request_id, armed_at_ms}`）；`AdminSummaryResponse` 续接 `timer_entries`；`family` 词表 + `timer` | 实现期按现表 descriptor diff 复核续接位 |
| 配置 | `max-timer-horizon-ms` 默认 86400000、[1000, 604800000]；`timer-ready-tick-ms` 默认 200、≥10 | 与队列 tick 分轨（D11）；等待深度骑 `max-queue-depth-per-key` |
| 指标 | `timer.total{op,status}`、`timer.fired.total` | 不设单键峰值 Gauge（装载态二值——与五口径之辨的避让注记） |
| 推送 | **零新增** | `AWAIT_NOTIFY` 复用（ref=等待方 request_id） |
| 握手 | 三处常量 10→11；v≤10 `TIMER_OP` 消息级拒绝不断连 | 判例 v3–v10 门 |

### D11 配置与护栏：两枚新配置各司一职

`max-timer-horizon-ms`（默认 24h）：装载入带钳制唯一在受理点（应用侧不复核，配置漂移不撕裂账簿——v10 受理点唯一判例），防"永远不响的钟"占驻账簿；上限 7d 防 tick 扫描全账簿扫到陈旧 PENDING 的治理面。`timer-ready-tick-ms`（默认 200ms、≥10）：与队列 `ready-tick-ms` **分轨**——两原语唤醒精度各自独立可调、互不耦合调参（共用的诱惑在"都叫 tick"，实为不同治理对象：队列 tick 挂元素可见性、timer tick 挂标记唤醒；分名分值的判例是 `max-queue-capacity`/`max-subscribers-per-key` 各归各主）。深度护栏骑 `max-queue-depth-per-key`（等待就是等待合并口径，v9/v10 延伸第三家）。条目常驻不回收（maxKeys 兜底，v5 D9 判例）。

### D12 等待闭环与重挂：v10 全套逐件复用

`QUEUED → AWAIT_NOTIFY(ref=等待项 request_id) → 原 request_id 重发命中谓词重评 → OK/DENIED 终态了结`；三路回收（CANCEL/会话死亡摘除/换主清零）+ `headReplyTimeoutMs` 清扫兜底 ghost；SDK 分片保活 + 车道激活事件双通道自动重挂（判例 v8/v10）；重挂**恒以原 request_id**——谓词重评天然幂等，但装载去重槽若被误换 rid 重放会造双换代假象（v10 过程真缺陷教训直接命中本原语，同-rid 回归用例与实现同轮）。换主语义：账簿复制态存续、等待集清零、重挂即重评——已到期即刻 OK、撤销即刻 DENIED、未到期续挂（**跨换主无损**，与 v9 signal 丢失窗的对照句随文，与 v10 同侧证据再强化一句）。

### D13 测试与守卫布局（判定族到夹具的映射）

core `TimerEntryTest`（D1/D3/D4/D6 判定矩阵）+ `CoreEngineTimerTest`；`StateMachineTimerTest`（每变异一条目、等待/取消/查询零条目、**到期零条目反向守卫**、digest 逐字节含到期后查询窗、重放单换代、编号证据 15、换主等待清零重挂）；`TimerGatingTest`（v≤10 门、五 op 形状矩阵、horizon 钳制、家族互拒、单机闭环）；`ClusterTimerTest`（E2E：一到多同刻齐过、**kill 装载者钟照响**（头号验收，对标破障/摘除 E2E 形态）、改期双方向、DISARM 唤醒 DENIED、换主重挂了结含首扫补偿、同 rid 重发单换代、Follower 五词零副作用、并发装载竞态——最后写者赢且等待者终见当代判定）；`RejectCodecTableTest` 扩行（W10 常驻）；`SnapshotFamilyRoundTripTest` 扩 timer 维 + 尺寸落界 + 等待集零足迹对偶构造夹具；客户端 `RemoteTimerScriptedTest` + `ClientTimerIT`；协议冻结 v11 全量表 + presence 往返；`FairOrderingSuite` 豁免登记（v5 D6/v10 正文同款）；换主类断言一律有界总超时+eventually（v9 D12 纪律）；基准 timer 相入 `target/benchmark/`；演练负载段扩 timer 相（RollingRestart/LeaderKill，`-Pdrill` 用户非沙箱单轮）。

## Risks / Trade-offs

- [钟偏移被误读为一致性漏洞（"两台机器说法不一"）] → D8 声明句三处成文 + 管理面原始读数不折算的呈现纪律 + 指南 09 判读段；如生产出现强一致判据需求，后立 change 评估"提交读 marked"变体（沿 v4 GET 判例，方向明确、暂不采）。
- [改期使在等旁观者多睡（T1→T2 推后）] → 契约句"以最新代为准" + timed await 自救推荐（条件"无人 signal 则永睡"运维提示同轴）；E2E 钉双方向形态。
- [tick 唤醒滞后一窗（P50 精度 = tick/2 量级）] → ODelayQueue 同款契约句"到点唤醒精度为服务端 tick 级，正确性不依赖精度"；基准 timer 相给滞后分布基线；W15 登记（滞后/规模压力 → 时间轮/最小堆评估触发线）。
- [重挂误换 request_id 致装载槽假象双换代] → v10 真缺陷教训直接复用：同-rid 回归用例与实现同轮（tasks 单列）；槽覆盖语义本身入判定矩阵测试。
- [编号证据基线 14→15 更替被漏改（守卫假绿/假红）] → 更替与 `TIMER_OP_ENTRY` 引入同一提交落地，既有"止于 14"断言改版本相对口径、`--strict` validate + 常驻守卫红先验；tasks 单列条目。
- [快照字段续接错槽（42 起与后续家族冲突）] → 实现期以生成 descriptor diff 复核续接位，`SnapshotFamilyRoundTripTest` 逐字段往返钉。
- [DISARM 与到期竞态（tick 推 OK 与 apply 推 DENIED 交叠）] → 终态以等待者重发时刻的当值判定为准（DISARMED 优先于到期——代终结粘滞，谓词重评序天然裁决），判定矩阵与 E2E 各钉一形态。
- [v11 客户端连回滚后的 v10 服务端区间外握手即拒] → 既有纪律显形非新风险；指南 05/10 回滚窗注记延续（timer 有持久条目与快照字段——降级前确认无在途 timer 流量或接受 timer key 在旧二进制不可用，屏障/队列/phaser 同型）。

## Migration Plan

1. 纯增量发布：proto/编号/守卫基线 → core 条目与引擎门面 → server 门控/驱动/配置/管理/指标 → client SDK/握手 → console/文档；握手三处常量与服务端 `PROTOCOL_VERSION` 10→11 同提交。
2. 升级序服务端先行（v3–v10 判例）：v≤10 客户端对 v11 服务端行为逐项不变；v11 客户端对 v10 服务端握手即拒，不存在旧服务端误执行新语义的窗口。
3. 回滚窗口延续口径：`TIMER_OP_ENTRY`/快照 timer 字段对 v10 二进制是未知条目形态（现行未知条目 error 口径）——降级前确认无在途 timer 流量，或接受 timer key 在旧二进制不可用；对照 topic/condition"天然干净"与队列/phaser 同型并列声明（指南 05/10）。
4. 验证收口：全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` 基线不降；`check-source-citations.sh` 全源零命中；`-Pdrill` 由用户在非沙箱单轮复跑（含 timer 负载段，红先保 `target/drill-logs/`）。
5. 文档三账：ROADMAP 决策记录补立项裁决行 + 档位行状态流转与"覆盖即可"收窄表述；WATCHLIST W15 登记 + W11 v11 注记；指南双语八处对账。

## Open Questions

- `AWAIT` 是否需要 `awaitUninterruptibly` 对偶与 `awaitUntil(Instant)`（JDK 无 Timer 等待面，SDK 形状自定）——v1 不做（中断语义 + timed await 已完备），如生产出现需求后立 change。
- 管理面是否需要 `timer_pending`（armed 且未到期的全集群计数）进 SUMMARY——可由 LIST_KEYS `family=timer` 行遍历导出，v1 不加列（防口径表膨胀；如 W15 观察需要再补，不阻塞）。
- `DISARM` 是否应同时"清代"（generation 保持 vs 回卷）——当前裁决保持不回卷（单调即观察序），实现期若与快照断言冲突回看，不阻塞规格。
