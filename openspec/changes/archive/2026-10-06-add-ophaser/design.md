# Design

## Context

见 proposal.md - Why（三档提前立项复刻、用户裁决五项、机制先例就位清单）。当前状态约束：

- 协议门已至 v9（`CONDITION_OP = 21`、`StatusCode` 止于 13、`LockType` 止于 13、`RaftEntryType` 止于 13、`Envelope.payload` 已用至槽 46、`SnapshotLock` 已用至字段 34、`AdminKeyInfo` 至 18、`AdminKeyDetailResponse` 至 32）。握手三处常量：服务端 `OpenLatchServer.PROTOCOL_VERSION = 9`、客户端 `ConnectionManager`/`RequestMultiplexer` 各一。
- v5 Barrier 确立"到场是状态迁移、全量进日志 + 复制侧账簿/Leader 内存队列切分"（D7）与"离场即破障、死亡破障"契约；v7 队列确立每会话去重槽、阻塞位折叠与双"满"分轨；v8 topic 确立落地纪律第 2 条豁免类目、直发车道与双通道自动重挂；v9 条件确立"等待不入日志"在折叠形态下的双拓扑落位、合并深度护栏与"signal 是事件"分层。
- 等待队列与推送桥（`WaitQueue`、`AWAIT_NOTIFY`、`AwaitTracker`/`queue_position`、`headReplyTimeoutMs` 清扫）、`ShadowTable` 镜像、管理四消息、指标框架、控制台、演练 harness 与基准 harness 均为现成装配位。
- W10 已销账（拒绝码形同型常驻门禁 `RejectCodecTableTest`）；W11 观察非 ACQUIRE 车道换主窗改道收敛（v8/v9 均有注记扩展先例）；W13 为条件换主窗 signal 丢失观察项——本原语的等待谓词形态与其构成同表对照。
- `FairOrderingSuite` 对 ATOMIC/BARRIER/TOPIC 已有豁免正文判例。

## Goals / Non-Goals

**Goals:**
- JDK `Phaser` 可判定子集保真落地：动态注册/离场、到场与等待解耦、按已见相位等待、相位单调、到场相位回显；死亡语义（隐式摘除、不空转、不破相）作为一等契约成文。
- 复制边界一句话可验：**每变异恰一条 `PHASER_OP_ENTRY`，等待/取消/查询零条目**——digest 一致与重放幂等由去重槽+了结记录承载。
- 编号面一次性冻结（D9 表）：`StatusCode` 零新增、三词表证据线更替为常驻守卫、`SnapshotLock`/管理面纯续接。
- 复用最大化：零新推送类型、零新 registry 类（等待集住条目）、零新定时器、单一新配置、零车道新语义（直发判例族）。

**Non-Goals:**
- 不做 `onAdvance` 钩子、不做终止态（含 `isTerminated`/`forceTerminated`）、不做父子分层派生（三砍裁决见 D4/D5/D6，均登记差异声明）。
- 不做 parties 定型断言（Phaser 的本质是动态成员，定型断言与语义相反——与 Barrier 的 `parties` 断言面刻意分轨）。
- 不做 key 后缀寻址、多 phaser 合一、载荷面（无 payload 字段）。
- 不动 starter、不动 `WaitQueue` 队首纪律本体（phaser 等待非队首批授）、不改 `QueueReadyDriver`。
- 不做等待/查询的可靠投递补偿与线性化承诺（查询 advisory、等待自愈靠重挂+谓词，无补偿通道可设——也无需：状态在账簿）。

## Decisions

### D1 变异全入日志、等待与观察零日志（不豁免落地纪律第 2 条）

REGISTER/ARRIVE/ARRIVE_AND_AWAIT（到场半程）/ARRIVE_AND_DEREGISTER 每一变异恰一条 `PHASER_OP_ENTRY`；AWAIT_ADVANCE/CANCEL/QUERY 零日志、Leader 本地受理。论据：相位推进与应到集合是**决定未来合拢的复制态**——若到场本地裁决（topic 型零日志），换主后新 Leader 应到/已到账簿无从恢复，会出现"同相位双推进"或"饿死"两种不可接受形态；v5 Barrier"到场是状态迁移"判例的直接延伸。与 v8/v9 的对照：topic 无复制态可恢复（豁免成立），condition 的 await 有既有 ACQUIRE 可折叠（半入日志成立），phaser 两者皆无——等待不折叠（无释放半程、无 ACQUIRE 语义可借），状态必须折叠（账簿即状态）。落地纪律第 2 条对本原语**不豁免**，本行即类目对照的证据。备选（否决）：AWAIT_ADVANCE 亦入日志（否决——登记不改变任何副本可见态，入日志只造条目率，v3/v5/v7"等待不入日志"判例族）；查询经提交（见 D7）。

### D2 单消息对 + 七操作词表，blocking 位折叠

`PHASER_OP` 单消息对、`PhaserOp` 七词（REGISTER/ARRIVE/ARRIVE_AND_AWAIT/ARRIVE_AND_DEREGISTER/AWAIT_ADVANCE/CANCEL/QUERY），`bulkRegister` 折叠为 REGISTER 携带 count。论据：barrier 三消息对的分裂根源是 ACTION_DONE 两阶段挂账（v5 D3）——phaser 无钩子即无该相，单对足矣（v7 单对先例的回归）；等待形态收进词表（`AWAIT_ADVANCE` 与 `ARRIVE_AND_AWAIT` 的阻塞位折叠，判例 `put/offer` 与 `take/poll` 合一），`CANCEL` 对偶 v9 `LEAVE`。形状互斥矩阵（`parties`/`expected_phase`/`await_request_id` 三字段各字单操作独占，违例同型拒绝零扰动）钉在 `PhaserOpRequest`，presence 纪律 `expected_phase` 用 `sint64`+未主张 0 哨兵（等待谓词 `expected_phase = 0` 是合法值——JDK `awaitAdvance(0)`，故主张位与值域分离，见 wire-protocol 增量）。备选（否决）：`ARRIVE_AND_AWAIT` 拆为 ARRIVE + AWAIT 两请求（否决——两次 RTT 且"到场后通知先于 AWAIT 登记"的真竞态窗被造出来，折叠单请求在 apply/登记序上天然免疫）；AWAIT 骑 `AWAIT_NOTIFY` 独立挂起消息（判例不存在，v7 读侧即折叠词表先例）。

### D3 party 身份 = 匿名计数 + 会话配额，去重键 (session, request_id)

服务端不追踪 party 级身份（JDK `bulkRegister` 本就是匿名计数），仅记：`registeredParties` 总数 + **每会话配额**（死亡摘除与 `arriveAndDeregister` 扣减的归属域）+ 每在场周期 `(session, request_id)` 去重槽（重发/重放单计数）。论据：party 身份粒度（会话×线程）在 JDK 本不存在，强加会制造"bulkRegister 的身份从哪来"的无解问题；会话配额是最小可摘除单元——死亡清扫必须知道"这个会话应到几个"。配额严格化是本原语对 JDK 唯一的行为**收紧**：`arriveAndDeregister` 仅扣调用会话配额、零配额即拒（JDK 此处为未定义行为；分布式下不严格化则死亡摘除无账可平），差异入降级清单。跨会话重连的重发不承诺到场幂等（新 request_id 即新到场——在途到场不跨会话自动重放，判例 v5 声明竞态同口径）。备选（否决）：(会话,线程) 身份（否决——JDK 无此语义且 bulk 不可归因）；op_seq 车道级幂等槽（判例 v7 写车道——phaser 无在途互斥需求（无持有概念），request_id 幂等已覆盖重发面，不引入 op_seq 保持直发车道最简）。

### D4 onAdvance 钩子不做（显式不采纳 v5 D3 两阶段判例）

v5 的执行者回报两阶段（ACTION_DONE 消息对 + 动作待决挂账）在 barrier 成立是因为动作**每世代一次**且执行者可被破障收口；phaser 的相位合拢是级联高频事件——每相位多一跳挂账、执行者分区卡相的治理面又要把"死亡→破相"请回来（与 D5 的死亡裁决正面冲突），复杂度全部放大而收益（"动作先于全体放行"）在分布式下本不可线性化保真。替代惯用法成文：应用以 `arriveAndAwaitAdvance()` 返回的到场相位号判别"我是该相位最后一个到场者"（服务端回显 `arrived` 达界的隐含信息），本地执行相位动作——与他人醒转无先后承诺，弱于 JDK 但诚实；若需强排序应用面另以锁编排。登记为 Non-Goal + 降级清单首条。

### D5 死亡 = 隐式摘除配额、已到场事实不撤销、归零空转不终止（用户裁决③②）

`SESSION_CLOSE` apply 内：`registered -= 该会话配额`、配额行删除；`arrived` 不回退（到场是复制态事实——v7"死亡不吞元素"对偶）；摘除后同关键区执行合拢判定（应到集合缩小可即时推进——**死亡不空转的机制本体**）。三条对比锚：Barrier 选"死亡即破障"因为 parties 定型不可逆、缺席者饿死全场是结构性的；Phaser 的动态成员性使"摘除应到集合"成为温和且完备的解——泛化兑现点。"归零即终止"否决：v5 D4 拒绝粘滞破障的同一论证（一批死者卡死后续无辜参与者）在跨进程共享 key 下更成立；无钩子则无终止来源，`isTerminated`/`forceTerminated` 不入面。已到场者死亡后仍计入 `arrived` 的语义歧义（"幽灵到场"）显式声明为契约面：到场发生时应到集合含它，事实成立；其等待位（若有）随会话灭。备选（否决）：死亡→破相（否决——杀一人炸一锅，与 Phaser 动态语义矛盾，且使 D4 拒绝的执行者卡相治理复活）；已到场随死亡撤销（否决——`arrived--` 会把"已发生事实"改写为"未发生"，破坏重放确定性的因果叙事且制造重投窗口）。

### D6 三砍之外无第四让渡：分层派生与定型断言同拒

父子分层（JDK `Phaser(Phaser parent)`）不收：分层是千级 party 本地整型分片的可扩展性优化，分布式每到场一次网络往返、瓶颈在 RTT 不在计数器争用（与 ROADMAP 不做清单 LongAdder 行同轴论证）；一 key 一 phaser，`REJECT_TYPE_MISMATCH` 机制互斥照常覆盖"phaser key 当 barrier 用"误用。parties 定型断言不收：动态注册是 Phaser 的语义本体，断言面（`permits_total` 判例）与本体矛盾——`REGISTER` 就是注册不是断言，无"首次主张"概念（建条目由首条 REGISTER 隐式完成，count 任意）。

### D7 查询 Leader 本地零日志（用户裁决④，显式背离 v4 GET 判例）

`QUERY` 由 Leader 本地读 core 状态机当值账簿（判例 v8 撞 key 只读探测），不回显线性化承诺。与 v4 `ATOMIC_OP.GET`"读亦全量经提交"的分岔论据钉三处（规格/client Javadoc/指南 03）：①ATOMIC 读值是 CAS 序列的参与者（读到什么版本决定后续操作），phaser 的 `(phase, registered, arrived)` 是 advisory 观察——JDK `getPhase()` 同款"返回即刻过期"是语言事实，线性化读数不改变任何合法程序行为；②免日志把" parties×相位频率"的条目率暴露面砍到只剩变异（W14 口径）；③提交读在换主窗显式超时（v4 GET 的 W11 暴露面），本地读走同一改道纪律但查询语义本就该容忍改道失败。备选（否决）：提交读（否决——为不存在的线性化需求付条目率与 W11 面）；句柄本地缓存（否决——SDK 不造伪读数，缓存与否应用自由，读数恒来自应答回显）。

### D8 StatusCode 零新增：两超限骑 OVERLOADED、op 标签分轨（用户裁决⑤）

parties 上限与合并等待深度上限都以 `OVERLOADED` 承载，判别由 `phaser.total` 的 `op` 标签（`register` vs `await_advance`）与日志/管理读数承担。与 v8 `REJECT_SUBSCRIBERS` 立枚的分岔理由：订阅者上限拒绝当时需要与 topic 的"恒零拒绝码面"（`QUEUED`/`DENIED`/`OVERLOADED` 均不可达）做**可达性**区分——topic 若不立枚则拒绝码无家可归；phaser 的 `OVERLOADED` 本就是资源护栏码且在多 op 可达（等待满 v7 先例同码），线路上不存在"无处安放"问题，立枚只增客户端解析面。**新增状态码是有成本的词汇扩张，能骑既有护栏码就不立枚**——此判例分岔本身登记入 design，防后续原语两头援引。`Outcome.REJECT_PHASER_PARTIES`/`REJECT_PHASER_QUOTA`/`REJECT_NO_ENTRY` 为 core 内部细分码（线路映射 `OVERLOADED`/`INVALID_REQUEST`/`INVALID_REQUEST`，判例 `REJECT_BARRIER_PARTIES` 家族）。

### D9 等待集住 `PhaserEntry`（v9 D4 判例延伸）、唤醒事件驱动零新定时器

等待集 `(session, request_id) → (expected_phase, 登记时刻)` 到达序队列住条目内：合拢发生在 apply 关键区，唤醒判据（`expected_phase < 新相位`）与账簿同锁域读取——server 侧 registry（v8 TopicRegistry 位）会把"账簿迁移→唤醒筛选"拆成跨模块同步，破坏条目单关键区不变式（v9 D4 原文论证复用）。回收三路：CANCEL（幂等）/`closeSession` 家族摘除（与配额摘除同一应用点钩子）/换主进程灭；`headReplyTimeoutMs` 清扫已通知未重发 ghost。唤醒全事件驱动（apply 点、死亡摘除点），无到期谓词、零 tick——与 v9 D7 同相，`QueueReadyDriver` 零改动。了结记录窗口 = 最近一次推进的已通知集（v5 D7 有界窗口纪律原样适用，出窗迟到按新等待入集、声明竞态入契约）。

### D10 `newPhaser(key, initialParties)` 的重连重执与超调方向自洽

句柄构造零网络；`initialParties` 在首操作前同步 REGISTER 一次（会话作用域）。重连（会话重建）后 SDK **重执 REGISTER(initialParties)**：一致性论证——若旧会话已死亡摘除，重执恰好恢复应用的注册意图；若旧会话尚未被摘除（重建快于失联判定），`registered` 短暂超调——超调方向是**保守侧**（更难合拢，不会误推进；随后旧会话摘除的 `SESSION_CLOSE` apply 自动收敛回正）。显式 `register()/bulkRegister` 不重执（应用意图面，SDK 不猜测；句柄本地最近读数亦不缓存伪造）。此条为隐式摘除（D5）与重挂通道（v8 判例）咬合处最容易实现错位的一点，E2E 必测（换主/重连双形）。

### D11 编号冻结表（新增面一次性钉定）

| 面 | 新增 | 编号/依据 |
|---|---|---|
| `MessageType` | `PHASER_OP` | 22 |
| `Envelope.payload` | `phaser_op_request` / `phaser_op_response` | 47 / 48 |
| `LockType` | `LOCK_TYPE_PHASER` | 14（proto 与 core/client 枚举同步） |
| `KeyFamily` | `PHASER` | 新家族、新条目类 `PhaserEntry` |
| `RaftEntryType` | `PHASER_OP_ENTRY` | 14——**v8/v9"止于 13"编号证据更替为"止于 14"**，守卫基线同轮改（`StateMachineTopicTest`/`StateMachineConditionTest` 等编号证据断言改版本相对口径，见 replicated-state-machine 增量） |
| `StatusCode` | **零新增** | `OVERLOADED`/`INVALID_REQUEST`/`QUEUED`/`NOT_LEADER` 复用（D8） |
| 新枚举 | `PhaserOp` 七值（REGISTER=0 … QUERY=6） | — |
| `PhaserOpRequest` | `{ key=1, op=2, parties=3(int32), expected_phase=4(sint64, presence), await_request_id=5(int64) }` | 形状互斥矩阵 |
| `PhaserOpResponse` | `{ status=1, op=2, phase=3(sint64), registered=4(int32), arrived=5(int32) }` | `unarrived` 客户端差值导出 |
| `SnapshotLock` | phaser 字段续接 | 35 起：phase/registered/配额表/arrived/去重槽/了结记录（新消息 `SnapshotPhaserArrival`/`SnapshotPhaserParty`/`SnapshotPhaserResolved` 按需） |
| 管理面 | `AdminKeyInfo` 19–21（phase/registered/arrived）；`AdminKeyDetailResponse` 33 起（三计数 + `phaser_waiters_info`/`phaser_parties_info` 两 repeated 新消息）；`AdminSummaryResponse` 续接 `phaser_entries`；`family` 词表 + `phaser` | 实现期按现表续接 |
| 配置 | `max-parties-per-phaser` 默认 1024、[1,65536] | 唯一新配置；等待深度骑 `max-queue-depth-per-key`（v9 D7 延伸） |
| 指标 | `phaser.total{op,status}`、`phaser.parties.registered.max` | 四口径互引扩五口径 |
| 推送 | **零新增** | `AWAIT_NOTIFY` 复用（ref=等待方 request_id） |
| 握手 | 三处常量 9→10；v≤9 `PHASER_OP` 消息级拒绝不断连 | 判例 v3–v9 门 |

### D12 观察面双速呈现与客户端车道定位

Follower 对 phaser key：**账簿三计数/配额明细可读（复制态）而等待明细如实零（本地态）**——与 topic 键"Follower 整体未命中"分轨（topic 无复制态可显示），与 barrier"在场数可读、在队位次零"同型（判例随行注记）。控制台/指南以"双速呈现"标注防运维误读"registered 无变化=无人等待"。客户端 PHASER 直发车道：变异走提交（NOT_LEADER 退避改道，无提示字段判例），等待/取消/查询 Leader 本地直受理；活跃等待项随句柄登记，双通道（车道激活事件+周期保活）重挂——W11 行 v10 注记义务（非 ACQUIRE 车道暴露面+1，等待侧自愈与变异/查询侧显式超时的非对称声明沿 v8 topic 注记口径）。

### D13 测试与守卫布局（判定族到夹具的映射）

core `PhaserEntryTest`（D1–D5 判定矩阵：合拢单推进/去重重放/配额扣减与提前合拢/死亡两形态摘除/归零空转复活/无条目拒绝/恢复往返）+ `CoreEnginePhaserTest`；`StateMachinePhaserTest`（每变异一条目、等待/取消/查询零条目、digest 逐字节、重放不双计数、死亡摘除跨副本同判、编号证据 14 基线）；`PhaserGatingTest`（v≤9 门、七 op 形状矩阵、钳制、家族互拒、QUERY 形状）；`ClusterPhaserTest`（E2E：跨进程合拢矩阵、中途注册入当相位、`arriveAndDeregister` 提前合拢、**kill 进程死亡摘除不空转**（头号验收，对标 barrier 破障 E2E 形态）、换主重挂无损耗含 D10 重执双形、同 request_id 重发单到场、Follower 五操作词零副作用、并发不丢不重）；`RejectCodecTableTest` 扩行（W10 常驻）；`SnapshotFamilyRoundTripTest` 扩 phaser 维 + 尺寸落界/等待零足迹两夹具；客户端 `RemotePhaserScriptedTest` + `ClientPhaserIT`；协议冻结 v10 全量表 + presence 往返；`FairOrderingSuite` 豁免登记（v5 D6 正文同款）；换主类断言一律有界总超时+eventually（v9 D12 纪律）；基准 phaser 相入 `target/benchmark/`；演练负载段扩 phaser 相（RollingRestart/LeaderKill，`-Pdrill` 用户非沙箱单轮）。

## Risks / Trade-offs

- [已到场者死亡的"幽灵到场"被误读为语义漏洞] → D5 因果叙事入规格（到场发生时应到集合含它）+ E2E 双形态钉住 + 指南对照表（破障 vs 摘除）；如生产出现误读信号，后立 change 评估"摘除时 arrived 同减"变体（保守方向相反、暂不采）。
- [parties 上限与等待上限共码 `OVERLOADED` 运维混读] → `op` 标签分轨为规格钉定条款 + 指南 08 判读段 + 告警建议分线（`{register,OVERLOADED}` 与 `{await_advance,OVERLOADED}` 分别成警）；D8 立枚/不立枚分岔论证存档防后续摇摆。
- [D10 重执 REGISTER 在重建快于失联判定时短暂超调 registered] → 超调方向保守（难合拢不误进）+ 摘除 apply 必收敛的论证入指南排查三分法；测试钉双时序形态。
- [变异/查询骑直发车道，换主窗显式超时面扩大（W11）] → W11 行 v10 注记登记 + 等待侧双通道自愈声明（谓词在账簿，重挂无损耗）——非对称性与 v8 topic 注记同型，升级序建议服务端先行缩窗。
- [到场条目率随 parties×相位频率增长（热键日志膨胀）] → W14 登记观察项（触发→读路径折案/批量评估流程，沿 W6/W7/W9 口径）；基准 phaser 相给量级基线；查询免日志（D7）已把暴露面砍半。
- [编号证据基线 13→14 更替被漏改或误改（守卫假绿/假红）] → 更替与 `PHASER_OP_ENTRY` 引入同一提交落地，`--strict` validate + 两条守卫（topic/condition 边界版本相对口径）红先验；tasks 单列条目。
- [快照字段复用错槽（35+ 续接冲突）] → 实现期以生成 descriptor diff 复核续接位，`SnapshotFamilyRoundTripTest` 逐字段往返钉。
- [ofAdvance 缺失导致应用误用（以为动作有排序）] → Javadoc 降级清单首条 + 指南 03 替代惯用法示例钉死；无排序承诺写为契约。
- [v10 客户端连回滚后的 v9 服务端区间外握手即拒] → 既有纪律显形非新风险；指南 05/10 回滚窗注记第四口径（在途 phaser 流量清零方可降级，否则 phaser key 旧二进制不可用——屏障/队列同型）。

## Migration Plan

1. 纯增量发布：proto/编号/守卫基线 → core 条目与引擎门面 → server 门控/车道/配置/管理/指标 → client SDK/握手 → console/文档；握手三处常量与服务端 `PROTOCOL_VERSION` 9→10 同提交。
2. 升级序服务端先行（v3–v9 判例）：v≤9 客户端对 v10 服务端行为逐项不变；v10 客户端对 v9 服务端握手即拒，不存在旧服务端误执行新语义的窗口。
3. 回滚窗口第四口径：`PHASER_OP_ENTRY`/快照 phaser 字段对 v9 二进制是未知条目形态（现行未知条目 error 口径）——降级前确认无在途 phaser 流量，或接受 phaser key 在旧二进制不可用；对照 topic/condition"天然干净"并列声明（指南 05/10 + WATCHLIST 无涉）。
4. 验证收口：全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` 基线不降（现 1916 测试 0 失败）；`check-source-citations.sh` 全源零命中；`-Pdrill` 由用户在非沙箱单轮复跑（含 phaser 负载段，红先保 `target/drill-logs/`）。
5. 文档三账：ROADMAP 决策记录补立项裁决行 + 档位行状态流转；WATCHLIST W14 登记 + W11 v10 注记；指南双语八处对账。

## Open Questions

- `arriveAndDeregister` 是否需要 `bulkArriveAndDeregister(n)` 对偶（JDK 无此面，`bulkRegister` 的反向）——按对称美感诱惑加面，v1 不加（应用可循环），如生产出现批量离场需求后立 change。
- `REGISTER` 是否接受 `count = 0`（JDK `bulkRegister(0)` 无操作）——当前形状矩阵按违例拒绝（`parties ∈ [1, cap]`，最严形状）；实现期若与 Scripted 夹具冲突再回看，不阻塞规格（矩阵两值皆已可测试钉定）。
- 管理面是否需要 `phaser_unarrived` 派生计数列（registered-arrived 线路直读导出 vs 客户端差值）——读数一致性同值，实现期按 admin 装配便利定，不阻塞。
