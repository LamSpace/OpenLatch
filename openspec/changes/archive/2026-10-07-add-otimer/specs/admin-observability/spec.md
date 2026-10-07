# Spec Delta

## MODIFIED Requirements

### Requirement: 管理查询应答契约

服务端 SHALL 对已握手且连接协商版本 ≥3 的会话提供四类只读管理查询，应答内容以本节点视角如实呈现：

- `ADMIN_SUMMARY`：按家族持有条目数（锁/Semaphore，Latch、ATOMIC、BARRIER、QUEUE、TOPIC、PHASER 与 TIMER 无持有语义单列条目数；ATOMIC 计数含标量与有值引用两形态，QUEUE 计数含两形态，TOPIC 计数为 Leader 本地订阅登记表的按 key 数——非 Leader 恒 0，**PHASER 与 TIMER 计数为复制态条目数——各节点读数经镜像收敛一致（与 topic 本地表口径分轨，v10/v11）**）、等待者总数（**含条件等待者、phaser 等待项与 timer 等待项——v9 口径"等待者是等待者"经 v10/v11 延伸：条件等待者、phaser 与 timer 等待项与锁/Semaphore 等待同口径合计；对照 topic 订阅者不计入的既有分轨随行**）、本节点活跃会话数、节点角色（单机/Leader/Follower）、自启动运行时长、服务端版本；
- `ADMIN_LIST_KEYS`：分页返回 key、家族、持有者（会话/线程/计数；ATOMIC、BARRIER、QUEUE、TOPIC、PHASER 与 TIMER 恒为空）、剩余租约毫秒（未持有为 0；ATOMIC、BARRIER、QUEUE、TOPIC、PHASER 与 TIMER 恒为 0）、等待者数（ATOMIC 恒为 0；BARRIER 为当前世代在队等待者数；QUEUE 为双轨挂起等待者合计，后两者仅 Leader 视角非零；TOPIC 恒为 0——订阅非等待；**PHASER 为该 key 等待集人数——Leader 本地等待集合计，Follower 恒 0 如实（v10，与条件计数同型）；TIMER 为该 key 等待集人数——Leader 本地等待集合计，Follower 恒 0 如实（v11，与 phaser 计数同型）**；**LOCK 键另列 `condition_waiters` 条件等待数——Leader 本地条件等待集合计，Follower 恒 0 如实**）、BARRIER 的 parties/当前世代号/当前世代到场数、**PHASER 的 `phaser_phase`/`phaser_registered`/`phaser_arrived` 三计数（复制态账簿，各节点读数一致——与等待计数的 Leader-only 口径双轨同行）**、**TIMER 的 `timer_generation`/`timer_armed`/`timer_fire_at_ms` 三元组（复制态账簿原始读数，各节点一致且不随节点时钟漂移——管理面 MUST NOT 呈现 marked 折算值：到期判定仅在 Leader 应答线发生，呈现面保持时钟无关）**、有值引用条目的载荷字节大小（`atomic_payload_size`）及截断预览（`atomic_payload_preview`）与队列条目的定型容量（`queue_capacity`）、当前深度（`queue_depth`）、首元素字节大小（`queue_head_payload_size`）及截断预览（`queue_head_payload_preview`）（非对应家族恒缺省零值/空串）；Leader 视角合并订阅登记表键（`family = "topic"`、`topic_subscribers` 订阅数；非 Leader 节点该类键无登记来源，列表如实不含——不呈现伪零行），支持前缀过滤，`total_matched` 为过滤后总条数；
- `ADMIN_KEY_DETAIL`：单 key 的持有明细（会话、线程、重入计数或持有许可数、读/写角色）、等待队列（位次、会话、请求 id、已等待时长；QUEUE 含 put/take 双轨，以 `queue_track` 判别——1=等容量、2=等元素、0=非队列）、**条件等待明细（`condition_waiters` 计数 + `condition_waiters_info` 列表：条件名、会话 id、请求 id、归属线程 id、登记时刻——集合到达序呈现；与等待队列区段并列、不并入位次编号（条件等待不在队，搬运后成为等待队列项）；复用 `wait_queue_leader_only` 同源口径标志——Follower 无集合来源，计数与明细如实零/空，不呈现伪零壳之外的歧义）**、**PHASER 区段：`phaser_phase`/`phaser_registered`/`phaser_arrived` 三计数（复制态账簿——Follower 照常呈现）+ `phaser_waiters_info` 等待明细（会话 id、请求 id、已见相位号 `expected_phase`、登记时刻——按登记到达序；复用 `wait_queue_leader_only` 同源口径——Follower 集合无来源、如实零/空）+ `phaser_parties_info` 配额明细（会话 id、parties——按会话 id 升序；复制态账簿，各节点一致呈现）；等待明细与等待队列区段并列不并号（phaser 等待非队首批授，无位次语义——判例条件并列纪律）**、**TIMER 区段：`timer_generation`/`timer_armed`/`timer_fire_at_ms` 三元组（复制态账簿——Follower 照常呈现、原始读数无时钟歧义）+ `timer_waiters_info` 等待明细（会话 id、请求 id、登记时刻——按登记到达序；复用 `wait_queue_leader_only` 同源口径——Follower 集合无来源、如实零/空）；等待明细与等待队列区段并列不并号（timer 等待同为非队首批授——判例 phaser 并列纪律）**、剩余租约到期时刻、Latch 的总量/剩余、ATOMIC 的形态/当前值/版本戳/定型初值（有值引用形态的初值与当前值以 `atomic_payload_size` + `atomic_payload_preview` 承载：预览为服务端构造的截断转义文本——可打印前缀原样、不可打印字节转义呈现，预览长度恒定不随 `maxValueBytes` 配置或实际载荷大小膨胀，null 值如实以缺省预览+零大小表达且与空串可区分——后者预览为空引号形态或等价显式标记）、BARRIER 的 parties/当前世代/当前到场数/动作挂账有无/最近完结世代的了结形态（`tripped`/`broken`/`none`）、QUEUE 的定型容量/当前深度/首元素到期时刻（`queue_head_expiry_ms`，DELAY 形态、受理节点时钟读数，非 DELAY 或空队为 0）/总驻留字节（`queue_total_payload_bytes`，元素载荷字节之和，观察驻留治理）/首元素大小与截断预览（`queue_head_payload_size`/`queue_head_payload_preview`，同一截断转义纪律）；TOPIC 键（Leader 视角）呈现 `topic_subscribers` 计数与订阅者明细列表 `{session_id, subscription_id, subscribed_at_ms}`（无持有/等待区段以"无此语义"呈现；非 Leader 节点对 topic 键回明确的未命中状态——复制态确实不存在，MUST NOT 空壳成功）；**PHASER 键无持有/租约区段以"无此语义"呈现（三计数与配额明细为复制态，非 topic 式未命中——Follower 对 phaser 键详情照常命中并呈现账簿，仅等待明细如实零，与 topic 键的 Follower 未命中分轨）；TIMER 键同型（三元组为复制态——Follower 照常命中呈现，仅等待明细如实零；已到期与未到期的区分以 `timer_armed` 与 `timer_fire_at_ms` 原始值由读侧自行判定，呈现面不做折算）**；key 不存在回明确的未命中状态；
- `ADMIN_LIST_SESSIONS`：本节点接入的会话列表——id、接入节点（由会话 id 高位导出）、建连时刻、持锁 key 数、在队等待数。

管理查询 MUST NOT 变更任何锁、租约、等待队列、条件等待集、**phaser 账簿与等待集、timer 账簿与等待集**、订阅登记表或会话状态（纯观察；BARRIER 的观察 MUST NOT 推进到场数、世代号或换代窗口；**PHASER 的观察 MUST NOT 推进相位、到场计数、配额、等待登记或清扫时序；TIMER 的观察 MUST NOT 推进代次、装载态、到期判定、等待登记或清扫时序**；队列的观察 MUST NOT 推进元素、深度、到期判定或去重槽；topic 的观察 MUST NOT 推进 topic_seq、缓冲、丢弃计数、去重槽或订阅摘除时序；**条件的观察 MUST NOT 推进登记、搬运、通知或清扫时序，亦不得使 ghost 登记提前消亡**）；应答 MUST NOT 依赖任何写路径成功（负载处理阻塞时查询仍可作答，读数允许弱一致）。观察面 MUST NOT 成为载荷放大器：全量载荷字节 MUST NOT 出现在任何管理应答中（仅大小+截断预览）；全量元素列表 MUST NOT 出现在任何管理应答中（仅深度、驻留总字节与首元素预览）；已交付消息内容与消息字节 MUST NOT 出现在任何管理应答中（topic 仅计数与会程/序号/时刻三维）；**条件维无内容面可外发——条件名即寻址文本，明细仅结构五字段**；**phaser 维无内容面可外发——三计数与 {会话, 请求, 相位, 时刻} 结构字段即全部，等待集人数上限受合并深度护栏钳制、配额受 max-parties-per-phaser 钳制，明细行数有界即尺寸有界**；**timer 维无内容面可外发——三元组与 {会话, 请求, 时刻} 结构字段即全部，等待集人数受合并深度护栏钳制、去重槽每会话恒一行，明细行数有界即尺寸有界**。

#### Scenario: 预置状态摘要与明细一致

- **WHEN** 预置多类型条目（重入/读写/信号量/Latch/标量原子/有值引用（含 null、空串与恰限大载荷三种值态）/循环屏障/队列（含满容量、未到期延时元素与双轨挂起等待者）/topic 键（含多会话订阅与缓冲内积压消息）/**推进两相位、多会话配额、含在集等待与已通知未重发项的 phaser 键**、**含 PENDING 未到期钟、已到期共现钟与代终结钟三形态且含多会话等待项的 timer 键**、**含两条件多等待者且有搬运在队项的互斥锁 key（持有者已 await 释放）**）、含等待者后依次请求 SUMMARY、LIST_KEYS、KEY_DETAIL、LIST_SESSIONS
- **THEN** 各应答逐字段与预置状态吻合，SUMMARY 聚合与 LIST_KEYS 明细、KEY_DETAIL 与 LIST_SESSIONS 的持锁/等待计数互相自洽；ATOMIC 条目在 SUMMARY 单列条目数（两形态合并计数）、在 KEY_DETAIL 呈现形态/值/版本戳；有值引用条目呈现字节大小与截断预览且预览不随配置膨胀；BARRIER 条目在 SUMMARY 单列条目数、在 KEY_DETAIL 呈现 parties/世代/到场数/动作挂账与了结形态；QUEUE 条目在 SUMMARY 单列条目数、在 LIST_KEYS 呈现容量/深度/首元素预览、在 KEY_DETAIL 呈现容量/深度/驻留总字节/到期时刻/首元素预览与双轨等待位次；TOPIC 键在 SUMMARY 计入 `topic_entries`、在 LIST_KEYS 以 `family=topic` 呈现订阅数、在 KEY_DETAIL 呈现订阅者明细三字段列表；**PHASER 键在 SUMMARY 计入 `phaser_entries`（各节点一致）、在 LIST_KEYS 呈现三计数与等待数（Leader 非零/Follower 等待数如实零）、在 KEY_DETAIL 呈现三计数+等待明细四字段+配额明细两字段，等待明细不含已了结项、配额明细逐会话呈现**；**TIMER 键在 SUMMARY 计入 `timer_entries`（各节点一致）、在 LIST_KEYS 呈现三元组原始读数与等待数（Leader 非零/Follower 等待数如实零；三形态钟的 armed/fire_at 逐一如实——呈现面无 marked 字段）、在 KEY_DETAIL 呈现三元组+等待明细三字段，代终结钟以 armed=false 如实呈现而非未命中**；**SUMMARY 等待者总数含条件等待者、phaser 等待项与 timer 等待项；LOCK 键在 LIST_KEYS 呈现 `condition_waiters`、在 KEY_DETAIL 呈现条件明细五字段且与已搬运入队项（等待队列区段）并列互斥计数、不重复**

#### Scenario: 大载荷观察零放大

- **WHEN** 对持有恰限字节载荷的有值引用 key、满容量恰限元素的队列 key 与缓冲内积压恰限消息的 topic key 分别请求 LIST_KEYS 与 KEY_DETAIL
- **THEN** 引用应答仅含大小与恒定长度截断预览；队列应答仅含深度/驻留总字节/首元素截断预览——全量元素字节不出现在应答中；topic 应答仅含计数与订阅者（会话/序号/时刻）——消息内容零外发；反复观察不改变条目任何状态

#### Scenario: 管理查询零扰动

- **WHEN** 反复执行全量管理查询后，业务客户端对同批 key 执行获取/释放/续租、ATOMIC 操作（含引用形态）、BARRIER await、队列 put/take/drainTo、topic PUBLISH/SUBSCRIBE、**phaser register/arrive/awaitAdvance/cancel/query**、**timer schedule/disarm/await/cancel/query** 与**条件 await/signal/LEAVE**
- **THEN** 全部业务语义与不执行管理查询时逐项一致（含租约时刻不被观察行为刷新、ATOMIC 版本戳不因读取而推进、BARRIER 世代号与到场账簿不因读取而推进、队列元素/深度/去重槽不因观察变动、topic_seq/缓冲/丢弃计数/订阅登记不因观察变动、**phaser 相位/到场计数/配额/去重槽不因观察变动，等待登记与清扫时序不因观察推进**、**timer 代次/装载态/到期时刻/去重槽不因观察变动，等待登记与到期判定不因观察推进或回卷**、**条件等待集的到达序/搬运时序/清扫推进不因观察变动**）

#### Scenario: 未知 key 详情明确未命中

- **WHEN** 对不存在的 key 请求 ADMIN_KEY_DETAIL
- **THEN** 应答携带明确的未命中状态码，MUST NOT 回空壳成功应答

#### Scenario: Follower 视角屏障等待者数为零

- **WHEN** 在 Follower 节点请求含在队等待者的 BARRIER key、含挂起等待者的 QUEUE key、**含在集等待项的 PHASER key** 或**含在集等待项的 TIMER key** 的 LIST_KEYS/KEY_DETAIL
- **THEN** 等待者数为 0 且 `wait_queue_leader_only=true`，BARRIER 的 parties/世代/到场数、队列的容量/深度/驻留字节/到期时刻、**PHASER 的相位/registered/arrived 三计数及配额明细与 TIMER 的 generation/armed/fire_at_ms 三元组**照常呈现（复制状态）；**PHASER 键与 TIMER 键详情命中（复制态存在），MUST NOT 以 topic 式未命中壳呈现**

#### Scenario: Follower 视角 topic 键如实缺席

- **WHEN** 在 Follower 节点 LIST_KEYS（集群中存在 Leader 侧 topic 订阅）或对该 topic key 请求 KEY_DETAIL
- **THEN** 列表不含该 topic 行、详情回明确未命中（Follower 无订阅登记表，MUST NOT 呈现伪零或空壳成功）；SUMMARY 的 `topic_entries` 为 0 且角色字段如实标注 Follower

#### Scenario: Follower 视角条件读数如实零

- **WHEN** 集群 Leader 上存在多条件等待者，在 Follower 节点对其承载 key 请求 LIST_KEYS/KEY_DETAIL
- **THEN** `condition_waiters` 为 0、明细列表为空且 `wait_queue_leader_only=true` 同源标注（Follower 无集合来源，等待集不入复制态——如实零读，MUST NOT 以未命中壳形态掩盖 key 本身存在）；该 key 的复制态（无持有者——await 已释放）照常呈现

#### Scenario: 换主窗口条件明细过渡如实

- **WHEN** Leader 更替后、旧等待项尚未完成自动重挂的窗口内查询新 Leader 的该 key 详情
- **THEN** 条件明细为空或仅含已重挂登记（新 term 集合无旧残留——搬运时序与旧集合随旧进程灭失），持有读数如实为无持有者；窗口后重挂项在明细中恢复可见

#### Scenario: 换主窗口 phaser 等待明细过渡如实

- **WHEN** Leader 更替后、旧 phaser 等待项尚未完成重挂的窗口内查询新 Leader 的该 phaser key 详情
- **THEN** 三计数与配额明细即时完整（复制态账簿随 apply 收敛，无过渡窗）；等待明细为空或仅含已重挂项（新 term 集合无旧残留）——**账簿完整而集合待重挂的"双速呈现"为契约面，与控制台标注联动，MUST NOT 以计数可读误导"无人等待"**

#### Scenario: 换主窗口 timer 等待明细过渡如实且呈现时钟无关

- **WHEN** Leader 更替后、旧 timer 等待项尚未完成重挂的窗口内分别查询新/旧（已下台）Leader 的该 timer key 详情；随后对同一 key 在两个时钟偏移 ±50ms 的节点各取一次呈现
- **THEN** 三元组读数在两节点逐字节相等（原始 `fire_at_ms` 呈现不折算 marked——管理面无"节点时钟差"形态；到期判定的钟偏差只出现在 Leader 应答线，不污染呈现面）；等待明细为空或仅含已重挂项（"账簿完整而集合待重挂"双速呈现沿 phaser 口径，MUST NOT 以三元组可读误导"无人等待"）
