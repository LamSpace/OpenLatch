# Spec Delta

## MODIFIED Requirements

### Requirement: topic 零复制日志边界

topic 为项目首个零复制日志原语：SUBSCRIBE/UNSUBSCRIBE/PUBLISH 与全部 fan-out 交付 MUST NOT 产生任何 Raft 日志条目，MUST NOT 触达状态机 apply 路径、`ApplyResult` 或 Leader 提交通道（判例对偶：队列"等待不入日志"，topic 更进一步——**连状态迁移都不入日志**，因 topic 无复制态）。由此推导的常驻守卫断言（防后续演进按"读写皆经提交"惯性误塞日志，改动本条款 MUST 先经 DECISIONS.md 决策记录登记）：

1. 纯 topic 流量（订阅、发布、退订、换主）下，各副本复制状态 digest MUST 逐字节一致且等于无 topic 流量时的基线；
2. 日志条目总数 MUST NOT 因 topic 流量增长（`SESSION_OPEN`/`SESSION_CLOSE`/NOOP 等既有系统条目不计入本断言增量）；
3. Follower 追赶、节点重启与快照安装 MUST NOT 导致任何陈旧 `TOPIC_MESSAGE` 被投递（重放无 topic 副作用可产生）；
4. `RaftEntryType` MUST NOT 为 topic 新增取值（v8 定型的编号证据以版本相对口径常驻：topic 维度自 v8 起条目类型零占用——v10 为 phaser 新增 `PHASER_OP_ENTRY = 14` 不构成 topic 证据的变更，1–14 各值与 topic 无涉）。

#### Scenario: 纯 topic 流量日志零条目

- **WHEN** 三节点集群在稳定 Leader 下执行 SUBSCRIBE × N、PUBLISH × M、UNSUBSCRIBE 与一次换主，全程无其他业务
- **THEN** 提交后的日志条目数与仅含系统条目（会话登记/NOOP）的对照运行一致，topic 操作贡献为零条；三副本 digest 逐字节相等

#### Scenario: 重启与追赶不重投

- **WHEN** 承载过 topic 流量的节点重启（快照加载 + 日志回放）或新节点经快照追赶
- **THEN** 回放正常完成且任何客户端未收到历史 `TOPIC_MESSAGE`；重启节点的订阅登记表为空（等待客户端重挂）

#### Scenario: 误入日志即构建红

- **WHEN** 未来变更在 topic 分发路径引入状态机提交调用
- **THEN** 本条款的日志零增长断言（topic 流量对照基线）转红，变更被打回规格修订流程

### Requirement: 条件复制边界——await 折叠既有条目、signal 家族零日志

条件原语采用半入日志边界：await 折叠形态（携带 `condition` 的 ACQUIRE）MUST 恰产生**一条既有 `LOCK_ACQUIRE_ENTRY` 类型**条目（`command_payload` 为请求消息序列化，新字段天然透传）——`RaftEntryType` MUST NOT 因本原语新增取值（v9 定型的编号证据以版本相对口径常驻：条件维度自 v9 起条目类型零占用、仅复用既有条目——v10 为 phaser 新增 `PHASER_OP_ENTRY = 14` 不构成条件边界的变更，14 非条件占用值）；SIGNAL/SIGNAL_ALL/LEAVE MUST NOT 产生任何日志条目、MUST NOT 触达提交通道与状态机 apply 路径（纯 Leader 本地裁决——v8 topic 零日志豁免判例的类目化：等待集→等待队列的位次搬运与推送事件不改变任何复制态，"signal 是事件不是状态"）。

await 条目的应用语义 MUST 跨副本确定：释放半程（持有归属匹配则重入一步清零+清租约，不匹配则零操作）为复制态迁移，全部副本一致重放；登记半程（条件等待集写入）MUST 为 Leader-only 应用副作用，不进入任何副本的复制状态、不产生观察 digest 差异。由此推导的换主/重启语义契约：**等待是承诺**——await 条目重放使释放半程确定生效，等待集随 term 清零后由客户端重挂补登记（重挂非双登记、重挂会话非持有者属常态）；**signal 是事件**——搬运与通知不经日志、不重放、不补偿，换主窗内已发出未送达的唤醒不保证。守卫断言（常驻，防后续演进误塞日志或误持久化等待集，改动本条款 MUST 先经 DECISIONS.md 决策记录登记）：

1. 纯条件流量（折叠 await、SIGNAL/SIGNAL_ALL/LEAVE、换主重挂）下，各副本复制状态 digest MUST 逐字节一致且等于对照基线（无 signal 类状态可分岔）；
2. await 数与既有类型条目增量 MUST 恰为一对一，signal 家族条目贡献恒零；
3. Follower 追赶、节点重启与快照安装 MUST NOT 投递任何陈旧唤醒（重放无 Leader 副作用可产生、无集合可恢复）；
4. 应用点 MUST NOT 依赖墙钟外生输入（`registered_at_ms` 等观察值经条目时刻注入且永不参与判定）。

#### Scenario: await 单条目、signal 零条目

- **WHEN** 三节点集群稳定 Leader 下执行 N 次折叠 await 与 M 次 SIGNAL/SIGNAL_ALL/LEAVE，全程无其他业务
- **THEN** 日志新增条目数恰为 N 且全部为 `LOCK_ACQUIRE_ENTRY`；三副本 digest 逐字节相等；`RaftEntryType` 中条件维度占用相对 v9 零变化（v10 的 `PHASER_OP_ENTRY = 14` 与条件边界无涉——证据线为版本相对口径而非绝对上界）

#### Scenario: 重放幂等与换主清零重挂

- **WHEN** 承载条件流量的 Leader 被 kill，新 Leader 回放既有日志，旧等待项经客户端重挂重发折叠 ACQUIRE，其间一方对旧集合发过 SIGNAL
- **THEN** 新 Leader 上释放半程已生效（重放确定）、等待集仅含重挂登记（旧 term 搬运态与通知时序零残留）、换主窗内发出的 SIGNAL 不重放也不报错（空集无操作或搬运对象已换主消失——有界契约面）；后续 SIGNAL 对重挂登记照常搬运

#### Scenario: 重启与追赶无陈旧唤醒

- **WHEN** 节点重启（快照加载+日志回放）或新副本经快照安装追赶
- **THEN** 复制态逐项恢复（含 await 已生效的释放），任何客户端未收到历史 `AWAIT_NOTIFY` 条件唤醒；重启节点条件等待集为空、管理读数如实零

#### Scenario: signal 误入日志即构建红

- **WHEN** 未来变更在 signal 家族分发路径引入状态机提交调用
- **THEN** 本条款的"signal 家族条目贡献恒零"断言转红，变更被打回规格修订流程

### Requirement: phaser 操作复制边界与重放确定性

phaser 采用**变异全入日志、等待与观察零日志**的复制边界（沿 v5 Barrier"到场是状态迁移"判例，与 v8/v9 的零日志豁免类目相对——落地纪律第 2 条对本原语不适用豁免）：REGISTER/ARRIVE/ARRIVE_AND_AWAIT（到场半程）/ARRIVE_AND_DEREGISTER 每一变异操作 MUST 恰产生**一条 `PHASER_OP_ENTRY`**（`command_payload` 为 `PhaserOpRequest` 序列化+会话包装，判例既有命令条目载荷形态）；AWAIT_ADVANCE/CANCEL/QUERY MUST NOT 产生任何日志条目、MUST NOT 触达提交通道与 apply 路径（等待登记/摘除/读数为 Leader 本地裁决——"等待是订阅不是状态"与"观察不是迁移"两类目并入既有"等待不入日志"判例族）。由此推导的常驻守卫断言（改动本条款 MUST 先经 DECISIONS.md 决策记录登记）：

1. 相位推进、配额与到场计数为纯确定性 apply 迁移：合拢判定只依赖账簿内计数（`arrived ≥ registered`），MUST NOT 依赖墙钟、 Leader 身份或本地随机数——跨副本 digest MUST 逐字节一致；
2. 三副本 digest 在含合拢、中途注册、离场减员、死亡摘除（`SESSION_CLOSE` apply 内确定性减配额）的全流量下逐项一致；了结时刻的唤醒为 Leader 本地副作用，不入 digest；
3. 重放幂等：到场去重槽与换代窗口随账簿回放恢复——重启/追赶后命中槽的旧重发 MUST NOT 双计数、MUST NOT 双推进；相位号 MUST 单调不回退（快照+尾部回放终态与不中断运行逐字段一致）；
4. 等待集为 Leader 进程易失态：换主/重启后恒空、由客户端重挂补登记——与"唤醒谓词在复制态"合流使重挂无损耗（对照 v9 signal 事件灭失窗，此为边界差异的另一侧：phaser 无第三方事件可丢失）；
5. 应用点 MUST NOT 依赖墙钟外生输入（`registered_at_ms` 等观察值经条目时刻注入且永不参与判定——phaser 无延时/到期语义，对照 v7 队列到期折算的缺失为有意设计）；
6. 编号证据：`PHASER_OP_ENTRY = 14` 为 v10 新增且仅 phaser 占用；1–13 既有值语义逐项不变、MUST NOT 复用为 phaser 变体；守卫基线由 v8/v9 时代的"RaftEntryType 止于 13"更替为"止于 14"（更替随本 change 常驻，防止"零新增"证据误锚旧上界）。

#### Scenario: 变异单条目、等待查询零条目

- **WHEN** 三节点集群稳定 Leader 下执行 REGISTER × a、ARRIVE 族 × b、AWAIT_ADVANCE × c、CANCEL × d、QUERY × e，全程无其他业务
- **THEN** 日志新增条目数恰为 a+b 且全部为 `PHASER_OP_ENTRY`；等待/取消/查询贡献零条；三副本 digest 逐字节相等

#### Scenario: 合拢跨副本确定与死亡摘除同判

- **WHEN** Leader 上执行至合拢临界（含一次 `SESSION_CLOSE` 使 registered 缩减后恰满足 `arrived ≥ registered`）的混合流量，Follower 回放同一日志
- **THEN** 各副本在各自 apply 的同一条目处观测到同一 (phase, registered, arrived) 三元组；死亡摘除的推进不依赖 Leader 身份——Follower apply 同样迁移账簿（唤醒仅发生在当时为 Leader 的进程）

#### Scenario: 重启与追赶不双计数

- **WHEN** 在场若干去重槽与换代窗口的 phaser 状态生成快照、节点重启并回放含未入快照的到场条目，其间客户端以快照前已计的旧 request_id 重发
- **THEN** 重发命中恢复的去重槽：回显同到场相位、不双计数、不双推进；换代窗口窗口外的迟到重发按新等待入集（声明竞态，v5 D7 同口径）

#### Scenario: 换主等待清零重挂无损耗

- **WHEN** 等待方 `expected_phase=5` 挂起期间换主且新相位 6 已提交，新 Leader 回放日志、客户端重挂 `AWAIT_ADVANCE(5)`
- **THEN** 新 Leader 账簿含相位 6（复制态存续）、等待集仅含重挂登记（旧集合随进程灭）；重挂即刻 `OK` 了结——换主窗内"未送达的通知"由谓词自愈，MUST NOT 出现任何丢失推进的形态（对照条件 signal 丢失窗的差分断言）

#### Scenario: 等待误入日志即构建红

- **WHEN** 未来变更在 AWAIT_ADVANCE/CANCEL/QUERY 分发路径引入状态机提交调用
- **THEN** 本条款"等待/取消/查询条目贡献恒零"断言转红，变更被打回规格修订流程

### Requirement: timer 操作复制边界与重放确定性

timer 采用**装载/撤销入日志、到期与观察零日志**的复制边界（半入日志家族：沿 v5/v10"迁移入日志"纪律——装载决定"未来某刻对谁可见"是复制态迁移，落地纪律第 2 条对本原语不适用豁免；**到期是派生谓词不是迁移**——`marked = armed ∧ 判定时刻 ≥ fire_at_ms` 为复制数据与钟的纯函数，v7 就绪驱动"到期是可见性判定而非复制状态迁移"公式的类目化，与 v3 租约到期"到期是状态迁移故入日志"构成判例族两端对照）：SCHEDULE/DISARM 每一变异操作 MUST 恰产生**一条 `TIMER_OP_ENTRY`**（`command_payload` 为 `TimerOpRequest` 序列化+会话包装，判例既有命令条目载荷形态）；AWAIT/CANCEL/QUERY MUST NOT 产生任何日志条目、MUST NOT 触达提交通道与 apply 路径（"等待是订阅不是状态"与"观察不是迁移"两判例族直接延伸）；**到期（时钟越过 `fire_at_ms`）MUST NOT 产生任何日志条目**——不存在"fire 事件"的复制形态，唤醒为 Leader 本地扫描提示、终态为等待方重发时的谓词重评。由此推导的常驻守卫断言（改动本条款 MUST 先经 DECISIONS.md 决策记录登记）：

1. 代次、装载态与绝对到期时刻为纯确定性 apply 迁移：SCHEDULE 的 `fire_at_ms` 由**条目携带时刻**（`wall_clock_ms`）加 `delay` 在应用点折算——回放与 live 应用同值（判例 v7 队列到期折算逐字适用），MUST NOT 依赖接收时刻、Leader 身份或本地随机数；跨副本 digest MUST 逐字节一致；
2. **到期零条目反向守卫**：纯 timer 流量下，日志仅在 SCHEDULE/DISARM 受理时新增；让时钟越过全体到期点后日志条数、digest 与快照 MUST 恒不变（等待/查询读数变化不进入任何复制面）——"到期误入日志即红"与"等待误入日志即红"构成对偶双守卫；
3. 重放幂等：每会话装载去重槽随账簿回放恢复——重启/追赶后命中槽的旧 rid 重发 MUST NOT 双换代、MUST NOT 双改态；槽被同会话新装载覆盖后的迟到重发按新变异执行（声明竞态与 v7 交付槽同口径）；代次 MUST 单调不回退（快照+尾部回放终态与不中断运行逐字段一致）；
4. 等待集为 Leader 进程易失态：换主/重启后恒空、由客户端重挂补登记——与"唤醒谓词在复制账簿"合流使重挂无损耗（已到期即刻 `OK` 了结、未到期续挂；对照 v9 signal 事件灭失窗：timer 无第三方事件可丢失，与 phaser 同侧证据再强化）；
5. 应用点 MUST NOT 依赖墙钟外生输入：`armed`/代次/`fire_at_ms` 迁移全部由条目数据决定；marked 折算仅发生在 Leader 应答读面（AWAIT 终态/QUERY），MUST NOT 回写账簿（无 `TRIPPED` 驻留位——对照 `LEASE_EXPIRE_ENTRY` 必须入日志的释放迁移：timer 到期无任何位需被置上）；
6. 编号证据：`TIMER_OP_ENTRY = 15` 为 v11 新增且仅 timer 占用；1–14 既有值语义逐项不变、MUST NOT 复用为 timer 变体；守卫基线由 v10 时代的"RaftEntryType 止于 14（14 为 phaser 专用）"更替为"止于 15（15 为 timer 专用）"（更替随本 change 常驻、断言按当前版本口径表述——防止两代上界证据同时锚死过期绝对值）。

#### Scenario: 变异单条目、等待查询零条目

- **WHEN** 三节点集群稳定 Leader 下执行 SCHEDULE × a、DISARM × b、AWAIT × c、CANCEL × d、QUERY × e，全程无其他业务
- **THEN** 日志新增条目数恰为 a+b 且全部为 `TIMER_OP_ENTRY`；等待/取消/查询贡献零条；三副本 digest 逐字节相等

#### Scenario: 时钟越过到期点日志与摘要恒不变

- **WHEN** 装载 `delay=T` 后推进集群时钟越过 T+tick×k（k≥2），期间执行若干 AWAIT 了结与 QUERY 读数，无新装载/撤销
- **THEN** `TIMER_OP_ENTRY` 计数与最后一条变异条目时刻相比零新增、三副本 digest 与快照字节不变、各副本对 `fire_at_ms` 的账簿读数不变——到期"发生"于谓词而不留复制足迹

#### Scenario: 到期时刻应用点折算跨副本与回放不改判

- **WHEN** 含 SCHEDULE 条目的日志在 Follower 回放、并经快照+尾部回放重建
- **THEN** 各副本对同一 key 的 `fire_at_ms` 逐字节相等（=条目 `wall_clock_ms`+`delay`，与各节点本地时钟无关）；换主后新 Leader 不改判既有到期时刻（判例 v7 确定化条款）

#### Scenario: 重启不双换代与迟到重发声明竞态

- **WHEN** 节点自含去重槽的快照重启并回放尾部 SCHEDULE/DISARM 条目，其间客户端以已覆盖槽位的旧 request_id 重发装载
- **THEN** 命中恢复槽的重发回放声（同代次）；槽被覆盖后的迟到重发按新变异执行（新代次、清钟——v5 D7/v7 同口径的声明竞态，测试钉两形态可观测边界）；代次全程单调不回退

#### Scenario: 换主等待清零重挂了结无损耗

- **WHEN** 等待者挂起于未到期当代期间换主、时钟于换主窗内越过 `fire_at_ms`，客户端经保活重发 `AWAIT(原 request_id)`
- **THEN** 新 Leader 账簿含原代次/到期时刻（复制态存续）、等待集仅含重挂登记（旧集合随进程灭）；重发即刻 `OK{marked=true}` 了结——"换主窗内已响的钟"由谓词自愈，MUST NOT 出现任何丢失触发的形态

#### Scenario: 到期误入日志即构建红

- **WHEN** 未来变更在到期判定/驱动扫描路径引入日志条目提交（如引入 `TIMER_FIRE_ENTRY` 型驻留位）
- **THEN** 本条款"时钟越过到期点日志恒零新增"断言转红，变更被打回规格修订流程（派生裁决的常驻守卫）
