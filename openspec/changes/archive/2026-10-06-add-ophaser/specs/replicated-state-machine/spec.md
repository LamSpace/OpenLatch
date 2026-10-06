# Spec Delta

## MODIFIED Requirements

### Requirement: topic 零复制日志边界

topic 为项目首个零复制日志原语：SUBSCRIBE/UNSUBSCRIBE/PUBLISH 与全部 fan-out 交付 MUST NOT 产生任何 Raft 日志条目，MUST NOT 触达状态机 apply 路径、`ApplyResult` 或 Leader 提交通道（判例对偶：队列"等待不入日志"，topic 更进一步——**连状态迁移都不入日志**，因 topic 无复制态）。由此推导的常驻守卫断言（防后续演进按"读写皆经提交"惯性误塞日志，改动本条款 MUST 先经 ROADMAP 决策记录登记）：

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

await 条目的应用语义 MUST 跨副本确定：释放半程（持有归属匹配则重入一步清零+清租约，不匹配则零操作）为复制态迁移，全部副本一致重放；登记半程（条件等待集写入）MUST 为 Leader-only 应用副作用，不进入任何副本的复制状态、不产生观察 digest 差异。由此推导的换主/重启语义契约：**等待是承诺**——await 条目重放使释放半程确定生效，等待集随 term 清零后由客户端重挂补登记（重挂非双登记、重挂会话非持有者属常态）；**signal 是事件**——搬运与通知不经日志、不重放、不补偿，换主窗内已发出未送达的唤醒不保证。守卫断言（常驻，防后续演进误塞日志或误持久化等待集，改动本条款 MUST 先经 ROADMAP 决策记录登记）：

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

## ADDED Requirements

### Requirement: phaser 操作复制边界与重放确定性

phaser 采用**变异全入日志、等待与观察零日志**的复制边界（沿 v5 Barrier"到场是状态迁移"判例，与 v8/v9 的零日志豁免类目相对——落地纪律第 2 条对本原语不适用豁免）：REGISTER/ARRIVE/ARRIVE_AND_AWAIT（到场半程）/ARRIVE_AND_DEREGISTER 每一变异操作 MUST 恰产生**一条 `PHASER_OP_ENTRY`**（`command_payload` 为 `PhaserOpRequest` 序列化+会话包装，判例既有命令条目载荷形态）；AWAIT_ADVANCE/CANCEL/QUERY MUST NOT 产生任何日志条目、MUST NOT 触达提交通道与 apply 路径（等待登记/摘除/读数为 Leader 本地裁决——"等待是订阅不是状态"与"观察不是迁移"两类目并入既有"等待不入日志"判例族）。由此推导的常驻守卫断言（改动本条款 MUST 先经 ROADMAP 决策记录登记）：

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
