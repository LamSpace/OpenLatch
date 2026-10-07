# Spec Delta

## ADDED Requirements

### Requirement: v11 timer 门控与入口裁决

服务端 SHALL 以 v11 门承载 timer 车道：握手中声明版本 ≤10 的会话发送 `TIMER_OP` MUST 以 `INVALID_REQUEST` 消息级拒绝且 MUST NOT 断开连接（判例 v3–v10 门），该会话其余请求行为逐项不变。`TIMER_OP` 请求的入口裁决顺序钉死：握手门 → 会话有效 → key 形状（非空、≤`maxKeyLength`，违例既有映射）→ 操作词合法（枚举界外 `INVALID_REQUEST`）→ 形状互斥矩阵（`delay_ms`/`await_request_id` 携带规则见 wire-protocol v11 增量；违例 `INVALID_REQUEST`、账簿与等待集零扰动）。集群形态下车道分派按状态迁移性分轨：SCHEDULE/DISARM 为**变异操作**，MUST 经 Leader 提交通道入状态机应用（`TIMER_OP_ENTRY`），受理点预检仅做形状/门控/delay 上限快速拒绝（越界在带拒绝零条目，判例 v7 入口钳制先于提交、lease 钳制先例）；AWAIT/CANCEL/QUERY 为**Leader 本地操作**，MUST NOT 触达提交通道，由 Leader 直接受理回执（等待登记/摘除/读数均为易失本地裁决——判例 v8 直受理、v10 AWAIT_ADVANCE/QUERY 零提交）。非 Leader 节点收到任何 `TIMER_OP` 操作词（含只读 `QUERY` 与本地类 `AWAIT`）MUST 以同型 `timer_op_response` 携带 `NOT_LEADER` 拒绝、零副作用、无 leader 提示字段（判例 QUEUE/TOPIC/CONDITION/PHASER 直发车道；拒绝码形同型条款常驻——W10 判例）。已定型他家族 key 上的 `TIMER_OP` 与 `ACQUIRE`/其余车道消息携带 `LOCK_TYPE_TIMER` 或命中 timer key 的跨家族请求，MUST 走既有 `REJECT_TYPE_MISMATCH` 线路映射 `INVALID_REQUEST`。

新增配置两枚：`max-timer-horizon-ms`（`openlatch.server.timer.max-horizon-ms`，默认 86400000、合法域 [1000, 604800000]——delay 入带钳制，防"永远不响的钟"占驻账簿，判例 `max-queue-capacity`/`max-subscribers-per-key` 取值双轨纪律）与 `timer-ready-tick-ms`（`openlatch.server.timer.ready-tick-ms`，默认 200、MUST ≥10——timer 唤醒扫描周期，与队列 `ready-tick-ms` **分名分值分轨**：两原语唤醒精度各自独立可调、互不耦合调参）。非法域启动 MUST 快速失败并给出可定位错误；启动日志按既有护栏行格式扩展呈现两值。delay 超限的在带拒绝以 `INVALID_REQUEST` 承载（参数越界线——租约越界判例，**非**资源护栏码，与 horizon 语义"该延迟不可受理"对齐）；等待登记超限骑既有 `max-queue-depth-per-key` 合并计数口径（等待队列+全部条件集+phaser 等待集+timer 等待集合计，超限 `OVERLOADED`、等待集零扰动——v9 D7 合并护栏延伸第三家）。

#### Scenario: v10 会话 timer 门控不断连

- **WHEN** 以 `client_protocol_version = 10` 握手的会话发送任意 `TIMER_OP` 操作词，随后在同一连接发送 v10 语义的 phaser 操作
- **THEN** `TIMER_OP` 回 `timer_op_response`/`INVALID_REQUEST` 且连接保持；phaser 操作照常服务（v≤10 行为逐项不变）

#### Scenario: horizon 超限在带拒绝零条目

- **WHEN** SCHEDULE 携带 `delay_ms` 超过 `max-timer-horizon-ms` 到达 Leader
- **THEN** 受理点回 `INVALID_REQUEST`，不提交任何日志条目、既有账簿与等待集零扰动；后续合规装载照常

#### Scenario: Follower 全操作词同型零副作用

- **WHEN** 集群中向 Follower 发送 SCHEDULE/DISARM/AWAIT/CANCEL/QUERY 各一
- **THEN** 五应答均为 `timer_op_response`/`NOT_LEADER` 且 `op` 回显，Leader 侧账簿与 Follower 本地均无任何变化；客户端按既有改道纪律重投

#### Scenario: 会话切换放弃未回执装载

- **WHEN** SCHEDULE 提交在途时会话死亡、`SESSION_CLOSE` 先于条目应用抵达
- **THEN** 既有提交轨纪律原样适用（会话失效裁决、无半装载形态）；timer 账簿不因死亡出现"装载半途"歧义——变异要么经提交生效、要么整体不存在

### Requirement: timer 到期唤醒驱动与等待闭环

Leader SHALL 运行 `TimerReadyDriver` 承载到期唤醒：单守护线程按 `timer-ready-tick-ms` 固定周期扫描（判例 `QueueReadyDriver`/`LeaseExpiryDriver` 逐件同型——Leader 角色短路且判定在网关内再核一道防角色事件与调度时序窄窗；**当选立即首扫**补偿换任窗漏扫：挂起者随换主清零重挂，其"挂起时已到期"存量状态需首扫即判即了结）；扫描仅对"有挂起等待者且当代 `PENDING ∧ now ≥ fire_at_ms`"的 timer key 向全部在集项推送 `AWAIT_NOTIFY`（ref=等待项 request_id，复用既有推送桥）；驱动 MUST NOT 提交任何日志条目、MUST NOT 改变复制状态（到期无位可置——谓词由等待方重发时在判定时刻重评，误唤醒与漏唤醒同由重评自愈：漏扫的钟不丢状态，只延迟唤醒；已到期无等待者的 key 扫描零动作，fire 的"发生"无需通知任何不在场者）。撤销唤醒不依赖本驱动：DISARM 在 apply 关键区即时收集唤醒（事件驱动，到期靠钟、撤销靠事件的唤醒双源成文）。

等待闭环：`AWAIT` 登记即回执 `QUEUED`（`queue_position` 恒 0——广播谓词等待非队首批授，判例 phaser）；被通知方 MUST 以**原 request_id** 重发 `AWAIT` 重评谓词取终态（`OK{marked=true}` 或 `DENIED`，等待即终结无回炉）；重挂/重发 MUST NOT 造成第二次换代或重复登记（装载去重槽与等待幂等双护栏——**v10 过程真缺陷教训：分片重挂误换 request_id 致重复计数，本原语装载幂等面同轮钉死**）。超时纪律：等待超时由客户端本地计时，到期 MUST 发 `CANCEL` fire-and-forget（失败不致命——三路回收兜底：显式 CANCEL/会话死亡摘除/换主清零，加 `headReplyTimeoutMs` 已通知 ghost 清扫），随后 SDK 以超时形态收束。换主语义：等待集与已通知时序为 Leader 进程易失态随 term 清零；复制账簿（代次/装载态/到期时刻/去重槽）经日志与快照跨换主存续；客户端双通道自动重挂（车道激活事件+周期保活，判例 v8/v10）使**已到期在等者于重发即 `OK` 了结、未到期者续挂新集合——换主窗不产生丢失的到期**（谓词在复制账簿，与 v9 条件 signal 事件灭失窗构成可靠性分层对照，防混读句随行）。

#### Scenario: 到期 tick 唤醒与重发了结

- **WHEN** 等待项 W 挂起于 `PENDING@T`，时刻越过 T 后驱动下一 tick 推 `AWAIT_NOTIFY`，W 以原 request_id 重发 `AWAIT`
- **THEN** W 被唤醒、重发谓词重评成立回 `OK{marked=true, generation=当代}`；重复通知/重复重发以首个终态为准（幂等取数）

#### Scenario: 换主窗已到期钟重挂即了结

- **WHEN** W 挂起期间换主且 `fire_at_ms` 早于换主时刻，新 Leader 首扫前/后 W 经保活重发 `AWAIT`
- **THEN** 新 Leader 账簿含原代次与到期时刻（复制态存续）、重发即刻 `OK` 了结——到期未被换主窗丢失（对照条件 signal 丢失窗的差分断言）；新 Leader 首扫对已重挂未了结者照常补偿唤醒

#### Scenario: 到期恒零条目驱动不改日志

- **WHEN** 驱动连续扫描含已到期未了结、已撤销、无等待者已到期三类 timer key 各若干
- **THEN** 全程 `TIMER_OP_ENTRY` 贡献为零、快照与 digest 不因扫描变动——唤醒提示是纯读副作用（`TimerReadyDriver` 与 `QueueReadyDriver` 的同型条款）

#### Scenario: 等待误入日志与到期误入日志双向即红

- **WHEN** 未来变更在 AWAIT/CANCEL/QUERY 分发路径引入状态机提交调用，或在到期判定路径引入 fire 条目提交
- **THEN** "等待/取消/查询条目贡献恒零"与"时钟走过到期点日志恒零新增"两断言任一即红，变更被打回规格修订流程
