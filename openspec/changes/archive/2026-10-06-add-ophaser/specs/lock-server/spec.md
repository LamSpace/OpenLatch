# Spec Delta

## ADDED Requirements

### Requirement: v10 phaser 门控与入口裁决

服务端 SHALL 以 v10 门承载 phaser 车道：握手中声明版本 ≤9 的会话发送 `PHASER_OP` MUST 以 `INVALID_REQUEST` 消息级拒绝且 MUST NOT 断开连接（判例 v3–v9 门），该会话其余请求行为逐项不变。`PHASER_OP` 请求的入口裁决顺序钉死：握手门 → 会话有效 → key 形状（非空、≤`maxKeyLength`，违例 `KEY_EMPTY`/`KEY_TOO_LONG`/`INVALID_REQUEST` 既有映射）→ 操作词合法（枚举界外 `INVALID_REQUEST`）→ 形状互斥矩阵（`parties`/`expected_phase`/`await_request_id` 携带规则见 wire-protocol v10 增量；违例 `INVALID_REQUEST`、账簿与等待集零扰动）。集群形态下车道分派按状态迁移性分轨：REGISTER/ARRIVE/ARRIVE_AND_AWAIT（到场半程）/ARRIVE_AND_DEREGISTER 为**变异操作**，MUST 经 Leader 提交通道入状态机应用（`PHASER_OP_ENTRY`），受理点预检仅做形状/门控/配额上限快速拒绝（超限在带拒绝零条目，判例 v7 入口钳制先于提交）；AWAIT_ADVANCE/CANCEL/QUERY 为**Leader 本地操作**，MUST NOT 触达提交通道，由 Leader 直接受理回执（等待登记/摘除/读数均为易失本地裁决——判例 v8 SUBSCRIBE/UNSUBSCRIBE 直受理、v9 signal 家族零日志）。非 Leader 节点收到任何 `PHASER_OP` 操作词（含 QUERY 只读与 AWAIT_ADVANCE）MUST 以同型 `phaser_op_response` 携带 `NOT_LEADER` 拒绝、零副作用、无 leader 提示字段（判例 QUEUE/TOPIC/CONDITION 直发车道；拒绝码形同型条款常驻——W10 判例）。已定型他家族 key 上的 `PHASER_OP` 与 `ACQUIRE`/其余车道消息携带 `LOCK_TYPE_PHASER` 或命中 phaser key 的跨家族请求，MUST 走既有 `REJECT_TYPE_MISMATCH` 线路映射 `INVALID_REQUEST`。

新增配置 `max-parties-per-phaser`（默认 1024、合法域 [1,65536]，判例 `max-queue-capacity`/`max-subscribers-per-key` 的"默认适中、域上限防单键吃内存"双轨取值纪律）：非法域启动 MUST 快速失败并给出可定位错误；启动日志按既有护栏行格式扩展呈现该值。parties 超限的在带拒绝以 `OVERLOADED` 承载（`StatusCode` 零新增，资源护栏语义骑既有码——v7 等待满轨判例；对照 v8 `REJECT_SUBSCRIBERS` 立枚分岔的理由由 design 记载）；等待登记超限骑既有 `max-queue-depth-per-key` 合并计数口径（等待队列+全部条件集+phaser 等待集合计，超限 `OVERLOADED`、等待集零扰动——v9 D7 合并护栏延伸）。

#### Scenario: v9 会话 phaser 门控不断连

- **WHEN** 以 `client_protocol_version = 9` 握手的会话发送任意 `PHASER_OP` 操作词，随后在同一连接发送 v9 语义的锁获取
- **THEN** `PHASER_OP` 回 `phaser_op_response`/`INVALID_REQUEST` 且连接保持；锁获取照常服务（v≤9 行为逐项不变）

#### Scenario: 配额超限在带拒绝零条目

- **WHEN** 某 phaser key 的 `registeredParties` 已达 `max-parties-per-phaser`，新 REGISTER 到达 Leader
- **THEN** 受理点回 `OVERLOADED`，不提交任何日志条目、既有配额与等待集零扰动；其余参与者后续到场/查询照常

#### Scenario: Follower 全操作词同型零副作用

- **WHEN** 集群中向 Follower 发送 REGISTER/ARRIVE/AWAIT_ADVANCE/QUERY/CANCEL 各一
- **THEN** 五应答均为 `phaser_op_response`/`NOT_LEADER` 且 `op` 回显，Leader 侧账簿与 Follower 本地均无任何变化；客户端按既有改道纪律重投

#### Scenario: 等待深度合并护栏

- **WHEN** 某 key 的等待队列+条件等待集+phaser 等待集合计已达 `max-queue-depth-per-key`，新 AWAIT_ADVANCE 到达 Leader
- **THEN** 回 `OVERLOADED`，等待集零扰动；既有等待项与到场账簿不受影响

### Requirement: phaser 等待服务端闭环与自动重挂

Leader SHALL 对 `AWAIT_ADVANCE`/`ARRIVE_AND_AWAIT` 的等待半程执行"登记—通知—了结"闭环：登记即回执 `QUEUED`（等待集 `(session, request_id) → expected_phase`，到达序）；合拢应用点对 `expected_phase < 新相位` 的在集项发 `AWAIT_NOTIFY`（ref=其 request_id，复用既有推送桥与 `AwaitTracker` 侧的按 id 过滤挂起纪律，`queue_position` 对 phaser 不承载语义）；被通知方以原 request_id 重发 `AWAIT_ADVANCE`/`ARRIVE_AND_AWAIT` 时 MUST 命中了结记录直接回 `OK`（相位回显为**当前**相位——重发是了结取数非重新等待；了结记录窗口外的迟到按新等待入集，声明竞态判例 v5 D7）。`ARRIVE_AND_AWAIT` 的到场若恰触发合拢，回执直接 `OK` 不发生 `QUEUED`（该次请求无等待半程）。超时纪律：等待超时由客户端本地计时，到期 MUST 发 `CANCEL` fire-and-forget（失败不致命——三路回收兜底：显式 CANCEL/会话死亡摘除/换主清零，加 `headReplyTimeoutMs` 已通知 ghost 清扫），随后 SDK 以 `TimeoutException` 形态收束（JDK `awaitAdvanceInterruptibly` 对偶）。

换主语义：等待集与已通知时序为 Leader 进程易失态，随 term 清零；复制账簿（相位/配额/arrived/去重槽/了结记录）经日志与快照跨换主存续。客户端 SHALL 以双通道自动重挂恢复等待服务（判例 v8 激活事件+周期保活）：车道激活事件与保活周期到达时，对句柄内全部活跃等待项重发 `AWAIT_ADVANCE(原 expected_phase)`——新 Leader 上该请求是**纯状态谓词**判定：`expected_phase < 当前相位` 即刻 `OK` 了结（换主窗内已发生的推进不丢失，重挂无损耗）、否则续挂新集合。重挂 MUST NOT 产生第二次计数或重复了结（请求身份幂等）。与 v9 的分层对照 MUST 在契约与指南呈现：**条件的唤醒依赖换主窗外不重放的 signal 事件（丢失窗），phaser 的唤醒谓词由复制态相位承载（重挂自愈）**——两者同用 `AWAIT_NOTIFY` 推送但可靠性分层不同，防混读。

`PHASER_OP` 车道 MUST NOT 引入新定时器（合拢唤醒全部由 apply 点事件驱动——v9 D7 同相；对照 v7 `ready-tick-ms` 的缺失为有意设计）；MUST NOT 改动 `QueueReadyDriver` 或其他既有驱动。

#### Scenario: 合拢通知与重发了结

- **WHEN** 等待项 W（`expected_phase=5`）挂起后该 key 合拢推进至相位 6，W 收到 `AWAIT_NOTIFY` 并以原 request_id 重发 `AWAIT_ADVANCE(5)`
- **THEN** W 被唤醒通知送达、重发命中了结记录回 `OK{phase=6}`；对同 request_id 的重复通知/重复重发以首个了结为准（幂等取数）

#### Scenario: 到场即合拢不经等待

- **WHEN** `ARRIVE_AND_AWAIT` 的到场恰使 `arrived ≥ registered`
- **THEN** 该请求回执直接 `OK{phase=到场相位}`，不发生 `QUEUED`、不入等待集；其唤醒通知的接收者是**其他**在集等待项

#### Scenario: 换主窗推进后重挂即刻了结

- **WHEN** 等待项 W（`expected_phase=5`）挂起期间换主，且相位在旧 Leader 已推进至 6（变异条目已提交），新 Leader 等待集为空，W 经保活通道重挂
- **THEN** 新 Leader 判定 `5 < 6` 直接回 `OK`，W 无损耗醒转（对照条件 signal 丢失窗：无需任何"补事件"——谓词即状态）

#### Scenario: 超时 CANCEL 与 ghost 兜底

- **WHEN** 等待超时到期、客户端发 CANCEL 但该请求在途丢失；其后合拢发生并通知该 request_id
- **THEN** 通知送达时客户端等待项已终止、按既有纪律忽略；条目侧 ghost 由 `headReplyTimeoutMs` 清扫出集；该会话配额与到场计数不受影响（ghost 只占等待集位，且已计入合并护栏）
