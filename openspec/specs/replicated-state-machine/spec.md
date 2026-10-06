# replicated-state-machine Specification

## Purpose

定义锁服务集群复制状态机的行为契约：哪些锁状态经日志复制到多数派、以何种条目格式复制、回放如何保持确定性（时间语义、幂等），以及 Leader 处理写请求、会话集群登记与租约到期驱动的可观察行为，使"已确认授予的锁不丢、任何时刻同 key 至多一个持有者"的保证可验证。
## Requirements

### Requirement: 复制边界
系统 SHALL 将锁持有关系（key、mode、持有者、计数）、租约（token、到期时刻、租期）与会话注册表（sessionId → nodeId）作为复制状态经 Raft 日志复制到多数派；FIFO 等待队列与服务端限额/配置 MUST NOT 进入复制状态、MUST NOT 进入快照与回放。

#### Scenario: failover 后已授予锁有效
- **WHEN** 锁经多数派确认授予后 Leader 切换
- **THEN** 新 Leader 上原持有者仍可正常释放/续租该锁，锁的到期时刻与切换前一致

#### Scenario: 等待队列不随切换迁移
- **WHEN** Leader 切换且旧 Leader 内存中有排队中的等待项
- **THEN** 等待位次丢失，等待者收到 NOT_LEADER 或超时后向新 Leader 重新排队；锁的互斥性与持有关系不受影响

### Requirement: 日志条目格式与编号稳定
系统 SHALL 在服务端内部消息 `raft.proto` 中定义 `RaftEntryType`（RAFT_ENTRY_UNKNOWN=0 / SESSION_OPEN=1 / SESSION_CLOSE=2 / LOCK_ACQUIRE_ENTRY=3 / LOCK_RELEASE_ENTRY=4 / LEASE_RENEW_ENTRY=5 / LEASE_EXPIRE_ENTRY=6 / NOOP=7）与 `RaftLogEntry`（type、seq、wall_clock_ms、command_payload），字段与枚举编号一经发布 MUST NOT 变更或复用；`command_payload` SHALL 复用 Phase 1 请求消息序列化并附 sessionId。该消息 MUST NOT 出现在客户端 wire format（Envelope oneof）中。

#### Scenario: 条目编号冻结
- **WHEN** 后续变更修改了 raft.proto 中任何已发布字段号或枚举值
- **THEN** 编号冻结校验失败，构建报错

#### Scenario: 客户端 wire format 零扰动
- **WHEN** 本变更合入后运行 Phase 1 协议回归测试
- **THEN** Envelope 及相关客户端消息定义与 Phase 1 逐字段一致，v1 客户端握手与业务路径行为不变

### Requirement: 回放确定性与条目时刻时间语义
状态机 SHALL 以"同一条目序列在任何副本、任何时刻回放产生同一复制状态"为契约：回放期间时间语义 MUST 取条目携带时刻（授予的到期时刻 = 条目内时刻 + 租期；到期判断不在回放侧以物理时钟发生）。锁语义核心 MUST NOT 为此改动其公开行为契约。

#### Scenario: 同一序列两次回放逐字段一致
- **WHEN** 任意随机生成的合法条目序列（获取/释放/续租/过期/会话开闭混排，含随机携带时刻）在干净状态机上重放两次
- **THEN** 两次回放后的复制状态摘要（key、mode、持有者+计数、leaseToken、到期时刻、会话注册表）逐字段一致

#### Scenario: 回放与物理时钟无关
- **WHEN** 同一条目序列分别立即重放与延迟（模拟追赶跨越真实到期点）重放
- **THEN** 两次终态一致；历史 `LEASE_EXPIRE_ENTRY` 照常重放生效，不因回放时刻"看起来已过期"而提前或跳过

### Requirement: Leader 写请求路径
Leader SHALL 按"预检查（快速失败）→ 提交日志 → 多数派确认并应用 → 按应用结果应答"处理 ACQUIRE 授予路径与 RELEASE / RENEW；"需排队"的 ACQUIRE SHALL 由 Leader 本地登记等待队列并即时应答 QUEUED，MUST NOT 写日志。预检查结果仅是快速失败通道，最终语义以应用结果为准：提交后因并发导致不可授予的 ACQUIRE 条目，Leader SHALL 在应用点将等待者登记进本地队列并应答 QUEUED。失去 Leadership 时未收到应用结果的在途请求 MUST 立即以可重试错误完成，MUST NOT 悬挂。

#### Scenario: 授予应答等于应用结果
- **WHEN** 客户端向 Leader 获取无竞争锁
- **THEN** 应答在条目多数派确认并应用之后返回，且应答内容与各副本应用结果一致

#### Scenario: 排队路径零日志增长
- **WHEN** 对已持有锁的 key 提交带排队的 ACQUIRE
- **THEN** 客户端即时收到 QUEUED + 位次，且集群日志条目数不变

#### Scenario: 并发同键预演失效
- **WHEN** 两个不同会话对同一空闲 key 的 ACQUIRE 并发到达 Leader，均通过预检查进入日志
- **THEN** 先到条目授予成功；后到条目应用结果为"需排队"，Leader 应答该客户端 QUEUED 并登记本地队列，任何副本不因此产生双授

#### Scenario: Leadership 丧失时在途请求快速失败
- **WHEN** 请求已提交未达多数派时节点失去 Leadership
- **THEN** 该请求的应答立即以错误完成（可重试语义），不等待原提交结果

### Requirement: 会话集群登记
系统 SHALL 在 HELLO 时由接入节点分配全局唯一 `sessionId = (nodeId, localSeq)`（高位编码 nodeId），并向集群提交 `SESSION_OPEN`；写请求的处理以复制状态中 sessionId 已登记为前置。接入节点检测到连接断开 SHALL 提交 `SESSION_CLOSE(sessionId)`，复制状态机对其执行与单机"断连清理"等价的语义（释放该会话全部持锁、摘除等待项）；Leader 检测到接入节点失联 SHALL 对归属该节点的会话批量补发 `SESSION_CLOSE`。

#### Scenario: 断连清理经复制一致生效
- **WHEN** 客户端与接入节点连接断开且该会话持有锁
- **THEN** 三副本各自应用 `SESSION_CLOSE` 后复制状态摘要一致，该会话的锁被释放且队首可被授予新持有者

#### Scenario: 接入节点失联触发批量清理
- **WHEN** 归属某宕机节点的会话在复制状态中仍有登记
- **THEN** Leader 为每个失联会话补发 `SESSION_CLOSE` 条目，各副本一致地释放其持有的锁

#### Scenario: 未登记会话的写请求被拒
- **WHEN** 写请求携带的 sessionId 不在复制状态的会话注册表中
- **THEN** 请求被拒绝（REJECT_SESSION），不产生日志条目

### Requirement: 租约到期 Leader 驱动复制
租约到期判断 SHALL 只发生在 Leader：扫描线程发现到期即追加 `LEASE_EXPIRE_ENTRY(key, leaseToken)` 日志，所有副本回放该条目时才真正释放。回放 SHALL 以 leaseToken 幂等校验：条目 token 与当前持有的 token 不匹配（锁已易主或已释放）时该条目 MUST 为空操作。Leader 切换后，新 Leader 的扫描线程 SHALL 按已复制的到期时刻继续驱动到期，误差 ≤ 扫描周期。

#### Scenario: 到期经复制全副本生效
- **WHEN** 一把锁的租约到期且 Leader 提交 `LEASE_EXPIRE_ENTRY`
- **THEN** 各副本一致释放该锁；到期释放对等待队首的唤醒仅发生在 Leader 本地

#### Scenario: 过期条目不误杀新持有者（ABA）
- **WHEN** `LEASE_EXPIRE_ENTRY(key, tokenA)` 提交在途期间，key 经完整释放-重新授予流程换了新 token
- **THEN** 该条目回放为空操作，新持有者的锁保持有效

#### Scenario: failover 后到期继续生效
- **WHEN** 锁的到期时刻落在 Leader 切换窗口内
- **THEN** 新 Leader 选出后该锁在"条目时刻 + 租期"的到期点后不超过一个扫描周期被释放，不因切换而永久漏扫或提前释放

### Requirement: 多数派可用性
3 节点集群 SHALL 在任意 1 节点不可用时正常授予/释放/续租（多数派仍满足）；SHALL 在任意 2 节点不可用时拒绝所有写请求（不可授予，以可重试错误或超时呈现），且 MUST NOT 因失联节点的在途条目产生双授。

#### Scenario: 停 1 节点仍可服务
- **WHEN** 3 节点集群停止任一 Follower
- **THEN** 客户端对 Leader 的授予/释放/续租持续成功，恢复该节点后其复制状态与 Leader 一致

#### Scenario: 停 2 节点不可授予
- **WHEN** 3 节点集群仅剩 1 个原 Leader 存活
- **THEN** 写请求全部失败（拒绝或超时），无任何请求收到"授予成功"的应答

### Requirement: Follower 写请求分车道

非 Leader 节点（Follower）对客户端写请求 SHALL 按车道区分处理：

1. **ACQUIRE（新授予/排队）拒绝**：ACQUIRE 的排队登记与 `AWAIT_NOTIFY` 推送是 Leader 本地状态，Follower 受理无法保证通知送达，因此 Follower 收到 ACQUIRE MUST 立即以 `NOT_LEADER` + 当前 Leader 提示应答（本节点无法给出 Leader 身份时提示为 `-1`；过渡窗内 MAY 为最后已知值，客户端兜底），MUST NOT 产生日志条目、MUST NOT 登记任何等待状态；
2. **RELEASE / RENEW（存量操作）转发**：连接所属会话（其 `sessionId` 已经 `SESSION_OPEN` 登记于复制状态）在 Follower 发出的 RELEASE/RENEW，SHALL 经内部提交通道转发至当值 Leader 复制执行，应答内容与客户端直发 Leader 的结果一致——会话归属节点不因 Leadership 变化而丢失对其存量锁的释放/续租能力；
3. **会话未登记的拒绝**：转发车道上，Leader 侧校验条目 `sessionId` 不在复制状态会话注册表时 MUST 以 `SESSION_EXPIRED` 拒绝（不产生条目效果）。

Leadership 变更后，Follower 的 Leader 提示（HELLO / `NOT_LEADER` 应答携带）SHALL 跟随新 Leader 更新；提示瞬时发现不可用（事件未达、选举中）MUST 以 `-1` 表达，客户端 MUST 能以重连/种子发现兜底。

#### Scenario: Follower 拒绝 ACQUIRE 并提示

- **WHEN** 客户端向 Follower 发送 ACQUIRE
- **THEN** 立即收到 `NOT_LEADER` + 当前 Leader 的 nodeId 与地址提示，且集群日志条目数不变、该 Follower 无等待队列变化

#### Scenario: 存活会话跨 failover 续租释放

- **WHEN** 客户端会话登记于节点 F，Leader 发生切换，客户端经 F 持续续租并在切换后释放其 failover 前持有的锁
- **THEN** 续租全程经转发车道成功（看门狗无失败计数增长），释放成功且各副本复制状态一致

#### Scenario: 无多数派快速失败

- **WHEN** 集群失去多数派（无 Leader 可当选）时客户端向存活节点发送 ACQUIRE
- **THEN** 在请求时限内收到 `NOT_LEADER` 应答、请求不悬挂；`leader_node_id` 为 `-1` 或最后已知 Leader

#### Scenario: 提示跟随 Leadership 更新

- **WHEN** Leadership 从节点 A 转移到节点 B 后，客户端向任意 Follower 发送 HELLO 或触发 ACQUIRE 拒绝
- **THEN** 响应提示的 Leader nodeId 为 B；Follower 自身重新当选时提示为自身

#### Scenario: 未登记会话的转发被拒

- **WHEN** 会话已被清理（其归属节点失联批量清理后）的客户端仍经原 Follower 连接发送 RENEW
- **THEN** 请求以 `SESSION_EXPIRED` 拒绝，不产生日志条目

### Requirement: Leader 提示的权威来源

服务端 SHALL 维护单一 Leadership 视图（当前 Leader 的 nodeId 与接入地址），作为 HELLO 提示、`NOT_LEADER` 随附提示与 `CLUSTER_VIEW` 作答的共同数据源；该视图的更新 MUST 源自 Raft 层的 Leadership 变更事件并保留新 Leader 身份（MUST NOT 仅折算为本节点布尔角色而丢弃）。写请求受理的权威角色判定与提示视图相互独立：提示视图滞后 MUST NOT 导致非 Leader 节点受理其不应受理的写。

#### Scenario: 单一数据源一致性

- **WHEN** 同一时刻分别经 HELLO、`NOT_LEADER` 应答与 `CLUSTER_VIEW` 查询 Leader 身份
- **THEN** 三者报告的 Leader nodeId 一致（选举空窗均为未知）

#### Scenario: 降级不误受理

- **WHEN** 本节点失去 Leadership、提示视图尚未更新的瞬间收到写请求
- **THEN** 权威角色判定拒绝受理（`NOT_LEADER`），无条目以旧任期提交

### Requirement: 失联判定的进度保护
节点失联的批量会话清理 MUST NOT 误伤存活且正在推进复制的节点：判定"失联"须以该节点复制位点在连续判定周期内**零推进**为必要条件（仅"落后于 Leader 位点"不足以判失联）——选举、快照安装与回放追赶等修复窗口内的暂时滞后不得触发清理。真实停止的节点（位点冻结）仍须在容忍周期内被判定失联并完成批量清理，该路径的既有语义不变。

#### Scenario: 修复窗口的存活副本不被误清
- **WHEN** 集群在多数派缺席后恢复（含重新选主与滞后副本回放追赶），期间某存活节点的提交位点暂时落后于 Leader
- **THEN** 该节点不被判失联，其存活会话与所持锁保持有效（续租/释放照常成功）

#### Scenario: 真实失联仍被批量清理
- **WHEN** 某节点停止（位点冻结）且 Leader 持续前推进
- **THEN** 连续容忍周期后该节点被判失联，其归属会话按每会话一条 SESSION_CLOSE 清理，各副本一致收敛

### Requirement: 扩展原语复制边界

Semaphore 的授予、释放、租约到期与 Latch 的 `countDown` SHALL 经复制日志多数派提交后应用，与锁的 `LOCK_ACQUIRE_ENTRY` 等既有条目模式一致（新增条目类型仅以新增枚举值表达，既有编号不变）；未达多数派时 MUST NOT 产生授予/扣减效果。Latch 的 `await` MUST NOT 进入日志：其为 Leader 内存队列表述 + 状态读取（计数已归零即答 OK），Leader 切换时在队 awaiter 的处置与锁等待队列同口径（不迁移、客户端重发现后重发）。

#### Scenario: 少数派分区不扣许可

- **WHEN** Leader 被隔离失去多数派时处理 Semaphore `acquire(2)`
- **THEN** 请求失败（无提交），全集群可用许可数不变

#### Scenario: countDown 经复制全副本生效

- **WHEN** `countDown` 提交后查询任一回放副本的 latch 状态
- **THEN** 计数一致下降；归零后各副本状态一致为放行态

#### Scenario: 池回收后的队首重发（Semaphore 集群生命周期）

- **WHEN** Semaphore 池随持有清零被回收后，Leader 队列中的队首以不携带总量断言的请求重发获取
- **THEN** 该请求以 `INVALID_REQUEST` 显式拒绝（纯加入者 MUST NOT 隐式重建池）；携带相符总量断言的重发则重建同规模池并按队首授予，位次序保持

#### Scenario: await 零日志增长

- **WHEN** 多个客户端在 Leader 上 await 同一未归零屏障
- **THEN** 复制日志无任何新条目

### Requirement: 许可数感知的队首推进

集群等待队列条目 SHALL 携带其请求许可数；Leader 在授予/释放/到期应用后 MUST 仅当可用许可数满足队首请求时通知队首（AWAIT_NOTIFY），队首不满足时 MUST NOT 通知队列中任何后续条目（与单机 SemaphoreEntry 判定语义等价）。Latch 归零时 Leader MUST 向该 key 全部 awaiter 广播通知。

#### Scenario: 队首不足不通知

- **WHEN** 集群模式下队首请求 2 个许可、归还 1 个后不足
- **THEN** 无任何等待者收到通知；再归还至满足队首时仅队首收到通知

#### Scenario: failover 后 latch 计数不丢

- **WHEN** latch 计数为 1 时 Leader 被杀，新 Leader 完成日志回放
- **THEN** 新 Leader 上计数为 1，`countDown(1)` 后向重连后重新 await 的等待者广播放行

### Requirement: ATOMIC 操作复制边界与重放确定性

ATOMIC 家族的全部操作（含 `GET`）SHALL 以新增日志条目类型 `ATOMIC_OP_ENTRY` 经复制日志多数派提交后应用，与既有 `LOCK_ACQUIRE_ENTRY`/`LATCH_COUNT_DOWN_ENTRY` 条目模式一致（新增条目类型仅以新增枚举值表达，既有编号不变）；未达多数派时 MUST NOT 产生值变更、版本戳推进或去重槽更新，且任一回放副本对该 key 的观察 MUST 与提交前一致。`GET` 条目应用为**零迁移**：读取当前 `(value, version)` 作为回执，MUST NOT 触碰值、版本戳与去重槽——该条目在任何副本上的重放 MUST NOT 改变状态机可观察状态。写操作条目 MUST 携请求的 `(session, op_seq)`，应用侧以条目内状态（非 Leader 本地内存）做单槽去重：同槽重放在任何副本上 MUST 得出与原应用一致的应答且版本戳不重复推进。应答四元组 MUST 由 apply 结果导出并在 Leader 侧回执客户端；Follower 回放的应答不入任何客户端连接。Leader 切换时，未提交条目 MUST 以可重试错误完成（与既有写路径同口径），已提交条目经追赶回放后在任何新 Leader 上产生逐位一致的值与版本戳。

#### Scenario: 少数派分区不加值

- **WHEN** Leader 被隔离失去多数派时处理 `ADD(1)`
- **THEN** 请求失败（无提交），全集群该 key 的值与版本戳不变

#### Scenario: 写条目经复制全副本一致

- **WHEN** `CAS_STAMPED` 成功提交后查询任一回放副本的条目状态
- **THEN** 各副本 `(value, version, 去重槽)` 逐字段一致

#### Scenario: GET 重放零迁移

- **WHEN** 含高频 `GET` 条目的日志在新增副本上全量回放
- **THEN** 回放后该 key 状态与无 GET 条目时逐字节一致（摘要相等）

#### Scenario: 换主后重试经去重槽不双加

- **WHEN** 客户端 `ADD(1)` 提交成功但应答丢失，随后 Leader 切换，客户端向新 Leader 重发同 `(session, op_seq)` 请求
- **THEN** 新 Leader 状态机含已回放的去重槽，重发命中同槽、返回一致应答，值不双加

### Requirement: BARRIER 命令复制边界与重放确定性

Barrier 的 `await`（到场）、`leave`（离场/破障）与 `actionDone`（动作了结）SHALL 分别以新增日志条目类型 `BARRIER_AWAIT_ENTRY`/`BARRIER_LEAVE_ENTRY`/`BARRIER_ACTION_DONE_ENTRY` 经复制日志多数派提交后应用（新增条目类型仅以新增枚举值表达，既有编号不变；条目载荷复用对应请求消息序列化附 sessionId，与 `LATCH_COUNT_DOWN_ENTRY`/`ATOMIC_OP_ENTRY` 模式一致）；未达多数派时 MUST NOT 产生到场计数、合拢、破障、世代回卷或了结记录变更。与 Latch `await` 零日志的边界差异 MUST 如此显式化：Barrier 到场改变的是复制状态（当前世代到场账簿、合拢判定、世代号、了结记录、执行者挂账），故 MUST 进日志；而**在队位次与通知时序**（哪个连接何时收到 `AWAIT_NOTIFY`、已通知等待者的响应超时窗口）仍是 Leader 内存表述，MUST NOT 进日志与快照。确定性契约：到场顺序与"最后到场者"判定、执行者指定、世代号推进与破障传播 MUST 由 apply 序承载并跨副本一致——同一命令序列在 Leader 与新副本上回放后，BARRIER 条目的 `(generation, 到场账簿, 动作挂账, 了结记录)` 逐项一致（状态摘要相等）；破障由 `SESSION_CLOSE` 条目应用触发时，其世代作用域与受影响等待者集合在任何副本上 MUST 一致。幂等重放：`(会话, 请求)` 到场去重与世代了结记录 MUST 是条目内复制状态而非 Leader 本地内存——Leader 切换后，客户端以同 `request_id` 重发的 `BARRIER_AWAIT` 在新 Leader 上 MUST 命中回放所得的去重/了结记录并得出与切换前一致的裁决，MUST NOT 重复计次。已通知但未重发的等待项其内存位次被超时摘除后，若其复制侧了结记录仍窗口内则重发仍可了结，窗口外重发按新到场计入当前世代（与锁/Latch 等待队列不随切换迁移同口径的显式竞态声明）。`SESSION_OPEN`/`SESSION_CLOSE` 既有条目对 BARRIER 的语义 MUST 仅为经引擎分派到 `BarrierEntry.removeSession` 的破障传播，MUST NOT 新增会话专用条目类型。

#### Scenario: 少数派分区不到场

- **WHEN** Leader 被隔离失去多数派时处理 `BARRIER_AWAIT`
- **THEN** 请求失败（无提交），全集群该 key 的到场账簿、世代号与合拢判定不变

#### Scenario: 合拢裁决跨副本一致

- **WHEN** parties=3 的三个到场命令提交后，在任一回放副本上查询该条目状态
- **THEN** 三副本的 `(generation, arrived=0, 了结记录=TRIPPED 定型)` 逐项一致，最后到场者判定一致

#### Scenario: 换主后旧请求重发不双计

- **WHEN** 到场命令提交成功但应答丢失，随后 Leader 切换；客户端向新 Leader 以同 `(会话, request_id)` 重发 `BARRIER_AWAIT`
- **THEN** 新 Leader 回放状态含该到场记录，重发命中去重、返回一致裁决，当前世代到场数不重复推进

#### Scenario: 执行者死亡破障经复制全副本一致

- **WHEN** 动作待决的 Leader 上执行者会话被关闭，`SESSION_CLOSE` 提交
- **THEN** 该世代在所有副本上经回放定型 BROKEN、世代号一致推进，无副本残留动作挂账

#### Scenario: 回放终态摘要相等

- **WHEN** 含交叠世代（合拢、回卷、破障交错）的同一命令序列分别在 Leader 与新建副本上全量回放
- **THEN** 两副本 BARRIER 条目状态摘要逐字节一致（含了结记录与动作挂账）

### Requirement: 有值引用复制边界与载荷字节级重放确定

ATOMIC 家族的有值引用形态操作 SHALL 复用既有条目类型 `ATOMIC_OP_ENTRY` 承载（其 `command_payload` 为携带 `lock_type = LOCK_TYPE_ATOMIC_REFERENCE` 与 `optional bytes` 载荷字段的 `AtomicOpRequest` 序列化，MUST NOT 新增 `RaftEntryType` 枚举值），复制边界与标量形态逐项一致：读写皆经多数派提交后应用、未达多数派 MUST NOT 产生值变更/版本戳推进/去重槽更新、`GET` 条目应用为零迁移、同 `(session, op_seq)` 重放在任何副本得出一致应答且不重复推进版本戳。`ApplyResult` SHALL 以纯增量字段承载载荷回执：`optional bytes atomic_old_value_bytes = 18`、`optional bytes atomic_value_bytes = 19`（有值引用形态 status=OK 时择用，显式 presence 口径与请求侧一致：缺省=null、零长度=""；标量形态与其余条目 MUST NOT 携带）。载荷的字节级比较与落值 MUST 跨副本确定：同一日志序列在任何副本、任何物理时刻重放，条目的值、版本戳与去重槽（含槽内载荷字节）逐字段一致，全量摘要可比对；快照序列化的载荷字节 MUST 确定性生成。状态机与条目侧 MUST NOT 以节点本地 `maxValueBytes` 复核条目载荷尺寸（超限命令在接入层即被拒绝、永不入日志，入日志者视为已钳制）——该纪律是回放确定性的前提：若 apply 按本地配置判定，跨节点配置漂移将导致同一日志一拒一受、副本分歧。

#### Scenario: 引用写条目经复制全副本一致

- **WHEN** `CAS_STAMPED`（载荷形态）成功提交后查询任一回放副本的条目状态
- **THEN** 各副本 `(value 字节, version, 去重槽含 old/value 字节)` 逐字段一致

#### Scenario: 引用 GET 重放零迁移

- **WHEN** 含有值引用 `GET` 条目的日志在新增副本上全量回放
- **THEN** 回放后该 key 状态与无 GET 条目时逐字节一致（摘要相等）

#### Scenario: 换主后引用写重发经去重槽不双写

- **WHEN** 载荷 `SET` 提交成功但应答丢失，Leader 切换后客户端向新 Leader 重发同 `(session, op_seq)` 请求
- **THEN** 新 Leader 命中已回放的去重槽，返回值字节级一致的应答，版本戳不重复推进

#### Scenario: 配置漂移不分歧

- **WHEN** 集群中一节点 `maxValueBytes` 配置低于产生载荷写入时的值，该节点回放含载荷写入的日志
- **THEN** 回放照常落值、与 Leader 终态逐字节一致（条目侧不复核），副本间无分歧

### Requirement: 队列操作复制边界与重放确定性

QUEUE 家族的队列操作 SHALL 以新条目类型 `RaftEntryType.QUEUE_OP_ENTRY = 13` 承载（`command_payload` 为 `QueueOpPayload{session_id, request_id, request}` 序列化，复用既有请求消息，MUST NOT 变更已发布编号）；复制边界：写（PUT/TAKE/DRAIN）与读（PEEK/SIZE）皆经多数派提交后应用（判例 v4 ATOMIC 读写全量经提交；读条目应用为零迁移），未达多数派 MUST NOT 产生元素入队/出队/去重槽变更。**挂起与唤醒不入日志**：阻塞式 PUT/TAKE 的等待登记（Leader 本地 `WaitQueue` 双轨）、`AWAIT_NOTIFY` 推送、已通知标记与超时清扫均为 Leader 本地态，MUST NOT 产生日志条目（判例 Latch/BARRIER"等待队列不入日志"）。应用点竞态回弹：预检放行提交后在应用点不可满足（元素被并发消费者摘走、容量被并发写入占满）的立即式与阻塞式命令 MUST 以 `ApplyStatus.DENIED` 回执、条目应用为**零迁移**（不改元素、不写去重槽），Leader 据该回执把阻塞式等待者**原位重新挂回**本地等待队列（位次不丢、不推错误给客户端）——回弹不引入副本分歧（DENIED 回执非复制状态）。到期时刻 MUST 于应用点以条目携带时刻折算（`expires_at_ms = wall_clock_ms + delay_ms`，加法溢出收纳至 `Long.MAX_VALUE`）——与租约 `expires_at_ms` 同构，任何副本任何物理时刻重放得出同一到期时刻；物理时钟与节点本地配置 MUST NOT 参与 apply 判定。`DRAIN` 的提取上限以**接入层钳定后的 N** 随条目进日志，应用点摘出 `min(N, 自队首起连续满足出队谓词段)`，全副本一致。每会话去重槽（`(session) → (op_seq, op, 回执)`）跨副本重放一致：同 `(session, op_seq)` 重放在任何副本得出**字节级相同**的回执（TAKE/DRAIN 重放交付同一份字节、不重复摘取），条目侧 MUST NOT 复核 `maxValueBytes`/`maxQueueCapacity`/`maxDrainBytes`（超限与钳制命令在接入层即拒、永不入日志，入日志者视为已钳制——判例 v6 配置漂移红线）。`ApplyResult` SHALL 以纯增量字段承载队列回执：`optional bytes queue_element_bytes = 20`、`repeated bytes queue_drained_bytes = 21`、`sint64 queue_size = 22`（仅队列条目择用，其余条目 MUST NOT 携带）。

#### Scenario: 并发队列写经复制全副本一致

- **WHEN** 多会话对同 key 并发提交 PUT/TAKE（含交错的元素字节），全部条目提交后在各副本比对队列状态
- **THEN** 各副本元素队列（载荷字节、到期时刻、顺序）、深度与每会话去重槽逐字段一致，全量摘要相等

#### Scenario: 延时条目跨副本同一到期时刻

- **WHEN** 携带 `delay_ms=3000` 的 PUT 在 Leader 应用后，落后副本于完全不同的物理时刻回放该条目
- **THEN** 回放副本得出的元素到期时刻与 Leader 逐毫秒一致（条目携带时刻折算，不读本地时钟）

#### Scenario: 读条目重放零迁移

- **WHEN** 含 PEEK/SIZE 条目的日志在新增副本上全量回放
- **THEN** 回放后该 key 元素与去重槽状态与无读条目时逐字节一致（摘要相等）

#### Scenario: 换主后重发经槽不双插不偷吃

- **WHEN** PUT 提交成功但应答丢失，Leader 切换后客户端向新 Leader 以同 `(session, op_seq)` 重发；另一场景为 TAKE 交付后应答丢失重发
- **THEN** PUT 重放命中回放所得槽、返回字节级一致回执且元素不双插；TAKE 重放重发**同一份已交付元素字节**、不重复摘取队首

#### Scenario: DENIED 回弹零迁移

- **WHEN** 两个阻塞 TAKE 的预检先后放行、仅一个元素在队，二者提交后一者应用点授予、一者应用点 `DENIED`
- **THEN** DENIED 条目不改任何复制状态（元素恰被摘取一次），被回弹的等待者仍在 Leader 本地队列原位等待下一次唤醒；各副本回放终态一致

#### Scenario: 配置漂移不分歧

- **WHEN** 一节点 `maxValueBytes`/`maxQueueCapacity` 配置低于写入时的值，该节点回放含大元素与大容量定型的日志
- **THEN** 回放照常落态、与 Leader 终态逐字节一致（条目侧不复核），副本间无分歧

### Requirement: topic 零复制日志边界

topic 为项目首个零复制日志原语：SUBSCRIBE/UNSUBSCRIBE/PUBLISH 与全部 fan-out 交付 MUST NOT 产生任何 Raft 日志条目，MUST NOT 触达状态机 apply 路径、`ApplyResult` 或 Leader 提交通道（判例对偶：队列"等待不入日志"，topic 更进一步——**连状态迁移都不入日志**，因 topic 无复制态）。由此推导的常驻守卫断言（防后续演进按"读写皆经提交"惯性误塞日志，改动本条款 MUST 先经 ROADMAP 决策记录登记）：

1. 纯 topic 流量（订阅、发布、退订、换主）下，各副本复制状态 digest MUST 逐字节一致且等于无 topic 流量时的基线；
2. 日志条目总数 MUST NOT 因 topic 流量增长（`SESSION_OPEN`/`SESSION_CLOSE`/NOOP 等既有系统条目不计入本断言增量）；
3. Follower 追赶、节点重启与快照安装 MUST NOT 导致任何陈旧 `TOPIC_MESSAGE` 被投递（重放无 topic 副作用可产生）；
4. `RaftEntryType` MUST NOT 为 topic 新增取值（现值表止于 13 即本条款的编号证据）。

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

条件原语采用半入日志边界：await 折叠形态（携带 `condition` 的 ACQUIRE）MUST 恰产生**一条既有 `LOCK_ACQUIRE_ENTRY` 类型**条目（`command_payload` 为请求消息序列化，新字段天然透传）——`RaftEntryType` MUST NOT 因本原语新增取值（现值表止于 13 即本条款的编号证据，`raft.proto` 字节零差异）；SIGNAL/SIGNAL_ALL/LEAVE MUST NOT 产生任何日志条目、MUST NOT 触达提交通道与状态机 apply 路径（纯 Leader 本地裁决——v8 topic 零日志豁免判例的类目化：等待集→等待队列的位次搬运与推送事件不改变任何复制态，"signal 是事件不是状态"）。

await 条目的应用语义 MUST 跨副本确定：释放半程（持有归属匹配则重入一步清零+清租约，不匹配则零操作）为复制态迁移，全部副本一致重放；登记半程（条件等待集写入）MUST 为 Leader-only 应用副作用，不进入任何副本的复制状态、不产生观察 digest 差异。由此推导的换主/重启语义契约：**等待是承诺**——await 条目重放使释放半程确定生效，等待集随 term 清零后由客户端重挂补登记（重挂非双登记、重挂会话非持有者属常态）；**signal 是事件**——搬运与通知不经日志、不重放、不补偿，换主窗内已发出未送达的唤醒不保证。守卫断言（常驻，防后续演进误塞日志或误持久化等待集，改动本条款 MUST 先经 ROADMAP 决策记录登记）：

1. 纯条件流量（折叠 await、SIGNAL/SIGNAL_ALL/LEAVE、换主重挂）下，各副本复制状态 digest MUST 逐字节一致且等于对照基线（无 signal 类状态可分岔）；
2. await 数与既有类型条目增量 MUST 恰为一对一，signal 家族条目贡献恒零；
3. Follower 追赶、节点重启与快照安装 MUST NOT 投递任何陈旧唤醒（重放无 Leader 副作用可产生、无集合可恢复）；
4. 应用点 MUST NOT 依赖墙钟外生输入（`registered_at_ms` 等观察值经条目时刻注入且永不参与判定）。

#### Scenario: await 单条目、signal 零条目

- **WHEN** 三节点集群稳定 Leader 下执行 N 次折叠 await 与 M 次 SIGNAL/SIGNAL_ALL/LEAVE，全程无其他业务
- **THEN** 日志新增条目数恰为 N 且全部为 `LOCK_ACQUIRE_ENTRY`；三副本 digest 逐字节相等；`RaftEntryType` 枚举值表与 `raft.proto` 相对 v8 零差异

#### Scenario: 重放幂等与换主清零重挂

- **WHEN** 承载条件流量的 Leader 被 kill，新 Leader 回放既有日志，旧等待项经客户端重挂重发折叠 ACQUIRE，其间一方对旧集合发过 SIGNAL
- **THEN** 新 Leader 上释放半程已生效（重放确定）、等待集仅含重挂登记（旧 term 搬运态与通知时序零残留）、换主窗内发出的 SIGNAL 不重放也不报错（空集无操作或搬运对象已换主消失——有界契约面）；后续 SIGNAL 对重挂登记照常搬运

#### Scenario: 重启与追赶无陈旧唤醒

- **WHEN** 节点重启（快照加载+日志回放）或新副本经快照安装追赶
- **THEN** 复制态逐项恢复（含 await 已生效的释放），任何客户端未收到历史 `AWAIT_NOTIFY` 条件唤醒；重启节点条件等待集为空、管理读数如实零

#### Scenario: signal 误入日志即构建红

- **WHEN** 未来变更在 signal 家族分发路径引入状态机提交调用
- **THEN** 本条款的"signal 家族条目贡献恒零"断言转红，变更被打回规格修订流程
