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
