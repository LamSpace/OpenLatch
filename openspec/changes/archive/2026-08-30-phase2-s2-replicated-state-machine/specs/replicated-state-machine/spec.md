# replicated-state-machine (delta)

## Purpose

定义锁服务集群复制状态机的行为契约：哪些锁状态经日志复制到多数派、以何种条目格式复制、回放如何保持确定性（时间语义、幂等），以及 Leader 处理写请求、会话集群登记与租约到期驱动的可观察行为，使"已确认授予的锁不丢、任何时刻同 key 至多一个持有者"的保证可验证。

## ADDED Requirements

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
