# replicated-state-machine delta — s3-leader-discovery-failover

## ADDED Requirements

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
