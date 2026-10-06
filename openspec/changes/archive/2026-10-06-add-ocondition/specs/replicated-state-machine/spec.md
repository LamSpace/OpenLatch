# Spec Delta

## ADDED Requirements

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
