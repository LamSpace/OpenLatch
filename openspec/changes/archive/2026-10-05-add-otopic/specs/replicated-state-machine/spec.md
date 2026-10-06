# Spec Delta

## ADDED Requirements

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
