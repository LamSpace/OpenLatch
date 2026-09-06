## ADDED Requirements

### Requirement: 客户端可选监控指标

客户端 SHALL 支持可选的自身请求观测指标，经构建器注入外部度量注册表启用：`openlatch.client.requests.total{type,status}`（按请求消息类型与结果计数，结果区分成功应答、业务拒绝、超时、发送失败）、`openlatch.client.request.duration`（请求发出至响应完成/失败的耗时）、`openlatch.client.reconnect.total`（重连发起次数）、`openlatch.client.locks.lost.total`（锁丢失判定次数）。未注入注册表时 MUST 默认关闭：零计数、零额外分配路径，客户端行为与不引入该特性时完全一致。启用指标 MUST NOT 改变任何既有行为契约（超时值、重试与同 id 重发、看门狗节奏、失锁判定）。对宿主应用依赖的度量库 MUST NOT 成为客户端的强制传递依赖（宿主不用则不引入）。

#### Scenario: 默认关闭零开销

- **WHEN** 未注入注册表构建客户端并执行获取/释放
- **THEN** 无任何指标记录路径被触发，行为与现状一致

#### Scenario: 注入后计数准确

- **WHEN** 注入注册表后执行固定脚本（成功获取若干、业务拒绝若干、一次超时）
- **THEN** `requests.total` 各 `{type,status}` 序列计数与脚本逐项吻合，duration 样本数与请求总数一致

#### Scenario: 断线与失锁计数

- **WHEN** 持锁连接断开且租约在重连窗口内到期触发锁丢失
- **THEN** `reconnect.total` 随重连尝试递增，`locks.lost.total` 恰好 +1
