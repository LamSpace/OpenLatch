## ADDED Requirements

### Requirement: 度量注册表自动注入

当应用上下文中存在度量注册表 bean 时，starter MUST 将其注入所创建的客户端（启用客户端可选指标）；不存在时 MUST 静默跳过，客户端保持默认关闭，上下文启动不受影响。该行为 MUST NOT 要求用户新增显式配置，且 MUST NOT 改变 starter 既有 bean 装配顺序与锁语义。

#### Scenario: 存在注册表即注入

- **WHEN** 应用上下文含注册表 bean 并通过 starter 使用锁客户端
- **THEN** 客户端指标注册进该注册表，抓取可见 `openlatch_client_requests_total` 等序列

#### Scenario: 无注册表不受扰

- **WHEN** 应用上下文不含任何度量注册表
- **THEN** starter 正常装配客户端，无报错、无指标
