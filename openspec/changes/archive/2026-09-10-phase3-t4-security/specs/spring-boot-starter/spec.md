# spring-boot-starter Delta Specification

## MODIFIED Requirements

### Requirement: 配置属性绑定与默认值

starter MUST 绑定 `openlatch.*` 前缀的配置属性：`enabled`（默认 `true`）、`server-host`（默认 `127.0.0.1`）、`server-port`（默认 `9410`）、`request-timeout`（默认 `5s`）、`default-wait-timeout`（默认 `30s`）、`reconnect-initial-backoff`（默认 `200ms`）、`reconnect-max-backoff`（默认 `10s`），以及可选的安全属性——`tls-enabled`（默认 `false`）、`tls-trust-store`（可选，PEM 路径）、`tls-client-cert`/`tls-client-key`（可选，mTLS）、`auth-token`（可选，业务令牌）。时长类属性 MUST 支持标准 Duration 写法（如 `5s`、`100ms`）。未提供任何属性时 MUST 全部按默认值装配（安全属性未配置即 TLS 关闭、无令牌，客户端按明文无令牌形态连接）。安全属性 MUST 转发到所建客户端（与 openlatch-client 的 TLS/认证配置语义一致，见 transport-security"客户端 TLS 与认证消费"）；未启用 TLS 或未提供 `auth-token` 时 MUST NOT 改变既有装配行为与锁语义。

#### Scenario: 零配置默认值生效

- **WHEN** 应用仅引入 starter 依赖、不写任何 `openlatch.*` 属性并启动上下文
- **THEN** 客户端以 `127.0.0.1:9410`、请求超时 5s、等待兜底 30s、退避 200ms/10s 装配成功，TLS 关闭、无业务令牌，行为与现状一致

#### Scenario: 属性覆盖生效

- **WHEN** 应用配置 `openlatch.server-port` 与 `openlatch.request-timeout=2s` 后启动上下文
- **THEN** 客户端 Bean 按覆盖值连接目标端口并以 2s 请求超时工作

#### Scenario: TLS/认证属性生效

- **WHEN** 应用配置 `openlatch.tls-enabled=true`、`openlatch.tls-trust-store=<PEM>` 与 `openlatch.auth-token=<token>` 后启动上下文
- **THEN** 客户端 Bean 以 TLS + 业务令牌装配，连接开启 TLS/认证的服务端握手成功；未配置安全属性时上述路径不触发
