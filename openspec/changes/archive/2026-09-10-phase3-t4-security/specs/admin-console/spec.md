# admin-console Delta Specification

## MODIFIED Requirements

### Requirement: 部署形态与配置

控制台 SHALL 以独立可执行 jar 部署（`openlatch-console-<ver>.jar`），与任何被观察节点不同进程亦可同机；HTTP 监听端口默认 **9413**（可配置）。配置项：目标节点地址列表（`host:port`，≥1 项，指向服务端**业务端口**）、管理令牌（与服务端 `admin-token` 一致，逐 `ADMIN_*` 消息携带）、监听端口，以及可选的安全项——业务令牌（`auth-token`，节点 HELLO 携带，服务端开启业务认证时必需）、TLS（`tls-enabled`/`tls-trust-store`，mTLS 时 `tls-client-cert`/`tls-client-key`）。非法配置（地址列表为空、端口冲突、管理令牌为空白）MUST 启动时快速失败并给出明确错误信息。控制台对节点的连接 MUST 按安全配置执行：配置了业务令牌/ TLS 时，AdminClient 的 HELLO 携带该令牌并执行 TLS 握手（与服务端 `transport-security` 语义一致）；未配置安全项时按明文无令牌形态连接。对要求 TLS/认证而控制台未作相应配置的节点，控制台 MUST 按该节点"认证失败/不可达"降级呈现，MUST NOT 以明文或无令牌请求去探测 TLS 化的节点（如实标注而非静默制造错误请求）。控制台 MUST NOT 依赖 Spring 宿主应用、MUST NOT 持有任何锁语义（不引业务 client SDK），关闭时优雅释放全部对节点的连接。

#### Scenario: 默认端口与配置覆盖

- **WHEN** 以最小配置（仅地址列表与管理令牌）启动控制台
- **THEN** 在 9413 提供服务；显式配置端口后在配置端口服务；未配置安全项时按明文无令牌连接节点

#### Scenario: 坏配置快速失败

- **WHEN** 地址列表为空或端口被占用时启动
- **THEN** 进程启动失败退出并输出可定位的错误信息，不进入半启动

#### Scenario: TLS/认证节点可观察

- **WHEN** 控制台配置业务令牌与 TLS trust-store，目标节点开启 TLS 与业务认证
- **THEN** AdminClient 以 TLS 握手并在 HELLO 携带业务令牌，随后逐消息携带管理令牌完成只读查询，页面照常呈现

#### Scenario: 服务端开启认证而控制台缺令牌 → 节点降级

- **WHEN** 目标节点开启业务认证，控制台未配置 `auth-token`（HELLO 被节点以 `INVALID_REQUEST` 拒绝并断连）
- **THEN** 控制台将该节点呈现为认证失败/不可达降级状态，不反复以无令牌请求重试冲击，其余节点不受影响
