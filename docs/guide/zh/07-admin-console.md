# 07 · 管理控制台

`openlatch-console` 是**独立部署**的只读 Web 观察面（默认 HTTP `9413`）：总览页展示各节点
角色/会话/锁量与 /metrics 迷你图，明细页展示锁表条目、等待队列、会话触及面。

**它不提供任何写操作**——没有强制解锁、没有踢会话。它是观察窗，不是遥控器。

## 部署

```bash
java -Dopenlatch.console.config=/path/to/console.properties \
     -jar openlatch-console/target/openlatch-console-1.0-SNAPSHOT-executable.jar
```

与控制端同机部署的被观察节点也完全兼容（不同进程即可）。配置模板见仓库
`openlatch-console/console.properties.example`。

## 配置键

| 键 | 默认 | 说明 |
|---|---|---|
| `openlatch.console.server-addresses` | **必填** | 逗号分隔 `host:port`，指向各节点**业务端口** |
| `openlatch.console.admin-token` | **必填** | 须等于各服务端 `openlatch.server.admin.token`；不符时页面降级为鉴权提示而非报错 |
| `openlatch.console.auth-token` | — | 业务令牌（目标节点开启业务认证时必填） |
| `openlatch.console.tls-enabled` | `false` | 对节点启用 TLS |
| `openlatch.console.tls-trust-store` | — | 节点证书信任锚（PEM CA） |
| `openlatch.console.tls-client-cert` / `tls-client-key` | — | mTLS 客户端证书/私钥（成对） |
| `openlatch.console.port` | `9413` | 控制台自身 HTTP 端口 |
| `openlatch.console.refresh-interval-seconds` | `5` | 页面轮询间隔 |
| `openlatch.console.metrics-port` | `9412` | 总览迷你图所用的节点指标端口 |
| `openlatch.console.request-timeout-ms` | `5000` | 单次管理请求超时 |

非法配置（地址列表为空、端口冲突、管理令牌空白）启动即快速失败。

## 与安全面的关系

- 服务端**未设** `openlatch.server.admin.token` 时，一切 `ADMIN_*` 请求被拒——控制台将
  呈现"所有节点未授权"，这是默认姿态（观察面默认关闭）；
- 节点开启 TLS/业务认证时，控制台必须配置对应安全项；未配置的节点被标记为
  认证失败/不可达降级，**控制台绝不会用明文/无令牌请求去探测 TLS 化节点**；
- 管理令牌 ≠ 业务令牌：前者逐消息携带、只读；后者握手承载、驱动锁操作。二者互不通用。

## 数据口径

观察面为**弱一致快照**：明细是请求时刻的尽力而为读数，多节点数据取自各自视图，
Leader 切换窗口内可能出现短暂缺位/降级标记——属预期，不构成告警条件。
指标含义见 [08](08-observability.md)。
