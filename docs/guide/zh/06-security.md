# 06 · 安全

OpenLatch 的传输安全覆盖**客户端业务端口**（业务消息与 `ADMIN_*` 观察消息共用）的两层能力，
**均默认关闭**，关闭时行为与明文基线逐字节一致：

1. **TLS / mTLS**——传输加密与双向证书认证；
2. **业务令牌认证**——握手时校验 HELLO 携带的 `auth_token`。

## 1. 服务端 TLS

```properties
openlatch.server.tls.enabled=true
openlatch.server.tls.cert=/etc/openlatch/server-cert.pem
openlatch.server.tls.key=/etc/openlatch/server-key.pem
# 可选 mTLS：
openlatch.server.tls.require-client-cert=true
openlatch.server.tls.trust-store=/etc/openlatch/ca.pem
```

行为要点：

- 开启后**拒绝明文连接**：非 TLS 记录在协议层直接断开；握手超时 5s，挂起的半开连接不会占资源；
- 证书链校验失败 / mTLS 缺客户端证书 → 断开且**不产生任何会话副作用**（无会话号、无登记、无复制条目）；
- 证书 PEM 文件不可读、cert/key 不成对、`require-client-cert=true` 却缺 trust-store——启动即快速失败，不半启动；
- **证书更新需重启**（无热加载）：滚动窗口按 [05 计划内重启](05-cluster-deployment.md) 逐台走即可。

自签测试 CA / 服务端 / 客户端证书可用 openssl 直接生成（一机演示）：

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout ca-key.pem -out ca.pem -subj "/CN=OpenLatch Test CA"
openssl req -newkey rsa:2048 -nodes -keyout server-key.pem -out server.csr -subj "/CN=localhost"
openssl x509 -req -in server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
  -out server-cert.pem -days 365 -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1")
```

生产请换用受信 CA 与真实主机名/SAN。

## 2. 业务令牌认证

```properties
openlatch.server.auth.enabled=true
openlatch.server.auth.tokens=tok-A-2026,tok-B-rotate   # 逗号分隔，支持多令牌并存
```

语义（单机与集群同一门闩）：

| 开关状态 | HELLO `auth_token` 的表现 |
|---|---|
| `enabled=false`（默认） | 兼容守卫：**非空令牌即被拒**（旧行为，未启用认证的部署不接受令牌） |
| `enabled=true` | 缺失 / 空 / 不匹配 / 服务端未配置 → 统一 `INVALID_REQUEST` + 断连 |

- 判定发生在**会话分配与任何登记/复制之前**——被拒握手零状态副作用；
- 比较为**常量时间**，失败响应不区分原因（防令牌枚举探测）；
- 令牌一次校验、会话生命周期有效（逐请求不重验）；
- 与**管理令牌完全独立**：`openlatch.server.admin.token` 是 `ADMIN_*` 逐消息携带的只读观察
  凭据，两套令牌不可互相借用。

## 3. 令牌轮换流程（三步，全程不断服）

1. **服务端加新**：`auth.tokens` 追加新令牌（新旧双活）→ 滚动重启服务端；
2. **客户端切换**：各客户端/控制台改配 `auth-token` 为新令牌 → 滚动重启客户端；
3. **服务端摘旧**：`auth.tokens` 移除旧令牌 → 滚动重启服务端。

> 注意顺序不可颠倒：服务端先摘旧令牌会立刻切断尚未切换的客户端（含任何未携带有效令牌的
> 旧版客户端）。

## 4. 消费端配置

客户端 SDK、Spring Boot starter、管理控制台暴露同一组语义（各自页面有表）：
`tls-enabled`、`tls-trust-store`、`tls-client-cert` / `tls-client-key`（mTLS 成对）、
`auth-token`。配置后 TLS 与令牌沿用于首连、重连与种子发现探针的**全部**连接构造点。

- SDK：[03 §安全与令牌](03-client-sdk.md)
- starter：[04 §配置键](04-spring-boot-starter.md)
- 控制台：[07 §安全项](07-admin-console.md)

## 5. 边界（诚实声明）

| 通道 | 是否覆盖 |
|---|---|
| 客户端业务端口（含 ADMIN 观察流量） | ✅ TLS / mTLS / 令牌 |
| Raft 节点间复制通道 | ❌ 明文——置于内网/防火墙/服务网格之后 |
| 指标端点 9412、控制台 Web 9413 | ❌ 明文——置于内网/反向代理之后 |

即：**不宣称全链路端到端加密**。控制台的鉴权依赖管理令牌 + 网络隔离；如需对外暴露控制台，
请以反向代理（TLS 终结）方式部署。指标端口无鉴权，抓取面 = 运行状态面，网络策略上请区别对待。
