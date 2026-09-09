# Design: phase3-t4-security

## Context

详设 §5 的 T4（TLS + Token 认证）落到现行代码的事实：① 两端（`openlatch-server`/`openlatch-client`）已依赖 `netty-handler`，`SslContext`/`SslHandler`/`SslContextBuilder` 无需新依赖；② `HelloRequest.auth_token`（field 3）Phase 1 预留后由 `ServerSessionHandler.handleHandshake:322-329` 硬编码"非空即断连"，cluster 分支（`SessionCoordinator.handleHello` → `SESSION_OPEN` 复制）在 `:330` 之后——认证门闩必须放在**这个分叉之前**才是单机/集群统一入口（吸取 T3 D3"双形态从第一天做成单入口"的教训）；③ 明文拒绝的断连路径现成——`EnvelopeCodecHandler.exceptionCaught:52` 已"解码异常 → close"，TLS 解码/握手失败只要异常沿入站方向后传即落到它；④ 配置形态先例齐备（`ClusterConfig`/`MetricsConfig`/`AdminConfig`：`KEY_PREFIX` + `load(path)` + `disabled()/unconfigured()`，同文件独立加载，`OpenLatchServer` 构造链逐层追加）；⑤ 常量时间令牌校验先例 `AdminRequestHandler.tokenOk:202`（`MessageDigest.isEqual`）——但**长度不等会提前返回**（令牌长度侧信道）；⑥ T3 design Non-Goal 明示"控制台与节点的 TLS 属后续"，且 console 的 HELLO 需过同一认证门闩——服务端开业务认证后 console 若无业务令牌将连 HELLO 都过不去（逐消息 admin-token 不构成 HELLO 放行依据），故 console 需同时获得 TLS + 业务令牌透传。动机与清单见 proposal.md 与 specs。

## Goals / Non-Goals

**Goals:**

- 服务端锁端口可配 TLS/mTLS，明文连接与未完成握手的连接被拒（§5.1）；
- HELLO 业务令牌认证：默认关 = 兼容守卫（既有行为逐字节不变），开启 = 多令牌/常量时间/失败不泄露/单机与集群统一门闩（§5.2/§5.3）；
- 客户端/控制台按配置消费 TLS 与认证，错误 trust-store 或令牌被拒不发明文降级路径、不忙转（spec"客户端 TLS 与认证消费"）；
- 安全拒绝用例三层聚合全绿（§7 T4/§8-5），默认关闭档既有全量 suite 与 `wire-protocol` 冻结测试全绿（§8-6）；
- 全部新 Java 源文件按 CLAUDE.md §5 配 Javadoc/License；README 双语知会加密范围边界与轮换步骤。

**Non-Goals:**

- 不做 Raft 节点间通道（Ratis）TLS、不做 9412 指标 HTTP 与 9413 控制台 Web 的 TLS（三者部署面隔离责任，文档显式声明）；
- 不做证书热加载（改动配置后滚动重启生效，§5.1 明示）；
- 不做逐请求鉴权/会话内角色（连接即身份，§5.3）；
- 不改 `wire-protocol`：无协议版本号变更、无消息字段变更，复用 `HelloRequest.auth_token`，冻结测试零触碰；
- 不做客户端令牌的运行时重载（轮换以进程/应用重启切换令牌完成）；
- 不把认证被拒做成无限重连的终结态机——沿用既有指数退避（有界），仅增强可区分诊断。

## Decisions

### D1 服务端 TLS：`SslHandler` 钉在 pipeline 首位，PEM 直供

`ServerChannelInitializer.initChannel` 在既有 `addLast` 链之前先 `addLast("ssl", sslHandler)`（首位即生效），出站方向 TLS 自然包裹全部外发字节。`SslContext` 由装配层构造、initializer 只接已建好的 handler，不感知证书文件逻辑：

- `SslContextBuilder.forServer(certPemFile, keyPemFile)`——PEM 直供，**不做 PKCS12/keystore 转换**（生产同路径 = 测试同路径）；
- mTLS：`trustManager(trustStorePemFile)` + `clientAuth(ClientAuth.REQUIRE)`（`require-client-cert=true` 时）；服务端 trust-store 为可信任 CA 的 PEM 证书集；
- `sslHandler.setHandshakeTimeoutMillis(5000)`（默认 5s，§5.1）；
- **明文拒绝的依赖面显式化**：SslHandler 收到明文协议字节抛 `NotSslRecordException`、握手超时抛 `SslHandshakeTimeoutException`，异常沿入站后传至 `EnvelopeCodecHandler.exceptionCaught` → close。该路径以测试断言（明文客户端被断开、无会话）锁定，不隐式依赖邻居 handler 顺序。

装配/启动快速失败对齐"端口占用启动失败"语义：`TlsConfig` 非法（文件不可读/解析失败/`require-client-cert` 未配 trust-store）在 `start()` 抛 `IllegalStateException`，`stop()` 回收已绑定端口与线程组，不进入半启动。

### D2 配置形态：`TlsConfig`/`AuthConfig` 独立类 + 构造链追加

按 `AdminConfig` 范式新增两个配置类，不动 `ServerConfig` 构造面：

| 类 | 键 | 语义 |
|---|---|---|
| `TlsConfig` | `openlatch.server.tls.enabled`（默认 false） | TLS 开关 |
| | `openlatch.server.tls.cert` / `tls.key` | 服务端 PEM cert/key 路径 |
| | `openlatch.server.tls.trust-store` | mTLS 可信任 CA PEM（可选） |
| | `openlatch.server.tls.require-client-cert`（默认 false） | mTLS 开关 |
| `AuthConfig` | `openlatch.server.auth.enabled`（默认 false） | 业务认证开关 |
| | `openlatch.server.auth.tokens` | 逗号分隔业务令牌列表（开启时 ≥1，令牌不得含逗号，文档注明） |

- `load(path)` 同文件 Properties 加载（与 Cluster/Metrics/Admin 一致）；`disabled()`/`unconfigured()` 回落默认。
- `validate()`：`tls.enabled && require-client-cert && trust-store 缺失` → 快速失败；`auth.enabled && tokens 为空` → 快速失败（对齐 `ClusterConfig`"启用时必填项缺失启动失败"）。运行时"开启而无令牌"的拒绝分支保留为纵深防御（正常不可达）。
- `OpenLatchServer` 构造链 +2：新增 6 参终构造 `(config, cluster, metrics, admin, auth, tls)`，既有 4 参构造委托之并填 `unconfigured()/disabled()`；全部调用点（main/launcher、`TestServers`）增量最小。
- 证书更新 = 重启生效（不做热加载）。

### D3 握手门闩分叉：认证判定在 cluster 分叉之前

`ServerSessionHandler.handleHandshake` 现 `:324` 的"版本越界 或 auth_token 非空 → 拒+断"改为分叉：

```
HELLO（合法 payload）
 ├─ 协议版本 ∉ [1,3] ──────────────────────────▶ INVALID_REQUEST + close
 ├─ auth.enabled == false（默认）：
 │    └─ auth_token 非空 ──────────────────────▶ INVALID_REQUEST + close（Phase 1 兼容守卫）
 │    └─ auth_token 空 ──────▶ 继续（cluster/sessionOpened 分叉原样）
 └─ auth.enabled == true：
      ├─ ConstantTime.matches(auth_token, 任一配置令牌) 命中 ─▶ 继续
      └─ 否则（缺失/为空/不符）─────────────────▶ INVALID_REQUEST + close（不泄露原因）
```

- 判定全部发生在 `:330 cluster != null` 分叉**之前**——单机与集群天然共用同一门闩，未认证 HELLO 不产生 `SESSION_OPEN` 复制条目/注册表登记（spec 场景"未认证 HELLO 零状态副作用"）。
- 应答复用既有 `helloResponse(msg, INVALID_REQUEST, 0)` + `ctx.close()` 形态（sessionId=0，与版本越界路径同形，不新增应答面）。
- 会话分配/登记仍在原处（`core.sessionOpened()` / `SessionCoordinator`），认证路径零状态副作用由"先判定后分配"的结构保证。

### D4 `ConstantTime.matches` 常量时间原语（admin 同步迁移）

新增 `io.github.lamspace.openlatch.server.security.ConstantTime.matches(String presented, String configured)`：null/长度处理先行、**补齐至二者最大长度**后在定长字节数组上做 `MessageDigest.isEqual`（长度差异不早退，防令牌长度侧信道）。`AdminRequestHandler.tokenOk` 换用该原语（行为不变：未配置即拒、presented null 即拒、命中返回 true），使业务令牌与 admin 令牌共用同一比较实现——**安全原语属于"该统一而不该复制"**：两处语义必须永久一致，且 §7"常量时间比较代码评审"收敛为单一评审面。迁移由既有 `AdminProtocolTest` 认证矩阵负路径直接背书。

### D5 客户端安全装配：builder/config + pipeline 首位 SslHandler + 双 HELLO 附令牌

- `ClientConfig`（record）追加可选字段：`tlsEnabled`、`tlsTrustStore`、`tlsClientCert`/`tlsClientKey`（mTLS）、`authToken`；既有 8 参兼容构造委托 11 参并填默认（全关）。`OpenLatchClient.Builder` 暴露 `tls(...)`/`authToken(...)`。
- `ConnectionManager.doConnect` 的 `initChannel` 在既有链前 `addLast("ssl", sslHandler)`——与 D1 同构，PEM trust-store/client cert 经 `SslContextBuilder.forClient().trustManager(file)`（mTLS 时 `.keyManager(clientCert, clientKey)`）构造；TLS 握手自动始于 channelActive。
- 认证：`ConnectionManager.sendHello`（主连接）与 `SeedDiscovery` 探针 HELLO **两个构造点**统一经同一 helper 附 `authToken`——探针漏配会被认证开启的服务端当成未认证连接断掉（spec 场景"种子发现探针附令牌"）。
- 失败语义：TLS 握手失败/HELLO 被 `INVALID_REQUEST` 拒 → 现有 `exceptionCaught → close → handleChannelInactive` 已把挂起操作以"服务不可达"快速失败；重连沿用既有指数退避（200ms→10s ±20%，有界，非忙转）。不新增终结态机（Non-Goal），但 `OpenLatchClient` 连接失败原因链携带"握手被拒/证书不可信"供上层诊断。
- token 明文不落日志（spec"认证被拒不当作已建会话"）。

### D6 令牌轮换流程（文档化）

服务端先加新令牌（`tokens` 列表双活）→ 重启服务端 → 客户端/控制台切换为新令牌（应用重启）→ 服务端摘除旧令牌 → 重启。双活窗口内旧/新令牌连接均放行（spec 场景"轮换期多令牌双活"）。步骤写入 README 双语安全章节。

### D7 console 安全透传（T3 Non-Goal 到期的"后续"）

console 的 `AdminClient`（自有 Netty 连接，不经 openlatch-client）与目标节点直连锁端口，须过同一 TLS/认证门闩：

- `ConsoleConfig` 追加可选：`openlatch.console.auth-token`（业务令牌，HELLO 携带——服务端开业务认证后 console 缺它连 HELLO 都过不去，见 Context ⑥）、`openlatch.console.tls-enabled`/`tls-trust-store`（mTLS 时 `tls-client-cert`/`tls-client-key`）。`admin-token` 仍逐 `ADMIN_*` 消息携带，两令牌互不借道（spec"认证开启下管理令牌不可借道"）。
- `AdminClient` 连接时按配置加 SslHandler + HELLO 附业务令牌；节点要求 TLS/认证而 console 未配 → 该节点按既有"认证失败/节点不可达"降级呈现，**不以明文/无令牌请求探测 TLS 化的节点**（spec"服务端开启认证而控制台缺令牌 → 节点降级"）。

### D8 证书夹具与测试三层

**证书夹具先定**：JDK 25 强封装下 Netty `SelfSignedCertificate` 经反射触 `sun.security.x509`（需 `--add-opens`），沙箱/CI 不可靠（前科）。改用**openssl 一次性预生成、提交 test resources**，测试只做文件加载，喂 `SslContextBuilder` 的**生产同一条 PEM 路径**：

```
src/test/resources/tls/
  ca.pem                    可信任 CA（签 server 与 client 叶证书）
  server-key.pem / server-cert.pem     CA 签的服务端叶证书
  client-key.pem / client-cert.pem     CA 签的客户端叶证书
  other-ca.pem / rogue-server-*.pem    异 CA 签的"坏"证书（双向不信任用例）
```

不引 BouncyCastle、不做运行时证书生成。

**测试三层**：

- **L1（openlatch-server 单元/协议层）**：`TlsConfig`/`AuthConfig` 解析与校验（仿 `ClusterConfigTest`）；`ConstantTime` 长度不等不早退结构性断言 + 语义等值；握手认证门闩矩阵（默认关兼容守卫/开+对/空/错/双令牌/摘令牌/版本越界，沿用 `HandshakeTest` 真 socket 形态）；ADMIN 认证矩阵回归（`AdminProtocolTest` 换用新原语后全绿）。
- **L2（同 JVM 真 server + 真 client / 真 console）**：服务端 TLS（明文被拒/正确证书/mTLS 无证书被拒/握手超时 5s 断开/坏配置启动失败）；客户端 TLS+认证（正确 trust-store+token 通过/错 trust-store 快速失败/探针附令牌/认证被拒不当作已建会话）；集群档 auth on 未认证 HELLO 不进 `SESSION_OPEN`（复用 `ClusterHarness`）；console→TLS/认证节点冒烟 + 缺令牌节点降级。
- **L3 部署证据**：`t4-acceptance-evidence.md`（变更目录）——真握手 + `openssl s_client`/抓取级记录 + §7 T4 ↔ §8-5 判据对照表，沿 T2/T3 证据文体。

**回归底线**：TLS/auth 关闭档（默认）既有全量 suite 逐字节不变——`HandshakeTest`/`MessageLegalityTest`/`InflightOverloadTest`/`AdminProtocolTest`/console 冒烟/`wire-protocol` 冻结测试（v1 基线零变更）全绿；收口全量 `clean verify` 与 `-Pdrill`（进程级演练环境不可靠的 A/B 判定沿用既有收尾纪律）。

### D9 范围边界与文档

README 双语安全章节显式声明：**本期加密范围 = 客户端接入端口**（业务 + `ADMIN_*` 消息共用该端口，一并受 TLS/认证保护）；Raft 节点间通道（Ratis）、9412 指标 HTTP、9413 控制台 Web 保持明文，属部署面网络隔离责任（非 TLS 范围）。启用 TLS + 认证 = 配置变更 + 滚动重启，无数据迁移；认证开启后旧的无令牌客户端（v1/v2/v3 或未配令牌的新客户端）会被拒——升级路径知会"客户端需与业务令牌同批滚动"。详设 §5.1/§5.2/§5.3 与实现一致，无行为勘误；§10.4 子任务口径微调（console 安全透传归 P3-15、admin 令牌分离为 P3-16 内校验项）回写详设勘误段。

## Risks / Trade-offs

- [明文拒绝依赖 `EnvelopeCodecHandler.exceptionCaught` 的断连] → 该路径以 L2 测试断言锁定（明文客户端被断开、零会话），不依赖隐式顺序；若未来挪 handler，测试即报警。
- [构造链 +2 至 6 层，调用面变宽] → 遵循既有逐层追加惯例（metrics→admin），6 参终构造为唯一装配点，旧构造委托回落默认；收敛为装配对象会重写已发布构造器 + 全量测试调用点，回归面不成比例。
- [客户端对"认证被拒"与普通断线都会退避重连（有界）] → 不做终结态机（Non-Goal）；增可区分诊断避免运维误判；误配令牌的最坏后果是有界退避 + 清晰错误，不会把被拒会话当已建立。
- [JDK 25 证书运行时生成不可靠] → 提交 PEM 夹具（D8），测试与生产同路径；不引 BC。
- [`MessageDigest.isEqual` 长度不等早退的历史陷阱] → `ConstantTime` 补齐定长比较 + admin 迁移收敛单面（D4），评审面唯一。
- [认证开启后旧无令牌客户端被拒的兼容性回归面] → 默认关闭分支保留 Phase 1 守卫（spec 兼容场景），开启是显式运维选择并文档化滚动顺序（D9）。
- [console 需同时携带业务令牌 + 管理令牌两套] → 职责清晰（HELLO 用业务、ADMIN 逐消息用管理），Config 分键，spec"不可借道"场景锁定。

## Migration Plan

- **默认（drop-in）**：全部安全能力关闭，行为与现状逐字节一致；服务端/客户端/控制台可各自独立升级，无协议/数据迁移。
- **启用 TLS**：服务端配置 + 重启；客户端/控制台配置 trust-store + 重启；证书更新 = 再次重启。管理指标 HTTP（9412）与控制台 Web（9413）不受影响（明文，部署面隔离）。
- **启用认证**：服务端配 `tokens` + 重启 → 客户端/控制台配 `auth-token` + 重启（轮换期服务端列表双活可先行）；旧无令牌客户端在切换窗口内被拒，按 D9 顺序滚动。
- **回滚**：关闭对应配置项重启即回明文/无认证，无数据迁移；协议无变更，无回滚安全面。
- 归档同步主规格：`transport-security` 新建 + `lock-server`/`spring-boot-starter`/`admin-console` 增量。

## Open Questions

- 9412 指标与 9413 控制台 Web 是否在后续进入 TLS：本期明示范围外（D9），若运维纳入公网暴露再另立阶段（或反代终结 TLS）。
- 客户端是否需要在握手被拒时提供"终结重连"的可选开关（如 `stopOnAuthReject=true`）：本期沿用有界退避 + 诊断（Non-Goal），确有运维诉求再作为 builder 小增量评估。
