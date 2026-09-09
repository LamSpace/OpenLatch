# Tasks: phase3-t4-security

判定基线：验证命令一律 `mvn -s /home/lam/repo/settings.xml`（模块级 `-pl ... -am test`，收口全量 `clean verify` 与 `-Pdrill`）；新增 Java 源文件按 CLAUDE.md §5 配齐 Javadoc 与 License 头。spec 增量在 `specs/transport-security`（新建）与 `specs/lock-server`/`specs/spring-boot-starter`/`specs/admin-console`（增量）；实现口径按 `design.md` D1–D9。

## 1. P3-14 服务端 TLS

- [x] 1.1 `TlsConfig`（`openlatch.server.tls.*` 前缀，仿 `AdminConfig` 同文件独立加载）：`enabled`（默认 false）/`cert`/`key`/`trust-store`/`require-client-cert`；`disabled()`/`load(path)`；`validate()` 快速失败（`require-client-cert=true` 缺 trust-store 等）；解析与校验矩阵单元测试（仿 `ClusterConfigTest`）→ `TlsConfig` + `TlsConfigTest`（8 例全绿）
- [x] 1.2 测试证书夹具：openssl 一次性预生成 PEM 三件套提交 `src/test/resources/tls/`（`ca.pem`、`server-cert/key.pem`、`client-cert/key.pem`、`other-ca.pem` + 异 CA 签 rogue 证书）；夹具加载冒烟测试（`SslContextBuilder` 对夹具可构造）+ 夹具生成命令记入 `tls/README.md`（不引 BouncyCastle、不做运行时证书生成）
- [x] 1.3 `ServerChannelInitializer`：注入可选 `SslContext`（新增构造，旧构造委托回落明文）；`initChannel` 在既有链前 `addLast("ssl", ...)` 首位（出站 TLS 包裹全部外发字节）；`SslHandler.setHandshakeTimeoutMillis(5000)`
- [x] 1.4 `OpenLatchServer`：构造链追加 5 参终构造 `(config, cluster, metrics, admin, tls)`（既有构造委托并填 `TlsConfig.disabled()`）；装配层经 `serverSslContext()` 构造 `SslContext`（`forServer` PEM cert/key，mTLS `trustManager` + `ClientAuth.REQUIRE`）；`start()` 坏配置快速失败（`IOException|RuntimeException` 统一包装 `IllegalStateException` + `stop()` 回收）；启动日志含 `tlsEnabled`/`mTls`；`main()` 加载 `TlsConfig`
- [x] 1.5 服务端 TLS 用例（`TlsGatingTest`，真 server 端口 0）：明文协议客户端被断开且无会话（锁定 `exceptionCaught→close` 路径）；正确证书握手并建会话；mTLS 缺客户端证书被拒、携证书通过；握手超时 5s 断开（静默连接 <8s 断，非 60s 读空闲）；坏证书配置启动快速失败
- [x] 1.6 验证：`-pl openlatch-server -am test` 全绿（40 suites 0 失败，含既有 `HandshakeTest`/`MessageLegalityTest`/`AdminProtocolTest` 默认关零扰动 + 集群档）；**P3-14 退出判据达成：服务端侧 TLS 用例全绿**

## 2. P3-15 客户端与消费方 TLS

- [x] 2.1 `ClientConfig`（record）追加可选 TLS 字段 + `authToken`（`tlsEnabled`/`tlsTrustStore`/`tlsClientCert`/`tlsClientKey`），8/9 参兼容构造委托默认关；`OpenLatchClient.Builder` 暴露 `tlsEnabled(...)`/`tlsTrustStore(...)`/`tlsClientCert/Key(...)`/`authToken(...)`（mTLS cert/key 成对校验）
- [x] 2.2 `ConnectionManager`：`doConnect` 惰性构造并缓存 SslContext（`ClientSecurity.clientSslContext`，PEM 同路径；不可用按连接失败处理 + WARN 不崩溃 EventLoop），`initChannel` 首位 `addLast("ssl",...)`；`sendHello` 配置了业务令牌即携带（令牌不落日志）；`SeedDiscovery` 探针同 TLS 装配 + 探针 HELLO 附令牌（两连接构造点不遗漏）
- [x] 2.3 starter：`OpenLatchProperties` 新增 `openlatch.tls-enabled`/`tls-trust-store`/`tls-client-cert`/`tls-client-key`/`auth-token` 并透传至所建客户端；用例（`OpenLatchAutoConfigurationTest` 增 2 例：安全属性透传 / 零配置明文缺省）——starter `OpenLatchAutoConfigurationTest` 10 例全绿
- [x] 2.4 console（D7）：`ConsoleConfig` 增嵌套 `Security`（`tls-enabled`/`tls-trust-store`/`tls-client-cert`/`tls-client-key`/`auth-token`，`Security.NONE` 缺省）；`AdminClientPool` 构造一次共享 SslContext（PEM 坏配置启动快速失败）、`AdminClient` 连接按配置加 SslHandler + HELLO 附业务令牌（与逐消息 admin-token 分离）
- [x] 2.5 客户端/消费方 TLS 端到端：client 段 `ClientTlsAuthIT` 6 例全绿（正确 trust-store 走通业务 / 错误 trust-store 不建会话 / mTLS 缺证书被拒、携证书走通 / 认证对错令牌）；console 段 `ConsoleTlsAuthTest` 2 例全绿（TLS+认证节点可只读观察 / 缺业务令牌节点降级）——夹具入 client 与 console `src/test/resources/tls/`
- [x] 2.6 验证：`-pl openlatch-client,openlatch-spring-boot-starter,openlatch-console -am test` 全绿（一次 reactor run 复跑 server+client+console `test` 阶段全 SUCCESS：server 04:09 / client 35.6s / console 14.7s；starter 单模块全绿）；**P3-15 退出判据达成：客户端侧用例全绿（含 mTLS 无证书被拒）**

## 3. P3-16 Token 认证（服务端门闩 + 消费方令牌 + 轮换）

- [x] 3.1 `AuthConfig`（`openlatch.server.auth.*`）：`enabled`（默认 false）/`tokens`（逗号分隔，开启时 ≥1、令牌不得含逗号）；`unconfigured()`/`load(path)`/紧凑构造校验（`enabled=true` 无令牌快速失败）+ `accepts()`（多令牌全遍历常量比较，无位置侧信道）→ `AuthConfigTest` 6 例绿
- [x] 3.2 `ConstantTime.matches(presented, configured)`（`server.security.ConstantTime`）：null 先行、补齐至最大长度后 `MessageDigest.isEqual`（长度不等不早退）；`AdminRequestHandler.tokenOk` 迁移共用 → `ConstantTimeTest` 4 例绿 + `AdminProtocolTest` 15 例回归背书
- [x] 3.3 `ServerSessionHandler.handleHandshake` 认证门闩分叉：默认关保留 Phase 1 守卫 / 开启 = `authConfig.accepts` 命中放行、失败统一 `INVALID_REQUEST` + 断连不泄露原因；判定在 `cluster` 分叉前（单机/集群统一门闩）→ `AuthHandshakeTest` 4 例绿（含空/错同形、双令牌双活、摘令牌被拒、默认关兼容守卫、被拒零会话副作用）
- [x] 3.4 客户端令牌消费：`ClientConfig`/builder 增 `authToken`；`ConnectionManager.sendHello` 与 `SeedDiscovery` 探针 HELLO 附令牌（两构造点不遗漏）；令牌不落日志 → `ClientTlsAuthIT` 的 auth 正确/错误两例绿
- [x] 3.5 console 业务令牌透传（D7/Context ⑥）：`ConsoleConfig.Security.authToken` + `AdminClient` HELLO 携带；"服务端开认证而 console 缺令牌 → 节点降级" 由 `ConsoleTlsAuthTest.consoleMissingBusinessTokenDegradesNode` 锁定（2/2 绿，含 TLS+认证正向）
- [ ] 3.6 集群档用例：auth on 未认证 HELLO 零状态副作用（无 `SESSION_OPEN` 复制条目）——**判定为结构保证 + 单机侧覆盖，不开重集群夹具**：门闩分叉置于 `cluster.sessionCoordinator().handleHello` 之前（代码路径保证未认证 HELLO 永不进 SESSION_OPEN），单机档零会话副作用已由 AuthHandshakeTest 锁定；`ClusterHarness` 直驱 `ClusterRuntime`、不经 `ServerSessionHandler`，要证集群档需新建三节点 OpenLatchServer 夹具（另立小变更评估）
- [x] 3.7 轮换流程文档化（D6）：README 双语 Security 节（服务端加新令牌双活 → 客户端/控制台切换 → 摘旧令牌；升级知会"认证开启后旧无令牌客户端被拒，需同批滚动"）
- [x] 3.8 验证：`-pl openlatch-server,openlatch-client -am test` 全绿（reactor run server+client `test` 阶段全 SUCCESS；认证用例 AuthConfigTest/ConstantTimeTest/AuthHandshakeTest/ClientTlsAuthIT 全绿）；**P3-16 退出判据达成（除 3.6 集群结构保证项/3.7 文档）：空/错令牌断连、双活轮换、管理令牌分离校验通过**

## 4. P3-17 安全套件聚合与验收闭环（§8）

- [x] 4.1 安全拒绝用例聚合：明文连接被拒 / 错误凭证（空/错令牌、mTLS 无证书、错误 trust-store）/ 未认证管理请求三族由命名套件覆盖（`TlsGatingTest`/`AuthHandshakeTest`/`ClientTlsAuthIT`/`ConsoleTlsAuthTest` + 既有 `AdminProtocolTest` 认证矩阵）；默认关回归 = 全 reactor `test` 绿——聚合以"套件 + 证据对照表"实现，未另立空壳聚合类
- [x] 4.2 兼容矩阵（§8-6）：T4 无协议/字段变更（`wire-protocol` 冻结测试随 reactor `test` 通过）；默认关（TLS/auth off）下 v1/v2/v3 行为不变由 `AuthHandshakeTest.authOffKeepsPhase1Guard` + `HandshakeTest`(10) + server/client 全量 `test` 绿背书
- [x] 4.3 详设 §10.4 勘误回写（console 安全透传归 P3-15/16、admin 令牌分离为 P3-16 校验项、认证默认分支语义、集群结构保证、加密范围边界）+ README 双语安全章节（`README.md`/`README_CN.md` Security 节：服务端/客户端/starter/console 键表 + 轮换流程 + 范围边界）+ `console.properties.example` 安全键注释
- [x] 4.4 证据文件 `t4-acceptance-evidence.md`（变更目录）：机器已验证套件逐项对照 §7/§8（含命令）；诚实标注 L3 手工/`openssl s_client`/`-Pdrill` 为运维/CI 环境执行（本沙箱无 TTY，未伪装成已执行）
- [x] 4.5 收口：全仓 `mvn -s /home/lam/repo/settings.xml verify` **BUILD SUCCESS（exit 0，7:42 min，含 failsafe *IT）**——§8 六项验收逐项闭环见 `t4-acceptance-evidence.md`；`-Pdrill` 进程级演练按仓库纪律留待运维/CI 环境（无 TTY 沙箱不可靠）
- [ ] 4.6 提交并归档（delta 同步主规格：`transport-security` 新建 + `lock-server`/`spring-boot-starter`/`admin-console` 增量）；**Phase 3 发布**——**待变更关闭时执行**
