# Proposal: phase3-t4-security

## Why

Phase 3 详设 §5/§10.4 的 T4 要求交付传输加密与 Token 认证（验收标准 §8-5：安全拒绝用例全绿——明文连接、错误凭证、未认证管理请求）。现状：锁端口协议全程明文；`HelloRequest.auth_token`（field 3）Phase 1 预留后一直维持"非空即断连"的占位规则（`ServerSessionHandler.handleHandshake` 硬编码，lock-server spec"会话握手"如此陈述），连接一旦被嗅探，锁名/会话/租约全部可读。T3 已把管理令牌逐消息落地，但其 design Non-Goal 明示"服务端开 TLS 后控制台与节点的 TLS 属后续"——该"后续"即本期。在交付 TLS 与认证之前，`openlatch-console` 无法观察 TLS 化的节点，生产部署也无法把锁流量放进加密通道。

## What Changes

- **服务端 TLS**（§5.1）：新增 `openlatch.server.tls.*` 配置族，`enabled`（默认关闭，行为与现状逐字节一致）开启后在监听端口 pipeline 首位置插入 `SslHandler`；证书以 PEM 文件提供；`require-client-cert=true` 时开启 mTLS（以 trust-store 校验客户端证书）。开启后拒绝明文连接：握手超时（默认 5s）内未完成 TLS 即断开，TLS 解码/握手失败断开且不产生任何会话副作用。配置非法（文件不可读/解析失败/互斥矛盾）启动即快速失败，不进入半启动。**加密范围 = 客户端接入端口**（业务与管理消息共用该端口）；Raft 节点间通道与指标/控制台 HTTP 不在本能力范围，文档显式声明。
- **服务端业务令牌认证**（§5.2）：启用 Phase 1 预留的 `HelloRequest.auth_token`。新增 `openlatch.server.auth.*` 配置族。**`enabled=false`（默认）维持 Phase 1 规则**：`auth_token` 非空 → `INVALID_REQUEST` 并断连（兼容守卫，既有客户端行为不变）；**`enabled=true`**：持 ≥1 个配置令牌（多令牌列表支持轮换），HELLO 令牌命中任一即放行，失败（缺失/为空/不符/未配置）统一 `INVALID_REQUEST` 并断连、不泄露原因（防探测枚举），常量时间比较，判定发生在任何会话分配/注册表登记/集群 `SESSION_OPEN` 复制**之前**（单机与集群同一门闩）。认证一次完成于握手，会话生命周期内不逐请求鉴权；管理令牌（T3 逐消息承载）与业务令牌严格独立、不得互替借道。令牌轮换流程文档化：服务端先加新令牌（双活）→ 客户端切换 → 摘旧令牌。
- **客户端 TLS 与认证消费**（§5.1/§5.2）：`openlatch-client` builder/config 新增可选 `tls.*`（enabled/trust-store，mTLS 附 client-cert/client-key）与 `auth-token`；连接建立、种子发现探针与重连全程按配置执行 TLS 握手并附令牌；默认全关 = 现有明文行为不变。
- **`spring-boot-starter` 透传**：`openlatch.*` 配置面新增 TLS/认证键并转发到所建客户端，使 Spring Boot 应用可用加密/认证连接。
- **`openlatch-console` TLS 客户端**（T3 design Non-Goal 到期的"后续"）：节点连接新增可选 TLS 配置，服务端开 TLS 后控制台仍可观察；配置缺失或信任失败按既有"节点不可达"降级呈现，不发明文请求到 TLS 端口。
- 无协议版本号变更、无消息字段变更：认证复用既有 `HelloRequest.auth_token`，TLS 是传输层而非协议层，`wire-protocol` 零触碰（冻结测试保持全绿）。
- 无破坏性变更：全部新能力默认关闭；关闭状态下既有 v1/v2/v3 客户端、控制台、指标/管理面行为与现状逐字节一致（§8-6）。

## Capabilities

### New Capabilities

- `transport-security`: 锁端口传输安全——服务端 TLS 接入与 mTLS/明文拒绝/握手超时语义、默认关闭兼容性、业务令牌 HELLO 认证（默认守卫、多令牌列表与轮换、常量时间比较、失败不泄露、单机/集群统一门闩、管理令牌独立）、客户端/控制台对 TLS 与认证的消费行为与重连语义。

### Modified Capabilities

- `lock-server`: "会话握手"需求语义变更——`auth_token` 从"Phase 1 无条件非空即断连"改为按认证开关门控（默认关闭分支维持原规则），并同步修正版本区间表述（支持区间 [1,3]，非"等于 1"）。
- `spring-boot-starter`: "配置属性绑定与默认值"需求新增 TLS/认证配置键（绑定并转发到客户端）。
- `admin-console`: "部署形态与配置"需求新增节点连接的可选 TLS 配置（trust-store/client-cert；mTLS 随需），TLS 化的服务端节点可被控制台只读观察。

## Impact

- **代码**：`openlatch-server`——新增 `TlsConfig`/`AuthConfig`（同文件独立加载，仿 `AdminConfig`/`MetricsConfig`）、`ServerChannelInitializer` 首位置 `SslHandler`、`ServerSessionHandler.handleHandshake` 认证门闩分叉、`ConstantTime` 常量时间比较工具（admin `tokenOk` 迁移）；`OpenLatchServer` 构造链 +2（最终 6 参全装配终构造，旧构造委托）。`openlatch-client`——`ClientConfig`/builder 增 TLS 与 token、`ConnectionManager.doConnect` 首位置 `SslHandler`、`sendHello` 与 `SeedDiscovery` 探针附令牌、握手失败语义。`openlatch-spring-boot-starter`——`OpenLatchProperties` 增键。`openlatch-console`——`ConsoleConfig`/`AdminClient` 增 TLS。`netty-handler` 两端已依赖，零新增第三方依赖。
- **测试**：三层——L1 协议/握手级拒绝矩阵（明文被拒、mTLS 无证书被拒、空/错令牌同形断连、轮换双活、版本区间）；L2 真 server + 真 client / 真 console 的端到端（TLS 正确通过、错误 trust-store 降级、console→TLS 节点冒烟、auth on 集群档不进 `SESSION_OPEN`）；L3 部署证据 `t4-acceptance-evidence.md`（真握手 + openssl 抓取级记录，仿 T2/T3 证据文体）。回归底线：全量 `clean verify` 与 `-Pdrill`、`wire-protocol` 冻结测试（v1 基线零变更）、TLS/auth 关闭档既有 Handshake/MessageLegality/AdminProtocol/console 用例全绿。
- **证书夹具**：预生成 PEM 证书三件套（CA + server/client 叶证书 + 异 CA rogue 证书）提交至 `src/test/resources`，测试走 `SslContextBuilder` 生产同一条 PEM 加载路径；不在测试期运行时生成（规避 JDK 25 强封装对 Netty `SelfSignedCertificate` 反射的限制）。
- **文档**：README 双语安全章节（加密范围边界、9412/9413 保持明文、令牌轮换步骤、证书更新需重启）；详设 §5 落地无勘误（原文 §5.1/§5.2/§5.3 与实现一致），仅 §5.1"tls.* 配置"与 §10.4 子任务按上述拆分微调口径并回写勘误。
