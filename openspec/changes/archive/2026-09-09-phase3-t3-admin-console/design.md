# Design: phase3-t3-admin-console

## Context

详设 §4.2 把 ADMIN 消息的认证指向"§5（T4）"，但装配事实是：现行握手对 `HelloRequest.auth_token` 非空**直接断连**（Phase 1 规则，`ServerSessionHandler.handleHandshake`），T4 未落地前管理通道没有任何现成认证承载——P3-11 必须自足解决。数据源侧的缺口与 T2 同构：单机 `CoreEngine` 只有聚合 `stats()`、无 key 级明细观察面；集群状态在 `LockStateMachineCore` 的复制态镜像（引擎条目）+ Leader 内存 `WaitQueue`（T2 已开聚合只读口、明细口未开）；会话"建连时间"目前无处存放（`ServerSession` 无时刻字段、core `SessionRegistry` 只有 touchedKeys）。既有先例可依：T2 的"双路径共用组件"（`ServerMetrics`）、"同文件独立配置类"（`MetricsConfig`/`ClusterConfig` 经 Properties `load(path)`）、"core 只读观察面"（`stats()`）。Boot BOM 4.0.3 已导入根 pom（starter 置后防覆盖），本地仓库已核实 `spring-boot-starter-web`/`spring-boot-starter-thymeleaf`/`spring-webmvc`/`tomcat-embed-core` 在案。动机与清单见 proposal.md 与 specs。

## Goals / Non-Goals

**Goals:**

- ADMIN 四消息在单机与集群双装配下均可应答、字段契约可逐字段断言（§7 T3 消息级）；
- 未认证管理连接被拒可验证（§8-4 后半），且认证实现与 T4 业务令牌零纠缠；
- 控制台五页面端到端冒烟全绿（§8-4 前半），全程只读；
- v1/v2/v3 既有客户端与指标词表零扰动（回归底线 + 隔离断言）。

**Non-Goals:**

- 不做写操作（强制解锁/踢会话，§9-1）；不做 ADMIN over HTTP（走 Netty 业务协议，理由见 D2）；不做控制台多用户/登录体系与会话保持（单运维内网工具，令牌即准入）；不做指标曲线跨重启持久化；不做服务端跨节点查询聚合/自动改道（聚合在控制台，见 D3 口径）；不做 TLS 下的管理通道（T4 只管锁端口 TLS；控制台与节点的 TLS 属后续）。

## Decisions

### D1 管理令牌逐消息携带，不进 HELLO

每个 `AdminXxxRequest` 自带 `token` 字段；`AdminRequestHandler` 入口以 `MessageDigest.isEqual`（常量时间）比对配置的 `admin-token`，失败统一 `INVALID_REQUEST` + 断连（对齐 §5.2"不泄露原因"原则）；未配置 = 拒绝一切。HELLO 握手与 `auth_token` 现行规则一字不改。

- 备选 A：HELLO 携带 admin-token 并给会话打管理角色标记——被否：要先推翻"auth_token 非空即断连"的握手规则（T4 地盘），且认证语义纠缠进会话生命周期；T4 落地"多令牌轮换"时还得二次改造。
- 备选 B：等 T4 先做——被否：§10.3 排期 T3 在前，且验收 §8-4"管理通道强制认证"要求 T3 自证。
- 代价：管理令牌出现在每条 ADMIN 报文体——管理连接由控制台独占、流量低，且 T4 前无 TLS 的现状下 HELLO 同样是明文，风险面不扩大；§5.2 的"管理令牌与业务令牌分离"天然成立。详设 §4.2 的"认证（§5）"表述收口时回写勘误。

### D2 ADMIN 走业务端口的独立连接，不新开管理端口

`MessageType` 10–13 续号即同一协议族的宣告；详设"请求走独立连接、不与锁流量共享连接"指连接维度而非端口维度。控制台配置的地址列表即各节点业务端口（+ 概览页另拉各节点 9412 `/metrics`）。复用既有分帧/编解码/握手/在途限额，服务端只增一个 handler 分支。

- 备选：仿 metrics 在 9412 加 ADMIN 的 HTTP/JSON 门面——被否：违背 §4.2 的 MessageType 协议定义，且控制台将同时依赖两种协议栈。

### D3 服务端：`AdminRequestHandler` 双数据源适配器，拦截点在业务分发之前

新组件 `io.github.lamspace.openlatch.server.admin.AdminRequestHandler`（含 `AdminConfig`），挂接 `ServerSessionHandler.channelRead0` 已握手分支的**最前**（`tryBeginRequest` 在途记账之后、`cluster`/`dispatcher` 分发之前），单机与集群共用同一入口——吸取 T2 勘误①"集群路径旁落"的教训，管理面天然双形态，从第一天就做成单组件双适配器：

```
channelRead0（已握手、inflight 已计入）
   ├─ type ∈ ADMIN_* ─▶ AdminRequestHandler ─┬─ token 校验（失败：INVALID_REQUEST + close）
   │                                          ├─ 单机源：CoreEngine.inspect()
   │                                          └─ 集群源：ShadowTable 管理投影 AdminEntryView
   │                                             （逻辑会话口径，实现期修正见 D4）
   │                                             + WaitQueue 明细只读口（Leader 权威/
   │                                               follower 空 + wait_queue_leader_only）
   │                                             + ServerSessionRegistry（本节点接入会话）
   │                                             + LeaderTracker / 启动时刻 / 版本（SUMMARY）
   └─ 其余 ─▶ 既有 cluster/dispatcher 路径（零改动，指标埋点不受扰）
```

- 应答同步写回 + `endRequest`（管理请求不进 Raft、不异步）；`page_size` 上限钉常量（如 200）防观察面放大攻击。
- 分页收口在 handler：`inspect()` 全量快照 → 过滤 → 字典序 → 切片；观察面与协议关注点分离（core 不懂分页）。
- follower 不改道 Leader：控制台地址列表本就让用户自选节点、节点视图页有 Leader 标识指路，只读协议里塞路由是复杂度（备选出：控制台经 leader_hint 自动改道——留待真有需求另立变更）。

### D4 core 只增 `inspect()`：明细快照值对象

`CoreEngine` 新增 `CoreInspection inspect()` + `inspectKey(String)` 定点形态：遍历 `LockTable.values()`，逐条目 `synchronized(e)` 内拷贝不可变值对象（家族、holders 带写/读/持有角色与计数、租约三元组、waiter 位次表、latch total/count/参与者、以采样时刻折算的 waited_ms/remaining_lease_ms），锁外组装。是 T2 `stats()` 先例的明细版：core 零新依赖、纯读、零行为变化。

**集群侧数据源（实现期修正，P3-11 装配事实核正）**：提案期"集群读引擎镜像条目（同 inspect 面）"的假设不成立——集群引擎条目以**内部 local sid** 归属（逻辑→本地映射仅在 kernel `sidMap`），管理应答必须以**逻辑会话 id** 为口径（跨节点可对齐）；且 `ShadowTable.locks`（含持有计数/许可/屏障明细的真相）为应用线程独占的 `LinkedHashMap`，跨线程读不合法、`HeldRef` 投影又不含 latch 与计数。故改为：`ShadowTable` 新增与 `heldIndex` 同构的管理明细投影 `adminView`（`ConcurrentHashMap<String, AdminEntryView>`，逐应用点整体重发布、O(1) 发布成本、不入 digest、LATCH 亦在列），管理面**单机读 `inspect()`、集群读 `adminView` + `WaitQueue`**——两形态同一 handler 双适配器，恰与 T2 的"双路径埋点 + 双数据源 gauge"同构。

**会话建连时间不进 core**：`ServerSession` 加 `connectedAtMs`（accept 挂载簿记时取 epoch 毫秒——"建连"早于握手，与 LIST_SESSIONS 仅报已接入会话的口径一致，server 模块簿记字段）——LIST_SESSIONS 只报本节点接入会话，本地字段即权威，无需动复制状态。

- 备选：把明细观察塞进 `stats()` 返回体——被否：聚合读数与明细快照的调用频率、拷贝成本差两个数量级，混在一个方法里语义糊。
- 备选：集群侧直读引擎 `inspect()`——被否（实现期修正后反转本决策原"ShadowTable 开明细口"备选）：local sid 口径不可对外、条目表跨线程读非法；唯一合法形态是应用点发布的不可变投影。

### D5 `WaitQueue` 新明细只读口

集群档等待队列在 `WaitQueue`（Leader 内存）：节点原不记录入队时刻——`Node` 增 `enqueuedAtMs`（构造即定型，入队 `now` 形参已是 epoch 口径），新增 `keyWaiters(String key, long now)`（位次/归属/`WaiterView` 明细快照）与 `waitCountsBySession()`（会话→在队数）级只读访问器（短临界区拷贝快照列表，T2 的 `totalWaiters()/maxQueueDepth()` 先例）。位次由队列序导出、`waited_ms` 由入队时刻与折算时刻相减（下限 0）。

### D6 console 模块形态

新 Maven 模块 `openlatch-console`（根 pom `<modules>` 追加，examples 之前）：

- 依赖 `openlatch-server`（取 `EnvelopeCodecHandler` 帧编解码 + 协议类）+ `spring-boot-starter-web` + `spring-boot-starter-thymeleaf`（版本全走 Boot BOM，零显式版本）；打包用 `spring-boot-maven-plugin` repackage 出可执行 jar。
- 内置 `AdminClient`：Netty 每节点一条独立连接（自有小 EventLoopGroup，daemon），HELLO（`client_protocol_version=3`、`auth_token` 留空、`client_name="openlatch-console"`）→ 同步 ADMIN 请求/应答（`request_id` 关联 + 超时，默认 5s），断线懒重连 + 认证失败退避；**不引 openlatch-client SDK**（看门狗/锁语义与控制台零相关）。
- 配置 `console.properties`：`openlatch.console.server-addresses`（逗号分隔 `host:port`）、`openlatch.console.admin-token`、`openlatch.console.port`（默认 9413）、`openlatch.console.refresh-interval-seconds`（默认 5）、`openlatch.console.metrics-port`（默认 9412，`/metrics` 拉取目标）；加载与校验仿 `ServerConfig.load` 风格。
- Web 层：五个 Thymeleaf 页面 + 每页面一个 Controller，模板自含轮询（`<meta http-equiv="refresh">` 或等价，零 JS 构建链；租约倒计时以刷新粒度呈现即可）。
- 全部新 Java 源文件按 CLAUDE.md §5 配 Javadoc、License 头（构建插件既有强制）。

### D7 sparkline：控制台服务端拉取 + 内联 SVG

概览页数据：`java.net.http.HttpClient` 定时拉各节点 `http://<host>:<metrics-port>/metrics`，解析既定线路（`openlatch_server_locks_held`、`waiters`、`sessions`、`acquire_total`），每节点每线保留内存环形缓冲（最近 N 样本）；Thymeleaf 渲染内联 SVG 折线。拉取失败/未启用 → 该节点曲线区降级标注（不整页错、不零值冒充）。不引图表 JS 库、不引前端构建（YAGNI，§4.1 明示）。

### D8 SUMMARY 的数据装配

`node_role`：单机报 `SINGLE`；集群经 `LeaderTracker.snapshot()` 报 `LEADER`/`FOLLOWER`/`UNKNOWN`。`uptime_ms`：`OpenLatchServer` 记 `start()` 成功时刻。`version`：协议常量 + jar `Implementation-Version`（manifest 缺失时回落构建常量——实现期核实 shade/jar 打包的 manifest 形，测试环境不依赖 manifest）。`session_count`：本节点 `ServerSessionRegistry.size()`（与 gauge 口径同源；管理连接自身也在册，如实计数，文档注明）。

### D9 测试三层

- **L1 消息级（openlatch-server）**：真 socket + 手写 ADMIN 帧客户端（复用 codec），手工时钟预置矩阵（多持有者重入、读写混合、Semaphore 部分许可+在队大请求、Latch 等待）→ 四消息逐字段断言；认证矩阵（对/错/空/未配置/握手前/低版本会话）；分页边界与前缀；**指标零污染断言**（执行 ADMIN 前后 `PrometheusMeterRegistry` 快照等值，复用 T2 词表测试夹具）；集群三节点 fixture：follower 镜像口径 + `wait_queue_leader_only` + 会话列表分治。
- **L2 console 端到端冒烟（openlatch-console）**：同 JVM 起真 `OpenLatchServer`（单机档 + 集群档各一组，端口 0）+ `@SpringBootTest` 起 console → `TestRestTemplate` 断言五页面 HTML（预置 key 名、持有者会话号、等待位次、Leader 标记、认证失败降级页）；`/metrics` 代理与降级用例（metrics 关档）。
- **L3 部署走查证据**：`docs` 先例立 `t3-acceptance-evidence.md` 于变更目录——单机与三节点集群实跑 console jar，页面走查记录（curl 断言 + 用户浏览器目检）、错令牌降级记录；与 §7 T3/§8-4 判据对照表。
- **回归底线**：全量 `clean verify` + `-Pdrill`；`wire-protocol` 冻结测试（v1 基线零变更 + `raft.proto` 字节零差异）；既有 Handshake/MessageLegality/InflightOverload 全绿。

## Risks / Trade-offs

- [管理连接占用业务会话，`sessions` gauge 与会话列表会含控制台自身] → 如实计入口径已钉进 D8/spec；控制台 HELLO 填 `client_name` 供辨认，文档知会。不改握手语义换清净（代价不成比例）。
- [高频轮询 + 大 key 量时 `inspect()` 全量快照放大 CPU/分配] → `page_size` 上限 + 轮询默认 5s + 快照仅拷贝必要字段；性能专项不在本变更，README 知会"控制台面向运维观察、非高保真监控替代"。
- [`WaitQueue` 明细读与 Leader 授予路径锁竞争] → 短临界区快照拷贝（与既有 `totalWaiters()` 同形）；L1 并发冒烟（队列活跃时连续查询无异常）。
- [`ServerSessionHandler` 是 Phase 1 冻结行为密集区，插分支易伤及既有路径] → ADMIN 分支仅 `type` 判别、置于既有 switch 之前独立早退；`HandshakeTest`/`MessageLegalityTest` 全量回归 + 新增"v1/v2 会话 ADMIN 被拒且业务不变"用例锁行为。
- [Boot 4 repackage 与 reactor 既有插件（javadoc show=private、license 头、enforcer）首次组合在 console 模块] → P3-12 骨架任务即跑全 reactor 验证；若 Boot 插件与 javadoc 聚合冲突，以 `spring-boot.repackage.classifier` 双工件方案兜底（不阻塞交付物）。
- [本地仓库缺 Boot 4.0.3 个别传递件（如 tomcat 补丁版差异）] → 骨架落地即 `mvn -o dependency:resolve` 验证，缺件清单报用户预取（用户已确认基本盘在案）。
- [管理令牌明文在线路（T4 前无 TLS）] → 与当前 HELLO/业务流量同级暴露面，D1 已述；T4 落地 TLS 后管理连接随之受保护，README 部署知会"控制台仅限内网"。

## Migration Plan

纯增量部署：服务端升级即获得 ADMIN 能力（未配置 `admin-token` 时该能力对所有人关闭，行为与现状逐字节一致）；控制台为新增独立进程，回滚 = 停掉 jar。协议回滚安全：旧服务端收到 ADMIN 走既有 `default → INVALID_REQUEST`，无崩溃面。README 双语知会 9413 端口与最小启动示例。

## Open Questions

- 锁列表页的"排序"（§4.3）：本期钉"服务端字典序 + 控制台当前页内交互排序"；跨页按等待数/租约排序需要服务端 `sort_by` 字段——若运维确有需求，作为 v3 协议小增量另立变更（不影响本期 spec/task 形状）。
