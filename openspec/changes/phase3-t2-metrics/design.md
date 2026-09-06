# Design: phase3-t2-metrics

## Context

详设 §3.4 假设"埋点全部在 `RequestDispatcher`"，但 Phase 2 的装配事实是：集群模式下 `OpenLatchServer.start()` 给 `ServerSessionHandler` 传的 dispatcher 为 `null`，业务流量走 `ClusterRequestHandler`（应答经 Raft 提交后在 `respondAsync` 异步写回）。单机侧的 gauge 数据源也有缺口：`CoreEngine` 的 `lockTable`/`sessions` 为私有字段，无统计观察面；集群侧 `ShadowTable.heldEntries()` 的投影并发口径需核实（提案期疑为 `LinkedHashMap`）。约束：core 零第三方依赖不变；`ServerConfig` 是 record 且 `ClusterConfig.load(path)` 已确立"同文件独立配置类"先例；客户端与 starter 依赖面保持轻。动机与清单见 proposal.md 与 specs。

## Goals / Non-Goals

**Goals:**

- §3.2 十项指标在单机与集群两种装配下落地的线路名/标签逐项可断言；
- 指标开启与否不改变任何协议行为（v1/v2/v3 客户端行为不变的验收 §8-6 延伸）；
- 三层测试（词表单测 / 固定脚本抓取 IT / 集群三节点 IT）构成 §8-3 验收证据。

**Non-Goals:**

- 不做指标推送/远程写入（仅拉取端点）；不做直方图桶位可配置（先取 Micrometer 默认桶，需要时另立变更）；不做按 key 维度的高基数指标（详设 §9-4）；不做 TLS 下的管理端口（Phase 3 T4 只管锁端口与 HELLO，管理端点认证留给 T3 管理协议体系统一处理）；console 对 `/metrics` 的消费属 T3。

## Decisions

### D1 双路径共用埋点组件 `ServerMetrics`（§3.4 勘误）

新增 `io.github.lamspace.openlatch.server.metrics.ServerMetrics`：包一个 `PrometheusMeterRegistry`，指标名/标签常量与记录方法（`recordAcquire(status)`、`recordAcquireDuration(result, nanos)`…）唯一定义。注入 `RequestDispatcher` 与 `ClusterRequestHandler` 两个装配点（经 `OpenLatchServer`/`ClusterRuntime` 构造链传递，可为 null——`metrics.enabled=false` 时不建 HTTP 服务但埋点仍累积进内存注册表，代价是几次判空分支，换来"关闭即不抓取"而非"关闭即无数据"的简单语义）。

- 备选：在 `ServerSessionHandler` 写回处统一埋点——被否：集群应答经异步回调多路写回（`writeSync`/`respondAsync`/推送），统一收口反而要复制状态机跟踪每条请求的起止；且错误路径（未握手拒绝、OVERLOADED）与业务请求的计数口径会纠缠不清。
- 备选：只埋单机路径按 §3.4 字面——被否：集群主部署形态无数据。
- 详设 §3.4 需在收口时回写勘误（T1 勘误先例）。

### D2 命名映射表钉进测试词表

Micrometer 把点分逻辑名翻译为 Prometheus 线名：counter `openlatch.server.acquire.total` → `openlatch_server_acquire_total`（已带 `.total` 尾不重复追加）；Timer `openlatch.server.acquire.duration` → `openlatch_server_acquire_duration_seconds_{bucket,count,sum}`（毫秒自动折秒；实现期核实：须 `publishPercentileHistogram()` 才产出 `_bucket`，Micrometer 默认桶集、非 summary 形态；新 Prometheus 客户端标签序为插入序 `result` 先于 `le`）；gauge `openlatch.server.locks.held` → `openlatch_server_locks_held`。§7"逐项一致"的判定口径钉为"清单 ↔ 翻译后线名"映射表，映射表以 L1 词表单测为唯一权威断言点。

- 备选：关闭翻译保留点分名——被否：违背 Prometheus 惯例，Prometheus 端反而不可用。

### D3 gauge 数据源：core 只读 `stats()` + 集群读复制态投影

- **单机**：`CoreEngine` 新增 `CoreStats stats()` 只读访问器，返回不可变值对象（按家族/held 条目数聚合、等待者总数、单 key 最大队深、会话数），内部弱一致遍历 `LockTable.values()` 与 `SessionRegistry`。这是 core 的新 public 面但不新增依赖、不改行为——"不侵入 core"的实质是 core 不感知 Micrometer，本决定与之相容（specs/core-lock-engine 已钉为契约）。
- **集群**：held 读 `ShadowTable` 投影、waiters/队深读 `WaitQueue` 新增 `totalWaiters()`/`maxQueueDepth()`（server 自有类，加只读方法无障碍）；sessions 读 `ServerSessionRegistry.size()`。Leader 上为权威值，非 Leader 上为回放镜像值——两种读法都是"该节点本地观察"，语义一致。
- **`heldIndex` 投影（实现期核实：本就是 `ConcurrentHashMap`）**：提案期 Context 的"LinkedHashMap"前提有误——该类自 Phase 2 起即以 CHM 承载跨线程"可旧不可错"读（`LeaseExpiryDriver` 消费），抓取线程弱一致读天然合法，容器零改造；digest 计算域核实为 `locks`+`sessions`（`toProto()` 不触碰投影）。为 gauge 的家族聚合给投影记录 `HeldRef` 增加定型 `lockType` 字段（内存记录，不入 proto，digest 字节形零扰动）。spec"摘要不受投影容器改造影响"场景由确定性测试常开承接。
- **`lease.expired.total`**：单机在 `startScheduler()` 里累加 `core.expireDue()` 返回值（零 core 改动）；集群在 `LockStateMachineCore` 应用 `LEASE_EXPIRE_ENTRY` 实际释放处计数（该类在 server/raft 包，注入自由）。重启回放重复计为已声明偏差（spec 钉住口径）。

### D4 `is_leader` 与 LATCH 的清单语义

`openlatch.cluster.is_leader{node_id}` 仅集群启用时注册（单机无集群身份，注册恒 0 是语义噪声）；取值 = `LeaderTracker.snapshot()` 判本节点。`locks.held` 的 `type` 标签取家族名 `lock`/`semaphore`（实现期勘误：`LockEntry` 不携带单一协议类型——REENTRANT/SIMPLE/FAIR 同族互通、READ/WRITE 是请求维度——协议六值在两数据源均不可导出；LATCH 无持有者、无租约、不进 `heldIndex`，仍排除）。`waiters` 统计含 latch awaiter（单机的 latch 等待者在条目内、集群的在 `WaitQueue`——两侧都计入，口径为"等待队列条目总数"）。

### D5 管理端点：Netty codec-http 极简两路由

`MetricsHttpServer`：独立 boss/worker（各 1 线程，daemon），pipeline = HttpServerCodec + aggregator + 一个 handler（精确匹配 `/metrics`→`registry.scrape()`、`/healthz`→"ok"，其余 404）。`netty-codec-http` 版本随既有 Netty BOM。端口冲突与锁端口同策略快速失败；port=0 支持测试。不引入任何 Web 框架（详设 §3.1）。

- 备选：JDK `com.sun.net.httpserver`——被否：省一个依赖但线程模型裸配、与"Netty HTTP"选型文档相悖，收益不抵偏离。

### D6 客户端指标：Maven optional + 构建器钩子

`openlatch-client` 编译期依赖 `micrometer-core`（`<optional>true</optional>`，不传递）；`Builder.meterRegistry(MeterRegistry)`（默认 null）。内部单点 `ClientMetrics` 门面：注册表为 null 时全部方法为静态可证明的 no-op（一次判空分支），启用时在 `RequestMultiplexer`（发送/完成/超时）、`ConnectionManager`（重连发起）、`OpenLatchClient.loseEntry`（失锁判定）三处收口记录。

- 备选 A：client 定义自有薄 SPI、starter 适配——被否：为单一消费者造抽象，且非 Spring 用户拿不到指标；Micrometer 即事实标准。
- `type` 标签取 `MessageType` 名、`status` 取应答状态码名/失败类别（timeout/unavailable），与客户端既有影子表回写语义无关。

### D7 starter 注入：条件装配

`OpenLatchAutoConfiguration` 增 `ObjectProvider<MeterRegistry>` 条件注入：存在即 `builder.meterRegistry(...)`，不存在跳过（`@ConditionalOnBean` 语义经 `ObjectProvider.getIfAvailable()` 实现，避免 bean 定义顺序问题）。starter 侧 `micrometer-core` 为 optional/provided（Spring Boot Actuator 场景由宿主提供）。

### D8 测试三层结构

- **L1 词表单测**（server）：`SimpleMeterRegistry` 直挂 `ServerMetrics`，驱动 `RequestDispatcher`/`ClusterRequestHandler` 的消息面（复用既有 dispatch 测试夹具），逐项断言 `registry.get/find`；含 D2 映射表断言（对 `PrometheusMeterRegistry` scrape 文本逐线名）。
- **L2 单机脚本 IT**：真 server（锁端口 0 + 管理端口 0）→ `openlatch-client` 执行固定脚本（N granted/queued/denied 获取、释放、renew 失败、短租约到期）→ HTTP 抓 `/metrics`，用 prometheus simpleclient textparser（test scope）解析，先抓基线做差逐项断言；另断言 `/healthz`、404、Content-Type。
- **L3 集群 IT**（`ClusterMetricsTest`，实现期口径：在 JVM 三节点基座上直读各节点注册表断言，管理 HTTP 生命周期已由单机档覆盖）：断言 `is_leader` 分布、Leader gauge 与影子表一致、follower `acquire.total{status="NOT_LEADER"}` 增长、到期集群路径计数不超计。复用既有 `-Pdrill`/集群测试框架。
- **P3-09 附加**：gauge 并发冒烟（压测中连续抓取无异常）；starter 注入用例（`ApplicationContextRunner`，带/不带 `MeterRegistry` bean 两态）。
- **Prometheus 真抓取联调**：CI 不跑真 Prometheus；一次性 docker compose 联调记录（config + 抓取到目标指标的 screen 输出）存入变更目录作为 §8-3 证据（T1 的 `t1-acceptance-evidence.md` 先例，本变更立 `t2-acceptance-evidence.md`）。

## Risks / Trade-offs

- [scrape 线程读非线程安全投影炸 `ConcurrentModificationException`] → D3 的 `heldIndex` CHM 化 + digest 不变回归钉死；实现首步先核实计算域。
- [Micrometer gauge 对绑定 obj 仅持弱引用，无人保活的 supplier lambda 被 GC 连带摘除仪表（全量 verify 首轮实证：`is_leader` 偶发缺失）] → `ServerMetrics` 内置保活表持有绑定对象；实现期教训回写。
- [Timer 默认桶下 duration 断言脆弱] → L2 只断言 `_count`/`_sum` 的存在与计数增长，不断言分位数（桶位分布不进断言口径）。
- [集群 follower 的 gauge 短暂落后于 Leader] → spec 已钉"本地观察值"语义，测试断言用"最终一致 + 稳态收敛"写法，不做跨节点即时等值断言。
- [metrics 埋点在热路径引入锁竞争] → counter 为 LongAdder 底、gauge 弱一致读；`recordX` 仅分支 + 累加。以既有 IT/演练全量通过佐证无劣化（无新微基准，性能专项不在本变更）。
- [optional 依赖被宿主旧版本 Micrometer 顶掉] → client 仅用 `MeterRegistry`/`Counter`/`Timer` 基础 API（1.x 稳定面），README 兼容性说明随收口回写。
- [重启回放致 expired 虚增] → D3/spec 明示口径；如需精确可后加"快照安装即归零"，不在本变更。

## Migration Plan

纯增量：默认开启管理端口 9412（防火墙/编排层需知会，属部署文档）；回滚 = 配置 `enabled=false` 或退回版本，无数据迁移。既有配置键与协议零变化。
