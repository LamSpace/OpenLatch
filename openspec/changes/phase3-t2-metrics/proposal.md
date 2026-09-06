# Proposal: phase3-t2-metrics

## Why

Phase 3 设计说明书 §3/§10.2 的 T2 要求交付 Micrometer 监控指标与 Prometheus 抓取端点（验收标准 §8-3：指标清单逐项有断言用例、抓取联调记录在案）。现状服务端零指标——运维对持锁量、等待深度、续租失败、租约到期等锁丢失前兆完全不可观测。且详设 §3.4"埋点全部在 `RequestDispatcher`"的假设在 Phase 2 之后已不成立：集群模式下请求根本不经过 `RequestDispatcher`（装配为 `null`，业务流量走 `ClusterRequestHandler`），照字面实现会让主要部署形态（集群）无数据。

## What Changes

- **新增服务端指标词表与注册表**：按详设 §3.2 十项指标（gauge/counter/histogram + 标签）建立唯一命名点；Micrometer + Prometheus registry；钉死 Micrometer 点分名 → Prometheus 线路名（下划线/`_total`/`_seconds_bucket` 等）的映射口径。
- **双路径埋点**（§3.4 勘误）：`RequestDispatcher`（单机）与 `ClusterRequestHandler`（集群）共用一个 `ServerMetrics` 组件记录 acquire/release/renew 计数与耗时；租约到期计数单机走 `expireDue()` 返回值、集群走条目应用侧；`openlatch.cluster.is_leader` 由 `LeaderTracker` 快照驱动、仅集群启用时注册。
- **管理端口 HTTP 端点**：独立端口（默认 9412，Netty `codec-http`）暴露 `/metrics`（Prometheus 文本）与 `/healthz`，仅此两路径（其余 404）；生命周期挂接 `OpenLatchServer.start()/stop()`；配置走新增 `MetricsConfig`（与 `ClusterConfig` 同文件独立加载的先例，不动 `ServerConfig` record 构造面）。
- **gauge 数据源扩展**：`CoreEngine` 新增只读统计观察面（按家族 held 数、等待者总数、单 key 最大队列深度、会话数——纯读、core 零第三方依赖底线保持）；集群侧读影子表投影与 `WaitQueue` 新增只读访问器；`ShadowTable` 持有投影改并发安全视图以允许抓取线程弱一致读。
- **客户端可选指标（默认关）**：`openlatch-client` 以 Maven optional 依赖 Micrometer；`Builder.meterRegistry(...)` 未注入即零埋点；四项客户端指标（请求计数/耗时/重连/锁丢失）挂 `RequestMultiplexer`、重连与失锁判定既有收口。
- **Starter 自动注入**：应用上下文存在 `MeterRegistry` bean 时 starter 注入客户端 builder（"若存在"语义）。
- 无破坏性变更：不开启时行为与现状一致；锁端口、协议、既有配置项零改动。

## Capabilities

### New Capabilities

- `metrics-observability`: 服务端指标清单与命名映射、双路径埋点语义（含集群耗时口径与到期计数口径）、管理端点与 `MetricsConfig`、gauge 取值语义与采样线程安全。

### Modified Capabilities

- `core-lock-engine`: 新增只读统计观察面要求（gauge 数据源契约；纯读、零新依赖、既有语义与线程模型不变）。
- `client-sdk`: 客户端可选指标接入契约（`meterRegistry` 构建钩子、默认关闭零开销、四项指标语义、埋点不改变重试/重发行为）。
- `spring-boot-starter`: 存在 `MeterRegistry` bean 时自动注入客户端指标。

## Impact

- **代码**：`openlatch-server`（新增 `metrics` 包：`MetricsConfig`/`ServerMetrics`/`MetricsHttpServer`；`RequestDispatcher`/`ClusterRequestHandler`/`OpenLatchServer`/`LeaseExpiryDriver`/`LockStateMachineCore`/`WaitQueue`/`ShadowTable` 插桩与只读面）、`openlatch-core`（`CoreEngine.stats()` 只读访问器）、`openlatch-client`（`Builder.meterRegistry`、multiplexer/重连/失锁处埋点）、`openlatch-spring-boot-starter`（自动配置条件注入）。
- **依赖**：server 新增 `micrometer-core`/`micrometer-registry-prometheus`/`netty-codec-http`；client 新增 optional `micrometer-core`；core 保持零第三方依赖。
- **测试**：三层断言（词表单测 / 固定脚本 + `/metrics` 抓取解析单机 IT / 集群三节点 IT）+ starter 环境注入用例；`ShadowTable` 摘要计算域不依赖投影序的回归钉定。
- **文档**：详设 §3.4 埋点位置勘误（双路径共用埋点组件）与 §3.2 命名映射口径需回写。
