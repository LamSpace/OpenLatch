# Proposal: phase3-t3-admin-console

## Why

Phase 3 详设 §4/§10.3 的 T3 要求交付管理控制台（验收标准 §8-4：控制台冒烟通过，且管理通道强制认证）。现状运维只能靠 `/metrics`（T2 已交付）看聚合数字，无法回答"这把锁谁持有、排了几个人、还剩多久租约、这个会话连在哪个节点"——锁系统的核心可运维问题（持锁者定位、等待链观察、会话关联）没有任何观察面。且详设 §4.2 把管理认证指向"§5（T4）"，而现行握手对非空 `auth_token` 直接断连（Phase 1 规则），T3 等不起 T4——管理通道的令牌校验必须在 P3-11 内自足落地。

## What Changes

- **管理协议四消息**（§4.2）：`ADMIN_SUMMARY=10`、`ADMIN_LIST_KEYS=11`、`ADMIN_KEY_DETAIL=12`、`ADMIN_LIST_SESSIONS=13` + 8 个请求/响应载荷（Envelope oneof 续号 24–31）。纯增量，`raft.proto` 零触碰——管理消息是只读观察面，不进复制日志。仅对协商 v3 的连接开放。
- **管理通道认证（§8-4 前半）**：每条 ADMIN 请求自带 `token` 字段，服务端常量时间比对配置的 `admin-token`；校验失败统一 `INVALID_REQUEST` 并断连（不泄露原因）；未配置令牌 = 一律拒绝（安全默认）。HELLO 握手语义一字不动，与 T4 业务令牌天然分离。
- **服务端 `AdminRequestHandler`**：挂在 `ServerSessionHandler` 单机/集群两条装配路径上、业务分发与 `ServerMetrics` 埋点之前——管理流量零指标污染（钉为断言）；分页/前缀过滤在此收口。数据源双形态：单机读 `CoreEngine`，集群读本节点复制态镜像 + `WaitQueue`（Leader）/角色提示（follower）。
- **core 只读明细观察面**：`CoreEngine` 新增 `inspect()`（T2 `stats()` 先例的明细版）：按 key 条目产出不可变快照——家族/类型、持有者 `(session, thread, count/permits)`、租约到期时刻、等待队列位次/会话/等待时长、Latch 的 `total/count`；弱一致逐条目取锁快照，零新依赖零行为变化。
- **`openlatch-console` 新模块**（概要设计 §5.2）：Spring Boot Web + Thymeleaf 服务端渲染 + 轻量轮询刷新，无前端构建链；独立部署可执行 jar，监听默认 **9413**；配置目标节点地址列表 + admin-token。内置轻量 `AdminClient`（复用 server 的帧编解码，每节点一条独立连接、懒重连），不经业务 client SDK。
- **五页面**（§4.3）：概览（SUMMARY 数字 + `/metrics` 拉取渲染 sparkline）、锁列表（分页/前缀过滤）、锁详情（持有者、等待队列、剩余租约倒计时）、会话列表（本节点接入会话：id/接入节点/建连时间/持锁数/等待数）、节点视图（`CLUSTER_VIEW` + 各节点 SUMMARY，Leader 标识）。
- **只读边界**：不提供强制解锁/踢会话等写操作（§4.2/§9-1）；集群档按节点如实呈现——等待队列仅 Leader 可见，follower 标注角色提示，不做自动改道。
- 无破坏性变更：v1/v2/v3 既有客户端行为零改动（ADMIN 类型对旧客户端不可达）；不开控制台、不发 ADMIN 请求时服务端行为与现状逐字节一致。

## Capabilities

### New Capabilities

- `admin-observability`: 服务端管理观察协议——四消息的字段契约与只读语义、令牌认证与未认证拒绝、v3 版本门、单机/集群双形态数据源口径（含 follower 等待队列语义与"仅本节点接入会话"）、分页/过滤/排序、与指标埋点的隔离保证。
- `admin-console`: 控制台 Web 应用——部署形态与配置（地址列表/令牌/端口 9413）、五页面内容与轮询刷新、`/metrics` 代理与 sparkline、多节点连接管理、认证失败与节点不可达的降级呈现。

### Modified Capabilities

- `wire-protocol`: `MessageType` 续号新增 ADMIN 四值、`Envelope.payload` 续号新增八字段；ADMIN 消息仅对协商版本 ≥3 的连接受理；编号纪律（只增不复用）与协议冻结测试相应增量。
- `core-lock-engine`: 新增 `inspect()` 明细级只读统计观察面（gauge 之外的管理协议数据源契约；弱一致快照、纯读、零新依赖、既有语义与线程模型不变）。

## Impact

- **代码**：`openlatch-protocol`（`openlatch.proto` 增量）；`openlatch-server`（新增 `admin` 包：`AdminRequestHandler`/`AdminConfig`；`ServerSessionHandler` 两路径拦截点、`ServerSession` 建连时刻、`OpenLatchServer` 装配与启动时长、`ClusterRuntime`/`ShadowTable`/`WaitQueue` 只读消费）；`openlatch-core`（`CoreEngine.inspect()` 与条目快照方法）；新模块 `openlatch-console`（根 pom `<modules>` 增项）。
- **依赖**：console 新增 `spring-boot-starter-web`/`spring-boot-starter-thymeleaf`（Boot BOM 4.0.3 已托管，本地仓库在案）；server/core/client 依赖面零变化。
- **测试**：三层——L1 ADMIN 消息级单测（含认证矩阵、指标零污染断言）；L2 同 JVM 真 server + 真 console 的端到端页面断言（单机档 + 集群三节点档）；L3 部署走查证据 `t3-acceptance-evidence.md`（T2 docker 联调先例）。回归底线：既有全量 `clean verify` 与 `-Pdrill`、v1/v2 兼容用例、`wire-protocol` 冻结测试。
- **文档**：详设 §4.2 认证承载通道留白需回写实施勘误（per-message 令牌）；README 部署段知会 9413 端口与控制台启动方式。
