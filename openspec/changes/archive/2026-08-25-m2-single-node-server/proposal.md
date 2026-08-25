# Proposal: m2-single-node-server

## Why

实施计划里程碑 M2 要求交付 OpenLatch 的第一个可运行形态：Netty 单节点锁服务器。M1 已交付线路协议（`openlatch-protocol`）与纯 Java 锁语义核心（`openlatch-core`），但锁语义目前只能在纯单元环境中验证——只有当核心被装进网络服务、暴露真实插座，M3 客户端 SDK 才有对接对象，§10.3 的端到端路径（通知 → 重发 → 授予、断连清理、租约兜底）才可能被真实验证。本变更完成设计说明书 §13.2 全部子任务（P1-11 ~ P1-17）。

## What Changes

- 实现 `openlatch-server` 模块（现为空占位）：按设计说明书 §5 交付配置加载、Netty pipeline、HELLO 握手门闩、请求分发（协议 ⇄ core 命令映射）、通知桥（`CoreEventListener` → `AWAIT_NOTIFY` 推送）、租约扫描调度（`expireDue` + `sweepNotifiedHeads` 周期驱动）、断连即时清理与空闲检测断连。
- 服务端业务逻辑直接在 Netty IO 线程同步执行（§5.2：短临界区、无阻塞），等待者零线程占用；core 的锁语义判定**一行不改**，本变更不触碰 `openlatch-core` 与 `openlatch-protocol` 的既有代码。
- 父 `pom.xml` 新增依赖与插件版本锁定：Netty 4.1.x 当前稳定版、SLF4J（server 日志）、`maven-shade-plugin`（可执行 jar）。
- 对设计说明书 §5 的四处实现裁决（均无上位文档冲突，详见 design.md）：① `NotifyEventBridge` 需要 `sessionId → Channel` 反向注册表（§5.1 类表未列出）；② 单连接 `maxInflightPerConnection` 限额在同步分发模型下恒不被触发，仍按设计实现计数器以保全自我保护语义；③ 日志选用 SLF4J + slf4j-simple；④ `HelloRequest.auth_token` 非空（Phase 1 要求为空）按版本校验同等处理：回 `INVALID_REQUEST` 并断连。
- 交付可执行 shade jar（`mainClass = OpenLatchServer`，§5.8）与 HELLO→ACQUIRE→RENEW→RELEASE 冒烟脚本（§13.2 P1-17）。
- 在 server 模块测试源码中提供最小协议测试客户端（Netty 驱动的 Envelope 收发夹具），支撑 §10.2 服务端行为用例与通知端到端用例；它是测试夹具，不承载任何客户端锁语义（不提前实现 M3）。

## Capabilities

### New Capabilities

- `lock-server`: OpenLatch 单节点锁服务器——服务生命周期与配置、连接与握手、请求分发与错误码映射、队首通知推送、租约到期扫描调度、断连/空闲会话清理、自我保护限额、可执行交付形态。

### Modified Capabilities

（无——`wire-protocol` 与 `core-lock-engine` 的需求不因本变更改变；M2 是协议与核心语义的服务端实现方，服务端行为用例（§10.2 中属 M2 的部分）由新能力 `lock-server` 承载。）

## Impact

- **代码**：新增 `io.github.lamspace.openlatch.server` 包族（含 `net`/`dispatch`/`session` 子包，§5.1）；`openlatch-server` 从占位 pom 变为可执行交付模块。
- **依赖**：`openlatch-server` 引入 `openlatch-core`、`openlatch-protocol`、Netty 4.1.x、protobuf-java（经 protocol 传递）、SLF4J；父 `pom.xml` 的 `dependencyManagement`/`pluginManagement` 新增对应版本锁定（沿用 M1"本地缓存优先、显式锁版本"的约束）。
- **构建产物**：新增 shade 可执行 jar（`java -jar` 直接启动，默认端口 9410，配置经 `-Dopenlatch.config=<path>`）。
- **后续里程碑**：M3 客户端 SDK（P1-18 起）将以本变更交付的服务器为集成测试对端；§10.3/§10.4 的集成与故障注入测试依赖本变更的会话清理与通知推送行为。
