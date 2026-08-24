# Proposal: m1-protocol-core

## Why

OpenLatch Phase 1（MVP）的实施计划以里程碑 M1 起步：先交付线路协议与纯 Java 锁语义核心。协议是后续所有模块（server / client / starter）的契约；锁语义核心（状态机、等待队列、租约、会话）是全系统正确性的地基，且按概要设计要求必须能在无网络、无协议依赖的条件下闭环测试（概要设计 §11 风险 5）。本变更完成实施计划里程碑 M1 的全部子任务（设计说明书 §13.1 P1-01 ~ P1-10），为 M2 单节点服务器提供可直接组装的两个底座模块。

## What Changes

- 将仓库根 `pom.xml` 改造为六模块聚合父工程（`packaging=pom`），建立设计说明书 §2 规定的模块依赖关系；M1 阶段 `openlatch-protocol` 与 `openlatch-core` 有实质内容，其余四模块为占位。
- 删除根目录下遗留的空 `src/` 目录树（`src/main/java`、`src/main/resources`、`src/test/java`）。
- 新增 `openlatch-protocol` 模块：按设计说明书 §3.2 编写 `.proto` 定义（Envelope、6 个 MessageType、4 个 LockType、12 个 StatusCode、9 个消息类型），配置构建期 protoc 代码生成，并提供编解码测试（round-trip、未知字段容忍）。
- 新增 `openlatch-core` 模块：纯 Java、零外部依赖的锁语义引擎，覆盖设计说明书 §4 全部内容——四种锁类型判定、严格 FIFO 等待队列与队首通知、租约最小堆与到期强制释放、会话登记与断连清理、`(sessionId, requestId)` 幂等去重、保护限额。
- 对设计说明书 §4.2 做一处已确认的微小修正：`AcquireCommand` 增加 `boolean queueIfBusy` 字段，使规则 6 的 `DENIED`（立即式失败）判定在 core 层闭环（core 仍不感知等待时限）。
- 插件与依赖版本在父 `pom.xml` 中显式锁定：protobuf-java 3.25.5、protobuf-maven-plugin、os-maven-plugin、JUnit Jupiter 5.11.4、AssertJ 3.27.7，以及本机已缓存的构建插件版本。

## Capabilities

### New Capabilities

- `wire-protocol`: OpenLatch 线路协议——Protobuf 消息定义、帧格式约定、协议版本规则与编解码正确性（含未知字段容忍）。
- `core-lock-engine`: 纯 Java 锁语义引擎——锁获取/释放/续租判定、可重入与读写锁、FIFO 等待队列与队首通知、租约到期强制释放、会话生命周期清理、幂等去重与保护限额。

### Modified Capabilities

（无——`openspec/specs/` 目前为空，本变更全部为新增能力。）

## Impact

- **构建结构**：根 `pom.xml` 从单模块 jar 变为聚合父工程；新增 6 个模块目录；根 `src/` 空目录树移除。
- **代码**：新增 `io.github.lamspace.openlatch.protocol`（生成代码）与 `io.github.lamspace.openlatch.core` 两个包族。
- **依赖**：引入 `com.google.protobuf:protobuf-java:3.25.5`（仅 protocol 模块）；构建期引入 `protobuf-maven-plugin` 与 protoc 二进制（经 aliyun 镜像，用户已授权联网拉取）；测试依赖 JUnit 5 + AssertJ（本机已缓存）。`openlatch-core` 保持零依赖（验收条件之一）。
- **后续里程碑**：M2（P1-11 起）将直接依赖本变更交付的 protocol 生成代码与 `CoreEngine` 门面；core 的 `CoreEventListener` 契约将由 M2 的 `NotifyEventBridge` 实现。
