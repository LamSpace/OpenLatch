# Proposal: add-client-sdk

## Why

M1（协议 + 纯 Java 核心）与 M2（单节点服务器）已交付，服务端锁能力已可通过线路协议访问，但目前只有测试用的裸协议客户端，没有面向应用的可复用 SDK。M3 交付 `openlatch-client`，是 M4（Spring Boot Starter）与用户接入的前置，也是概要设计 §4.3 成功标准 2、3（故障可见、无死等）的最终落点。

## What Changes

- 实现 `openlatch-client` 模块（对应详设 §6、子任务 P1-18~P1-26）：
  - 公开 API：`OpenLatchClient`（builder、`acquireAsync`/`releaseAsync`、`newReentrantLock`/`newSimpleLock`/`newReadWriteLock`、锁丢失监听、`shutdown`）与 JUC 风格同步包装 `OLock`/`OReadWriteLock`；
  - 连接与重连状态机（HELLO 握手、指数退避重连、断连快速失败、`lostAt` 锁丢失裁决）；
  - 单连接请求多路复用（`request_id` 关联 future、每请求超时）；
  - 等待跟踪（QUEUED 挂起、`AWAIT_NOTIFY` 同 id 重发、重复授予补偿归还）；
  - 看门狗续租（`lease/3` 周期、失败判定、锁丢失回调）；
  - 本地持锁簿记（归属记录不记计数，重入计数以服务端为准）。
- 补全模块依赖（netty + openlatch-protocol，不依赖 core）与配套集成测试（§10.3）与故障注入测试（§10.4）。

## Capabilities

### New Capabilities

- `client-sdk`: 客户端 SDK 的可观察行为契约——公开 API 语义（同步/异步获取、限时等待、解锁守卫）、请求超时与无死等保证、等待-通知-重发闭环、看门狗续租与锁丢失通知、断连重连与持锁簿记的正确性。

### Modified Capabilities

（无。协议与服务端需求已由 `wire-protocol`、`lock-server`、`core-lock-engine` 规格覆盖：幂等去重、队首响应超时回收、断连清理均已在服务端规格中，客户端仅作为其行为的一方参与者，不改变服务端契约。）

## Impact

- **代码**：`openlatch-client` 从占位 pom 变为完整模块；新增 `io.github.lamspace.openlatch.client` 与 `.client.internal` 包。
- **依赖**：client pom 引入 netty、`openlatch-protocol`；不依赖 `openlatch-core`（语义裁决全在服务端，防止双份语义漂移）。
- **测试**：新增单元测试 + 依赖 M2 可执行 jar/进程内服务器的集成与故障注入套件（§10.3/§10.4）。
- **下游**：M4 的 `openlatch-spring-boot-starter` 依赖本模块公开 API，API 签名以详设 §6.3 为准冻结。
