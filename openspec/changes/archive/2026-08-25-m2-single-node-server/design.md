# Design: m2-single-node-server

## Context

M1 已交付 `openlatch-protocol`（Envelope 全部消息与编解码测试）与 `openlatch-core`（`CoreEngine` 门面、条目级锁并发模型、`CoreEventListener` 事件出口，四组语义测试全绿，已归档）。`openlatch-server` 目前是空占位 pom。本机环境约束沿用 M1：Java 25.0.3、Maven 3.9.16、`-s /home/lam/repo/settings.xml`、本地仓库 `/home/lam/repo`（aliyun 镜像）、构建期联网拉取已获授权；**本地仓库尚未缓存任何 Netty / SLF4J / shade 工件**，具体可用版本在首个任务中探测后锁定。

行为需求见 specs/lock-server/spec.md；动机见 proposal.md。设计说明书 §5 与 §13.2 是上位依据，本文只记录实现层面的取舍与对 §5 的四处补全裁决。

## Goals / Non-Goals

**Goals:**

- 交付 `openlatch-server`：配置 → core 组装 → Netty 服务一条链打通，`mvn verify` 全绿，shade jar 可 `java -jar` 独立启动并通过冒烟序列；
- core 语义零改动：本变更不改 `openlatch-core` 与 `openlatch-protocol` 的任何既有代码，协议 ⇄ core 的映射全部收敛在 server 层；
- 端到端路径在 M2 即有真实插座证据：通知 → 重发 → 授予、持锁断连即时释放、租约到期释放，均以真实端口测试锁定（不等 M3 客户端）。

**Non-Goals:**

- 不实现 §5.5 幂等响应回放窗口（YAGNI，沿用 M1 判定）；
- 不实现任何客户端锁语义——测试用协议客户端只是收发夹具；
- 不做 TLS/认证（Phase 3）、集群（Phase 2）、指标与监控。

## Decisions

### D1：依赖选型与版本锁定——Netty 4.1.x + SLF4J/slf4j-simple + shade

父 `pom.xml` 的 `dependencyManagement` 锁定 Netty 4.1.x（取 aliyun 镜像可得的当前稳定版，探测后定案）、`slf4j-api` 与 `slf4j-simple`；`pluginManagement` 锁定 `maven-shade-plugin`。server 模块依赖 `openlatch-core`、`openlatch-protocol`、`netty-all`（或 transport/handler/codec 三件，视探测结果取最小集）、`slf4j-api`；`slf4j-simple` 仅随 shade jar 打包（运行时日志后端）。

**备选**：JDK `System.Logger` 代替 SLF4J。弃用原因：可执行 jar 需要可控的日志输出（级别、格式），SLF4J 是 Netty 生态的事实标准，且 `slf4j-simple` 无传递依赖负担。
**备选**：Netty 4.2.x（若镜像已有）。弃用原因：设计说明书 §1.2 明确 4.1.x 当前稳定版，不引入计划外版本跳跃。

### D2：会话簿记——Channel 属性 + `sessionId` 反向注册表

`ServerSession`（持有 `sessionId`、握手状态、inflight 计数）经 `AttributeKey` 绑定到 Channel（正向：入站处理随手可取）。另设 `ServerSessionRegistry`（`ConcurrentHashMap<Long, ServerSession>`，含 Channel 反向引用）：握手成功时登记，供 `NotifyEventBridge` 按 `sessionId` 反查 Channel。这是对 §5.1 类表的必要补全——core 的 `notifyHead(sessionId, requestId, key)` 只携带 `sessionId`，无反向索引则无法落笔推送。

**备选**：让 core 的事件接口直接携带"连接句柄"。弃用原因：违背"core 不持有任何连接相关对象"（§4.3 事件模型说明）。

### D3：断连时序——先摘注册表，后清会话

`channelInactive` 的执行序：① 从 `ServerSessionRegistry` 移除该会话 → ② `core.sessionClosed(sessionId)`。先摘注册表保证：`sessionClosed` 执行期间，并发释放事件触发的 `notifyHead` 已查不到该会话，推送被静默丢弃，不会写向将死连接。反序亦正确（写失败无害），但先摘后清的失败窗口更小、语义更干净。注册表移除与 `ServerSession` 上的"已关闭"标记使清理幂等（`channelInactive` 与空闲断连可能对同一连接触发两次路径）。

### D4：inflight 限额——按设计实现，接受其在同步模型下不可触发

`ServerSession` 持 `AtomicInteger` 计数：分发入口自增并检查 `maxInflightPerConnection`（超限回 `OVERLOADED`），响应完成后递减。由于业务在单 Channel 单 IO 线程上同步执行，计数实际恒 ≤ 1，限额在生产路径不可触发——仍予实现，理由：① 设计说明书 §5.4 明确要求；② 计数逻辑可经直接调用做单元测试锁定；③ Phase 2 若引入异步处理该语义即刻生效。

**备选**：注释说明后省略。弃用原因：使规格中"超限回 `OVERLOADED`"失去实现载体，验收依赖对实现的信任而非测试。

### D5：IO 线程同步分发 + 纯函数映射层

pipeline 末端 `ServerSessionHandler` 收到解码后的 `Envelope`，直接调用 `RequestDispatcher.dispatch(session, envelope)`，同步完成 core 调用与响应写回（§5.2：短临界区、无阻塞、零线程等待者）。映射收敛为两个纯函数：`Envelope → core command` 与 `core result → Envelope`（`Outcome`/`ReleaseStatus` → `StatusCode` 查表，`AcquireResponse.lease_expires_at_ms` 由 dispatcher 以 `now + grantedLeaseMs` 补齐——core 结果只带时长，诊断用到期时刻是服务端视角的派生值）。纯函数形态使分发表可脱离 Netty 单测。

**备选**：业务线程池。弃用原因：§5.2 明确业务无阻塞，加池只增加上下文切换与背压复杂度。

### D6：扫描调度与关停序列

单线程 `ScheduledExecutorService`，`scheduleAtFixedRate(leaseTickIntervalMs)` 串行调用 `core.expireDue()` 与 `core.sweepNotifiedHeads()`；调度线程触发的 `notifyHead` 经 `channel.writeAndFlush` 写出（Netty 写线程安全，无需回 IO 线程）。`NotifyEventBridge` 内捕获写出异常并记日志，绝不让 Channel 故障回灌进扫描线程。关停序列（JVM 退出钩子与 `stop()` 共用）：`scheduler.shutdown()` → 关闭 boss/worker 组（`shutdownGracefully` 并 `awaitTermination`）→ 主端口先行 `close().sync()` 停止收新连接，顺序对齐规格"扫描先停、不再产生新通知"。

**备选**：复用 Netty `EventExecutor`/`GlobalEventExecutor` 跑扫描。弃用原因：把语义扫描与网络线程池耦合，关停序难以显式控制。

### D7：测试分层——EmbeddedChannel / 纯单元 / 真实端口 + 最小测试协议客户端

```
编解码与分帧行为（半包/粘包/超帧长断连）  → EmbeddedChannel，无端口
分发表映射、握手状态机、配置解析          → 纯单元（无 Netty 依赖）
通知端到端、断连清理、空闲超时、冒烟      → 真实端口（bind 0 取临时端口）
```

真实端口测试需要一个协议客户端：在 **server 测试源码**中实现最小夹具——Netty 驱动，支持建连、发任意 `Envelope`、同步等待指定 `request_id` 的响应、接收推送、主动断连。它不含看门狗、重连、本地簿记等任何 M3 语义（防止提前实现漂移）。

**备选**：把集成测试推迟到 M3 用正式客户端覆盖。弃用原因：M2 退出标准（P1-15/P1-17 验证列）要求通知端到端与冒烟在本里程碑有证据；且 M3 的测试需要把"服务端行为"与"客户端行为"分层定位，服务端必须先有独立可测层。

### D8：握手裁决细则

① 握手前业务请求回 `INVALID_REQUEST` 且不断连（连接仍有机会补发 HELLO）；② 版本 ≠ 1 与非空 `auth_token` 均回 `INVALID_REQUEST` 后断连（对齐 §3.2.1 版本校验的既有处理，auth_token 违反"必须为空"属同类协议违规）；③ 重复 HELLO 回 `INVALID_REQUEST` 不断连（原会话保持）；④ HELLO 自身的 `request_id` 按常规回显。

## Risks / Trade-offs

- **Netty 4.1.x × Java 25 兼容性未在本机验证过** → 首个任务即完成依赖解析与最小启动验证（P1-11 内含），若 4.1 稳定版与 Java 25 存在冲突，在同一任务内升级 4.1 补丁版本解决，并记录于本文修订。
- **本地仓库无 Netty/SLF4J/shade 缓存，版本存在镜像可得性不确定** → 探测步骤放在任务清单第一步，版本定案后才写锁；拉取已获授权（M1 先例）。
- **真实端口测试的稳定性**（端口竞争、时序抖动）→ 一律 `bind(0)` 取临时端口；等待断言用带超时的轮询而非固定 `sleep`；空闲测试用可配置的短时限（如 1s）而非默认 60s。
- **扫描线程与 IO 线程并发写同一 Channel** → Netty `writeAndFlush` 线程安全；桥内捕获所有写出异常，日志降级处理（规格"静默丢弃"）。
- **shade jar 与模块 jar 并存的构件管理** → shade 绑定 `package` 阶段产出主构件即可（本模块无下游依赖者，不需要 `shade:reduced` 之外的原构件保留策略）。

## Migration Plan

不适用——`openlatch-server` 从空占位起步，无存量行为可破坏；既有 `mvn verify` 链路只增不改。

## Open Questions

无阻塞项。Netty 与插件的确切版本号属采购细节，在任务 1 探测后直接锁定，不影响规格与任务结构。
