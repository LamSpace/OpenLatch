# OpenLatch

[English](README.md) | **简体中文**

[![build](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml)
[![drill](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml?query=branch%3Amaster)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-25-orange)](#要求与兼容性)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.x-green)](#4-spring-boot-声明式starter)
[![Protocol](https://img.shields.io/badge/protocol-v3-lightgrey)](#要求与兼容性)

OpenLatch 是一个轻量级分布式锁服务。它提供 JUC 风格的协调原语——可重入锁 / 简单锁 / 读写锁 /
公平锁、分布式**信号量（Semaphore）**与分布式**倒计数屏障（CountDownLatch）**——后端可以是
**Raft 集群**（Apache Ratis 复制状态机，含快照）或**单节点**，经由 Netty 长连接上的 Protobuf
线路协议通信。租约 + 客户端看门狗续租、等待-通知-重发的 **FIFO 公平队列**、Leader 感知路由
（改道提示 + 种子发现）、Spring Boot 4 声明式注解 `@OpenLatch`、只读 Web **管理控制台**、
Prometheus 风格的**指标与健康端点**、可选的 **TLS / mTLS 与 Token 认证**共同构成完整平台。

锁是*协调手段，不是共识*：锁带租约、可能被剥夺——关键区必须处理失锁回调
（见[语义与警示](#语义与警示)）。

## 架构

```
                        ┌─────────────────────────── Raft 集群（Apache Ratis） ─────────────────────────────┐
   OpenLatchClient ◀────┤   node1 :9410        node2 :9410        node3 :9410                               │
   可连任意节点，        │        │  ◀────── 日志复制 ──────────▶   │        │                                │
   Leader 感知路由：     │        │        （raft 端口 :9411…）      │        │                                │
   HELLO 提示、          │        ▼                                 ▼        ▼                                │
   NOT_LEADER 改道、     │   metrics :9412                      metrics :9412  …  （/metrics、/healthz）      │
   种子发现              │        ▲                                  ▲                                       │
   Spring Boot starter ◀┤        │ admin（只读，令牌鉴权）            │                                       │
   @OpenLatch           │   openlatch-console :9413 ──────────────────┘ （经 v3 ADMIN_* 查询业务端口 +        │
                        │                                                 抓取 /metrics 迷你图）              │
                        └─────────────────────────────────────────────────────────────────────────────────────┘
```

## 模块

| 模块 | 说明 |
|---|---|
| `openlatch-protocol` | `.proto` 协议定义与编解码 |
| `openlatch-core` | 纯 Java 锁语义核心（状态机 / 等待队列 / 租约 / 会话） |
| `openlatch-server` | Netty 服务器，单节点或 Raft 集群（可执行 jar） |
| `openlatch-client` | 客户端 SDK（异步内核 + JUC 风格同步包装 + 看门狗 + 重连 + Leader 感知路由） |
| `openlatch-spring-boot-starter` | Spring Boot 4 自动装配 + `@OpenLatch` 注解与切面 |
| `openlatch-console` | 只读管理控制台（Web，`ADMIN_*` 协议） |
| `openlatch-examples` | 示例与基准 harness（不发布） |

## 要求与兼容性

| 项 | 支持情况 |
|---|---|
| Java | 全库 **25**（所有构件以 `release=25` 编译） |
| Spring Boot | **仅 4.x。** starter 以 Boot 4 编译并依赖 Boot 4 独有构件（如 `spring-boot-starter-aspectj`），**不兼容** Boot 3.x；Boot 3 应用可手动装配 `openlatch-client`，官方不提供 Boot 3 starter |
| 线路协议 | v1 / v2 / v3，HELLO 握手协商（支持区间 `[1,3]`）；区间外拒绝，不做隐式兼容 |
| 构件 | 尚未发布 Maven Central——先本地 `mvn clean install` |
| 默认端口 | 业务 **9410**（可选 TLS/令牌）、Raft **9411**、指标管理 **9412**（`/metrics`、`/healthz`，明文）、控制台 **9413**（明文） |

## 构建

要求 **Java 25**。本地构建安装后按下述坐标依赖：

```bash
mvn clean verify        # 单测、进程内 IT、javadoc 门禁
mvn clean install       # + 本地仓库安装
```

进程级故障演练（杀进程 / 滚动重启 / netns 真分区，需特权）单独经
`mvn verify -Pdrill` 执行——nightly 与手动运行在 CI 上进行，报告以 run artifacts
交付，不入仓库。

## Quick Start

### 1. 启动服务器（单节点）

```bash
java -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar
# 监听 9410；默认开放指标管理端口 9412（/metrics + /healthz）；
# -Dopenlatch.config=<properties-file> 覆盖任意配置
```

### 2. 启动三节点集群

每节点一份 properties（运维全貌——部署、重启顺序、故障转移行为——见
[《集群部署与故障转移指南》](docs/guide/zh/OpenLatch-Phase2-集群部署与故障转移.md)）：

```properties
# node1.properties — 逐节点重复：node-id / raft-port / data-dir 唯一
openlatch.server.port=9410
openlatch.cluster.enabled=true
openlatch.cluster.node-id=1
openlatch.cluster.raft-port=9501
openlatch.cluster.peers=1@127.0.0.1:9501,2@127.0.0.1:9502,3@127.0.0.1:9503
openlatch.cluster.client-addresses=1@127.0.0.1:9410,2@127.0.0.1:9420,3@127.0.0.1:9430
openlatch.cluster.data-dir=/var/lib/openlatch/node-1
java -Dopenlatch.config=node1.properties -jar openlatch-server-1.0-SNAPSHOT-executable.jar
```

客户端可连**任意**节点：非 Leader 节点以 `NOT_LEADER` + Leader 提示应答，
种子发现负责改道。

### 3. 编程式使用（客户端 SDK）

```xml
<dependency>
    <groupId>io.github.lamspace</groupId>
    <artifactId>openlatch-client</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

```java
try (OpenLatchClient client = OpenLatchClient.builder()
        .address("127.0.0.1:9410")
        .build()) {
    client.connectAsync().join();          // 可选：等待首次连接
    OLock lock = client.newReentrantLock("order:123");
    lock.lock();
    try {
        // 关键区
    } finally {
        lock.unlock();
    }
}
```

除 `OLock`（可重入 / 简单 / 读写 / 公平）外，SDK 另提供 `OSemaphore`（N 许可共享门闸）
与 `OCountDownLatch`（一次性倒计数屏障），共享同一套租约 / 看门狗 / 失锁语义。

### 4. Spring Boot 声明式（starter）

```xml
<dependency>
    <groupId>io.github.lamspace</groupId>
    <artifactId>openlatch-spring-boot-starter</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

构建**必须**开启 `-parameters`（SpEL 需解析方法参数名）：

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <parameters>true</parameters>
    </configuration>
</plugin>
```

```java
@Service
public class OrderService {

    @OpenLatch(key = "#orderId")                       // SpEL：以入参作锁键
    public void createOrder(String orderId) { ... }

    @OpenLatch(key = "report", waitTime = 3, leaseTime = 60)  // 限时等待、自定义租约
    @Transactional
    public Order settle(long report) { ... }           // 锁在事务外层
}
```

加依赖、打注解即可：`OpenLatchClient` bean 自动装配，容器关闭时优雅停机
（尽力归还所持锁）。

## 示例

每个示例自包含（进程内嵌服务器 + 临时端口——仅为演示夹具，生产请独立部署服务器）：

```bash
mvn -pl openlatch-examples compile exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.QuickStartExample
# 另有：ConcurrencyExample / ReadWriteExample / WatchdogExample /
# SpringAnnotationExample / BenchmarkMain（约 60s，产出基线报告）
```

## 管理控制台

`openlatch-console` 是独立部署的**只读** Web 控制台（默认 HTTP `9413`）。
它经 v3 `ADMIN_*` 协议在节点**业务端口**上逐项查询，凭服务端
`openlatch.server.admin.token` 鉴权；总览页并抓取各节点 `/metrics` 端口绘制迷你图。
控制台不提供写操作（无强制解锁 / 踢会话）。节点开启 TLS/业务认证时，控制台按自身
TLS + 业务令牌配置连接；握手被拒的节点呈现为认证失败/降级节点（绝不静默明文探测）。

```bash
java -Dopenlatch.console.config=/path/to/console.properties \
     -jar openlatch-console/target/openlatch-console-1.0-SNAPSHOT-executable.jar
```

配置键（模板见 `openlatch-console/console.properties.example`）：

| 键 | 默认 | 说明 |
|---|---|---|
| `openlatch.console.server-addresses` | *(必填)* | 逗号分隔 `host:port`，指向节点**业务端口** |
| `openlatch.console.admin-token` | *(必填)* | 须等于服务端 `openlatch.server.admin.token`；不符则页面降级为鉴权提示 |
| `openlatch.console.auth-token` | — | HELLO 携带的业务令牌（节点开启业务认证时必填） |
| `openlatch.console.tls-enabled` | `false` | 对节点启用 TLS |
| `openlatch.console.tls-trust-store` | — | 节点证书的 PEM CA 信任锚 |
| `openlatch.console.tls-client-cert` / `tls-client-key` | — | mTLS 客户端证书/私钥（成对） |
| `openlatch.console.port` | `9413` | 控制台 HTTP 端口 |
| `openlatch.console.refresh-interval-seconds` | `5` | 页面轮询间隔 |
| `openlatch.console.metrics-port` | `9412` | 总览迷你图所用的节点指标端口 |
| `openlatch.console.request-timeout-ms` | `5000` | 单次管理请求超时 |

## 安全（TLS 与 Token 认证）

TLS 与业务令牌认证保护**客户端接入的业务端口**（业务与 `ADMIN_*` 消息共用该端口）。
两项能力默认关闭——缺省行为与明文基线完全一致。

**服务端 TLS**（`-Dopenlatch.config=<path>`）：

| 键 | 默认 | 说明 |
|---|---|---|
| `openlatch.server.tls.enabled` | `false` | 业务端口启用 TLS；明文连接被拒，握手超时 5s |
| `openlatch.server.tls.cert` / `tls.key` | — | 服务端 PEM 证书 / 私钥（启用时二者必填） |
| `openlatch.server.tls.trust-store` | — | 校验客户端证书的 PEM CA（mTLS） |
| `openlatch.server.tls.require-client-cert` | `false` | 要求并校验客户端证书（mTLS） |

证书更新需重启生效（无热加载）。

**业务令牌认证**（HELLO `auth_token`）：

| 键 | 默认 | 说明 |
|---|---|---|
| `openlatch.server.auth.enabled` | `false` | 关闭（默认）时兼容守卫保持：非空 `auth_token` 即被拒；开启时令牌须命中 `tokens` 之一 |
| `openlatch.server.auth.tokens` | — | 逗号分隔令牌列表（开启时 ≥1，令牌内不得含逗号）；轮换期新旧令牌并存 |

拒绝响应不可区分（`INVALID_REQUEST` + 断连，常量时间比较，不泄露原因）；认证一次性完成于
握手、先于任何会话分配/登记与 `SESSION_OPEN` 复制（单机与集群同一门闩）。管理令牌
（`admin-token`，逐消息承载）与业务令牌严格独立。客户端 / starter / 控制台以同名的
`tls-enabled`、`tls-trust-store`、`tls-client-cert`/`tls-client-key`、`auth-token` 配置面消费。

**令牌轮换流程**——先服务端、后客户端、再摘旧令牌：
1. 服务端 `openlatch.server.auth.tokens` 加入新令牌并重启（新旧双活）；
2. 客户端 / 控制台切换新 `auth-token` 并重启；
3. 服务端移除旧令牌并重启。

> 注意：开启业务认证会拒绝一切不持有效令牌的客户端（含旧版客户端）——客户端须随服务端变更同步滚动。

**加密边界**——仅业务端口在范围内。Raft 节点间通道（Ratis）、指标 HTTP 端口 `9412`、
控制台自身 Web `9413` 保持明文，请置于内网/网络隔离之后。不宣称全链路端到端 TLS。

## 配置参考

### 服务器（Properties，`-Dopenlatch.config=<path>`）

| 键 | 默认 | 说明 |
|---|---|---|
| `openlatch.server.port` | `9410` | 监听端口 |
| `openlatch.server.worker-threads` | `2 × CPU` | Netty worker 线程数 |
| `openlatch.server.session.idle-timeout-ms` | `60000` | 连接空闲超时 |
| `openlatch.server.lease.default-ms` | `30000` | 默认租约 |
| `openlatch.server.lease.min-ms` / `max-ms` | `1000` / `3600000` | 租约钳制区间 |
| `openlatch.server.lease.tick-interval-ms` | `500` | 到期扫描间隔 |
| `openlatch.server.queue.head-reply-timeout-ms` | `5000` | 通知后的队头应答超时（放弃的等待者最多占据 FIFO 队头这么长时间） |
| `openlatch.server.limit.max-key-length` | `512` | 键长上限（字节） |
| `openlatch.server.limit.max-queue-depth-per-key` | `4096` | 单键队列深度上限 |
| `openlatch.server.limit.max-inflight-per-connection` | `1024` | 单连接在途请求上限 |
| `openlatch.server.metrics.enabled` | `true` | 指标管理端点（Prometheus 抓取 `http://<host>:<port>/metrics`） |
| `openlatch.server.metrics.port` | `9412` | 指标管理端口（`0` = 临时端口）；绑定冲突即启动失败——同机多节点须逐节点配互异端口或 `0` |
| `openlatch.server.admin.token` | *(未配置)* | 只读 `ADMIN_*` 协议（控制台）的管理令牌；未配置 ⇒ 一切管理请求被拒 |

集群模式另有 `openlatch.cluster.*`（`enabled` / `node-id` / `peers` / `raft-port` /
`client-addresses` / `data-dir` / `election-timeout-ms` / `snapshot-threshold`）——
见[集群指南](docs/guide/zh/OpenLatch-Phase2-集群部署与故障转移.md)。

### 客户端（`OpenLatchClient.builder()`）

| 参数 | 默认 | 说明 |
|---|---|---|
| `address` | 必填 | `host:port` |
| `requestTimeout` | 5s | 单请求超时 |
| `defaultWaitTimeout` | 30s | `lock()` 总兜底 |
| `connectTimeout` | 3s | TCP + 握手超时 |
| `reconnectInitialBackoff` / `reconnectMaxBackoff` | 200ms / 10s | 指数退避 |
| `workerThreads` | 1 | 客户端 Netty EventLoop 线程数 |
| `meterRegistry` | 默认关闭 | 注入宿主 Micrometer `MeterRegistry` 即启用客户端指标（宿主自备 `micrometer-core`，不传递） |

### starter（`application.yaml`）

| 属性 | 默认 | 说明 |
|---|---|---|
| `openlatch.enabled` | `true` | 置 false 注解失效（客户端 bean 仍创建） |
| `openlatch.server-host` / `server-port` | `127.0.0.1` / `9410` | 服务端地址 |
| `openlatch.request-timeout` | `5s` | 时长 |
| `openlatch.default-wait-timeout` | `30s` | `lock()` 兜底 |
| `openlatch.reconnect-initial-backoff` / `reconnect-max-backoff` | `200ms` / `10s` | 退避 |

## 语义与警示

- **租约必到期**：每次授予携带服务端租约（默认 30s，钳制区间内）；未续租的锁保证被回收。客户端看门狗按 `lease/3` 续租（自定义 `leaseTime` 的注解锁同样受保护）。
- **锁可能被剥夺**：断连或租约到期即失锁；客户端经 `LockLostListener`（全局或按锁）通知。锁下写关键状态的业务**必须**处理回调（如中止提交并告警）——锁是协调，不是共识。
- **无界阻塞不存在**：同步调用始终带总超时兜底（默认 30s），超时抛 `LockAcquisitionTimeoutException`。
- **`SIMPLE` 非可重入**：同线程持有时再次获取会排到自己的队、直至租约到期。这是语义，不是 bug。
- **无升降级特判**：读→写、写→读均不实现特判，一律通用排队规则。
- **FIFO 公平**：严格按到达顺序，仅通知队头（无惊群）。队头处已放弃的等待者最多占据一个队头应答超时窗（默认 5s）。
- **锁在事务外层**：同方法 `@OpenLatch` + `@Transactional` 时，锁先于事务开启获取、提交之后释放（自动化测试断言）。
- **Spring AOP 自调用**：`this.method()` 绕过代理，注解不生效——与 `@Transactional` 同款标准局限。
- **重启语义**：*单机*模式为内存锁——重启后所有锁消失（客户端超时→重连→重新竞争；无幽灵锁）。*集群*模式经 Raft 日志 + 快照复制——节点重启回队并恢复状态；授予跨 Leader 迁移与滚动重启存续（切换窗口的瞬态错误属预期，演练如实报告）。
- **异步回调线程**：`acquireAsync`/`releaseAsync` 的 future 在网络线程完成——链式回调内不得阻塞。失锁回调走专用单线程，同律。

## 已知局限

1. 读热点下读者逐个放行——批量授予为暂缓项（评估后未采纳：与 FairLock 顺序回归套件冲突）；
2. 放弃等待不主动撤销队列席位，经队头应答超时回收；
3. `waitTime > 0` 为客户端侧计时，时钟回拨可能轻微延长等待；
4. CountDownLatch 条目一旦定型即存续至节点重启，参与者散尽不回收；废弃屏障以键命名约定管理（每轮一个轮次标识）；
5. 集群 Semaphore 许可池回收后，纯加入者的显式获取被拒绝（既定规格分支，返回明确错误码）。

## 基准基线

基线 harness 是防退化参考，不作发布门槛。本地复跑：

```bash
mvn -pl openlatch-examples exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.BenchmarkMain
# 产出 target/benchmark/benchmark-baseline-<date>.md（-Dbenchmark.output=<path> 可覆盖）
```

CI 定期运行的报告以
[workflow run artifacts](https://github.com/LamSpace/OpenLatch/actions/workflows/benchmark.yml) 交付。

## 文档

- **用户指南**（[docs/guide/](docs/guide/)）：现有
  [集群部署与故障转移指南（zh）](docs/guide/zh/OpenLatch-Phase2-集群部署与故障转移.md)；
  双语完整指南体系建设中。
- **设计与规划**（内部材料，中文）：
  [概要设计](docs/design/OpenLatch-概要设计说明书.md) ·
  [Phase 1 详设](docs/design/OpenLatch-Phase1-详细设计说明书.md) ·
  [Phase 2 详设](docs/design/OpenLatch-Phase2-详细设计说明书.md) ·
  [Phase 3 详设](docs/design/OpenLatch-Phase3-详细设计说明书.md) ·
  [总体实施计划](docs/design/OpenLatch-总体实施计划与验证方案.md) ·
  [Raft 选型报告](docs/design/raft-selection-report.md)
- **质量报告**（内部）：[Phase 1](docs/quality/Phase1-验收报告.md) ·
  [Phase 2](docs/quality/Phase2-验收报告.md) · [Phase 3](docs/quality/Phase3-验收报告.md)

## 许可

[Apache License 2.0](LICENSE)
