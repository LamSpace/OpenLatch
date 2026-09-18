# 🔒 OpenLatch

[English](README.md) | **简体中文**

[![build](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml)
[![drill](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml?query=branch%3Amaster)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-25-orange)](#-要求)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.x-green)](docs/guide/zh/04-spring-boot-starter.md)
[![Protocol](https://img.shields.io/badge/protocol-v3-lightgrey)](docs/guide/zh/10-compatibility.md)

OpenLatch 是一个轻量级分布式锁服务：JUC 风格协调原语（可重入 / 简单 / 读写 / 公平锁、
信号量、倒计数屏障、带每 key 版本戳的原子 long/int/boolean），后端为 **Raft 集群**（Apache Ratis，含快照）或**单节点**；
Netty 长连接上的 Protobuf 协议，租约 + 看门狗续租、等待-通知-重发的 **FIFO 公平队列**；
Spring Boot 4 声明式注解 `@OpenLatch`、只读 Web **管理控制台**、Prometheus **指标与健康
端点**、可选 **TLS/mTLS 与令牌认证**。

**锁是协调，不是共识**——锁带租约、会被剥夺；所有关键路径都要实现失锁回调。
完整语义见下方指南。

## 🏗️ 架构

```
                        ┌─────────────────────────── Raft 集群（Apache Ratis） ─────────────────────────────┐
   OpenLatchClient ◀────┤   node1 :9410        node2 :9410        node3 :9410                              │
   可连任意节点，         │        │  ◀────── 日志复制 ─────────▶    │        │                               │
   Leader 感知路由        │        │       （raft 端口 :9411…）       │        │                               │
   （HELLO 提示、          │        ▼                                ▼        ▼                               │
   NOT_LEADER 改道、      │   metrics :9412                     metrics :9412  …   （/metrics、/healthz）    │
   种子发现）             │        ▲                                  ▲                                     │
                        │        │ admin（只读，令牌鉴权）             │                                     │
   Spring Boot ◀────────┤   openlatch-console :9413 ──────────────────┘   （v3 ADMIN_* + /metrics 抓取）    │
   @OpenLatch            └──────────────────────────────────────────────────────────────────────────────────┘
```

## 📦 模块

| 模块 | 说明 |
|---|---|
| `openlatch-protocol` | `.proto` 协议定义与编解码 |
| `openlatch-core` | 纯 Java 锁语义核心（状态机 / 等待队列 / 租约 / 会话） |
| `openlatch-server` | Netty 服务器，单节点或 Raft 集群（可执行 jar） |
| `openlatch-client` | 客户端 SDK（异步内核 + JUC 同步包装 + 看门狗 + 重连 + Leader 感知路由） |
| `openlatch-spring-boot-starter` | Spring Boot 4 自动装配、`@OpenLatch` 注解与切面 |
| `openlatch-console` | 只读管理控制台（Web，`ADMIN_*` 协议） |
| `openlatch-examples` | 示例与基准 harness（不发布） |

## ⚙️ 要求

| 项 | 支持 |
|---|---|
| Java | **25**（全部构件 `release=25`） |
| Spring Boot starter | **仅 4.x**（依赖 Boot 4 独有构件）；Boot 3.x 请手动装配 SDK |
| 线路协议 | v1 / v2 / v3，HELLO 协商区间 `[1,3]`，不做隐式兼容 |
| 构件 | 未发布 Maven Central——先 `mvn clean install` |

## 🚀 快速上手

```bash
mvn clean install -DskipTests                                   # 本地构建安装
java -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar   # 单节点 :9410
```

```java
try (OpenLatchClient client = OpenLatchClient.builder().address("127.0.0.1:9410").build()) {
    client.connectAsync().join();
    OLock lock = client.newReentrantLock("order:123");
    lock.lock();
    try {
        // 关键区——锁是租约不是权利：处理失锁回调
    } finally {
        lock.unlock();
    }
}
```

Spring Boot：引入 starter、编译器开启 `-parameters`、标注 `@OpenLatch(key = "#orderId")`
——三步。集群三节点、令牌轮换、TLS、控制台部署、指标、排障：
**逐篇见[用户指南](docs/guide/zh/index.md)**。

## 🔑 必知语义

- **租约会到期，看门狗按 `lease/3` 续租**；未续持的锁必被回收。
- **锁会被剥夺**（断连 / 到期 / failover 回滚）——实现 `LockLostListener` 并在回调中中止在途提交。
- **无界阻塞不存在**：`lock()` 有总超时兜底（默认 30s）。
- **FIFO 公平**：严格到达序、只通知队头（无惊群）；以单个 Leader 任期为界——切换后队列重排。
- **重启语义**：单机=内存（重启全释放）；集群=Raft 日志+快照（授予跨 Leader 迁移与滚动重启存续）。

## 🚧 已知局限

1. 读热点下读者逐个放行（批量授予经评估暂缓）；
2. 放弃等待经队头应答超时回收席位，不即时撤销；
3. `waitTime > 0` 为客户端计时，时钟回拨可能轻微延长等待；
4. 屏障条目存续至节点重启——按轮次命名键隔离；
5. 集群信号量许可池回收后，纯加入者被显式拒绝。

## 📚 文档

- **用户指南**：[简体中文](docs/guide/zh/index.md) | [English](docs/guide/en/index.md)
  ——概念、上手、SDK、starter、集群运维、安全、控制台、可观测、排障、兼容性、术语表。
- **设计与质量**（内部材料，中文）：[design/](docs/design/) · [quality/](docs/quality/)

## 💡 示例

```bash
mvn -pl openlatch-examples compile exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.QuickStartExample
# ConcurrencyExample / ReadWriteExample / WatchdogExample / SpringAnnotationExample / BenchmarkMain
```

## 📜 许可

[Apache License 2.0](LICENSE)
