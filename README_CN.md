# OpenLatch

[English](README.md) | **简体中文**

OpenLatch 是一个轻量级分布式锁服务（Phase 1 / MVP）：单节点内存锁、租约 + 看门狗续租、等待-通知-重发的 FIFO 公平队列、Netty 长连接 + Protobuf 协议，并提供 JUC 风格客户端 SDK 与 Spring Boot 声明式注解 `@OpenLatch`。

**Phase 1 不包含**：集群/ Raft（Phase 2）、FairLock/Semaphore/CountDownLatch、监控控制台、TLS 与认证（Phase 3）。协议为后续能力预留字段。

> Phase 2（Raft 集群）正在分阶段交付（S1 选型 / S2 复制状态机 / S3 Leader 发现与故障转移已完成，S4 快照与容错进行中）；集群部署与客户端故障转移行为见 [《Phase 2 集群部署与故障转移指南》](docs/OpenLatch-Phase2-集群部署与故障转移.md)。

## 模块

| 模块 | 说明 |
|---|---|
| `openlatch-protocol` | `.proto` 协议定义与编解码 |
| `openlatch-core` | 纯 Java 锁语义核心（状态机/等待队列/租约/会话） |
| `openlatch-server` | Netty 单节点服务器（可执行 jar） |
| `openlatch-client` | 客户端 SDK（异步内核 + JUC 风格同步包装 + 看门狗 + 重连） |
| `openlatch-spring-boot-starter` | Spring Boot 4 自动装配 + `@OpenLatch` 注解与切面 |
| `openlatch-examples` | 示例与基准 harness（不发布） |

## 构建

要求 **Java 25**。Phase 1 制品尚未发布到 Maven Central，请先本地安装：

```bash
mvn clean install
```

## Quick Start

### 1. 启动服务器

```bash
java -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar
# 默认监听 9410；-Dopenlatch.config=<path> 指定 Properties 配置文件
```

### 2. 编程式使用（客户端 SDK）

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
    client.connectAsync().join();          // 可选：等待首连就绪
    OLock lock = client.newReentrantLock("order:123");
    lock.lock();
    try {
        // 临界区
    } finally {
        lock.unlock();
    }
}
```

### 3. Spring Boot 声明式（starter）

> **适用版本**：Spring Boot **4.0.x+**（Java 25，见详设 §8.4 定案）。starter 按 Boot 4 编译，不向下兼容 Boot 3 应用。

```xml
<dependency>
    <groupId>io.github.lamspace</groupId>
    <artifactId>openlatch-spring-boot-starter</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

**必须**为工程开启 `-parameters` 编译选项（SpEL 按参数名求值依赖它）：

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

    @OpenLatch(key = "#orderId")                       // SpEL 取参数为锁键
    public void createOrder(String orderId) { ... }

    @OpenLatch(key = "report", waitTime = 3, leaseTime = 60)  // 限时等待 3s、请求租约 60s
    @Transactional
    public Order settle(long report) { ... }           // 锁在事务外层：提交后才释放
}
```

仅加依赖与注解即可使用：`OpenLatchClient` Bean 自动装配，上下文关闭时自动优雅关停（尽力释放持锁）。

## 示例

每个示例自包含（进程内内嵌服务器，临时端口；这是演示夹具，生产请独立部署服务器）：

```bash
mvn -pl openlatch-examples compile exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.QuickStartExample
# 同法运行 ConcurrencyExample / ReadWriteExample / WatchdogExample /
# SpringAnnotationExample / BenchmarkMain（约 60s，产出基线报告）
```

## 配置参考

### 服务器（Properties，`-Dopenlatch.config=<path>`）

| 配置键 | 默认值 | 说明 |
|---|---|---|
| `openlatch.server.port` | `9410` | 监听端口 |
| `openlatch.server.worker-threads` | `2 × CPU` | Netty Worker 线程数 |
| `openlatch.server.session.idle-timeout-ms` | `60000` | 连接空闲断开 |
| `openlatch.server.lease.default-ms` | `30000` | 默认租约 |
| `openlatch.server.lease.min-ms` / `max-ms` | `1000` / `3600000` | 租约钳制区间 |
| `openlatch.server.lease.tick-interval-ms` | `500` | 到期扫描周期 |
| `openlatch.server.queue.head-reply-timeout-ms` | `5000` | 队首收到通知后的重发响应时限 |
| `openlatch.server.limit.max-key-length` | `512` | key 最大字节数 |
| `openlatch.server.limit.max-queue-depth-per-key` | `4096` | 单 key 等待队列上限 |
| `openlatch.server.limit.max-inflight-per-connection` | `1024` | 单连接未完成请求上限 |

### 客户端（`OpenLatchClient.builder()`）

| 参数 | 默认值 | 说明 |
|---|---|---|
| `address` | 必填 | `host:port` |
| `requestTimeout` | 5s | 单个请求超时 |
| `defaultWaitTimeout` | 30s | `lock()` 总超时兜底 |
| `connectTimeout` | 3s | TCP + 握手超时 |
| `reconnectInitialBackoff` / `reconnectMaxBackoff` | 200ms / 10s | 指数退避 |
| `workerThreads` | 1 | 客户端 Netty EventLoop 线程数 |

### starter（`application.yaml`）

| 属性 | 默认 | 说明 |
|---|---|---|
| `openlatch.enabled` | `true` | 关闭后注解不生效（客户端 Bean 仍装配） |
| `openlatch.server-host` / `server-port` | `127.0.0.1` / `9410` | 服务地址 |
| `openlatch.request-timeout` | `5s` | Duration |
| `openlatch.default-wait-timeout` | `30s` | `lock()` 兜底 |
| `openlatch.reconnect-initial-backoff` / `reconnect-max-backoff` | `200ms` / `10s` | 退避 |

## 语义与警示

- **租约必到期**：所有锁带服务端租约（默认 30s，钳制区间内）；不续租的锁必然在到期后被回收。看门狗按 `lease/3` 自动续租（`leaseTime` 自定义的注解锁同样受保护）。
- **锁可能丢失**：断连、租约失效等情况下持锁会丢失，客户端经 `LockLostListener`（全局或单锁）通知。持锁写关键状态的业务**必须**处理锁丢失回调（如放弃提交并告警）——锁是协调手段，不是共识保证。
- **`lock()` 无无限阻塞**：同步 API 一律受总超时兜底（默认 30s），超时抛 `LockAcquisitionTimeoutException`。
- **`SIMPLE` 不可重入**：同线程持锁期间再次获取会排队等待自己，直至租约到期——这是不可重入语义的直接推论，不是 bug。
- **无锁升降级**：持写取读、持读取写不做特判，一律按通用规则排队。
- **FIFO 公平**：等待队列严格先来先得，只通知队首（无惊群）；放弃等待者若位于队首，其队列位置最长占用一个队首响应超时窗口（默认 5s）。
- **锁在事务外层**：`@OpenLatch` + `@Transactional` 同标注时，锁获取先于事务开启、释放晚于提交（顺序有自动化测试锁定）。
- **Spring AOP 自调用**：同类内 `this.method()` 不经过代理，注解不生效——与 `@Transactional` 同一限制。
- **重启即全释放**：Phase 1 为单机内存锁，服务器重启后所有锁消失（客户端感知为超时→重连→重新竞争，无残留假锁）。
- **异步回调线程**：`acquireAsync`/`releaseAsync` 的 future 在网络线程完成，链接的回调不得执行阻塞操作；锁丢失回调在专用单线程执行器上，同样不得阻塞。

## 已知局限（Phase 1）

1. 高竞争读场景读者逐个串行推进（批量授予优化留 Phase 3 评估）；
2. 客户端放弃等待不主动取消排队，靠队首响应超时回收；
3. 单机内存锁，无持久化与集群（Phase 2）；
4. `waitTime > 0` 的计时在客户端，时钟回拨可能使等待略长。

## 基准基线

最近一次基线：[docs/benchmark-baseline-2026-08-29.md](docs/benchmark-baseline-2026-08-29.md)
（手写 harness，防退化参考，不作发布门槛；复跑 `BenchmarkMain`。）

## 文档

- [Phase 1 详细设计说明书](docs/OpenLatch-Phase1-详细设计说明书.md)
- [Phase 1 验收报告](docs/Phase1-验收报告.md)

## 许可

[Apache License 2.0](LICENSE)
