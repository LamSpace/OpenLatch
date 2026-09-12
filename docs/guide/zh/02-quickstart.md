# 02 · 快速上手

目标：十分钟内跑通"单机锁"与"三节点集群 + 故障改道"两条路径。前提：Java 25、Maven。

## 路径 A：单机最小闭环

### 1. 取构件

中央仓库发布前，本地构建安装：

```bash
git clone https://github.com/LamSpace/OpenLatch.git && cd OpenLatch
mvn clean install -DskipTests
```

### 2. 启动服务器

```bash
java -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar
# 日志确认：OpenLatch server started: port=9410, ... metricsPort=9412
```

### 3. 写第一个锁

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
    client.connectAsync().join();
    OLock lock = client.newReentrantLock("demo:hello");
    if (lock.tryLock(5, TimeUnit.SECONDS)) {
        try {
            System.out.println("临界区执行中…");
        } finally {
            lock.unlock();
        }
    }
}
```

开两个终端各跑一次，可观察到互斥与排队。验证健康：

```bash
curl -s localhost:9412/healthz          # 存活探测，任何 200 即在线
curl -s localhost:9412/metrics | grep openlatch_server_acquire   # 指标已产生
```

仓库内另有一组自包含示例（进程内嵌服务器，无需先起服务）：

```bash
mvn -pl openlatch-examples compile exec:java \
    -Dexec.mainClass=io.github.lamspace.openlatch.examples.QuickStartExample
# 另有 ConcurrencyExample / ReadWriteExample / WatchdogExample
```

## 路径 B：三节点集群 + 改道验证

### 1. 三份配置

```properties
# node1.properties（node2/3 同构，改 node-id / raft-port / server.port / data-dir）
openlatch.server.port=9410
openlatch.cluster.enabled=true
openlatch.cluster.node-id=1
openlatch.cluster.raft-port=9501
openlatch.cluster.peers=1@127.0.0.1:9501,2@127.0.0.1:9502,3@127.0.0.1:9503
openlatch.cluster.client-addresses=1@127.0.0.1:9410,2@127.0.0.1:9420,3@127.0.0.1:9430
openlatch.cluster.data-dir=/tmp/openlatch-demo/node-1
# 同机三节点必须错开接入端口与指标端口：
openlatch.server.metrics.port=0
```

> 单机演示用 9410/9420/9430 错开端口即可；生产推荐每节点默认 9410 + 独立机器/容器，
> 指标端口同机互异或 `0`。`client-addresses` 让 Leader 提示直接给出可连地址，不配则
> 客户端退化为种子自报发现（组网不受影响）。

### 2. 起三节点，找到 Leader

```bash
java -Dopenlatch.config=node1.properties -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar &
java -Dopenlatch.config=node2.properties -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar &
java -Dopenlatch.config=node3.properties -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar &
curl -s localhost:9412/metrics | grep openlatch_cluster_is_leader   # 每节点看自己是否为 Leader
```

### 3. 客户端配全种子

```java
OpenLatchClient client = OpenLatchClient.builder()
        .seeds("127.0.0.1:9410", "127.0.0.1:9420", "127.0.0.1:9430")
        .build();
client.connectAsync().join();   // 自动落到 Leader；连到 Follower 时按提示改道
```

### 4. 亲手制造一次 Leader 故障

```bash
kill -9 $(jps -l | awk '/openlatch-server.*executable.jar/{print $1}' | head -1)   # 杀掉当前 Leader 进程
```

保持客户端持锁循环运行的话，会观察到：短暂失败（`ServerUnavailable`/超时）→ 种子重连 →
改道新 Leader → **已确认的授予仍在**（续租成功），未复制完成的授予可能回滚（失锁/
`INVALID_TOKEN`——预期内，见 [05 一致性声明](05-cluster-deployment.md)）。实测恢复窗口通常在 2s
量级（演练口径 <10s）。

## 路径 C：Spring Boot 声明式（4 行接入）

依赖 `openlatch-spring-boot-starter` + 构建开启 `-parameters` + 一个注解：

```yaml
openlatch:
  server-host: 127.0.0.1
  server-port: 9410
```

```java
@OpenLatch(key = "#orderId")
public void createOrder(String orderId) { ... }
```

完整属性表、事务边界与常见坑见 [04 Spring Boot Starter](04-spring-boot-starter.md)。

## 下一步

- 用哪把锁、怎么用好 → [03 客户端 SDK](03-client-sdk.md)
- 生产集群怎么摆 → [05 集群部署与运维](05-cluster-deployment.md)
