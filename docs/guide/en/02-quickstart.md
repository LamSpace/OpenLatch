# 02 · Quick Start

Goal: run the "single-node lock" and the "3-node cluster + reroute" paths in ten minutes.
Prereq: Java 25, Maven.

## Path A: minimal single node

### 1. Get the artifacts

Before Central publication, build and install locally:

```bash
git clone https://github.com/LamSpace/OpenLatch.git && cd OpenLatch
mvn clean install -DskipTests
```

### 2. Start the server

```bash
java -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar
# log confirms: OpenLatch server started: port=9410, ... metricsPort=9412
```

### 3. Take your first lock

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
            System.out.println("in the critical section…");
        } finally {
            lock.unlock();
        }
    }
}
```

Run it from two terminals to observe mutual exclusion and queuing. Health check:

```bash
curl -s localhost:9412/healthz          # any 200 means alive
curl -s localhost:9412/metrics | grep openlatch_server_acquire   # metrics flowing
```

Self-contained examples (embedded in-process server, no running service needed):

```bash
mvn -pl openlatch-examples compile exec:java \
    -Dexec.mainClass=io.github.lamspace.openlatch.examples.QuickStartExample
# also: ConcurrencyExample / ReadWriteExample / WatchdogExample
```

## Path B: 3-node cluster + failover by hand

### 1. Three config files

```properties
# node1.properties (nodes 2/3 identical except node-id / ports / data-dir)
openlatch.server.port=9410
openlatch.cluster.enabled=true
openlatch.cluster.node-id=1
openlatch.cluster.raft-port=9501
openlatch.cluster.peers=1@127.0.0.1:9501,2@127.0.0.1:9502,3@127.0.0.1:9503
openlatch.cluster.client-addresses=1@127.0.0.1:9410,2@127.0.0.1:9420,3@127.0.0.1:9430
openlatch.cluster.data-dir=/tmp/openlatch-demo/node-1
# same-host demo must also stagger the metrics port:
openlatch.server.metrics.port=0
```

> For a same-machine demo stagger access ports 9410/9420/9430; in production give each node
> the default 9410 on its own host/container and keep metrics ports distinct (or `0`).
> `client-addresses` lets leader hints name directly-connectable addresses; without it the
> client falls back to seed self-reporting discovery (clustering is unaffected).

### 2. Boot three nodes, find the leader

```bash
java -Dopenlatch.config=node1.properties -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar &
java -Dopenlatch.config=node2.properties -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar &
java -Dopenlatch.config=node3.properties -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar &
curl -s localhost:9412/metrics | grep openlatch_cluster_is_leader   # per node: am I the leader?
```

### 3. Give the client all seeds

```java
OpenLatchClient client = OpenLatchClient.builder()
        .seeds("127.0.0.1:9410", "127.0.0.1:9420", "127.0.0.1:9430")
        .build();
client.connectAsync().join();   // lands on the leader; follows hints off followers
```

### 4. Kill the leader yourself

```bash
kill -9 $(jps -l | awk '/openlatch-server.*executable.jar/{print $1}' | head -1)   # kill the current leader
```

With a lock-holding loop running you will see: a brief error burst
(`ServerUnavailable`/timeouts) → seed reconnect → reroute to the new leader →
**confirmed grants survive** (renewal succeeds); grants not yet replicated may roll back
(lock-lost / `INVALID_TOKEN` — expected, see the consistency declaration in
[05](05-cluster-deployment.md)). Observed recovery is ~2s (drill criterion: <10s).

## Path C: Spring Boot declarative (four lines)

Depend on `openlatch-spring-boot-starter`, enable `-parameters` in the compiler, annotate:

```yaml
openlatch:
  server-host: 127.0.0.1
  server-port: 9410
```

```java
@OpenLatch(key = "#orderId")
public void createOrder(String orderId) { ... }
```

Full property tables and caveats: [04 Spring Boot Starter](04-spring-boot-starter.md).

## Next

- Which lock, and how to use it well → [03 Client SDK](03-client-sdk.md)
- Production cluster layout → [05 Cluster Deployment & Operations](05-cluster-deployment.md)
