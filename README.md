# OpenLatch

**English** | [简体中文](README_CN.md)

OpenLatch is a lightweight distributed lock service (Phase 1 / MVP): a single-node in-memory lock with leases and client watchdog renewal, a wait–notify–resend FIFO fair queue over Netty long connections with a Protobuf wire protocol, a JUC-style client SDK, and a Spring Boot declarative annotation `@OpenLatch`.

**Not in Phase 1**: clustering/Raft (Phase 2), FairLock/Semaphore/CountDownLatch, monitoring console, TLS and authentication (Phase 3). The protocol reserves fields for these.

## Modules

| Module | Purpose |
|---|---|
| `openlatch-protocol` | `.proto` definitions and codecs |
| `openlatch-core` | Pure-Java lock semantics (state machine / wait queue / lease / session) |
| `openlatch-server` | Netty single-node server (executable jar) |
| `openlatch-client` | Client SDK (async core + JUC-style sync wrapper + watchdog + reconnect) |
| `openlatch-spring-boot-starter` | Spring Boot 4 auto-configuration, `@OpenLatch` annotation and aspect |
| `openlatch-examples` | Examples and benchmark harness (not published) |

## Build

Requires **Java 25**. Phase 1 artifacts are not yet published to Maven Central; install locally first:

```bash
mvn clean install
```

## Quick Start

### 1. Start the server

```bash
java -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar
# listens on 9410 by default; point -Dopenlatch.config=<path> at a Properties file to override
```

### 2. Programmatic use (client SDK)

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
    client.connectAsync().join();          // optional: await first connection
    OLock lock = client.newReentrantLock("order:123");
    lock.lock();
    try {
        // critical section
    } finally {
        lock.unlock();
    }
}
```

### 3. Spring Boot declarative (starter)

> **Supported version**: Spring Boot **4.0.x+** on Java 25 (see §8.4 of the design spec). The starter is compiled against Boot 4 and is not backward compatible with Boot 3 applications.

```xml
<dependency>
    <groupId>io.github.lamspace</groupId>
    <artifactId>openlatch-spring-boot-starter</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Enabling `-parameters` in your build is **required** (SpEL resolves method parameter names):

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

    @OpenLatch(key = "#orderId")                       // SpEL: lock key from argument
    public void createOrder(String orderId) { ... }

    @OpenLatch(key = "report", waitTime = 3, leaseTime = 60)  // bounded wait, custom lease
    @Transactional
    public Order settle(long report) { ... }           // lock outside the transaction
}
```

Add the dependency, annotate, done: the `OpenLatchClient` bean is auto-configured and gracefully shut down (best-effort release of held locks) when the context closes.

## Examples

Every example is self-contained (embedded server in-process on an ephemeral port — a demo fixture; deploy the server standalone in production):

```bash
mvn -pl openlatch-examples compile exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.QuickStartExample
# likewise: ConcurrencyExample / ReadWriteExample / WatchdogExample /
# SpringAnnotationExample / BenchmarkMain (~60s, writes the baseline report)
```

## Configuration Reference

### Server (Properties via `-Dopenlatch.config=<path>`)

| Key | Default | Notes |
|---|---|---|
| `openlatch.server.port` | `9410` | Listen port |
| `openlatch.server.worker-threads` | `2 × CPU` | Netty worker threads |
| `openlatch.server.session.idle-timeout-ms` | `60000` | Idle connection timeout |
| `openlatch.server.lease.default-ms` | `30000` | Default lease |
| `openlatch.server.lease.min-ms` / `max-ms` | `1000` / `3600000` | Lease clamping range |
| `openlatch.server.lease.tick-interval-ms` | `500` | Expiry scan interval |
| `openlatch.server.queue.head-reply-timeout-ms` | `5000` | Head-reply timeout after notify (§4.5) |
| `openlatch.server.limit.max-key-length` | `512` | Max key bytes |
| `openlatch.server.limit.max-queue-depth-per-key` | `4096` | Per-key queue depth limit |
| `openlatch.server.limit.max-inflight-per-connection` | `1024` | Inflight limit per connection |

### Client (`OpenLatchClient.builder()`)

| Parameter | Default | Notes |
|---|---|---|
| `address` | required | `host:port` |
| `requestTimeout` | 5s | Per-request timeout |
| `defaultWaitTimeout` | 30s | `lock()` overall fallback |
| `connectTimeout` | 3s | TCP + handshake timeout |
| `reconnectInitialBackoff` / `reconnectMaxBackoff` | 200ms / 10s | Exponential backoff |
| `workerThreads` | 1 | Client Netty EventLoop threads |

### Starter (`application.yaml`)

| Property | Default | Notes |
|---|---|---|
| `openlatch.enabled` | `true` | When false the annotation is inert (client bean still created) |
| `openlatch.server-host` / `server-port` | `127.0.0.1` / `9410` | Server address |
| `openlatch.request-timeout` | `5s` | Duration |
| `openlatch.default-wait-timeout` | `30s` | `lock()` fallback |
| `openlatch.reconnect-initial-backoff` / `reconnect-max-backoff` | `200ms` / `10s` | Backoff |

## Semantics & Caveats

- **Leases always expire**: every grant carries a server lease (default 30s, clamped); unrenewed locks are guaranteed to be reclaimed. The client watchdog renews at `lease/3` (annotation locks with a custom `leaseTime` are protected too).
- **Locks can be lost**: disconnection or lease expiry revokes a held lock; the client notifies via `LockLostListener` (global or per-lock). Businesses writing critical state under a lock **must** handle the callback (e.g. abort the commit and alert) — the lock is coordination, not consensus.
- **No unbounded blocking**: synchronous calls always carry a total-timeout fallback (default 30s); `LockAcquisitionTimeoutException` on timeout.
- **`SIMPLE` is non-reentrant**: re-acquiring while the same thread holds it queues against itself until the lease expires. That is the semantics, not a bug.
- **No upgrade/downgrade**: read→write or write→read special cases are not implemented; generic queuing applies.
- **FIFO fairness**: strict arrival order, only the queue head is notified (no thundering herd). An abandoned waiter at the head keeps its slot for at most one head-reply timeout window (default 5s).
- **Lock outside transaction**: with `@OpenLatch` + `@Transactional` on one method, the lock is acquired before the transaction begins and released after commit (asserted by an automated test).
- **Spring AOP self-invocation**: `this.method()` bypasses the proxy, so the annotation does not apply — same standard limitation as `@Transactional`.
- **Restart = full release**: Phase 1 is a single-node in-memory lock; after a server restart all locks are gone (clients see timeouts → reconnect → re-compete; no phantom locks).
- **Async callback threads**: futures from `acquireAsync`/`releaseAsync` complete on the network thread — never block in chained callbacks. Lock-lost callbacks run on a dedicated single thread; same rule.

## Known Limitations (Phase 1)

1. Under hot read contention readers advance one at a time (batch granting deferred to Phase 3);
2. Abandoning a wait does not proactively cancel the queue slot; it is reclaimed via the head-reply timeout;
3. Single-node in-memory locks, no persistence or clustering (Phase 2);
4. `waitTime > 0` is timed client-side; clock rollback may slightly extend a wait.

## Benchmark Baseline

Latest baseline: [docs/benchmark-baseline-2026-08-29.md](docs/benchmark-baseline-2026-08-29.md)
(hand-written harness; a regression reference, not a release gate — re-run `BenchmarkMain`).

## Documentation

- [Phase 1 Detailed Design (zh)](docs/OpenLatch-Phase1-详细设计说明书.md)
- [Phase 1 Acceptance Report (zh)](docs/Phase1-验收报告.md)

## License

[Apache License 2.0](LICENSE)
