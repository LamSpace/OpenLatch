# OpenLatch

**English** | [简体中文](README_CN.md)

[![build](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml)
[![drill](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml?query=branch%3Amaster)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-25-orange)](#requirements--compatibility)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.x-green)](#3-spring-boot-declarative-starter)
[![Protocol](https://img.shields.io/badge/protocol-v3-lightgrey)](#requirements--compatibility)

OpenLatch is a lightweight distributed lock service. It provides JUC-style coordination
primitives — reentrant / simple / read-write / fair locks, a distributed **semaphore** and a
distributed **count-down latch** — backed either by a **Raft cluster** (Apache Ratis replicated
state machine, with snapshots) or by a **single node**, over a Netty long-connection Protobuf
wire protocol. Leases with client-side watchdog renewal, a wait–notify–resend **FIFO fair
queue**, leader-aware routing (redirect hints + seed discovery), a Spring Boot 4 declarative
`@OpenLatch` annotation, a read-only web **admin console**, Prometheus-style **metrics /
health** endpoints, and optional **TLS / mTLS with token authentication** complete the platform.

Locks are *coordination, not consensus*: they carry leases and can be lost — your critical
sections must handle the lock-lost callback (see [Semantics & Caveats](#semantics--caveats)).

## Architecture

```
                        ┌─────────────────────────── Raft cluster (Apache Ratis) ───────────────────────────┐
   OpenLatchClient ◀────┤   node1 :9410        node2 :9410        node3 :9410                               │
   (any node; leader-   │        │  ◀────── log replication ──────▶  │        │                             │
    aware: HELLO hint,  │        │            (raft ports :9411…)    │        │                             │
    NOT_LEADER reroute, │        ▼                                   ▼        ▼                             │
    seeds discovery)    │   metrics :9412                       metrics :9412  …  (/metrics, /healthz)      │
                        │        ▲                                   ▲                                     │
   Spring Boot starter ◀┤        │ admin (read-only, token-gated)    │                                     │
   @OpenLatch           │   openlatch-console :9413 ─────────────────┘  (queries business ports over v3    │
                        │                                               ADMIN_* + scrapes /metrics)         │
                        └────────────────────────────────────────────────────────────────────────────────────┘
```

## Modules

| Module | Purpose |
|---|---|
| `openlatch-protocol` | `.proto` definitions and codecs |
| `openlatch-core` | Pure-Java lock semantics (state machine / wait queue / lease / session) |
| `openlatch-server` | Netty server, standalone or Raft-clustered (executable jar) |
| `openlatch-client` | Client SDK (async core + JUC-style sync wrappers + watchdog + reconnect + leader-aware routing) |
| `openlatch-spring-boot-starter` | Spring Boot 4 auto-configuration, `@OpenLatch` annotation and aspect |
| `openlatch-console` | Read-only admin console (web; `ADMIN_*` protocol) |
| `openlatch-examples` | Examples and benchmark harness (not published) |

## Requirements & Compatibility

| Item | Support |
|---|---|
| Java | **25** for everything (all artifacts compile with `release=25`) |
| Spring Boot | **4.x only.** The starter is compiled against Boot 4 and depends on Boot-4-only artifacts (e.g. `spring-boot-starter-aspectj`) — it is **not** compatible with Boot 3.x. Boot 3 applications can assemble `openlatch-client` manually; no Boot 3 starter is offered. |
| Wire protocol | v1 / v2 / v3, negotiated in the HELLO handshake (`[1,3]` supported); out-of-range versions are rejected — no implicit compatibility |
| Artifacts | Not yet published to Maven Central — install locally first (`mvn clean install`) |
| Defaults | business port **9410** (optionally TLS/token-gated), Raft **9411**, metrics admin **9412** (`/metrics`, `/healthz`, plaintext), console **9413** (plaintext) |

## Build

Requires **Java 25**. Build and install locally, then depend on the coordinates below:

```bash
mvn clean verify        # unit tests, in-process ITs, javadoc gate
mvn clean install       # + local-repository install
```

Process-level fault drills (kill / rolling restart / real netns partition, needs privileges)
run separately via `mvn verify -Pdrill` — nightly and manual runs execute on CI and publish
their reports as run artifacts, not into the repository.

## Quick Start

### 1. Start the server (single node)

```bash
java -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar
# listens on 9410; metrics admin port 9412 (/metrics + /healthz) enabled by default;
# override any setting via -Dopenlatch.config=<properties-file>
```

### 2. Start a 3-node cluster

One properties file per node (full operational guide — deployment, restart ordering,
failover behaviour — in the [cluster deployment & failover guide](docs/guide/zh/OpenLatch-Phase2-集群部署与故障转移.md)):

```properties
# node1.properties — repeat per node: unique node-id / raft-port / data-dir
openlatch.server.port=9410
openlatch.cluster.enabled=true
openlatch.cluster.node-id=1
openlatch.cluster.raft-port=9501
openlatch.cluster.peers=1@127.0.0.1:9501,2@127.0.0.1:9502,3@127.0.0.1:9503
openlatch.cluster.client-addresses=1@127.0.0.1:9410,2@127.0.0.1:9420,3@127.0.0.1:9430
openlatch.cluster.data-dir=/var/lib/openlatch/node-1
java -Dopenlatch.config=node1.properties -jar openlatch-server-1.0-SNAPSHOT-executable.jar
```

Clients may connect to **any** node: non-leader nodes answer `NOT_LEADER` with a leader hint,
and seed discovery reroutes writes.

### 3. Programmatic use (client SDK)

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

Beyond `OLock` (reentrant / simple / read-write / fair) the SDK offers
`OSemaphore` (N-permit shared gate) and `OCountDownLatch` (one-shot barrier) with the same
lease / watchdog / lock-lost semantics.

### 4. Spring Boot declarative (starter)

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

Add the dependency, annotate, done: the `OpenLatchClient` bean is auto-configured and
gracefully shut down (best-effort release of held locks) when the context closes.

## Examples

Every example is self-contained (embedded server in-process on an ephemeral port — a demo
fixture; deploy the server standalone in production):

```bash
mvn -pl openlatch-examples compile exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.QuickStartExample
# likewise: ConcurrencyExample / ReadWriteExample / WatchdogExample /
# SpringAnnotationExample / BenchmarkMain (~60s, writes the baseline report)
```

## Admin Console

`openlatch-console` is a separately deployed **read-only** web console (default HTTP `9413`).
It queries each configured node over the v3 `ADMIN_*` protocol on the node's business port,
authenticated against the server's `openlatch.server.admin.token`; the overview page also
scrapes each node's `/metrics` port for sparklines. It offers no write operations
(no force-unlock / session eviction). When a node enables TLS/business auth, the console
connects with its own TLS + business-token settings; a node that rejects the handshake shows
as an auth/degraded node (never silently probed in plaintext).

```bash
java -Dopenlatch.console.config=/path/to/console.properties \
     -jar openlatch-console/target/openlatch-console-1.0-SNAPSHOT-executable.jar
```

Config keys (a template lives at `openlatch-console/console.properties.example`):

| Key | Default | Notes |
|---|---|---|
| `openlatch.console.server-addresses` | *(required)* | comma-separated `host:port` node **business** ports |
| `openlatch.console.admin-token` | *(required)* | must equal the server's `openlatch.server.admin.token`; mismatch ⇒ pages degrade to an auth banner |
| `openlatch.console.auth-token` | — | business token sent in HELLO (required when a node enables business auth) |
| `openlatch.console.tls-enabled` | `false` | enable TLS toward nodes |
| `openlatch.console.tls-trust-store` | — | PEM CA(s) trust anchor for nodes |
| `openlatch.console.tls-client-cert` / `tls-client-key` | — | mTLS client cert/key (paired) |
| `openlatch.console.port` | `9413` | console HTTP port |
| `openlatch.console.refresh-interval-seconds` | `5` | page polling interval |
| `openlatch.console.metrics-port` | `9412` | per-node metrics port used for the overview sparklines |
| `openlatch.console.request-timeout-ms` | `5000` | per admin-request timeout |

## Security (TLS & Token Auth)

TLS and business-token auth secure the **client-facing business port** (business and
`ADMIN_*` traffic share it). Both capabilities default OFF — with defaults the server
behaves exactly as the plaintext baseline.

**Server TLS** (`-Dopenlatch.config=<path>`):

| Key | Default | Notes |
|---|---|---|
| `openlatch.server.tls.enabled` | `false` | enable TLS on the business port; plaintext connections are rejected, handshake timeout 5s |
| `openlatch.server.tls.cert` / `tls.key` | — | server PEM certificate / private key (both required when enabled) |
| `openlatch.server.tls.trust-store` | — | PEM CA(s) for client certificates (mTLS) |
| `openlatch.server.tls.require-client-cert` | `false` | require & verify a client certificate (mTLS) |

Certificate updates take effect on restart (no hot reload).

**Business token auth** (HELLO `auth_token`):

| Key | Default | Notes |
|---|---|---|
| `openlatch.server.auth.enabled` | `false` | when OFF (default) a compatibility guard stays active: a non-empty `auth_token` is rejected; when ON the token must match one of `tokens` |
| `openlatch.server.auth.tokens` | — | comma-separated token list (≥1 when enabled; no commas inside a token); multiple tokens keep old + new active during rotation |

Rejections are indistinguishable (`INVALID_REQUEST` + disconnect, constant-time compare, no
reason leak); auth is decided once at handshake, before any session/session-open replication
(single-node and cluster share the same gate). The admin `admin-token` stays independent and
per-message. Client / starter / console surface the same settings via `tls-enabled`,
`tls-trust-store`, `tls-client-cert`/`tls-client-key`, `auth-token`.

**Rotation procedure** — server first, then clients, then remove the old token:
1. add the new token to `openlatch.server.auth.tokens` and restart the server (old + new both accepted);
2. switch clients / console to the new `auth-token` and restart them;
3. remove the old token from the server config and restart.

> Note: enabling business auth rejects clients that do not carry a valid token (including older
> clients) — roll the clients together with the server change.

**Scope boundary** — only the business port is covered. The Raft inter-node channel (Ratis),
the metrics HTTP port `9412`, and the console's own web UI `9413` stay plaintext; keep them on
the intranet / behind network isolation. Full end-to-end TLS is intentionally not claimed.

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
| `openlatch.server.queue.head-reply-timeout-ms` | `5000` | Head-reply timeout after notify (abandoned waiter vacates the FIFO head at most this long) |
| `openlatch.server.limit.max-key-length` | `512` | Max key bytes |
| `openlatch.server.limit.max-queue-depth-per-key` | `4096` | Per-key queue depth limit |
| `openlatch.server.limit.max-inflight-per-connection` | `1024` | Inflight limit per connection |
| `openlatch.server.metrics.enabled` | `true` | Enable the metrics admin endpoint (Prometheus scrapes `http://<host>:<port>/metrics`) |
| `openlatch.server.metrics.port` | `9412` | Metrics admin port (`0` = ephemeral); bind conflict fails startup — assign one distinct port (or `0`) **per node** when several nodes share a host |
| `openlatch.server.admin.token` | *(unset)* | Management token for the read-only `ADMIN_*` protocol (console). Unset ⇒ all admin requests rejected |

Cluster mode adds `openlatch.cluster.*` (`enabled` / `node-id` / `peers` / `raft-port` /
`client-addresses` / `data-dir` / `election-timeout-ms` / `snapshot-threshold`) —
see the [cluster guide](docs/guide/zh/OpenLatch-Phase2-集群部署与故障转移.md).

### Client (`OpenLatchClient.builder()`)

| Parameter | Default | Notes |
|---|---|---|
| `address` | required | `host:port` |
| `requestTimeout` | 5s | Per-request timeout |
| `defaultWaitTimeout` | 30s | `lock()` overall fallback |
| `connectTimeout` | 3s | TCP + handshake timeout |
| `reconnectInitialBackoff` / `reconnectMaxBackoff` | 200ms / 10s | Exponential backoff |
| `workerThreads` | 1 | Client Netty EventLoop threads |
| `meterRegistry` | off by default | Inject a host Micrometer `MeterRegistry` to enable client metrics (host must provide `micrometer-core`; not transitive) |

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
- **Restart semantics**: *single-node* mode is in-memory — after a restart all locks are gone (clients see timeouts → reconnect → re-compete; no phantom locks). *Cluster* mode replicates through the Raft log with snapshots — a restarting node rejoins and recovers state; locks survive leader moves and rolling restarts (a transient error window during leader changes is expected and reported by drills).
- **Async callback threads**: futures from `acquireAsync`/`releaseAsync` complete on the network thread — never block in chained callbacks. Lock-lost callbacks run on a dedicated single thread; same rule.

## Known Limitations

1. Under hot read contention readers advance one at a time — batch granting is deferred (evaluated and not adopted: it conflicts with the FairLock ordering regression suite);
2. Abandoning a wait does not proactively cancel the queue slot; it is reclaimed via the head-reply timeout;
3. `waitTime > 0` is timed client-side; clock rollback may slightly extend a wait;
4. A CountDownLatch entry, once initialized, lives until node restart and is not reclaimed when participants disperse; abandoned barriers are managed by key naming convention (one round identifier per barrier);
5. Cluster Semaphore grants are refused for pure joiners after the permit pool is reclaimed (explicit rejection, documented spec branch).

## Benchmark Baseline

The baseline harness is a regression reference, not a release gate. Re-run locally:

```bash
mvn -pl openlatch-examples exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.BenchmarkMain
# writes target/benchmark/benchmark-baseline-<date>.md (override with -Dbenchmark.output=<path>)
```

Recurring CI runs publish their reports as
[workflow run artifacts](https://github.com/LamSpace/OpenLatch/actions/workflows/benchmark.yml).

## Documentation

- **User guide** ([docs/guide/](docs/guide/)) — start with the
  [cluster deployment & failover guide (zh)](docs/guide/zh/OpenLatch-Phase2-集群部署与故障转移.md);
  the full bilingual guide set is under construction.
- **Design & planning** (internal material, zh):
  [overview design](docs/design/OpenLatch-概要设计说明书.md) ·
  [Phase 1](docs/design/OpenLatch-Phase1-详细设计说明书.md) ·
  [Phase 2](docs/design/OpenLatch-Phase2-详细设计说明书.md) ·
  [Phase 3](docs/design/OpenLatch-Phase3-详细设计说明书.md) detailed designs ·
  [master plan](docs/design/OpenLatch-总体实施计划与验证方案.md) ·
  [Raft library selection report](docs/design/raft-selection-report.md)
- **Quality reports** (internal): [Phase 1](docs/quality/Phase1-验收报告.md) ·
  [Phase 2](docs/quality/Phase2-验收报告.md) · [Phase 3](docs/quality/Phase3-验收报告.md)

## License

[Apache License 2.0](LICENSE)
