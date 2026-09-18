# 🔒 OpenLatch

**English** | [简体中文](README_CN.md)

[![build](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/build.yml)
[![drill](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml/badge.svg)](https://github.com/LamSpace/OpenLatch/actions/workflows/drill.yml?query=branch%3Amaster)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/Java-25-orange)](#-requirements)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.x-green)](docs/guide/en/04-spring-boot-starter.md)
[![Protocol](https://img.shields.io/badge/protocol-v3-lightgrey)](docs/guide/en/10-compatibility.md)

OpenLatch is a lightweight distributed lock service: JUC-style primitives (reentrant /
simple / read-write / fair locks, semaphore, count-down latch, atomic long/int/boolean
with per-key version stamps) on a **Raft cluster**
(Apache Ratis, with snapshots) or a **single node**; Protobuf over Netty with leases,
watchdog renewal and a wait–notify–resend **FIFO fair queue**; a Spring Boot 4 declarative
`@OpenLatch`, a read-only web **console**, Prometheus **metrics/health**, and optional
**TLS/mTLS + token auth**.

**Locks are coordination, not consensus** — they carry leases and can be lost; handle the
lock-lost callback in every critical path. The full semantics live in the guides below.

## 🏗️ Architecture

```
                        ┌─────────────────────────── Raft cluster (Apache Ratis) ───────────────────────────┐
   OpenLatchClient ◀────┤   node1 :9410        node2 :9410        node3 :9410                               │
   (any node;           │        │  ◀────── log replication ──────▶  │        │                              │
    leader-aware:       │        │        (raft ports :9411…)        │        │                              │
    HELLO hint,         │        ▼                                   ▼        ▼                              │
    NOT_LEADER          │   metrics :9412                       metrics :9412  …   (/metrics, /healthz)     │
    reroute, seeds      │        ▲                                   ▲                                      │
    discovery)          │        │ admin (read-only, token-gated)    │                                      │
   Spring Boot ◀────────┤   openlatch-console :9413 ─────────────────┘   (v3 ADMIN_* + /metrics scrape)    │
   @OpenLatch           └───────────────────────────────────────────────────────────────────────────────────┘
```

## 📦 Modules

| Module | Purpose |
|---|---|
| `openlatch-protocol` | `.proto` definitions and codecs |
| `openlatch-core` | Pure-Java lock semantics (state machine / wait queue / lease / session) |
| `openlatch-server` | Netty server, standalone or Raft-clustered (executable jar) |
| `openlatch-client` | Client SDK (async core + JUC wrappers + watchdog + reconnect + leader-aware routing) |
| `openlatch-spring-boot-starter` | Spring Boot 4 auto-configuration, `@OpenLatch` annotation and aspect |
| `openlatch-console` | Read-only admin console (web; `ADMIN_*` protocol) |
| `openlatch-examples` | Examples and benchmark harness (not published) |

## ⚙️ Requirements

| Item | Support |
|---|---|
| Java | **25** (all artifacts, `release=25`) |
| Spring Boot starter | **4.x only** (Boot-4-only dependencies); Boot 3.x → assemble the SDK manually |
| Wire protocol | v1 / v2 / v3 via HELLO negotiation, range `[1,3]`, no implicit compatibility |
| Artifacts | Not yet on Maven Central — `mvn clean install` locally |

## 🚀 Quick Start

```bash
mvn clean install -DskipTests                                   # build & install locally
java -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar   # single node on :9410
```

```java
try (OpenLatchClient client = OpenLatchClient.builder().address("127.0.0.1:9410").build()) {
    client.connectAsync().join();
    OLock lock = client.newReentrantLock("order:123");
    lock.lock();
    try {
        // critical section — handle lock loss: the lock is a lease, not a right
    } finally {
        lock.unlock();
    }
}
```

Spring Boot: add the starter, enable `-parameters` in the compiler, annotate
`@OpenLatch(key = "#orderId")` — three steps.

Cluster (3 nodes), token rotation, TLS setup, console deployment, metrics, troubleshooting:
**the [user guide](docs/guide/en/index.md) covers each end to end.**

## 🔑 Semantics you must know

- **Leases expire; the watchdog renews** at `lease/3`; unrenewed locks are reclaimed.
- **Locks can be lost** (disconnect / expiry / failover rollback) — implement
  `LockLostListener` and abort in-flight commits when it fires.
- **Nothing blocks unboundedly**: `lock()` has a total-timeout fallback (default 30s).
- **FIFO fairness** is strict arrival order, head-only notification (no thundering herd),
  per leader term — queues reshuffle on leader change.
- **Restart semantics**: single node = in-memory (restart releases all); cluster = Raft
  log + snapshots (grants survive leader moves and rolling restarts).

## 🚧 Known Limitations

1. Readers advance one at a time under hot read contention (batch granting deferred by design);
2. Abandoning a wait reclaims the seat via the head-reply timeout, not instantly;
3. `waitTime > 0` is timed client-side; clock rollback may slightly extend a wait;
4. Latch entries live until node restart — namespace barrier keys per round;
5. Cluster semaphore: pure joiners are explicitly refused after the pool was reclaimed.

## 📚 Documentation

- **User guide**: [English](docs/guide/en/index.md) | [简体中文](docs/guide/zh/index.md)
  — concepts, quick start, SDK, starter, cluster ops, security, console, observability,
  troubleshooting, compatibility, glossary.
- **Design & quality** (internal material, zh): [design/](docs/design/) ·
  [quality/](docs/quality/)

## 💡 Examples

```bash
mvn -pl openlatch-examples compile exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.QuickStartExample
# ConcurrencyExample / ReadWriteExample / WatchdogExample / SpringAnnotationExample / BenchmarkMain
```

## 📜 License

[Apache License 2.0](LICENSE)
