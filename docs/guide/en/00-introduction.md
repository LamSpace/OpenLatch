# 00 · Introduction & Architecture

## What OpenLatch is

OpenLatch is a lightweight distributed lock service offering JUC-style coordination primitives:

- **Mutex family** (`OLock`): reentrant, simple (non-reentrant), read-write (`OReadWriteLock`), fair;
- **Distributed semaphore** (`OSemaphore`): an N-permit shared gate;
- **Distributed count-down latch** (`OCountDownLatch`): a one-shot barrier.
- **Distributed cyclic barrier** (`OBarrier`): multi-party rendezvous with reusable generations (v5).

Backends: a **Raft cluster** (Apache Ratis replicated state machine with snapshots, default 3
nodes tolerating 1 failure) or a **single node** (in-memory, zero dependencies). The wire
protocol is Protobuf over Netty long connections (negotiated v1–v5). Semantics are underpinned by **leases**
with client-side **watchdog renewal** and a **wait–notify–resend FIFO fair queue**.

One sentence: **coordination, not consensus**. Locks carry leases and can be lost — your
critical sections must handle the lock-lost callback (see [Core Concepts](01-concepts.md)).

## Component map

```
                        ┌──────────────────────── Raft cluster (Apache Ratis) ────────────────────────┐
   OpenLatchClient ◀────┤   node1 :9410        node2 :9410        node3 :9410                        │
   (any node; leader-   │        │  ◀─────── log replication ────▶   │        │                      │
    aware: HELLO hint,  │        │      (raft ports :9411…)          │        │                      │
    NOT_LEADER reroute, │        ▼                                   ▼        ▼                      │
    seed discovery)     │   metrics :9412                       metrics :9412 …  (/metrics,/healthz) │
                        │        ▲                                   ▲                               │
   Spring Boot starter ◀┤        │ admin (read-only, token-gated)    │                               │
   @OpenLatch           │   openlatch-console :9413 ──────────────────┘ (v3 ADMIN_* queries +        │
                        │                                                /metrics sparklines)         │
                        └─────────────────────────────────────────────────────────────────────────────┘
```

- **Clients may connect to any node**: writes landing on a Follower get `NOT_LEADER` plus a
  leader hint and are rerouted automatically; reconnection discovers the new leader across seeds.
- **Business and admin planes share the business port** (independent per-message admin token
  for read-only observation); metrics/health live on 9412; the console serves its UI on 9413.
- **Observability**: ten server metrics + four client metrics in Prometheus format — see [08](08-observability.md).

## Port map (defaults, all configurable)

| Port | Purpose | Plaintext/TLS |
|---|---|---|
| 9410 | Client business traffic (incl. `ADMIN_*`) | optional TLS / mTLS + business token |
| 9411 | Raft inter-node replication (cluster only) | plaintext (keep on a trusted network) |
| 9412 | `/metrics` + `/healthz` admin endpoints | plaintext (trusted network) |
| 9413 | Admin console web UI | plaintext (trusted network / reverse proxy) |

> When several server nodes share one host, give each a distinct
> `openlatch.server.metrics.port` (or `0` for ephemeral) — a bind conflict fails startup
> by design (fail-fast).

## Next

- Understand the semantics first → [01 Core Concepts](01-concepts.md)
- Get it running now → [02 Quick Start](02-quickstart.md)
