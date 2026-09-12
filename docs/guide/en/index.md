# OpenLatch User Guide (English)

The full learning path for users and operators. Chinese version: [../zh/index.md](../zh/index.md).

| Chapter | Contents |
|---|---|
| [00 Introduction & Architecture](00-introduction.md) | What OpenLatch is, component map, port map |
| [01 Core Concepts](01-concepts.md) | Leases, watchdog, lock loss, wait–notify–resend, FIFO fairness, timeout discipline |
| [02 Quick Start](02-quickstart.md) | Single node and 3-node cluster in ten minutes |
| [03 Client SDK](03-client-sdk.md) | Every lock type, semantics, lock-lost handling, async rules |
| [04 Spring Boot Starter](04-spring-boot-starter.md) | `@OpenLatch` declarative locks, SpEL, transaction ordering |
| [05 Cluster Deployment & Operations](05-cluster-deployment.md) | Topology, config, leader discovery, membership changes, restarts, supervisor |
| [06 Security](06-security.md) | TLS / mTLS, business tokens and rotation, honest boundaries |
| [07 Admin Console](07-admin-console.md) | Deploying and configuring the read-only observer |
| [08 Observability](08-observability.md) | Metrics reference, /metrics & /healthz, client metrics |
| [09 Troubleshooting & FAQ](09-troubleshooting.md) | Error-code semantics, failover baselines, stall self-healing, common traps |
| [10 Compatibility & Protocol Versions](10-compatibility.md) | Java / Spring Boot / protocol support matrix |
| [Appendix · Glossary](appendix-glossary.md) | Key terms |

> Reading advice: newcomers go 00 → 01 → 02 in order; if you already run a single node and
> want a cluster, jump to 05; for incidents, enter through the error-code table in 09.
