# 07 · Admin Console

`openlatch-console` is a **separately deployed, read-only** web observer (default HTTP
`9413`): the overview page shows per-node role/sessions/lock counts with /metrics sparklines;
detail pages show lock-table entries, wait queues and session footprints.

**It performs no writes** — no force-unlock, no session eviction. It is a window, not a remote.

## Deployment

```bash
java -Dopenlatch.console.config=/path/to/console.properties \
     -jar openlatch-console/target/openlatch-console-1.0-SNAPSHOT-executable.jar
```

Co-locating with observed nodes is fine (separate process). Template:
`openlatch-console/console.properties.example` in the repo.

## Configuration keys

| Key | Default | Notes |
|---|---|---|
| `openlatch.console.server-addresses` | **required** | comma-separated `host:port` of node **business ports** |
| `openlatch.console.admin-token` | **required** | must equal each server's `openlatch.server.admin.token`; mismatch degrades pages to an auth banner instead of erroring |
| `openlatch.console.auth-token` | — | business token (required when nodes enable business auth) |
| `openlatch.console.tls-enabled` | `false` | TLS toward nodes |
| `openlatch.console.tls-trust-store` | — | PEM CA trust anchor |
| `openlatch.console.tls-client-cert` / `tls-client-key` | — | mTLS pair |
| `openlatch.console.port` | `9413` | console HTTP port |
| `openlatch.console.refresh-interval-seconds` | `5` | polling interval |
| `openlatch.console.metrics-port` | `9412` | node metrics port for sparklines |
| `openlatch.console.request-timeout-ms` | `5000` | per admin request |

Invalid config (empty address list, port clash, blank admin token) fails startup fast.

## Interaction with the security layer

- with no `openlatch.server.admin.token` on a server, every `ADMIN_*` request is refused —
  the console shows that node as unauthorized; that is the **default posture** (observation
  starts closed);
- TLS/auth-enabled nodes require matching console settings; misconfigured nodes surface as
  auth-failed/degraded — the console **never probes a TLS node with plaintext requests**;
- admin token ≠ business token: per-message read-only vs handshake-scoped write-driving.

## Data semantics

Observation reads are **weakly consistent snapshots**: detail reflects a best-effort read at
request time from each node's local replica; brief gaps or degraded markers during leader
changes are expected and not alert-worthy by themselves. Metric meanings: [08](08-observability.md).
