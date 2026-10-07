# 07 · Admin Console

`openlatch-console` is a **separately deployed, read-only** web observer (default HTTP
`9413`): the overview page shows per-node role/sessions/lock counts with /metrics sparklines;
detail pages show lock-table entries, wait queues and session footprints.

**It performs no writes** — no force-unlock, no session eviction. It is a window, not a remote.

## Deployment

Grab the executable jar from
[GitHub Releases](https://github.com/LamSpace/OpenLatch/releases), then:

```bash
java -Dopenlatch.console.config=/path/to/console.properties \
     -jar openlatch-console-1.0.0-executable.jar
```

(If you built from source instead, the jar is at
`openlatch-console/target/openlatch-console-1.0.0-executable.jar`.)

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

Queue entries (v7) render as: overview `QUEUE entries` count; the keys table shows
capacity · depth · head element size + constant-length truncated preview (delay kinds
also the head expiry); the detail page adds total resident bytes and a waiters column
splitting the two tracks (waiting-for-capacity / waiting-for-elements). Full element
bytes never ship in any admin response — the same anti-amplification rule as v6 payloads.
Topic keys (v8) render the subscriber count and a subscriber list (session id,
subscription id, subscribed-at); the registry is Leader-local, so only Leader-sourced
pages show topic rows (follower views are an honest NOT_HELD, never an empty-shell
success) and delivered message content never leaves the observation surface.
Phasers (v10) render the ledger's three counters (phase, registered, current
arrivals), a per-session quota table, and a phase-waiters section (session, request,
observed phase, registered-at). The ledger and quotas are replicated state, readable
identically on every node (unlike topic keys, a phaser key's detail MATCHES on a
Follower); the waiter bookkeeping is Leader-local, honestly empty on Followers under
the same `wait_queue_leader_only` annotation — the "two-speed projection" (full
ledger, waiters pending re-hang) MUST NOT mislead "nobody is waiting" from readable
counters. Pure counts and identities: no content egress.
Conditions (v9) add a "condition waiters" section on the lock detail page, rendered
**beside — never double-counted with — the wait queue**: each row shows the condition
name, session id, request id, thread id and registration time (the wait duration
follows from it); waiters already carried by a signal leave this section (they then
belong to the wait-queue counts). The wait set is Leader-local state, so condition
detail is non-zero only from the Leader's view (followers read an honest zero — the
same Leader-only surface as the topic registry); the condition name is addressing
text itself and this surface ships no content.

TIMER keys (v11) share the two-speed shape: the ledger triple
(`timer_generation`/`timer_armed`/`timer_fire_at_ms`) is replicated and
byte-identical on every node, while the waiter list is Leader-only and
honestly empty elsewhere. The timer projection **never converts** the fired
verdict — raw absolute expiry times only (`marked` is folded solely on
Leader reply lines at the evaluating node's clock); a DISARMED key shows
`timer_armed=false` honestly rather than disappearing, and no "rang" flag is
ever rendered.
