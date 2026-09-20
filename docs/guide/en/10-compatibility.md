# 10 · Compatibility & Protocol Versions

## Support matrix

| Dimension | Support | Notes |
|---|---|---|
| Java runtime/compile | **25** (only) | all artifacts build with `release=25`; 17/21 cannot load them |
| Spring Boot starter | **4.x** | depends on Boot-4-only artifacts; Boot 3.x incompatible — wire the SDK manually ([03](03-client-sdk.md)) |
| Spring Framework | whatever Boot 4 ships (Framework 7) | starter targets Boot 4 contexts only |
| Wire protocol | servers accept **v1 / v2 / v3 / v4 / v5** (HELLO negotiation, range [1,5]) | out-of-range rejected at handshake — no implicit compatibility |
| Micrometer | host-provided (not transitive) | inject a `MeterRegistry` to enable client metrics |
| Maven Central | not yet published | `mvn clean install` locally, use `1.0-SNAPSHOT` coordinates |

## Protocol capabilities by version

| Version | Surface |
|---|---|
| v1 | single-node lock semantics: acquire/release/renew, wait–notify–resend, leases |
| v2 | clustering: leader hints & reroute, `CLUSTER_VIEW`, forwarding lane |
| v3 | extended primitives (fair lock / semaphore / latch), `ADMIN_*` read-only observation, field-shape tightening |
| v4 | atomic variables (`OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean`): the `ATOMIC_OP` message pair, per-key version stamps and dedup slots |
| v5 | cyclic barrier (`OBarrier`): `BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` message pairs, replicated generation ledgers, leave-breaks-the-generation contract |

Mixed-version rule: **server ≥ client**. Old clients (v1/v2) work fully against new servers;
a newer client against an older server is rejected at handshake (explicit failure beats
silent behavioral downgrade). Atomic capabilities require server v4 and barrier capabilities
server v5 (v≤4 sessions sending `BARRIER_*` get an `INVALID_REQUEST` message-level
rejection without disconnect); after the server is upgraded, v≤4 clients keep their
byte-for-byte behavior.

## Upgrade & rollback order

Rolling upgrade (servers first, clients second) and rollback constraints (including the
snapshot-index point of no return for older binaries) live in
[05, upgrade/rollback](05-cluster-deployment.md).

## Configuration compatibility

- every capability is default-off: with defaults behavior matches the earlier baseline
  (TLS, auth, cluster, metrics each behind their own switch);
- config keys are added, never repurposed; deprecations would be announced one release
  ahead (currently: none).

## Data compatibility

Raft log/snapshot formats are backward-compatible within 1.x (a newer binary reads an older
data dir). **The reverse does not hold** — once a snapshot/truncation exists, older binaries
cannot mount that `data-dir` (see [05 rollback](05-cluster-deployment.md)). Once v4/v5 snapshots
carry ATOMIC or BARRIER entries, rolling back to older binaries falls under the same rule —
either confirm no such keys were written before rollback, or accept the state resetting to zero
(barrier generations restart from scratch).
