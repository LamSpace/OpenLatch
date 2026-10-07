# 10 · Compatibility & Protocol Versions

## Support matrix

| Dimension | Support | Notes |
|---|---|---|
| Java runtime/compile | **25** (only) | all artifacts build with `release=25`; 17/21 cannot load them |
| Caller thread flavor | **platform & virtual both supported** | blocking APIs are virtual-thread-friendly (no pinning; ownership and interrupt semantics identical), see [03](03-client-sdk.md) |
| Spring Boot starter | **4.x** | depends on Boot-4-only artifacts; Boot 3.x incompatible — wire the SDK manually ([03](03-client-sdk.md)) |
| Spring Framework | whatever Boot 4 ships (Framework 7) | starter targets Boot 4 contexts only |
| Wire protocol | servers accept **v1 / v2 / v3 / v4 / v5 / v6 / v7 / v8 / v9 / v10 / v11** (HELLO negotiation, range [1,11]) | out-of-range rejected at handshake — no implicit compatibility |
| Micrometer | host-provided (not transitive) | inject a `MeterRegistry` to enable client metrics |
| Artifact delivery | client SDK chain (`openlatch-protocol` / `openlatch-client` / `openlatch-spring-boot-starter`) published on Maven Central (1.0.0+); server & console executable jars on GitHub Releases | client: use the coordinates directly; server/console: download a jar or build locally |

## Protocol capabilities by version

| Version | Surface |
|---|---|
| v1 | single-node lock semantics: acquire/release/renew, wait–notify–resend, leases |
| v2 | clustering: leader hints & reroute, `CLUSTER_VIEW`, forwarding lane |
| v3 | extended primitives (fair lock / semaphore / latch), `ADMIN_*` read-only observation, field-shape tightening |
| v4 | atomic variables (`OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean`): the `ATOMIC_OP` message pair, per-key version stamps and dedup slots |
| v5 | cyclic barrier (`OBarrier`): `BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` message pairs, replicated generation ledgers, leave-breaks-the-generation contract |
| v6 | atomic reference (`OAtomicReference`): `optional bytes` payload fields on the ATOMIC message pair (no new `MessageType`), ingress `maxValueBytes` clamp, payload snapshot/preview |
| v7 | bounded & delay queues (`OBlockingQueue`/`ODelayQueue`): the `QUEUE_OP` message pair, `LOCK_TYPE_QUEUE`/`LOCK_TYPE_DELAY_QUEUE` kinds, per-session dedup slots, two-fulls split (DENIED vs OVERLOADED), capacity/drain ingress clamps, apply-point expiry folding, queue snapshot fields |
| v8 | broadcast pub/sub (`OTopic`): `TOPIC_OP` message pair + `TOPIC_MESSAGE` push, `REJECT_SUBSCRIBERS` in-band code, weak backpressure = drop-newest across two buffer tiers, per-session publish dedup at the accepting Leader, **zero log / zero snapshot contribution** (first zero-persistence primitive), `max-subscribers-per-key`/`max-subscription-buffer` ingress clamps |
| v9 | condition variables (`OCondition`): the `CONDITION_OP` message pair (SIGNAL/SIGNAL_ALL/LEAVE — **AWAIT is not in the vocabulary**; awaits fold into the ACQUIRE loop via `AcquireRequest`'s `optional condition` presence), zero-log signal family / zero-snapshot-footprint wait sets, no new `StatusCode`/`LockType`/`RaftEntryType` values, waiter count merged into the `max-queue-depth-per-key` guardrail (zero new config) |
| v10 | phaser (`OPhaser`): the single `PHASER_OP` message pair (REGISTER / ARRIVE / ARRIVE_AND_AWAIT / ARRIVE_AND_DEREGISTER / AWAIT_ADVANCE / CANCEL / QUERY — arrival and waiting decoupled), **all mutations logged** (each commits a `PHASER_OP_ENTRY`, entry type 14; wait/cancel/query stay zero-log), previous-generation window + per-session registration idempotence slots, death removes quotas without rolling back counted arrivals (no stall), waits self-heal across leader change (contrast the condition's signal window), three Non-Goals (no onAdvance hook / no termination / no parent-child tiering), new `max-parties-per-phaser` guardrail (default 1024), `StatusCode` unchanged (both caps ride `OVERLOADED`, separated by `op`), phases are `long` |
| v11 | timer (`OTimer`): the single `TIMER_OP` message pair (SCHEDULE / DISARM / AWAIT / CANCEL / QUERY), **half-logged ruling** — arms/withdrawals each commit a `TIMER_OP_ENTRY` (type 15) while **expiry logs nothing** (`marked` is a derived predicate of ledger plus verdict clock: "expiry is not a transition but time cashing in", the two ends against lease-expiry-logs); wait/cancel/query stay Leader-local zero-log; sticky shared mark + generation-clearing re-arm (round semantics need no generation argument — contrast phaser's observed-phase input); DISARM terminates the generation and settles waits through the previously-unreachable `DENIED` terminal (the zero-new-status-code streak continues); **the clock still rings when the loader dies** (third death caliber); three Non-Goals (no periodicity / no callbacks / no absolute-time arm); `max-timer-horizon-ms` + `timer-ready-tick-ms` (tick split from the queue); `StatusCode` unchanged again; the admin projection never converts marked (clock-free raw reads) |

Mixed-version rule: **server ≥ client**. Old clients (v1/v2) work fully against new servers;
a newer client against an older server is rejected at handshake (explicit failure beats
silent behavioral downgrade). Atomic capabilities require server v4, barrier capabilities
server v5 (v≤4 sessions sending `BARRIER_*` get an `INVALID_REQUEST` message-level
rejection without disconnect), and reference-form payloads require server v6 (v≤5 sessions
sending a reference-form `ATOMIC_OP` get an `INVALID_REQUEST` message-level rejection
without disconnect — scalar atomics stay untouched), and queue operations require server
v7 (v≤6 sessions sending `QUEUE_OP` get an `INVALID_REQUEST` message-level rejection
without disconnect, every other primitive unaffected), topic operations require
server v8 (v≤7 sessions sending `TOPIC_OP` get an `INVALID_REQUEST` message-level
rejection without disconnect, every other primitive unaffected), and condition
operations require server v9 (v≤8 sessions sending `CONDITION_OP` or an ACQUIRE
carrying the `condition` field get an `INVALID_REQUEST` message-level rejection
without disconnect, every other primitive unaffected), and phaser operations
require server v10 (v≤9 sessions sending `PHASER_OP` get an `INVALID_REQUEST`
message-level rejection without disconnect, every other primitive unaffected),
and timer operations require server v11 (v≤10 sessions sending `TIMER_OP` get
an `INVALID_REQUEST` message-level rejection without disconnect, every other
primitive unaffected); after the server is upgraded, v≤10 clients keep their
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
carry ATOMIC or BARRIER entries — or v6 snapshots carry reference payloads (`atomic_ref_*`
fields) — rolling back to older binaries falls under the same rule —
either confirm no such keys were written before rollback, or accept the state resetting to zero
(barrier generations restart from scratch; reference payloads stop being understood).
**v8 topics are exempt from this rule**: topics produce no log entries and no snapshot
fields at all (zero persistence, pinned by the zero-log and zero-snapshot-delta guard
regressions), so rollback windows are independent of topic traffic (see
[05 v8 rollback window](05-cluster-deployment.md)). **v9 conditions likewise produce no
new persistent format**: the wait set is process-volatile, snapshots gain no
condition-specific fields (`SnapshotLock` carries nothing new in v9), the signal family
never logs, and each await rides the existing `LOCK_ACQUIRE_ENTRY` — rollback leaves
**no keys to drain**, and the only constraint is active-v9-session convergence
(out-of-range handshakes failing fast, the pre-existing discipline surfacing; the
queue's "drain first", the topic's "clean by construction" and the condition's
"nothing to drain, session convergence only" are three parallel per-primitive tracks —
never conflate them; see [05 v9 rollback window](05-cluster-deployment.md)).
**v10 phasers are bound by the reverse rule (barrier/queue shape, fourth
caliber)**: the ledger is logged per mutation and written to snapshot fields
(`phaser_*`, entry type 14) — confirm no in-flight phaser traffic before
downgrading or accept phaser keys unavailable on older binaries; there is no
residency to "drain", the only pre-action is clearing in-flight traffic (parallel
to the topic/condition "clean by construction" tracks, never conflated; see
[05 v10 rollback window](05-cluster-deployment.md)). **v11 timers carry the same
shape**: arms/withdrawals log and the ledger triple plus slot rows hit snapshots
(`timer_*`, entry type 15) — clear in-flight timer traffic or accept timer keys
unavailable when rolling back; **"zero-entry expiry" is not "zero persistent
state"**, strictly separated from the clean-by-construction calibers (see
[05 v11 rollback window](05-cluster-deployment.md)).
