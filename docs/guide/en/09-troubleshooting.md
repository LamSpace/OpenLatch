# 09 · Troubleshooting & FAQ

## 1. Error-code master table

| Error (protocol code / client exception) | Origin | Meaning | What you should do |
|---|---|---|---|
| `QUEUED` | server | enqueued, awaiting notify-resend | normal queueing, no action |
| `LOCK_HELD` | server | immediate-mode try when someone else holds it | retry per business or use `lock()` |
| `NOT_LEADER` | server (forward/role path) | this node is not the leader — **retryable**, hint attached | client reroutes automatically; if persistent see section 3 |
| `NOT_HELD` | server (authoritative verdict) | this session provably does not hold the lock (post-quorum apply); from v9 the code is **ambiguous by design** — on release/unlock paths it means lock loss, on `CONDITION_OP` (signal family) paths it means signal-without-ownership: discriminate by request type | split by request type: lock ops → treat as lock loss (abort, re-compete); signal → surfaces as `IllegalMonitorStateException` (fix the call site) |
| `INVALID_TOKEN` | server (authoritative verdict) | credentials don't match current ownership (typical: failover rollback / lease expired) | same — lock-lost handling |
| `SESSION_EXPIRED` | server | session closed | re-compete after reconnect (automatic; business gets the callback) |
| `BARRIER_BROKEN` | server (in-band verdict) | the waited generation of a cyclic barrier was broken (a party left/died/timed out or `breakBarrier()`) | treat the rendezvous as failed: abort this round; a fresh generation opens on the next arrivals |
| `INVALID_REQUEST` | server | protocol violation: bad fields, pre-handshake request, version out of range, auth failure (indistinct) | fix caller/config; auth-related → token config |
| `INTERNAL_ERROR` | server | unexpected internal failure | retryable; persistent → escalate with logs |
| `OBrokenBarrierException` | client | the waited barrier generation broke (leave-breaks contract) | abort this rendezvous round; if unintended, hunt participant timeouts under network jitter |
| `LockAcquisitionTimeoutException` | client | wait budget (default 30s) exhausted | size budgets / shorten sections / degrade |
| `OpenLatchTimeoutException` | client | one request unanswered for 5s (connection alive) | check node load/clocks; a lone blip is tolerable |
| `ServerUnavailableException` | client | connection unavailable (incl. failover fast-fail) | retry; verify seed config |
| `IllegalMonitorStateException` | client | unlock/release/signal without holding (v9: signaling has two same-shaped sources — the local pre-check, or the server's `NOT_HELD` mapped to this exception) | fix the lifecycle code path |
| `TimeoutException` (phaser `awaitAdvanceInterruptibly` expiry) | client | the phase did not advance beyond the observed value within the budget (a best-effort CANCEL was sent first) | enlarge the budget or loop re-entering; same bounded-wait family as the phaser `OpenLatchTimeoutException` (v10) |

| `LockLostException` (callback) | client | the lock was taken away | abort the in-flight commit — by-design obligation |

## 2. Failover behavior baseline (measured)

Magnitudes from process-level drills on reference hardware (3 nodes, one host) — calibrate
alerts against these:

- `kill -9` leader → new grants recover in **1.6–1.7s measured** (criterion: <10s);
- killing a follower: no client-visible impact (quorum intact);
- rolling restarts: visible errors cluster into 2–3s windows per node; after the last
  window + 45s self-heal budget, **residual errors must be 0**;
- queue positions reshuffle on leader change (expected, not an incident).

## 3. Repeated `NOT_LEADER` / no leader found

Work the list in order:

1. **All seeds given?** A single dead seed with no alternates = no automatic recovery;
2. `client-addresses` matching the real reachable endpoints (a wrong hint costs one extra
   seed-fallback round, not correctness);
3. quorum actually alive: `curl :9412/metrics | grep is_leader` — all zero means leaderless;
4. leaderless + frequent elections → clock drift or partition (NTP drift must be ≤1s);
5. a leader exists but writes keep failing and the log shows `replication stall detected` → section 4.

## 4. Replication stall (frozen writes) and self-healing logs

Watchdog log sequence (on the stalled leader):

```
replication stall detected          # verdict: term age past threshold + zero commit progress
... leadership abdicated ...        # action 1: yield to the freshest peer; quorum re-elects
escalating to process restart       # action 2: still frozen -> process exits with code 1, awaits supervisor
```

Operational moves:

- **with a supervisor (systemd/K8s)**: ensure restart backoff ≥ 5 minutes (in-process
  cooldown); the event self-heals — record it;
- **without**: the abdication half still self-heals; after `escalating to process restart`
  the node **needs a manual restart** — production clusters should have a supervisor
  ([05 stall section](05-cluster-deployment.md));
- client-side during a stall: bursts of acquisition timeouts / `ServerUnavailable` that go
  away on recovery. Errors persisting past "last window + budget" are **not** an ops
  problem but a product regression — file it with logs.

## 5. Deployment/build gotchas

| Symptom | Cause | Fix |
|---|---|---|
| startup fails with a metrics-port bind error (log line `startup failed: ... 9412 ...` / "address already in use") | metrics-port clash on a shared host (fail-fast by design) | distinct `metrics.port` per node, or `0` |
| drill "passes" without doing anything | missing shaded jar / missing sudo ⇒ **assume-skip** (not a failure) | `mvn -pl openlatch-server -am package` first; check `Skipped: 0` |
| Spring annotation does nothing | no `-parameters`; or self-invocation past the proxy | set the compiler flag; call via proxy |
| second same-thread `lock()` stalls to timeout | SIMPLE lock (non-reentrant by design) | switch to REENTRANT |
| client errors on first call after boot | server down / address unreachable — bean creation never blocks on connect | verify 9410 reachability |
| "the lock vanished on its own" | renewal lost the race (long GC/pause) or session closed | watch the callback + `locks.lost`; shorten sections / raise lease |
| a specific lock lost after failover | un-replicated grant rolled back | by design ([05 consistency declaration](05-cluster-deployment.md)) — implement the callback |

## 6. Drill criteria quick reference (cluster self-check)

```bash
mvn -pl openlatch-server -am package
mvn -pl openlatch-client verify -Pdrill        # kill / rolling / partition (partition needs passwordless sudo)
mvn -pl openlatch-server verify -Pdrill        # stall sampler (instrument, not a gate)
# reports: <module>/target/drill-reports/<suite>-<date>.md
```

Criteria: failover recovery <10s with no double-award; rolling both orders "last window +
45s budget ⇒ zero residual"; partitioned minority cannot grant or release, lock survives,
auto-convergence after healing; sampler red only if all rounds ABORTED.

## Queue (v7) triage cheat sheet

- **`offer` returning `false` vs an `OVERLOADED` throw**: the first is the normal
  element-full immediate outcome (queue full, reject like JDK `offer`); the second
  means the **waiter queue** for that key hit `max-queue-depth-per-key` — parking
  requests are being rejected. Both signal consumers falling behind producers, at
  different severities (the second says the backlog itself has grown);
- **`put`/`take` returning nothing for a long time**: check three things — (1) whether
  the other side really frees capacity / delivers elements (console queue section:
  depth plus the two waiter tracks); (2) Leader stability — parkings die with the
  term and are re-parked by the client (ranks reset per term, fairness holds within
  one); (3) on delay queues the due-time wake is tick-granular (`ready-tick-ms`,
  ~200ms): if nothing is ever delivered and depth includes unexpired elements, audit
  the business-side `delay` inputs (absolute expiry folds from server entry clocks,
  not client clocks);
- **`OpenLatchException(INVALID_REQUEST)` on a queue op** means one of: over-large
  element (beyond `max-value-bytes`), mismatched capacity claim, QUEUE vs DELAY_QUEUE
  form conflict on one key, or a v≤6 session — the status text pinpoints which;
- **"elements nobody consumed still occupy memory"**: by contract — elements bind to
  the key, nothing reclaims them server-side. Scheduled cleanup rides on business key
  naming (round/tenant prefixes) plus the console's residency readouts.

## Topic (v8) triage quick sheet

- **"Messages stopped after a leader change" — split the diagnosis in three**:
  (1) check the SDK reconnect logs — re-registration rides connection re-establishment
  (a 30s keep-alive sweep covers the "same-node re-election without a dropped
  connection" case), and the gap window is never replayed (at-most-once contract);
  (2) confirm the subscription reappeared in the Leader's registry (console /
  `ADMIN_KEY_DETAIL` topic section); (3) if registered yet still silent, compare
  `openlatch_server_topic_dropped_total` growth with the subscriber's
  `droppedCount()` — drops from a full buffer are contract behavior, not faults;
- **`REJECT_SUBSCRIBERS`**: the key hit `max-subscribers-per-key` (default 64) —
  first hunt leaked subscriptions (live processes that never `unsubscribe()`);
  existing subscribers are untouched by the rejection (no eviction, no disconnect);
- **`droppedCount()` climbing**: slow-consumer signal (either buffer tier). Fix by
  moving heavy work out of the handler, scaling consumer parallelism, or splitting
  the topic key. Remember it is a same-term gap estimate — it rebases on
  re-subscription and never accumulates across terms;
- **`OpenLatchException(INVALID_REQUEST)` on a topic op** means one of: over-large
  or missing message body, shape violation (payload/op_seq mismatched with the op),
  a colliding key occupied by another family (best-effort probe), or a v≤7 session —
  the status text pinpoints which;
- **Suspected double delivery**: under at-most-once the only source is a publish
  **retry crossing a leader change** (old Leader fanned out, new Leader's dedup
  slot is empty). Compare the two deliveries' `publisherSessionId` and term
  boundary; consumer idempotence is a contract obligation, not a server defect;
- **Empty topic readouts on followers**: the registry is Leader-local — follower
  SUMMARY shows `topic_entries=0`, keys lists omit topic rows and details answer
  an honest NOT_HELD. This is faithful presentation, not data loss.

## Condition (v9) triage quick sheet

- **"Await never wakes" — split the diagnosis in three**: (1) **signal denied
  by permission** — check the `openlatch_server_condition_total{status="NOT_HELD"}`
  line: the signaler was not the holder at that moment (calling without holding
  throws locally and sends nothing; a request that reached the wire means an
  ownership-already-lost window, e.g. signaling after lock loss) — fix the
  permission path; (2) **signal lost in a leader-change window** — compare the
  await re-registration log against the leader-change timestamp: waiting is a
  promise, signal is an event — in-window signals are never replayed or
  compensated, and a re-registered waiter waits for the next signal or its
  timeout. A match means contract behavior, not a fault: self-rescue with
  `await(timeout, unit)` and observe per WATCHLIST W13; (3) **a missing guard
  loop** — spurious wake-ups are allowed by contract (head-timeout sweep
  promotions, LEAVE/SIGNAL race convergence), and a bare await mistakes a wake
  for a true predicate, looking exactly like "the signal got lost": audit the
  caller's `while (!predicate)` guard first;
- **Discriminating the two faces of `NOT_HELD`**: one code, two natures — on
  the release/unlock path it is **lock loss** (abort the commit, re-compete);
  on the `CONDITION_OP` path it is **signal-without-ownership** (mapped to
  `IllegalMonitorStateException`, fix the call site). The only discriminating
  surface is the request type; cross-read the separate `acquire.total` and
  `condition.total` lines to localize it;
- **`OVERLOADED` on an await**: condition waiters + the wait queue together hit
  the merged `max-queue-depth-per-key` (zero new config, one shared guardrail)
  — same basis as the queue's "waiter-full": shed parked load or raise the
  limit; note the throwing thread does **not** hold the lock;
- **Holder died yet waiters keep sleeping**: by contract — lease-expiry and
  death sweeps wake the wait-queue entrants only and **never signal for
  condition waiters**; production code should use timed awaits
  ([03 conditions](03-client-sdk.md));
- **`INVALID_REQUEST` / `UnsupportedOperationException` on condition ops** means
  one of: empty or over-`max-key-length` condition name, a read/write-form
  acquisition carrying the `condition` field, `CONDITION_OP` on a non-LOCK
  family key, a shape-matrix violation (`thread_id`/`await_request_id`
  mismatched with the op), or a v≤8 session — the status text pinpoints which;
  `newCondition` on a read/write lock handle is a **local**
  `UnsupportedOperationException` (zero requests);
- **Zero condition readouts on followers**: the wait set is Leader-local state,
  so follower KEY_DETAIL condition sections honestly read zero (the same
  Leader-only surface as the wait queue) — faithful presentation, not data
  loss.

## Phaser (v10) triage quick sheet

- **"The phase won't advance" — three-way split** (an `arriveAndAwaitAdvance`/
  `awaitAdvance` that never resolves): ① **arrivals missing** — compare
  `getArrivedParties()` with `getRegisteredParties()` (or the admin phaser
  section): registered members that are alive but doing other work simply
  haven't arrived; a dead member whose quota has not yet been swept means its
  SESSION_CLOSE hasn't landed (within the probe window this is expected, not a
  defect — the sweep trips the phase on the spot); ② **lane window** —
  mutations ride the leader-routed direct lane and can time out explicitly
  during leader change (the W11 shape): resending the same request id is
  idempotent-safe; persistent NOT_LEADER/timeout falls back to section 3; the
  wait side self-heals via chunked re-sends and is not a second exposure;
  ③ **application bookkeeping** — a rejected `arriveAndDeregister` overdraft
  means that session already spent its quota while the code still waits for a
  rendezvous that now needs one arrival less/more than assumed: audit the
  per-session register/deregister pairing;
- **`OVERLOADED` tells you which gate by request type**: on REGISTER it is
  `max-parties-per-phaser` (existing quotas untouched — split the key or raise
  the cap); on AWAIT_ADVANCE it is the merged `max-queue-depth-per-key` wait
  budget (`arriveAndAwaitAdvance`'s wait half is never refused — its arrival
  already counts);
- **The "scheduled mark never rings" trichotomy** (`await` hangs or `isFired`
  stays false): ① **tick latency and the gate** — wake precision is one
  `timer-ready-tick-ms` window (default 200ms), "just past the instant, still
  parked" is in-contract; check the `timer.total` lines — `INVALID_REQUEST`
  growth usually means v≤10 sessions hitting the v11 gate (confirm both ends
  upgraded), `{await,OVERLOADED}` growth means the depth guardrail is
  refusing new waits; ② **generation was re-armed** — read the admin triple:
  `timer_generation` advanced while `timer_fire_at_ms` sits later than you
  expected = someone re-armed (newest generation wins; "never armed" is a key
  miss / generation 0, a different diagnosis); `timer_armed=false` = the
  generation was disarmed (new `await`s settle DENIED/exception at once — no
  sleeping on a withdrawn clock); ③ **failover re-hang** — the wake predicate
  lives in the replicated ledger and re-hangs self-heal (unlike the
  condition's signal-loss window, a timer's "not ringing" is rooted in
  "the clock was moved / the precision window", not a lost event); two machines
  waking in different orders = the declared clock-skew surface — judge by the
  raw projected `fire_at_ms`, not each node's instantaneous `isFired` (v11);
- **Late cross-trip resends count as new arrivals**: a lost reply re-sent after
  two further trips rolls out of the previous-generation window and is counted
  afresh (declared race). React within `requestTimeout`-scale latency; never
  build correctness on "exactly one arrival" — cross-check with the counters;
- **A wait "missed its wake-up"? Rule out the predicate first**: phasers have
  no signal — the wake predicate is the ledger phase. If the phase really
  hasn't advanced, it's case ①/③; if it advanced but your call returned by
  timeout, that is the bounded-wait contract (`awaitAdvance(int)` caps at the
  wait budget) — loop to re-enter. **Contrast with the condition's signal loss
  window**: here no event is lost; if it looks like a lost wake-up, the root
  cause is ①/②/③, never the contract;
- **Reads are independent round trips**: `getPhase()` then
  `getArrivedParties()` are two QUERYs — between them the phase may advance,
  so a naive diff can look "negative". Anchor reconciliation on the monotonic
  phase, not on cached cross-read deltas.

## 7. FAQ

**Q: Can I use it for distributed transactions?** No — locks are coordination primitives
with no isolation levels. Pair critical writes with fencing credentials (e.g. a version
number written under the same lock) or use actual consensus storage.

**Q: Is there a key-count limit?** No hard cap (per-connection inflight and per-key queue
depth are bounded — see config surfaces). Lock/semaphore entries are reclaimed when the last
holder releases; latch entries persist until node restart — namespace barrier keys per round
([03](03-client-sdk.md)).

**Q: Can leadership be moved deliberately?** Yes — `transferLeadership` on the runtime yields
to the healthiest peer; do it before touching the current leader's node.

**Q: Do reads go through the leader?** No — stats/detail/`CLUSTER_VIEW` answer locally from
the connected replica (weakly consistent, may lag briefly). State-changing writes always
commit through the leader's quorum.
