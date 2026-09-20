# 09 · Troubleshooting & FAQ

## 1. Error-code master table

| Error (protocol code / client exception) | Origin | Meaning | What you should do |
|---|---|---|---|
| `QUEUED` | server | enqueued, awaiting notify-resend | normal queueing, no action |
| `LOCK_HELD` | server | immediate-mode try when someone else holds it | retry per business or use `lock()` |
| `NOT_LEADER` | server (forward/role path) | this node is not the leader — **retryable**, hint attached | client reroutes automatically; if persistent see section 3 |
| `NOT_HELD` | server (authoritative verdict) | this session provably does not hold the lock (post-quorum apply) | treat as lock loss: abort, re-compete |
| `INVALID_TOKEN` | server (authoritative verdict) | credentials don't match current ownership (typical: failover rollback / lease expired) | same — lock-lost handling |
| `SESSION_EXPIRED` | server | session closed | re-compete after reconnect (automatic; business gets the callback) |
| `BARRIER_BROKEN` | server (in-band verdict) | the waited generation of a cyclic barrier was broken (a party left/died/timed out or `breakBarrier()`) | treat the rendezvous as failed: abort this round; a fresh generation opens on the next arrivals |
| `INVALID_REQUEST` | server | protocol violation: bad fields, pre-handshake request, version out of range, auth failure (indistinct) | fix caller/config; auth-related → token config |
| `INTERNAL_ERROR` | server | unexpected internal failure | retryable; persistent → escalate with logs |
| `OBrokenBarrierException` | client | the waited barrier generation broke (leave-breaks contract) | abort this rendezvous round; if unintended, hunt participant timeouts under network jitter |
| `LockAcquisitionTimeoutException` | client | wait budget (default 30s) exhausted | size budgets / shorten sections / degrade |
| `OpenLatchTimeoutException` | client | one request unanswered for 5s (connection alive) | check node load/clocks; a lone blip is tolerable |
| `ServerUnavailableException` | client | connection unavailable (incl. failover fast-fail) | retry; verify seed config |
| `IllegalMonitorStateException` | client | unlock/release without holding | fix the lifecycle code path |
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
