# 05 · Cluster Deployment & Operations

Scope: growing a single node into a 3/5-node Raft group, and routine operations (scaling,
restarts, incident handling). Client-side behavior pairs with
[03 construction](03-client-sdk.md).

## 1. Topology

| Topology | Fault tolerance | Advice |
|---|---|---|
| 3 nodes (default) | tolerate 1 failure | odd counts; start here |
| 5 nodes | tolerate 2 | spread across racks/AZs |
| single node | none | dev/demo; `enabled=false` reverts to pure single-node behavior (no Raft, no replication port) |

Each node is **the same binary + its own properties file**. One Raft group carries all lock state.

## 2. Server configuration

### 2.0 Base keys (single node and cluster)

| Key | Default | Meaning |
|---|---|---|
| `openlatch.server.port` | `9410` | business listen port |
| `openlatch.server.worker-threads` | `2 × CPU` | Netty worker threads |
| `openlatch.server.session.idle-timeout-ms` | `60000` | idle connection timeout |
| `openlatch.server.lease.default-ms` | `30000` | default lease |
| `openlatch.server.lease.min-ms` / `max-ms` | `1000` / `3600000` | lease clamp range |
| `openlatch.server.lease.tick-interval-ms` | `500` | expiry scan interval |
| `openlatch.server.queue.head-reply-timeout-ms` | `5000` | head-reply timeout after notify (an abandoned waiter keeps the head seat at most this long) |
| `openlatch.server.limit.max-key-length` | `512` | max key bytes |
| `openlatch.server.limit.max-queue-depth-per-key` | `4096` | per-key queue depth cap |
| `openlatch.server.limit.max-inflight-per-connection` | `1024` | inflight cap per connection |
| `openlatch.server.limit.max-value-bytes` | `4096` | per-key payload cap for atomic references (v6); enforced only at ingress (over-limit commands never enter the log), range [1, 512KiB]; lowering the cap never retro-affects stored values |
| `openlatch.server.limit.max-queue-capacity` | `1024` | ceiling for declared queue capacity claims (v7); an over-ceiling PUT claim is ingress-rejected with zero log entries, range [1, 65536]; combined with per-element `max-value-bytes` it bounds a key's residency |
| `openlatch.server.limit.max-drain-bytes` | `262144` | drainTo reply byte budget (v7): the DRAIN extraction limit is clamped by budget/`max-value-bytes` and the clamped N rides the log entry (apply never reads local config), range [1, 512KiB] |
| `openlatch.server.queue.ready-tick-ms` | `200` | delayed-queue ready-scan period (v7, Leader/standalone scheduler; affects wake-up latency precision only, never state adjudication), minimum 10ms |
| `openlatch.server.limit.max-subscribers-per-key` | `64` | per-key topic subscriber ceiling (v8); an over-limit SUBSCRIBE is ingress-rejected (`REJECT_SUBSCRIBERS`, existing subscribers untouched), range [1, 1024]; bounds broadcast fan-out amplification |
| `openlatch.server.limit.max-subscription-buffer` | `256` | server-side in-flight buffer depth per subscription (v8, drop-newest trigger line), range [1, 65536]; the SDK keeps a matching local tier (256) |
| `openlatch.server.metrics.enabled` | `true` | metrics admin endpoint (Prometheus scrapes `http://host:port/metrics`) |
| `openlatch.server.metrics.port` | `9412` | metrics port (`0` = ephemeral); bind conflict fails startup — distinct per node on shared hosts |
| `openlatch.server.admin.token` | unset | read-only `ADMIN_*` management token; unset ⇒ every admin request refused |

TLS/auth keys: see [06](06-security.md).

### 2.1 Cluster keys (`openlatch.cluster.*`)

Node 1 of three (others differ in `node-id`, ports, `data-dir`):

```properties
openlatch.server.port=9410
openlatch.cluster.enabled=true
openlatch.cluster.node-id=1
openlatch.cluster.peers=1@node1:9411,2@node2:9411,3@node3:9411
openlatch.cluster.client-addresses=1@node1:9410,2@node2:9410,3@node3:9410
openlatch.cluster.raft-port=9411
openlatch.cluster.data-dir=/var/lib/openlatch
openlatch.cluster.election-timeout-ms=3000
openlatch.cluster.snapshot-threshold=1000000
```

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | off → same binary behaves as a plain single node |
| `node-id` | required (≥1) | unique id; encodes session ids; immutable at runtime |
| `peers` | required | `id@host:port` voter list, includes self, identical on all nodes |
| `client-addresses` | empty (optional) | makes leader hints name directly-connectable addresses; without it discovery falls back to seed self-reporting (clustering unaffected) |
| `raft-port` | `9411` | this node's replication port |
| `data-dir` | `./data` | Raft log & snapshot dir (own disk; size ≈ write rate × retention) |
| `election-timeout-ms` | `3000` | election timeout upper bound; smaller fails over faster, larger rides out jitter |
| `snapshot-threshold` | `1000000` | snapshot cadence by applied entries; last 2 snapshots kept per node |
| `log-segment-bytes` | `0` (library default) | log segment cap — ops do not set this |

**Hard requirements**

- NTP across nodes, **drift ≤ 1s** (lease expiry is leader-driven; the 30s default dwarfs drift tolerance);
- `enabled=true` with missing/invalid required keys **fails startup naming the key** — no silent single-node fallback;
- multiple nodes on one host need distinct `openlatch.server.metrics.port` (default 9412; bind conflict fails startup; `0` = ephemeral is the easy demo setting).

## 3. Client access & leader discovery

**Provide all seeds** — the first operational rule in cluster mode:

```java
OpenLatchClient client = OpenLatchClient.builder()
        .seeds("node1:9410", "node2:9410", "node3:9410")
        .build();
```

Automatic client behavior:

- **Startup**: connect any seed → HELLO carries leader hint/address → land on the leader; a write on a Follower gets `NOT_LEADER` + hint and reroutes;
- **Leader change while you hold locks**: if your node is **alive but stepped down**, renew/release ride its forwarding lane to the new leader — **the lock survives, the connection stays up**; if your node **died**, in-flight operations fail fast, reconnect tries the old address then polls seeds, and reroutes via hints;
- 3 consecutive `NOT_LEADER` (stale hint) → concurrent `CLUSTER_VIEW` force-discovery across seeds;
- inside the switchover window acquisitions stay bounded by request/total timeouts — **fail fast first**, retry policy is yours.

### 3.1 Consistency declaration (read this)

> OpenLatch clusters guarantee "**confirmed grants are not lost; at any moment a key has at
> most one holder**". **Grants not yet replicated when a leader dies may roll back**: the old
> leader answered "granted" but never reached a quorum — after failover it does not exist;
> the client learns via `INVALID_TOKEN`/`NOT_HELD`/`SESSION_EXPIRED` and must **re-compete**
> (same trade-off as Redis master failover). **Wait queues do not migrate across leader
> changes** — waiters re-queue against the new leader; fairness is "strict FIFO within one
> leader term".

Therefore: every lock holder must implement the lock-lost callback ([03](03-client-sdk.md)) —
abort the in-flight commit and re-acquire.

### 3.2 Error-code shapes

`NOT_LEADER` only ever comes from the forwarding/role path (retryable, carries a hint);
`NOT_HELD`/`INVALID_TOKEN` only from the authoritative ownership verdict at the apply point
after quorum commit. The two shapes never overlap — client routing cannot confuse them.

## 4. Upgrades, restarts, rollback

### 4.1 Rolling order across protocol versions

Servers first, clients second (v3 servers accept v1/v2/v3 clients; a newer client against an
older server is handshake-rejected). Support matrix: [10](10-compatibility.md).

### 4.2 Planned restarts (rolling)

Restart node-by-node keeping a quorum alive at all times. The historical ordering tip
"followers first, leader last" **only lowers probability — it is not immunity**: a
low-probability replication stall exists in the underlying Raft library (section 5 below); unattended
availability is carried by the server-side self-healing watchdog, not by restart order.

### 4.3 Rollback

- Clients may roll back a version (existing locks ride the forwarding lane losslessly);
- Before rolling the server back past snapshot support: once the cluster has produced a
  snapshot (log truncated at the snapshot index), the data dir **cannot** be reopened by the
  older binary — clear & re-add, or stay on the current line.
- **v6 atomic-reference rollback window**: reference payload log entries and snapshot
  fields (`atomic_ref_*`) are v6 additions that pre-v6 binaries cannot interpret — before
  rolling back below v6, drain the reference keys (a full `set(null)` does not reclaim
  the entry, so either accept the memory residency during the rollback window, or clear
  & re-add per the previous bullet). While on the older binary, any reference-form write
  is shape-rejected at the old leader's ingress and never pollutes the replication face.
- **v7 queue rollback window**: queue entries (`queue_*` snapshot fields,
  `QUEUE_OP_ENTRY` log records, kinds 12/13) are v7 additions pre-v7 binaries
  cannot interpret — before rolling back below v7, drain queue keys to empty
  (`take`/`drainTo` everything; the entry itself stays, so accept an empty
  entry's residency or clear & re-add per the earlier bullet). On the older
  binary any `QUEUE_OP` message is rejected as an unknown message type and
  never pollutes the replication face.
- **v8 topic rollback window (clean by construction)**: topics are the first
  zero-persistence primitive — the subscription registry, buffers, dedup slots
  and `topic_seq` are Leader memory and MUST NOT reach the log or snapshots
  (pinned by standing guard regressions). Rolling back to v7 leaves nothing to
  drain: topic traffic contributed zero entries, so old binaries see no topic
  traces at all — no "clear the keys first" chore unlike v6/v7 payloads. Note
  the general mixed-version rule still applies: v8 SDK clients are rejected at
  handshake against a rolled-back v7 server (server first on the way up,
  client first on the way down), and in-flight subscriptions naturally stop
  when connections drop (same loss semantics as a leader-change window).

### Queue-dimension snapshot and log governance (v7)

- A single queue key's snapshot residency is bounded by `capacity ×
  max-value-bytes` (the element list) plus one `max-value-bytes` per session
  dedup slot (latest delivery receipt; dropped at SESSION_CLOSE). Total snapshot
  size is linear in queue keys × capacity × element ceiling and must never
  accumulate with put/take rounds (no element history ships) — a gated
  regression with full-capacity max-size elements plus delivery slots pins this.
- Log entry rate: every queue write and read rides the commit (the ATOMIC GET
  precedent), so hot queue keys are a real log-growth source; `drainTo` is the
  built-in amortizer (many elements, one entry), and delayed wake-ups/parking/
  bounces never enter the log (Leader-local). Production pressure on a hot
  queue key's entry rate triggers WATCHLIST W9's evaluation path.
- Lowering `max-value-bytes` while a queue holds oversized legacy elements can
  push an actual drain reply above the `max-drain-bytes` budget (the direct
  cost of "never retro-affect"); drain such keys first or size budgets ahead —
  the frame ceiling remains the hard backstop.

### Topic-dimension capacity governance (v8)

- Topics contribute **nothing** to snapshots and the log (guard-pinned); their
  governance surface is the **Leader write path**: fan-out amplification =
  subscribers × publish rate × message size, all of it landing on Leader heap
  buffers and socket writes;
- Two ingress limits are the capacity clamp: `max-subscribers-per-key`
  (default 64) caps per-key amplification; `max-subscription-buffer`
  (default 256) caps per-subscription residency — the heap bound per
  subscription ≈ `256 × max-value-bytes`, per key = that × subscriber count;
- Readouts: `openlatch_server_topic_dropped_total` growth rate (dropping IS
  "consumers can't keep up"), `topic.subscribers.max` watermark, the console's
  Leader-side subscriber view. On sustained drops: scale consumer parallelism,
  split topic keys, or trigger the WATCHLIST topic-row evaluation (batched
  frames / request-gated backpressure as a separate change);
- Size `data-dir`/heap estimates with an added term "active topic keys ×
  subscribers × buffer depth × mean message bytes" on the Leader; topics
  carry no durable-delivery duty — use `OBlockingQueue` for event streams that
  must survive leader restarts.

### Payload snapshot & log size governance (v6)

- The payload channel adds a **bytes** dimension to snapshot growth on top of the
  entry-count dimension: one reference entry occupies at most `2 × max-value-bytes +
  constant` in a snapshot (the current value plus the dedup slot each carry a payload);
- trigger and retention semantics **do not change with payloads** — `snapshot-threshold`
  still counts entries and each node keeps 2 snapshots; the reference-key cardinality is
  the dominant snapshot-size variable, and combined with entry permanence it is the
  residency cost operations must watch: namespace reference keys per tenant/round;
- `data-dir` capacity planning gains a term: reference keys × `max-value-bytes` × constant,
  on top of the existing write-rate × log-retention estimate;
- observability: the console/admin protocol expose per-entry payload size plus a
  constant-length truncated preview (full bytes never leave the admin plane), and the
  benchmark suite carries a "batch of exactly-capped payloads → snapshot stays within
  bounds" gate case.

## 5. Replication-stall self-healing & the supervisor (operational must-read)

The underlying Raft library can, under rolling-restart timing, freeze the new leader's
commit path with low probability. The server embeds a **replication-stall watchdog**:

- Detection: leader term older than `T_stall = max(10s, 5×election-timeout)` **and**
  commitIndex shows zero progress across 3 consecutive samples;
- Action 1 (abdicate): yield leadership once to the healthiest peer; the quorum re-elects and commits flow;
- Action 2 (escalate): still frozen inside the observation window → **the process exits with
  code 1** — an external supervisor must restart it (rejoining as a plain follower is the
  deterministic reset);
- Cooldown: one escalation per 5 minutes in-process — **configure supervisor backoff ≥ that window**;
- Log signatures: `replication stall detected` / `escalating to process restart` — record them as ops events;
- **Without a supervisor you only get the abdication half** — post-escalation needs a manual restart. Strongly prefer systemd/K8s restart policies in production.

> A second failure shape (election storm: no leader in office, nothing to abdicate from)
> presents as prolonged leaderlessness and wait-budget-exhausted client errors. Same handling
> — backed-off restart + observation; a zero-hit drill round does not eliminate it, which is
> exactly why the supervisor is the unattended-operation prerequisite.

## 6. Membership changes (programmatic API, no CLI)

Entry point is the runtime object (`ClusterRuntime`, in embedded deployments the instance
your application holds). All changes go through the current leader as single-step configs;
success means committed.

**Rules**: add before remove (the new node must be **caught up** before an old one leaves);
never move the quorum in one step (the wrapper mechanically rejects "add+remove together"
and "net voter change > 1"); already-confirmed locks are unaffected during changes (holds
and leases are replicated; writes keep committing).

```java
// add a node — three steps: join as listener → catch up → promote
leaderRuntime.subsystem().setMembers(
        List.of("1@node1:9411", "2@node2:9411", "3@node3:9411"),  // voters unchanged
        List.of("4@node4:9411"));                                  // node 4 joins as listener
// wait for catch-up (digest / CLUSTER_VIEW), then promote (+1 net, allowed):
leaderRuntime.subsystem().setMembers(
        List.of("1@node1:9411", "2@node2:9411", "3@node3:9411", "4@node4:9411"),
        List.of());

// remove a node (its sessions close via log entries; its locks release and re-compete)
leaderRuntime.removeMember(3);   // if 3 leads, transferLeadership first

// compact the log ahead of a maintenance window (any member may trigger)
leaderRuntime.subsystem().triggerSnapshot();
```

Lagging or empty-dir nodes catch up via **snapshot install + incremental replay**; during
catch-up writes return `NOT_LEADER` while reads serve normally.

## 7. Validation & drills

Recommended after deployment (and the standing regression channel):

```bash
# build the shaded jar first, then process-level drills (kill / rolling / real partition)
mvn -pl openlatch-server -am package
mvn -pl openlatch-client verify -Pdrill
# reports land in <module>/target/drill-reports/
```

Criteria (recovery <10s; both rolling orders "last window + 45s self-heal budget ⇒ zero
residual errors"; the partition minority can neither grant nor release) and full reading
guidance: [09, drill-criteria section](09-troubleshooting.md).

> **Privilege prerequisite for the partition drill** (Linux, one-time): `PartitionDrillIT`
> runs `ip`/`iptables`/`modprobe` via non-interactive `sudo -n`. A least-privilege sudoers
> snippet is the recommended setup:
>
> ```bash
> sudo tee /etc/sudoers.d/openlatch-drill >/dev/null <<'EOF'
> <user> ALL=(ALL) NOPASSWD: /usr/bin/true, /usr/sbin/ip, /usr/sbin/iptables, /usr/sbin/modprobe
> EOF
> sudo chmod 440 /etc/sudoers.d/openlatch-drill
> sudo visudo -cf /etc/sudoers.d/openlatch-drill   # must print parsed OK; verify paths with `which`
> ```
>
> Without it the suite **explicitly skips** (never fails silently) — automation should
> assert `Skipped: 0` to guard against false green. The drills are compute-sensitive
> (800 ms election windows): run numeric gates on dedicated/performance hardware —
> shared hosted runners (2 vCPU) do not meet this prerequisite, and the CI nightly is
> paused accordingly (manual dispatch retained; see ci-github-actions ledger).
