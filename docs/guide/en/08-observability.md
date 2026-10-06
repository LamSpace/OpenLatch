# 08 · Observability

## Admin endpoints (per node, :9412)

| Path | Semantics |
|---|---|
| `GET /metrics` | Prometheus text-format snapshot of the full registry; scraping never blocks the business port |
| `GET /healthz` | liveness, always 200 (never probes lock semantics or cluster state) |
| anything else | 404 (no registry leakage on unknown paths) |

Config: `openlatch.server.metrics.enabled` (default true) / `metrics.port` (default 9412,
`0` = ephemeral; **a bind conflict fails startup** — distinct ports per node on shared hosts).

## Server metrics

Logical name → wire name: dots to underscores, counters get a `_total` suffix, timers emit
`_seconds_bucket/_count/_sum`.

| Logical name | Type | Labels | Meaning |
|---|---|---|---|
| `openlatch.server.locks.held` | Gauge | `type`=`lock`/`semaphore` | current held volume by family |
| `openlatch.server.waiters` | Gauge | — | total waiting depth (v9: **includes condition waiters** under one merged count, same track as lock/semaphore waits; topic subscribers are never included) |
| `openlatch.server.sessions` | Gauge | — | active sessions |
| `openlatch.server.acquire.total` | Counter | `status` (protocol status name) | acquisition outcomes |
| `openlatch.server.acquire.duration` | Timer | `result`=`granted`/`queued`/`denied` | acquisition latency distribution |
| `openlatch.server.release.total` | Counter | `status` | releases |
| `openlatch.server.renew.total` | Counter | `status` | renewals |
| `openlatch.server.lease.expired.total` | Counter | — | lease-expiry reclaims |
| `openlatch.server.queue.depth.max` | Gauge | — | peak per-key **wait-queue** depth (waiter count, not elements — beside `condition.waiters.max`: un-carried condition waiters are never counted here, while carried ones join this count with the queue) |
| `openlatch.server.atomic.total` | Counter | `kind`, `op`, `status` | atomic variable operations (v4; v6 adds `kind="reference"` — no `add` op for it; CAS hit/miss rides the `applied` field, not a label; over-limit/low-version rejections land on `status="INVALID_REQUEST"`) |
| `openlatch.server.barrier.total` | Counter | `op`=`await`/`leave`/`action_done`, `status` | cyclic barrier operations (v5; broken settlements surface as `status="BARRIER_BROKEN"`) |
| `openlatch.server.queue.total` | Counter | `op`=`put`/`take`/`drain`/`peek`/`size`, `status` | queue operations (v7; the two "fulls" split: element-full immediate `status="DENIED"` vs waiter-queue-full `status="OVERLOADED"`; parking `QUEUED`, ingress rejects `INVALID_REQUEST`; reads count too) |
| `openlatch.server.elements.depth.max` | Gauge | — | peak per-key **element depth** (v7, scrape-time sample, residency incl. unexpired delay items) — a different dimension from `queue.depth.max` (waiter depth); never conflate the two |
| `openlatch.server.topic.total` | Counter | `op`=`subscribe`/`unsubscribe`/`publish`, `status` | topic operations (v8; subscribes are always immediate — no `QUEUED` line; `REJECT_SUBSCRIBERS` is its own in-band line; dedup-slot replays count as `OK`; **slow-consumer drops never take a rejection shape** — see `topic.dropped.total`) |
| `openlatch.server.topic.dropped.total` | Counter | — | server-side drop-newest losses (v8; the only server-side loss counter — sustained growth means consumers can't keep up) |
| `openlatch.server.topic.subscribers.max` | Gauge | — | peak per-key **subscriber count** (v8, scrape-time) — the third distinct depth dimension beside `queue.depth.max` (waiters) and `elements.depth.max` (elements); subscribers are never counted in `waiters` |
| `openlatch.server.condition.total` | Counter | `op`=`signal`/`signal_all`/`leave`, `status` | condition operations (v9; the signal family is always immediate — `QUEUED`/`DENIED`/`OVERLOADED` are unreachable on this line since waiter-full rejects arise on the await fold side; `NOT_HELD` is its **own permission line** (the count projection of signal-authority verdicts; idempotent LEAVE replays count as `OK`); **await counts fold into the existing `acquire.total` lines (QUEUED/OK/OVERLOADED each on their track) and this metric has no `await` op** — an await is exactly an ACQUIRE lifecycle, the counting-side evidence of the fold boundary) |
| `openlatch.server.condition.waiters.max` | Gauge | — | peak per-key **condition wait-set size** (v9, scrape-time, carried items excluded — they join the wait-queue dimension after a carry) — the fourth member of the **four-dimension distinction** beside `queue.depth.max` (wait-queue depth), `elements.depth.max` (elements) and `topic.subscribers.max` (subscriptions): names are near, meanings are not — never conflate; condition waiters ARE counted in the `waiters` gauge (unlike subscribers) |
| `openlatch.cluster.is_leader` | Gauge | `node_id` | leadership gauge (registered in cluster mode only) |

### Starter alerts (calibrate per workload)

- `rate(openlatch_server_lease_expired_total[5m]) > 0` sustained — renewals losing the race
  (GC / network / clock);
- `openlatch_cluster_is_leader` all-zero or flapping — election storm / quorum loss, see
  [09 stall section](09-troubleshooting.md);
- persistently high `openlatch_server_waiters` — hot key, shard it or split read/write;
- spiking `increase(openlatch_server_acquire_total{status="NOT_LEADER"}[1m])` — leader
  changes in flight or incomplete seed config;
- `openlatch_server_elements_depth_max` pinned near `max-queue-capacity` — a hot queue
  running full (consumers lagging or leaking); read it together with the console's
  `queue_entries` cardinality for total residency (v7).
- `rate(openlatch_server_topic_dropped_total[5m])` above your business-calibrated
  tolerance — some subscription can't keep up with the broadcast rate (scale the
  consumer, or split the topic key per [05 topic governance](05-cluster-deployment.md));
  nonzero `increase(openlatch_server_topic_total{op="subscribe",status="REJECT_SUBSCRIBERS"}[5m])`
  — the per-key subscriber cap is reached: hunt leaked subscriptions (missing
  `unsubscribe`) or raise `max-subscribers-per-key` (v8).
- `openlatch_server_condition_waiters_max` pinned near the merged
  `max-queue-depth-per-key` — condition waiters and normal acquisitions
  squeezing the same guardrail (a hot key's await buildup; over the limit new
  awaits surface on the `acquire_total{status="OVERLOADED"}` line);
  sustained nonzero `increase(openlatch_server_condition_total{status="NOT_HELD"}[5m])`
  — someone signals without holding (code defect, or continued signaling after
  lock loss — the authoritative layer of the two-layer permission check);
  production reports of "await never wakes" corroborated against a
  leader-change timestamp — in-window signal loss is contract behavior
  (waiting is a promise, signal is an event; no compensation): observe per
  the WATCHLIST W13 basis (loss rate + watermark) and self-rescue with
  `await(timeout, unit)` on the application side (v9).

## Client metrics

Enable by injecting a host `MeterRegistry` (Micrometer is compile-optional, not transitive);
zero cost when unset.

| Logical name | Type | Meaning |
|---|---|---|
| `openlatch.client.requests.total` | Counter | requests by type/outcome |
| `openlatch.client.request.duration` | Timer | request latency |
| `openlatch.client.reconnect.total` | Counter | reconnect count |
| `openlatch.client.locks.lost.total` | Counter | lock-loss events — **alert on non-zero in production** |

## Prometheus scrape sample

```yaml
scrape_configs:
  - job_name: openlatch
    static_configs:
      - targets: ['node1:9412', 'node2:9412', 'node3:9412']
```

## Three observation layers

Metrics (this page, aggregates) → console ([07](07-admin-console.md), structural detail:
tables/queues/topics/sessions) → node logs (watchdog & stall event lines). Troubleshooting usually
descends in this order.
