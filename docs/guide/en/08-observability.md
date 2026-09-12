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
| `openlatch.server.waiters` | Gauge | — | total queue depth |
| `openlatch.server.sessions` | Gauge | — | active sessions |
| `openlatch.server.acquire.total` | Counter | `status` (protocol status name) | acquisition outcomes |
| `openlatch.server.acquire.duration` | Timer | `result`=`granted`/`queued`/`denied` | acquisition latency distribution |
| `openlatch.server.release.total` | Counter | `status` | releases |
| `openlatch.server.renew.total` | Counter | `status` | renewals |
| `openlatch.server.lease.expired.total` | Counter | — | lease-expiry reclaims |
| `openlatch.server.queue.depth.max` | Gauge | — | deepest single-key queue |
| `openlatch.cluster.is_leader` | Gauge | `node_id` | leadership gauge (registered in cluster mode only) |

### Starter alerts (calibrate per workload)

- `rate(openlatch_server_lease_expired_total[5m]) > 0` sustained — renewals losing the race
  (GC / network / clock);
- `openlatch_cluster_is_leader` all-zero or flapping — election storm / quorum loss, see
  [09 stall section](09-troubleshooting.md);
- persistently high `openlatch_server_waiters` — hot key, shard it or split read/write;
- spiking `increase(openlatch_server_acquire_total{status="NOT_LEADER"}[1m])` — leader
  changes in flight or incomplete seed config.

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
tables/queues/sessions) → node logs (watchdog & stall event lines). Troubleshooting usually
descends in this order.
