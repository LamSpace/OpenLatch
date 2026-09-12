# 08 · 可观测性

## 管理端点（每节点 :9412）

| 路径 | 语义 |
|---|---|
| `GET /metrics` | Prometheus 文本格式全量快照；抓取不阻塞、不影响锁端口 |
| `GET /healthz` | 进程存活探测，恒 200（不探测锁语义、不依赖集群状态） |
| 其余任意路径/方法 | 404（不泄露注册表内容） |

配置：`openlatch.server.metrics.enabled`（默认 true）/ `metrics.port`（默认 9412，`0`=临时
端口；**绑定冲突即启动失败**——同机多节点务必互异）。

## 服务端指标

逻辑名 → 线路名映射规则：点转下划线、counter 加 `_total` 尾、Timer 输出
`_seconds_bucket/_count/_sum`。

| 逻辑名 | 类型 | 标签 | 含义 |
|---|---|---|---|
| `openlatch.server.locks.held` | Gauge | `type`=`lock`/`semaphore` | 当前持有量（屏障不计入家族） |
| `openlatch.server.waiters` | Gauge | — | 等待队列总深度 |
| `openlatch.server.sessions` | Gauge | — | 活跃会话数 |
| `openlatch.server.acquire.total` | Counter | `status`（协议状态码名） | 获取请求计数 |
| `openlatch.server.acquire.duration` | Timer | `result`=`granted`/`queued`/`denied` | 获取耗时分布 |
| `openlatch.server.release.total` | Counter | `status` | 释放计数 |
| `openlatch.server.renew.total` | Counter | `status` | 续租计数 |
| `openlatch.server.lease.expired.total` | Counter | — | 租约到期回收数 |
| `openlatch.server.queue.depth.max` | Gauge | — | 单键最深队列 |
| `openlatch.cluster.is_leader` | Gauge | `node_id` | 本节点是否为 Leader（仅集群启用时注册） |

### 告警参考（起步阈值，按业务校准）

- `rate(openlatch_server_lease_expired_total[5m]) > 0` 持续——有客户端续租跑不赢租约（GC/网络/时钟）；
- `openlatch_cluster_is_leader` 全零或高频翻转——选举风暴/多数派丢失，查 [09 §停摆](09-troubleshooting.md)；
- `openlatch_server_waiters` 长期高企——锁热点，考虑键拆分或读写锁改造；
- `increase(openlatch_server_acquire_total{status="NOT_LEADER"}[1m])` 陡增——Leader 切换进行中或客户端种子配置不全。

## 客户端指标

builder 注入宿主 `MeterRegistry`（Micrometer，依赖自备不传递）即启用；未注入时零开销。

| 逻辑名 | 类型 | 含义 |
|---|---|---|
| `openlatch.client.requests.total` | Counter | 请求计数（按类型/结果） |
| `openlatch.client.request.duration` | Timer | 请求耗时 |
| `openlatch.client.reconnect.total` | Counter | 重连次数 |
| `openlatch.client.locks.lost.total` | Counter | 失锁事件数——**生产环境建议对非零值告警** |

## Prometheus 抓取样例

```yaml
scrape_configs:
  - job_name: openlatch
    static_configs:
      - targets: ['node1:9412', 'node2:9412', 'node3:9412']
```

## 观察面三层

指标（本篇，聚合数值）→ 控制台（[07](07-admin-console.md)，结构明细：锁表/队列/会话）→
日志（节点进程输出，含看门狗与停摆事件行）。排障通常按此顺序收窄。
