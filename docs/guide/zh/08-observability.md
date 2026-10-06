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
| `openlatch.server.waiters` | Gauge | — | 等待总深度（v9 起**含条件等待者**合计口径，与锁/Semaphore 等待同轨计数；topic 订阅者不计入） |
| `openlatch.server.sessions` | Gauge | — | 活跃会话数 |
| `openlatch.server.acquire.total` | Counter | `status`（协议状态码名） | 获取请求计数 |
| `openlatch.server.acquire.duration` | Timer | `result`=`granted`/`queued`/`denied` | 获取耗时分布 |
| `openlatch.server.release.total` | Counter | `status` | 释放计数 |
| `openlatch.server.renew.total` | Counter | `status` | 续租计数 |
| `openlatch.server.lease.expired.total` | Counter | — | 租约到期回收数 |
| `openlatch.server.queue.depth.max` | Gauge | — | 单键**等待队列**深度峰值（等待者口径，非元素数——与 `condition.waiters.max` 并列：条件等待者未搬运时不计入本线，搬运入队后即属本口径） |
| `openlatch.server.atomic.total` | Counter | `kind`、`op`、`status` | 原子变量操作计数（v4；v6 `kind` 增 `reference`、`op` 无 `add`；CAS 成败由应答 `applied` 承载、不占维度；超限/低版本拒绝走 `status="INVALID_REQUEST"` 线） |
| `openlatch.server.barrier.total` | Counter | `op`=`await`/`leave`/`action_done`、`status` | 循环屏障操作计数（v5；破障了结以 `status="BARRIER_BROKEN"` 单列） |
| `openlatch.server.queue.total` | Counter | `op`=`put`/`take`/`drain`/`peek`/`size`、`status` | 队列操作计数（v7；双"满"分轨：元素满立即式 `status="DENIED"`、等待深度超限 `status="OVERLOADED"`；挂起 `QUEUED`、入口拒绝 `INVALID_REQUEST` 各走其线；读写皆计） |
| `openlatch.server.elements.depth.max` | Gauge | — | 单键队列**元素深度**峰值（v7，抓取时刻采样、驻留口径含未到期项）——与 `queue.depth.max`（等待者口径）是两条互不相干的量纲线 |
| `openlatch.server.topic.total` | Counter | `op`=`subscribe`/`unsubscribe`/`publish`、`status` | topic 操作计数（v8；订阅恒立即回执、无 `QUEUED` 线；`REJECT_SUBSCRIBERS` 在带裁决单列；去重槽命中的重放照常计 `OK`；**慢消费者丢弃不走拒绝码形**，见 `topic.dropped.total`） |
| `openlatch.server.topic.dropped.total` | Counter | — | 服务端侧 drop-newest 累计丢弃条数（v8；弱背压的唯一服务端计数，持续增长=消费跟不上广播速率） |
| `openlatch.server.topic.subscribers.max` | Gauge | — | 单键 topic **订阅数**峰值（v8，抓取时刻采样）——与 `queue.depth.max`（等待者）、`elements.depth.max`（元素）并列为第三口径；订阅者 MUST NOT 计入 `waiters` |
| `openlatch.server.condition.total` | Counter | `op`=`signal`/`signal_all`/`leave`、`status` | 条件操作计数（v9；signal 家族恒即时回执——`QUEUED`/`DENIED`/`OVERLOADED` 对本线恒不可达，等待满产生于 await 折叠侧；`NOT_HELD` 权限拒绝线**单列**（signal 权限裁决的计数投影，LEAVE 幂等重放回执照常计 `OK`）；**await 计数折叠归 `acquire.total` 既有线（QUEUED/OK/OVERLOADED 各走其线），本线不设 `await` op**——await 就是 ACQUIRE 生命周期，折叠边界的计数面证据） |
| `openlatch.server.phaser.total` | Counter | `op`=`register`/`arrive`/`arrive_and_await`/`arrive_and_deregister`/`await_advance`/`cancel`/`query`、`status` | 相位器操作计数（v10）。两超限共码 `OVERLOADED` 以 `op` 分轨：`{register,OVERLOADED}`=注册配额护栏、`{await_advance/arrive_and_await,OVERLOADED}`=合并等待深度护栏（判例队列双"满"分轨的 op 侧对偶）；`QUEUED` 为 `arrive_and_await`/`await_advance` 的挂起回执正常态；`DENIED`/`BARRIER_BROKEN`/`NOT_HELD`/`REJECT_SUBSCRIBERS` 对本线恒不可达（无立即式拒绝、无破相、无持有概念） |
| `openlatch.server.phaser.parties.registered.max` | Gauge | — | 单键**注册 party 总数**峰值（v10，抓取时刻采样）——**应到集合口径而非等待口径**：与 `queue.depth.max`（等待队深）、`elements.depth.max`（元素）、`topic.subscribers.max`（订阅）、`condition.waiters.max`（条件等待）并列为**五口径**，名近义异 MUST NOT 混读；`registered` 是应到数，与挂起等待无关（后者入 `waiters` Gauge 合计） |
| `openlatch.server.condition.waiters.max` | Gauge | — | 单键**条件等待集人数**峰值（v9，抓取时刻采样，不含已搬运入队项——搬运后即转等待队列口径）——与 `queue.depth.max`（等待队深）、`elements.depth.max`（元素）、`topic.subscribers.max`（订阅）**五口径之一**（与 `queue.depth.max`/`elements.depth.max`/`topic.subscribers.max`/`phaser.parties.registered.max` 并列），名近义异 MUST NOT 混读；条件等待者计入 `waiters` Gauge 合计（对照订阅者不计入；phaser 挂起等待亦计入合计，但 `phaser.parties.registered.max` 是应到数不计入） |
| `openlatch.cluster.is_leader` | Gauge | `node_id` | 本节点是否为 Leader（仅集群启用时注册） |

### 告警参考（起步阈值，按业务校准）

- `rate(openlatch_server_lease_expired_total[5m]) > 0` 持续——有客户端续租跑不赢租约（GC/网络/时钟）；
- `openlatch_cluster_is_leader` 全零或高频翻转——选举风暴/多数派丢失，查 [09 §停摆](09-troubleshooting.md)；
- `openlatch_server_waiters` 长期高企——锁热点，考虑键拆分或读写锁改造；
- `increase(openlatch_server_acquire_total{status="NOT_LEADER"}[1m])` 陡增——Leader 切换进行中或客户端种子配置不全；
- `openlatch_server_elements_depth_max` 长期贴近 `max-queue-capacity`——热点队列持续满员（消费能力不足或消费者泄漏）；配合控制台 `queue_entries` 基数看驻留总量（v7）。
- `rate(openlatch_server_topic_dropped_total[5m])` 持续大于按业务校准的容忍线——某订阅消费跟不上广播速率（消费侧扩容/提速或按 [05 topic 治理](05-cluster-deployment.md) 拆键）；`increase(openlatch_server_topic_total{op="subscribe",status="REJECT_SUBSCRIBERS"}[5m])` 非零——订阅数达 `max-subscribers-per-key` 上限，排查订阅者泄漏（忘退订）或上调限额（v8）。
- `openlatch_server_condition_waiters_max` 长期贴近 `max-queue-depth-per-key` 合并额度——条件等待与正常获取挤占同一护栏（热点 key 的 await 堆积；超限后新 await 在 `acquire.total{OVERLOADED}` 线显形）；`increase(openlatch_server_condition_total{status="NOT_HELD"}[5m])` 持续非零——存在非持有者 signal（代码缺陷或失锁窗口后的持续 signal，双层权限的服务端权威面）；生产报"await 永睡"且日志比对到换主时点——换主窗 signal 丢失为契约面（等待是承诺、signal 是事件，无补偿），按 WATCHLIST W13 口径观察丢失率与水位，应用侧以 `await(timeout, unit)` 自救（v9）。

- `increase(openlatch_server_phaser_total{op="register",status="OVERLOADED"}[5m])` 持续非零——有键逼近 `max-parties-per-phaser` 注册配额（容量规划信号，处置为拆键或调上限）；`{op="await_advance",status="OVERLOADED"}` 增速——旁观等待逼近合并深度护栏（对照 `phaser.parties.registered.max` 水位是否同步攀升：前者高、后者低=纯等待方堆积，两者同高=热点键）（v10）；

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

指标（本篇，聚合数值）→ 控制台（[07](07-admin-console.md)，结构明细：锁表/队列/topic/会话）→
日志（节点进程输出，含看门狗与停摆事件行）。排障通常按此顺序收窄。
