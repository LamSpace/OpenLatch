## MODIFIED Requirements

### Requirement: 服务端指标清单与线路命名

服务端 SHALL 注册并暴露以下指标（Micrometer 逻辑名 → Prometheus 线路名按"点转下划线、counter 带 `_total` 尾、Timer 输出 `_seconds_bucket/_count/_sum`"的映射口径逐项钉定）：

| 逻辑名 | 类型 | 标签 |
|---|---|---|
| `openlatch.server.locks.held` | Gauge | `type` |
| `openlatch.server.waiters` | Gauge | — |
| `openlatch.server.sessions` | Gauge | — |
| `openlatch.server.acquire.total` | Counter | `status` |
| `openlatch.server.acquire.duration` | Timer | `result`（`granted`/`queued`/`denied`） |
| `openlatch.server.release.total` | Counter | `status` |
| `openlatch.server.renew.total` | Counter | `status` |
| `openlatch.server.lease.expired.total` | Counter | — |
| `openlatch.server.queue.depth.max` | Gauge | — |
| `openlatch.server.atomic.total` | Counter | `kind`（`long`/`integer`/`boolean`）、`op`（`get`/`set`/`get_and_set`/`add`/`cas`/`cas_stamped`）、`status` |
| `openlatch.cluster.is_leader` | Gauge | `node_id` |

`status` 标签值 MUST 取响应的协议状态码名（如 `OK`、`QUEUED`、`LOCK_HELD`、`NOT_LEADER`、`INVALID_REQUEST`、`INTERNAL_ERROR`）。`locks.held` 的 `type` 标签取条目家族名（`lock`/`semaphore`）——core 条目由首次请求定型家族但不携带单一协议类型（`REENTRANT`/`SIMPLE`/`FAIR` 同族互通、`READ`/`WRITE` 是请求维度），家族是两部署形态唯一一致可导出的维度。指标名与标签 MUST 在单一命名点定义，任何部署形态下落地的线路名以映射表为准。

#### Scenario: 指标逐项可见

- **WHEN** 服务器启动后抓取 `/metrics`
- **THEN** 上表十一项（`is_leader` 仅集群启用时）各至少一条线存在，名称与标签与映射口径逐项一致

#### Scenario: 未使用指标不虚构

- **WHEN** 服务运行期间从未发生续租请求
- **THEN** `openlatch_server_renew_total` 无输出线或计数为 0（依注册时机），但一旦有首次续租即出现且计数准确

#### Scenario: 原子操作计数按维度归线

- **WHEN** 依次执行一次成功的 `ATOMIC_CAS`（long 形态）与一次失败的 `ATOMIC_CAS`（integer 形态）
- **THEN** `openlatch_server_atomic_total{kind="long",op="cas",status="OK"}` 与 `{kind="integer",op="cas",status="OK"}` 各 +1（CAS 成败均为 OK 应答，成败区分由 `applied` 承载、不新增线维度）

### Requirement: Gauge 取值语义与采样安全

`locks.held{type}` MUST 统计"当前持有中"的条目数（每 key 条目计 1，`type` 取家族名 `lock`/`semaphore`）；LATCH 与 ATOMIC 条目无持有者与租约，MUST NOT 计入。`waiters` 为全部等待队列条目总数；`queue.depth.max` 为抓取时刻单 key 队列深度的最大值（采样语义，允许错过两次抓取之间的瞬时峰高）。`sessions` 为本节点活跃（已握手未断连）会话数。集群模式下 gauge 读复制态投影：Leader 上为权威值，非 Leader 上为其回放状态；`is_leader{node_id}` 依当前 Leader 视图取 1/0，单机部署 MUST NOT 注册该指标。全部 gauge 的取值 MUST 可与并发状态变更安全共现：抓取线程观察弱一致快照，MUST NOT 抛并发修改异常、MUST NOT 阻塞状态机应用或请求路径。状态机摘要（digest）的计算域 MUST NOT 因投影容器的并发化改造而改变。

#### Scenario: LATCH 不计 held

- **WHEN** 存在一个已定型归零的 latch 条目与一个被持有的互斥锁条目
- **THEN** `locks.held` 仅 `type="lock"` 线为 1，latch 条目不出现在任何 `locks.held` 线上

#### Scenario: ATOMIC 不计 held

- **WHEN** 存在一个有值有版本戳的 ATOMIC 条目与一个被持有的信号量条目
- **THEN** `locks.held` 仅 `type="semaphore"` 线计数，ATOMIC 条目不出现在任何 `locks.held` 线上

#### Scenario: 并发抓取无异常

- **WHEN** 高并发授予/释放进行中连续抓取 `/metrics` 多次
- **THEN** 全部抓取返回 200 且可解析，值单调合理；锁请求延迟不因此劣化（以既有 IT 全绿佐证）

#### Scenario: 角色指标随切换翻转

- **WHEN** 集群发生 Leader 切换后抓取两节点 `/metrics`
- **THEN** 新 Leader `is_leader=1`、旧 Leader `is_leader=0`，`node_id` 标签与本节点身份一致

#### Scenario: 摘要不受投影容器改造影响

- **WHEN** 以既有确定性测试对相同应用序列计算摘要
- **THEN** 摘要值与改造前一致，三副本摘要互等
