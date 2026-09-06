# metrics-observability Delta Specification

## Purpose

定义 OpenLatch 服务端的监控指标能力：指标清单与线路命名、单机与集群双路径的请求埋点语义、独立管理端口上的抓取端点与健康检查、gauge 取值语义与采样线程安全，以及配套的指标配置加载。

## ADDED Requirements

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
| `openlatch.cluster.is_leader` | Gauge | `node_id` |

`status` 标签值 MUST 取响应的协议状态码名（如 `OK`、`QUEUED`、`LOCK_HELD`、`NOT_LEADER`、`INVALID_REQUEST`、`INTERNAL_ERROR`）。`locks.held` 的 `type` 标签取条目家族名（`lock`/`semaphore`）——core 条目由首次请求定型家族但不携带单一协议类型（`REENTRANT`/`SIMPLE`/`FAIR` 同族互通、`READ`/`WRITE` 是请求维度），家族是两部署形态唯一一致可导出的维度。指标名与标签 MUST 在单一命名点定义，任何部署形态下落地的线路名以映射表为准。

#### Scenario: 指标逐项可见

- **WHEN** 服务器启动后抓取 `/metrics`
- **THEN** 上表十项（`is_leader` 仅集群启用时）各至少一条线存在，名称与标签与映射口径逐项一致

#### Scenario: 未使用指标不虚构

- **WHEN** 服务运行期间从未发生续租请求
- **THEN** `openlatch_server_renew_total` 无输出线或计数为 0（依注册时机），但一旦有首次续租即出现且计数准确

### Requirement: 指标配置与管理端点生命周期

指标经独立配置类从同一 Properties 文件加载：`openlatch.server.metrics.enabled`（默认 `true`）、`openlatch.server.metrics.port`（默认 `9412`，允许 `0` 表示操作系统分配临时端口）。非法值 MUST 启动时快速失败并给出明确错误信息；既有配置项与构造面 MUST NOT 变化。启用时管理 HTTP 服务 MUST 随服务器启动绑定端口、随关停序列解除绑定；绑定失败 MUST 与锁端口冲突同策略（启动失败退出，不进入半启动）。关闭时管理端口 MUST NOT 监听。

#### Scenario: 默认启用与端口

- **WHEN** 未提供指标配置启动服务器
- **THEN** 指标启用并监听 9412，锁端口监听不受影响

#### Scenario: 显式关闭

- **WHEN** 配置 `openlatch.server.metrics.enabled=false`
- **THEN** 管理端口不监听；埋点照常累积但不对外服务不构成错误（关闭即不抓取）

#### Scenario: 关停解除绑定

- **WHEN** 服务器收到关停信号
- **THEN** 管理端口先于或随同资源回收解除监听，进程在有限时间内退出

### Requirement: 管理端点仅两路径

管理端口 SHALL 仅暴露两个路径：`GET /metrics` 返回 Prometheus 文本格式的全量注册表快照（HTTP 200）；`GET /healthz` 返回 HTTP 200（进程存活即可，MUST NOT 探测锁语义或依赖状态）。其余任何路径与方法 MUST 返回 404（非 `/metrics` 路径 MUST NOT 泄露注册表内容）。`/metrics` 抓取 MUST NOT 阻塞或影响锁端口的请求处理。

#### Scenario: 抓取返回可解析文本

- **WHEN** Prometheus 语法解析器处理 `GET /metrics` 响应体
- **THEN** 解析成功且含指标线

#### Scenario: 未知路径 404

- **WHEN** `GET /`、`GET /admin` 或 `POST /metrics`
- **THEN** 返回 404（`POST /metrics` 不执行抓取）

#### Scenario: 健康检查恒简单

- **WHEN** 服务器已启动且无任何客户端连接
- **THEN** `GET /healthz` 返回 200

### Requirement: 请求埋点覆盖单机与集群双路径

获取/释放/续租的计数与获取耗时埋点 MUST 在单机与集群两种装配下由同一指标词表记录：任何一条经服务端受理并应答的业务请求 MUST 恰好计入一次对应 `*.total{status}`；因部署形态、路径差异或指标启用与否 MUST NOT 改变请求的应答行为。集群模式下非 Leader 节点对写请求的 `NOT_LEADER` 拒绝同样 MUST 计入 `acquire.total{status="NOT_LEADER"}`。

#### Scenario: 集群路径非盲

- **WHEN** 集群部署下客户端向 Leader 完成一次获取与释放
- **THEN** Leader 的 `acquire.total{status="OK"}` 与 `release.total{status="OK"}` 各 +1，`acquire.duration_seconds_count{result="granted"}` +1

#### Scenario:  follower 拒绝计数

- **WHEN** 集群部署下客户端向非 Leader 节点发起获取请求并收到 `NOT_LEADER`
- **THEN** 该节点 `acquire.total{status="NOT_LEADER"}` +1

#### Scenario: 指标不影响行为

- **WHEN** 同一请求脚本分别在 `metrics.enabled=true/false` 下执行
- **THEN** 全部协议应答逐字段一致

### Requirement: 耗时与到期计数口径

获取耗时 `acquire.duration{result}`：单机口径为分发处理起止；集群口径 MUST 为请求受理至应答写回（含 Raft 提交等待），`result` 按应答结果取 `granted`/`queued`/`denied`（提交失败与拒绝同归 `denied`）。租约到期强制释放计数 `lease.expired.total`：单机按每轮扫描实际释放数累加；集群按到期条目在状态机应用侧的实际释放计数，同一到期事件的落地 MUST NOT 计数超过一次；提交后未落地或守卫跳过的到期条目 MUST NOT 计数。节点重启后该计数从零起算（重启期内日志回放产生的重复计入为已声明的可接受偏差）。

#### Scenario: 单机到期计数

- **WHEN** 两个短租约持有到期被同一轮扫描释放
- **THEN** `lease.expired.total` 增加 2

#### Scenario: 集群到期计数恰好一次

- **WHEN** 集群下一个租约到期、到期条目经共识落地释放
- **THEN** 每个副本节点的 `lease.expired.total` 各 +1（本地观察值），不随在途重试增加

#### Scenario: 集群耗时含提交

- **WHEN** 集群下一次获取经 Raft 提交后授予
- **THEN** 该请求计入 `result="granted"` 且观测样本时长覆盖提交等待区间

### Requirement: Gauge 取值语义与采样安全

`locks.held{type}` MUST 统计"当前持有中"的条目数（每 key 条目计 1，`type` 取家族名 `lock`/`semaphore`）；LATCH 条目无持有者与租约，MUST NOT 计入。`waiters` 为全部等待队列条目总数；`queue.depth.max` 为抓取时刻单 key 队列深度的最大值（采样语义，允许错过两次抓取之间的瞬时峰高）。`sessions` 为本节点活跃（已握手未断连）会话数。集群模式下 gauge 读复制态投影：Leader 上为权威值，非 Leader 上为其回放状态；`is_leader{node_id}` 依当前 Leader 视图取 1/0，单机部署 MUST NOT 注册该指标。全部 gauge 的取值 MUST 可与并发状态变更安全共现：抓取线程观察弱一致快照，MUST NOT 抛并发修改异常、MUST NOT 阻塞状态机应用或请求路径。状态机摘要（digest）的计算域 MUST NOT 因投影容器的并发化改造而改变。

#### Scenario: LATCH 不计 held

- **WHEN** 存在一个已定型归零的 latch 条目与一个被持有的互斥锁条目
- **THEN** `locks.held` 仅 `type="lock"` 线为 1，latch 条目不出现在任何 `locks.held` 线上

#### Scenario: 并发抓取无异常

- **WHEN** 高并发授予/释放进行中连续抓取 `/metrics` 多次
- **THEN** 全部抓取返回 200 且可解析，值单调合理；锁请求延迟不因此劣化（以既有 IT 全绿佐证）

#### Scenario: 角色指标随切换翻转

- **WHEN** 集群发生 Leader 切换后抓取两节点 `/metrics`
- **THEN** 新 Leader `is_leader=1`、旧 Leader `is_leader=0`，`node_id` 标签与本节点身份一致

#### Scenario: 摘要不受投影容器改造影响

- **WHEN** 以既有确定性测试对相同应用序列计算摘要
- **THEN** 摘要值与改造前一致，三副本摘要互等
