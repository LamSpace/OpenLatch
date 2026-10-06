# metrics-observability Specification

## Purpose

定义 OpenLatch 服务端的监控指标能力：指标清单与线路命名、单机与集群双路径的请求埋点语义、独立管理端口上的抓取端点与健康检查、gauge 取值语义与采样线程安全，以及配套的指标配置加载。
## Requirements

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
| `openlatch.server.atomic.total` | Counter | `kind`（`long`/`integer`/`boolean`/`reference`）、`op`（`get`/`set`/`get_and_set`/`add`/`cas`/`cas_stamped`）、`status` |
| `openlatch.server.barrier.total` | Counter | `op`（`await`/`leave`/`action_done`）、`status` |
| `openlatch.server.queue.total` | Counter | `op`（`put`/`take`/`drain`/`peek`/`size`）、`status` |
| `openlatch.server.elements.depth.max` | Gauge | — |
| `openlatch.server.topic.total` | Counter | `op`（`subscribe`/`unsubscribe`/`publish`）、`status` |
| `openlatch.server.topic.dropped.total` | Counter | — |
| `openlatch.server.topic.subscribers.max` | Gauge | — |
| `openlatch.server.condition.total` | Counter | `op`（`signal`/`signal_all`/`leave`）、`status` |
| `openlatch.server.condition.waiters.max` | Gauge | — |
| `openlatch.server.phaser.total` | Counter | `op`（`register`/`arrive`/`arrive_and_await`/`arrive_and_deregister`/`await_advance`/`cancel`/`query`）、`status` |
| `openlatch.server.phaser.parties.registered.max` | Gauge | — |
| `openlatch.cluster.is_leader` | Gauge | `node_id` |

`status` 标签值 MUST 取响应的协议状态码名（如 `OK`、`QUEUED`、`LOCK_HELD`、`NOT_LEADER`、`INVALID_REQUEST`、`INTERNAL_ERROR`；屏障在带裁决新增可取值 `BARRIER_BROKEN`；队列的在带裁决 `DENIED`（元素不可满足的立即式）与 `OVERLOADED`（等待深度超限）为既有状态码取值，双"满"语义在计数线上以状态码区分；topic 在带裁决新增可取值 `REJECT_SUBSCRIBERS`（订阅数达上限），`QUEUED`/`DENIED`/`OVERLOADED` 对 topic 恒不可达——慢消费者缓冲满不产生拒绝码形，仅入 `topic.dropped.total`）。`condition.total` 的 status 可达面为 `OK/NOT_HELD/NOT_LEADER/INVALID_REQUEST/SESSION_EXPIRED/INTERNAL_ERROR`——**`NOT_HELD` 权限拒绝线单列可观测（signal 权限裁决的计数投影）；`QUEUED`/`DENIED`/`OVERLOADED` 对 condition 恒不可达**（signal 家族恒即时回执，等待满拒绝产生于 await 折叠侧、归 `acquire.total` 线）。**await 折叠形态的计数归线钉死为 `acquire.total`（QUEUED/OK/OVERLOADED/NOT_LEADER 各走既有线）——它就是 ACQUIRE 生命周期，`condition.total` 不设 `await` op 值（折叠边界的计数面证据，防"条件自成一线"的口径漂移）**。`phaser.total` 的 status 可达面为 `OK/QUEUED/OVERLOADED/NOT_LEADER/INVALID_REQUEST/SESSION_EXPIRED/INTERNAL_ERROR`——与 condition 相反，phaser 等待不经 ACQUIRE 车道（无折叠红利可归线），其 `QUEUED`（等待登记回执）与两类超限 `OVERLOADED`（parties 上限、合并等待深度上限）**都落在本线**，判别由 `op` 标签承载（`register` 线 OVERLOADED=配额护栏、`await_advance`/`arrive_and_await` 线 OVERLOADED=深度护栏——两超限共码不共线，与 v7 队列双"满"以状态码分轨对偶：phaser 以 op 分轨）；`DENIED`/`BARRIER_BROKEN`/`REJECT_SUBSCRIBERS`/`NOT_HELD` 对 phaser 恒不可达（无持有概念、无破相概念）。`locks.held` 的 `type` 标签取条目家族名（`lock`/`semaphore`）——core 条目由首次请求定型家族但不携带单一协议类型（`REENTRANT`/`SIMPLE`/`FAIR` 同族互通、`READ`/`WRITE` 是请求维度），家族是两部署形态唯一一致可导出的维度；BARRIER、LATCH、ATOMIC（含 `reference` 形态）、QUEUE、TOPIC、**PHASER** 与**条件（骑 LOCK 家族、非独立形态）**同判例不入该 Gauge（无持有语义/无独立条目）——**PHASER 的注册数观察面由 `phaser.parties.registered.max` 承载（配额是应到集合非持有，与"held"语义正交）**。**既有 `queue.depth.max` 的口径钉死为"等待队列深度"**（等待者计数，抓取时刻单键峰值），新增 `elements.depth.max` 为"队列条目元素深度"（驻留元素数，抓取时刻单键峰值），新增 `topic.subscribers.max` 为"topic 键订阅数"（抓取时刻单键峰值），新增 `condition.waiters.max` 为"锁键条件等待集人数"（抓取时刻单键峰值，不含已搬运入队项——搬运后即转等待队列口径），新增 `phaser.parties.registered.max` 为"phaser 键注册 party 总数"（抓取时刻单键峰值，**元素数口径而非等待——与等待无关，等待面由 `waiters` 合计承载；对照 `condition.waiters.max` 防"registered 是不是等待数"的混读**）——**五者口径互注（命名点注释互引），MUST NOT 混名混义**；`waiters` Gauge 计数 MUST 涵盖队列双轨挂起等待者、**条件等待者与 phaser 挂起等待项**（v9"等待者就是等待者"合并口径经 v10 延伸——**phaser 等待项不入 `condition.waiters.max` 亦不入任何单键深度 Gauge 专项，仅入合计 `waiters`；已通知未重发的了结窗项不入合计——与条件"已搬运项不重复计数"对偶同型，了结记录非等待态亦非队列态，是有界取数窗**）且 MUST NOT 计入 topic 订阅者（订阅非等待）与已搬运项二次计数（搬运项已属等待队列口径）。队列计数线 `queue.total`、topic 计数线 `topic.total`、条件计数线 `condition.total` 与 **phaser 计数线 `phaser.total`** 均按 `{op, status}` 归线（topic 的 `subscribe`/`unsubscribe` 直发直回、`publish` 含去重槽命中重放——重放回执照常计 `OK`；signal 家族的 LEAVE 幂等重放回执同样计 `OK`；**phaser 的去重槽命中重发与了结记录命中重发亦照常计 `OK`（重放安全的幂等回执在计数面不特殊化——与 v7/v8/v9 各线同款）**；入口拒绝走各自错误码线）。指标名与标签 MUST 在单一命名点定义，任何部署形态下落地的线路名以映射表为准。

#### Scenario: 指标逐项可见

- **WHEN** 服务器启动后抓取 `/metrics`
- **THEN** 上表二十一项（`is_leader` 仅集群启用时）各至少一条线存在，名称与标签与映射口径逐项一致

#### Scenario: 未使用指标不虚构

- **WHEN** 服务运行期间从未发生续租请求
- **THEN** `openlatch_server_renew_total` 无输出线或计数为 0（依注册时机），但一旦有首次续租即出现且计数准确

#### Scenario: 原子操作计数按维度归线

- **WHEN** 依次执行一次成功的 `ATOMIC_CAS`（long 形态）、一次失败的 `ATOMIC_CAS`（integer 形态）、一次成功的 `ATOMIC_SET`（reference 形态）与一次超限被拒的 `ATOMIC_SET`（reference 形态）
- **THEN** `{kind="long",op="cas",status="OK"}`、`{kind="integer",op="cas",status="OK"}`、`{kind="reference",op="set",status="OK"}` 与 `{kind="reference",op="set",status="INVALID_REQUEST"}` 各 +1（CAS 成败均为 OK 应答、区分由 `applied` 承载；超限拒绝走错误码线）

#### Scenario: 屏障操作计数按维度归线

- **WHEN** 依次发生一次 `QUEUED` 到场、一次 TRIPPED 重发了结（`OK`）、一次破障了结（`BARRIER_BROKEN`）、一次超时离场（`leave`/`OK`）与一次执行者动作了结（`action_done`/`OK`）
- **THEN** `openlatch_server_barrier_total` 按 `{op, status}` 五线各 +1，破障计数以 `status="BARRIER_BROKEN"` 单列可观测

#### Scenario: 队列操作计数按维度归线与两满分轨

- **WHEN** 满容量队列上依次发生一次成功 `PUT`（`OK`）、一次挂起 `PUT`（`QUEUED`）、一次立即式 `offer` 被拒（`DENIED`）、一次超等待深度的挂起 `PUT`（`OVERLOADED`）、一次空队 `poll`（`DENIED`）与一次超限元素入口拒绝（`INVALID_REQUEST`），随后队列驻留深度非零
- **THEN** `openlatch_server_queue_total` 六线 `{put,OK}`、`{put,QUEUED}`、`{put,DENIED}`、`{put,OVERLOADED}`、`{take,DENIED}`、`{put,INVALID_REQUEST}` 各 +1；`openlatch_server_elements_depth_max` 反映抓取时刻单键最大元素数，且 `openlatch_server_queue_depth_max` 仍为等待者口径不受元素影响

#### Scenario: topic 操作计数归线与丢弃不入拒绝面

- **WHEN** 依次发生一次成功 SUBSCRIBE（`OK`）、一次达上限 SUBSCRIBE（`REJECT_SUBSCRIBERS`）、一次成功 PUBLISH（`OK`）、一次命中去重的同序号重发 PUBLISH（`OK`）、一次缓冲满致一条消息被丢、一次 v7 会话 TOPIC_OP（`INVALID_REQUEST`）与一次对 Follower 的 PUBLISH（`NOT_LEADER`），随后某 topic 键订阅数非零
- **THEN** `openlatch_server_topic_total` 按 `{subscribe,OK}`、`{subscribe,REJECT_SUBSCRIBERS}`、`{publish,OK}`（含重放 +2）、`{publish,INVALID_REQUEST}`、`{publish,NOT_LEADER}` 归线；`openlatch_server_topic_dropped_total` +1（该丢弃无任何拒绝应答或状态码线伴随）；`openlatch_server_topic_subscribers_max` 反映抓取时刻单键最大订阅数，且 `waiters` Gauge 不含订阅者

#### Scenario: 条件操作计数归线与 await 折叠口径

- **WHEN** 依次发生一次成功 SIGNAL（`OK`）、一次非持有者 SIGNAL（`NOT_HELD`）、一次 SIGNAL_ALL（`OK`）、一次空集 LEAVE（`OK`）、一次对 Follower 的 SIGNAL（`NOT_LEADER`）、一次 v8 会话 CONDITION_OP（`INVALID_REQUEST`）、一次成功折叠 await（`acquire` 线 `QUEUED`）与一次等待项超限的折叠 await（`acquire` 线 `OVERLOADED`），随后某锁键条件等待数非零
- **THEN** `openlatch_server_condition_total` 按 `{signal,OK}`、`{signal,NOT_HELD}`、`{signal_all,OK}`、`{leave,OK}`、`{signal,NOT_LEADER}`、`{signal,INVALID_REQUEST}` 六线归数且**无 `{*,QUEUED}`/`{*,OVERLOADED}` 线**；`openlatch_server_acquire_total` 伴随 `{QUEUED}` +2、`{OVERLOADED}` +1（await 计数全落 acquire 线，`condition.total` 不含 await op）；`openlatch_server_condition_waiters_max` 反映抓取时刻单键条件等待峰值（不含搬运项），`waiters` Gauge 含条件等待者、`queue.depth.max` 口径不因集合人数抬升

#### Scenario: phaser 操作计数归线与两超限 op 分轨

- **WHEN** 依次发生一次成功 REGISTER（`OK`）、一次达 parties 上限的 REGISTER（`OVERLOADED`）、一次成功 ARRIVE（`OK`）、一次命中去重槽的同请求重发 ARRIVE（`OK`）、一次 `AWAIT_ADVANCE` 登记（`QUEUED`）、一次超等待深度的 `AWAIT_ADVANCE`（`OVERLOADED`）、一次 `QUERY`（`OK`）、一次对 Follower 的 REGISTER（`NOT_LEADER`）与一次 v9 会话 PHASER_OP（`INVALID_REQUEST`），随后某 phaser 键 registered 非零、另一键有挂起等待项
- **THEN** `openlatch_server_phaser_total` 按 `{register,OK}`（含重放 +1）、`{register,OVERLOADED}`、`{arrive,OK}`（含重放 +1）、`{await_advance,QUEUED}`、`{await_advance,OVERLOADED}`、`{query,OK}`、`{register,NOT_LEADER}`、`{register,INVALID_REQUEST}` 归线——**两超限以 op 标签分轨（register 线=配额护栏、await_advance 线=合并深度护栏）**；`openlatch_server_phaser_parties_registered_max` 反映抓取时刻单键注册 party 峰值（非等待口径）；`waiters` Gauge 含 phaser 挂起等待项、不含 topic 订阅者与 phaser 了结窗项；`condition.waiters.max` 不受 phaser 等待影响（两集不混计）
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

### Requirement: 同宿主多节点测试夹具管理端口隔离

单机同时拉起多个服务端节点的进程级测试夹具（故障演练、混沌测试等），其每节点配置 MUST 显式设置互不相同或操作系统分配（`openlatch.server.metrics.port=0`）的管理端口，MUST NOT 依赖默认 9412 在多实例间共存（默认端口按 fail-fast 规格占用即整机启动失败，多节点必失多数派）。夹具 MUST 保持管理 HTTP 的真实绑定路径（以临时端口而非关闭指标实现隔离），使指标端点行为在被测链路上依然成立。

#### Scenario: 三节点同宿主演练可启动

- **WHEN** 进程级演练以 shaded jar 在单机拉起三节点集群
- **THEN** 三节点全部完成启动并就绪（业务/raft/管理端口零冲突），集群在选举超时内产生 Leader

#### Scenario: 滚动重启后管理端口不冲突

- **WHEN** 演练逐台重启节点（配置模板含 `metrics.port=0`）
- **THEN** 重启节点获得新临时管理端口，与存活节点无冲突
