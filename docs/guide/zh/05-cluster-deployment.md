# 05 · 集群部署与运维

适用：把单机服务升级为 3/5 节点 Raft 复制组，以及日常运维（扩缩容、重启、故障处置）。
客户端行为面（改道、发现）请配合 [03 §构造](03-client-sdk.md) 阅读。

## 1. 拓扑选择

| 拓扑 | 容错 | 建议 |
|---|---|---|
| 3 节点（默认） | 容忍 1 节点故障 | 奇数节点；写吞吐随节点数略降、容错上升，5 节点为高规模场景 |
| 5 节点 | 容忍 2 节点故障 | 跨机架/可用区摆放时选它 |
| 单节点 | 无 | 开发/演示；`enabled=false` 即回退纯单机行为（不启 Raft、不监听复制端口） |

每节点是**同一份二进制 + 一份自己的 properties**。一个 Raft 副本组承载全部锁状态。

## 2. 服务端配置

### 2.0 基础键（单机与集群通用）

| 键 | 默认 | 说明 |
|---|---|---|
| `openlatch.server.port` | `9410` | 业务监听端口 |
| `openlatch.server.worker-threads` | `2 × CPU` | Netty worker 线程数 |
| `openlatch.server.session.idle-timeout-ms` | `60000` | 连接空闲超时 |
| `openlatch.server.lease.default-ms` | `30000` | 默认租约 |
| `openlatch.server.lease.min-ms` / `max-ms` | `1000` / `3600000` | 租约钳制区间 |
| `openlatch.server.lease.tick-interval-ms` | `500` | 到期扫描间隔 |
| `openlatch.server.queue.head-reply-timeout-ms` | `5000` | 通知后的队头应答超时（放弃的等待者最多占据队头这么久） |
| `openlatch.server.limit.max-key-length` | `512` | 键长上限（字节） |
| `openlatch.server.limit.max-queue-depth-per-key` | `4096` | 单键队列深度上限 |
| `openlatch.server.limit.max-inflight-per-connection` | `1024` | 单连接在途请求上限 |
| `openlatch.server.limit.max-value-bytes` | `4096` | 有值引用（v6）单 key 载荷字节上限；判定仅在接入层（超限命令不入日志），取值 [1, 512KiB]；下调不追溯存量值 |
| `openlatch.server.limit.max-queue-capacity` | `1024` | 队列（v7）定型容量主张上限；PUT 容量主张超限在入口拒绝（零日志），取值 [1, 65536]；配合每元素 `max-value-bytes` 构成单 key 驻留上界 |
| `openlatch.server.limit.max-drain-bytes` | `262144` | drainTo 应答字节预算（v7）：DRAIN 提取上限按 `预算/max-value-bytes` 折算钳定，钳定值随条目入日志（apply 不读本地配置），取值 [1, 512KiB] |
| `openlatch.server.queue.ready-tick-ms` | `200` | 延时队列到点唤醒扫描周期（v7，Leader/单机调度消费；仅影响唤醒延迟精度，MUST NOT 参与状态判定），下限 10ms |
| `openlatch.server.limit.max-subscribers-per-key` | `64` | 单键 topic（v8）订阅数上限；超限的 SUBSCRIBE 入口拒绝（`REJECT_SUBSCRIBERS`，既有订阅零扰动），取值 [1, 1024]；钳广播 fan-out 放大面 |
| `openlatch.server.limit.max-subscription-buffer` | `256` | 每订阅服务端在途缓冲条数（v8，drop-newest 触发线），取值 [1, 65536]；SDK 本地另有二级缓冲（256 条）同策略 |
| `openlatch.server.limit.max-parties-per-phaser` | `1024` | 单键相位器（v10）注册总数上限；超限的 REGISTER 入口拒绝（`OVERLOADED`，既有配额零扰动），取值 [1, 65536]；判定唯一在受理点（条目应用侧不复核，配置漂移不撕裂账簿）。挂起等待另受 `max-queue-depth-per-key` 合并口径（既有行，v9 起三原语共用） |
| `openlatch.server.metrics.enabled` | `true` | 指标管理端点开关（Prometheus 抓取 `http://host:port/metrics`） |
| `openlatch.server.metrics.port` | `9412` | 指标管理端口（`0`=临时）；绑定冲突即启动失败——同机多节点须互异 |
| `openlatch.server.admin.token` | 未配置 | 只读 `ADMIN_*` 管理令牌；未配置 ⇒ 一切管理请求被拒 |

TLS/认证键见 [06](06-security.md)。

### 2.1 集群键（`openlatch.cluster.*`）

以 3 节点为例，节点 1 的配置（其余节点改 `node-id`、端口、`data-dir`）：

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

| 键 | 默认 | 说明 |
|---|---|---|
| `enabled` | `false` | 关闭时同一二进制回退单机行为（不启动 Raft、不监听 raft 端口） |
| `node-id` | 必填（≥1） | 节点唯一 id；参与会话 id 高位编码，运行期不可改 |
| `peers` | 必填 | `id@host:port` 投票成员列表，必须含本节点、各节点取值一致 |
| `client-addresses` | 空（可选） | `id@host:port` 接入地址映射，让 Leader 提示直接给出可连地址；**不配不影响组网**，提示降级为空串，客户端以种子自报兜底发现 |
| `raft-port` | `9411` | 本节点 Raft 复制监听端口 |
| `data-dir` | `./data` | Raft 日志与快照目录（建议独立盘，容量按写入频率×日志保留估算） |
| `election-timeout-ms` | `3000` | 选举超时上界（随机窗取其内）；调小加快故障切换、调大抗抖动 |
| `snapshot-threshold` | `1000000` | 快照触发条目数：已应用位点距上次快照越过阈值即在应用线程产出快照；每节点保留最近 2 份，旧快照随快照周期清理 |
| `log-segment-bytes` | `0`（库默认） | Raft 日志段上限字节，运维无需配置 |

**硬性要求**：

- 节点间 **NTP 时钟同步，漂移 ≤ 1s**（租约到期由 Leader 驱动，默认 30s 租约 ≫ 漂移容限）；
- `enabled=true` 而必填项缺失/非法时**启动失败并指明配置键**，不静默降级为单机；
- 同机多节点时除业务/复制端口外，**指标管理端口也要互异**（`openlatch.server.metrics.port`
  默认 9412，绑定冲突即启动失败；演示机用 `0` 取临时端口最省事）。

## 3. 客户端接入与 Leader 发现

**种子给全**——这是集群形态第一运维纪律：

```java
OpenLatchClient client = OpenLatchClient.builder()
        .seeds("node1:9410", "node2:9410", "node3:9410")
        .build();
```

客户端行为（自动，无需干预）：

- **启动**：连任一种子 → HELLO 回 Leader 提示/地址 → 自动直连 Leader；连到 Follower 的写请求收 `NOT_LEADER` + 提示自动改道；
- **Leader 切换**：持有锁所连节点**存活但降级**（让位/少数派）→ 续租/释放经其转发道给新 Leader，**锁不丢、连接不断**；节点**宕机** → 挂起操作快速失败，重连先试原址、失败轮询种子，落存活节点后按提示改道；
- 连续 3 次 `NOT_LEADER`（提示陈旧）→ 对种子列表并发 `CLUSTER_VIEW` 强制发现；
- 切换窗口内单次获取受请求/总超时约束，**快速失败优先**，重试由应用决定。

### 3.1 一致性声明（务必阅读）

> OpenLatch 集群提供"**已确认授予的锁不丢、任何时刻同一 key 至多一个持有者**"的保证。
> **切换窗口内未完成复制的授予可能回滚**：旧 Leader 已回复"授予成功"但未达多数派的锁，
> 在故障切换后失效；客户端以 `INVALID_TOKEN`/`NOT_HELD`/`SESSION_EXPIRED` 感知，据此**重新竞争**
> （与 Redis 主从切换同理）。**等待队列不随 Leader 切换迁移**——排队位次重置为向新 Leader
> 重新排队；公平性是"单个 Leader 任期内的严格 FIFO"。

因此持锁任务必须能处理失锁回调（[03 §失锁](03-client-sdk.md)）：回调中放弃临界区、重新竞争。

### 3.2 错误码形状速记

`NOT_LEADER` 只出自转发/角色道（可重试，随附提示）；`NOT_HELD`/`INVALID_TOKEN` 只产生于
多数派提交后应用点的权威归属裁决——两者语义不相交，客户端路由不会混淆。

## 4. 升级、重启与回滚

### 4.1 协议版本滚动顺序

服务端先、客户端后（v3 服务端兼容 v1/v2/v3 客户端；高版本客户端连旧服务端会被握手拒绝）。
版本支持矩阵见 [10](10-compatibility.md)。

### 4.2 计划内重启（滚动）

逐台重启，任意时刻存活 ≥ 多数派。历史推荐序"先从不先主"**只降概率、非免疫**——
存量复制库存在低概率停摆形态（见下文"复制停摆自愈"一节），无人值守可用性由服务端看门狗自愈承载，
不以重启顺序为安全前提。

### 4.3 回滚

- 客户端回退旧版本仍可用（存量锁经转发车道无损）；
- 服务端回退到不支持快照的版本前确认：一旦集群产出过快照（日志按快照位点截断），
  数据目录**不可**由更旧二进制恢复——需清目录全量重加，或保持当前版本线。
- **v6 有值引用的回滚窗口**：引用载荷写入的日志条目与快照字段（`atomic_ref_*`）
  为 v6 新增，早于 v6 的二进制无法理解——回滚至 v6 前须先排空引用 key（写入
  全部 `set(null)` 不回收条目，故须接受回滚期内存驻留，或按上条清目录全量重加）。
  回滚期任何引用形态写入在旧 Leader 入口即被形状拒绝，不会污染复制面。
- **v7 队列的回滚窗口**：队列条目（`queue_*` 快照字段、`QUEUE_OP_ENTRY` 日志条目、
  形态 12/13）为 v7 新增，早于 v7 的二进制无法理解——回滚至 v7 前须把队列 key
  消费清空（元素被 `take`/`drainTo` 摘净；条目本身仍常驻，接受回滚期空条目驻留
  或按上条清目录全量重加）。回滚期任何 `QUEUE_OP` 消息在旧服务端以未知消息类型
  拒绝，不会污染复制面。
- **v8 topic 的回滚窗口（天然干净）**：topic 是首个零持久态原语——订阅登记、
  缓冲、去重槽与 `topic_seq` 全为 Leader 内存态，MUST NOT 入日志与快照（有常驻
  守卫回归钉死）。回滚至 v7 二进制**无残留状态要清理**：topic 流量本就零条目，
  旧二进制读不到任何 topic 痕迹，不存在 v6/v7 式的"回滚前排空 key"前置动作。
  通用混布规则依旧成立：v8 SDK 客户端连已回退的 v7 服务端会在**握手即拒**
  （升级序服务端先行；回退序客户端先行），既有连接随回退断开、订阅自然停摆
  （丢档窗语义同换主窗口）。
- **v9 条件的回滚窗口（无 key 可清，唯一约束为会话收敛）**：与队列（回滚前
  排空 key 或接受不可用）、topic（零持久态天然干净）三者的回滚约束**按原语
  分轨、互不混读**。条件等待集与搬运态全为 Leader 进程易失，不入日志、不入
  快照（快照零增量与 signal 家族零条目有常驻守卫回归钉死）；await 的释放半程
  骑**既有条目类型**（`LOCK_ACQUIRE_ENTRY` 的请求载荷透传 `condition` 字段，
  Raft 日志与快照格式零扩展）——回滚至 v8 二进制**没有条件专属持久态要清**。
  唯一约束是**活跃 v9 会话收敛**：v9 SDK 客户端连已回退的 v8 服务端**握手即拒**
  （区间外握手即拒属既有纪律的显形，fail-fast 而非静默降级；升级序 v8/v9 统一
  为服务端先行、客户端后行，回退序客户端先行）。回滚前确认 v9 客户端已降回
  SDK 或不再活跃；在途 await 随会话终结收束，等待项不复活。

- **v10 相位器的回滚窗口（屏障/队列同型，第四口径）**：相位器是持久态原语——
  注册配额、到场计数与相位号逐条经 `PHASER_OP_ENTRY` 入日志并写入快照字段
  （`phaser_*`，14 号条目类型），早于 v10 的二进制无法理解——降级前确认无在途
  phaser 流量（等待簿记本就 Leader 易失无需处理），或接受 phaser key 在旧二进制
  上不可用（条目按未知类型 error 路径现行口径处置）。与队列"回滚前清 key"、
  topic/condition"天然干净"并列第四种口径，**互不混读**：phaser 没有可"排空"的
  驻留数据（账簿即状态本体），只有"在途流量清零"一条前置动作。混布规则同前：
  v10 SDK 客户端连已回退的 v9 服务端握手即拒（升级序服务端先行、回退序客户端先行）。

### 载荷下的快照与日志尺寸治理（v6）

- 载荷通道把"条目数"维度的快照膨胀引入"字节数"维度：单条有值引用条目快照占用
  上界 ≈ `2 × max-value-bytes + 常数`（当前值 + 去重槽各一份载荷）；
- 触发与保留口径**不因载荷改变**——`snapshot-threshold` 仍按条目数、每节点保留
  2 份；载荷基数（引用 key 数）是快照尺寸的主导变量，与条目常驻叠加即为运维须
  关注的驻留成本，建议按租户/轮次命名引用 key 并纳入基数治理；
- `data-dir` 容量估算需在原"写入频率 × 日志保留"基础上叠加"引用 key 数 ×
  `max-value-bytes` × 常数"项；
- 观测与断言：控制台/管理协议呈现每条目载荷大小与恒定长度的截断预览（全量字节
  不入管理应答），基准侧有"恰限载荷批量写 → 快照尺寸落界"的守门用例。

### 队列维度的快照与日志治理（v7）

- 单条队列 key 的快照驻留上界 ≈ `capacity × max-value-bytes`（元素全列表）
  + `1 × max-value-bytes`（每会话去重槽的最近交付回执，SESSION_CLOSE 即摘除）；
  快照尺寸随**队列 key 数 × 容量 × 元素上限**线性有界，MUST NOT 随 put/take
  操作轮次累积（历史元素与版本不入快照）——有满容量恰限元素 + 交付槽的守门
  回归断言钉死；
- 日志条目率：队列写读皆全量经提交（判例 ATOMIC），热点队列 key 的条目率是
  真实增长源；`drainTo` 是既有摊薄通道（一次摘多只一条日志）；延时到点唤醒
  与挂起/回弹**不入日志**（Leader 本地等待队列承载）。生产观测到热点队列
  条目率成为运维压力时按 WATCHLIST W9 触发评估；
- 队列存续期间下调 `max-value-bytes`：既有更大元素的 drain 应答可能超出
  `max-drain-bytes` 预算（钳制下调不追溯存量的直接后果，上界为 N × 历史元素
  上限）——先清大元素或按容量估算预留，避免撞上帧上限。

### topic 维度的容量治理（v8）

- topic 对快照与日志**零贡献**（守卫回归钉死），其治理面不在持久侧而在
  **Leader 写出侧**：fan-out 放大 = 订阅数 × 发布速率 × 消息大小，全部落在
  Leader 进程内存缓冲与网络写出上；
- 两道入口限额即容量钳：`max-subscribers-per-key`（默认 64）封顶单键放大面，
  `max-subscription-buffer`（默认 256 条）封顶每订阅驻留——每订阅缓冲堆上界
  ≈ `256 × max-value-bytes`，全键上界 = 二者乘积再 × 订阅数；
- 观测口径：`openlatch_server_topic_dropped_total` 增速（丢弃即"消费跟不上
  广播速率"的直接信号）、`topic.subscribers.max` 水位、控制台 Leader 侧订阅
  读数；持续 drop 的处置选项：扩订阅方消费并行度、拆分 topic 键降扇出、
  或按 WATCHLIST topic 行触发评估批量帧/请求门控（另立 change）；
- 容量估算需叠加"活跃 topic 键数 × 订阅数 × 缓冲条数 × 消息均值字节"的
  Leader 堆内存项；topic 不承担持久投递义务——需要抗 Leader 重启的事件流
  请用 `OBlockingQueue`。

### phaser 维度的条目率治理（v10）

- 相位器是**全入日志**原语：注册、到场、离场逐条 `PHASER_OP_ENTRY`——条目率
  ≈ 参与者数 × 相位频率 × 每相到场数，无 topic/condition 的零日志豁免面。
  治理杠杆因此在"到场频率"与"键切分"两处：
  - 高频小相位（每相位 2-3 方、秒级推进）是设计内形态，基准 phaser 相给出
    单发到场与两方会合的吞吐/延迟基线（`target/benchmark/` 报告）；
  - 热点键拆分（按业务分片多开 phaser key）是首选泄压，注册配额上限
    `max-parties-per-phaser`（默认 1024）封顶单键账簿体量；
- 快照侧账簿**有界且滚动**：只驻留当前 (phase, registered, arrived, 配额表,
  在场槽, 换代窗口)——相位历史零驻留、去重槽随推进清空，快照字节不随轮次
  增长（回归断言钉死）；等待簿记为 Leader 易失零足迹；
- 观测口径：`phaser.total{op,status}` 两超限线增速（`{register,OVERLOADED}`
  配额、`{await_advance,OVERLOADED}` 深度）、`phaser.parties.registered.max`
  水位；条目率/快照时长成为运维压力时按 WATCHLIST W14 行触发评估批量到场
  合并或读路径折案（另立 change）。

### 条件维度的治理注记（v9）

- **零新增配置**：条件不引入任何新服务端键——等待人数与等待队列按"本 key
  等待项"合计口径共用 `max-queue-depth-per-key`（超限的 await 得 `OVERLOADED`），
  signal 搬运恒发生于调用者持有的时点、由后续释放/到期/会话关闭的队首通知接力，
  **零新定时器**（对照 v7 `ready-tick-ms` 的扫描需求；与 topic 即时性同相）；
- **持久侧零足迹**：等待集为 Leader 进程易失、不入快照（`SnapshotLock` v9 零
  扩展即编号证据），SIGNAL/SIGNAL_ALL/LEAVE 零日志条目；await 仅贡献**一条
  既有条目类型**（条件维自 v9 起条目类型零占用——v10 上界升至 14 为 phaser
  专用值，不构成该证据的例外）——快照与
  日志治理无须为条件增设估算项；
- **容量与观察口径**：纯条件等待的大集群与正常获取挤占同一合并护栏——
  `condition.waiters.max` 水位（单键条件等待集峰值，不含已搬运项）应配合
  `queue.depth.max`（等待队深）与 `waiters` Gauge 合计一并看；水位逼近护栏、
  或生产报"await 永睡且比对到换主时点"（换主窗 signal 丢失为契约面）时按
  WATCHLIST W13 触发评估；
- **升级序统一表述（v8/v9 同口径）**：服务端先升、客户端后升；区间外客户端
  握手即拒（回滚窗口的唯一条件约束即此既有纪律的显形，见上节回滚条目）。

## 5. 复制停摆自愈与 supervisor（必读运维事实）

存量 Raft 库在滚动重启时序下，新 Leader 任期提交存在低概率永久冻结的缺陷。服务端已内置
**复制停滞看门狗**：

- 判定：Leader 任期超 `T_stall = max(10s, 5×election-timeout)` 且 commitIndex 连续 3 个
  采样周期零推进；
- 处置①（让位）：向日志最新的健康对侧让位一次，多数派重选恢复；
- 处置②（升级）：观察窗内仍冻结 → **进程以退出码 1 自杀退出**——此时需要外部 supervisor
  把它拉起来（以纯 follower 身份归群是确定的复位路径）；
- 重启冷却：进程内 5 分钟一次，**supervisor 侧请配置 ≥ 冷却窗的重启退避**；
- 日志特征行：`replication stall detected` / `escalating to process restart`——出现即记运维事件；
- **无 supervisor 的部署只获得让位段自愈与告警**，升级段需人工重启该节点。

> 另有一类"选举风暴"形态（无在任 Leader，看门狗无从让位）：表现为持续无主 + 客户端
> 等待预算耗尽类错误。处置同样是退避重启 + 观察；单轮演练零命中不代表该类风险消除，
> 保持 supervisor 是无人值守的前提。

## 6. 成员变更（编程接口，无命令行）

入口为运行时对象（`ClusterRuntime`，内嵌部署中即应用持有的实例），所有变更经当值 Leader
提交单步配置，返回成功即已提交。

**铁律**：先加后删（新节点纳入并**追赶完成**才可移旧）；禁止同时变更多数派成员
（封装层机械拒绝"单次净变更 > 1 投票者"与"同时加删"）；变更期间已确认的锁不受影响。

```java
// 加节点三段式：listener 加入 → 追赶 → 升票
leaderRuntime.subsystem().setMembers(
        List.of("1@node1:9411", "2@node2:9411", "3@node3:9411"),  // 投票者全集（暂不变）
        List.of("4@node4:9411"));                                  // 新节点以监听者入组
// 等待追赶一致（digest / CLUSTER_VIEW 观测）后升票：
leaderRuntime.subsystem().setMembers(
        List.of("1@node1:9411", "2@node2:9411", "3@node3:9411", "4@node4:9411"),
        List.of());

// 删节点（会话自动清理：被移除节点上的锁以日志条目批量关闭并释放）
leaderRuntime.removeMember(3);   // 若 3 是 Leader，先 transferLeadership 让位再移除

// 发布窗口前主动压缩日志（可选）
leaderRuntime.subsystem().triggerSnapshot();
```

落后过多的节点与空目录新节点都经**快照安装 + 增量回放**追平，追赶期间写请求返回
`NOT_LEADER`、查询照常。

## 7. 验证与演练

部署完成的推荐验证（也是持续回归的常态通道）：

```bash
# 构建可执行 jar 后，进程级演练（杀主/滚动重启/真分区）
mvn -pl openlatch-server -am package
mvn -pl openlatch-client verify -Pdrill
# 报告落 openlatch-client/target/drill-reports/ 与 openlatch-server/target/drill-reports/
```

判据口径（恢复 <10s、两序滚动"末重启窗 + 45s 自愈预算后零残留"、分区少数派不可授予）
与判读细节见 [09 演练判据](09-troubleshooting.md)。

> **分区演练的特权前置**（Linux，一次性配置）：`PartitionDrillIT` 以非交互 `sudo -n` 执行
> `ip`/`iptables`/`modprobe`，推荐最小授权 sudoers 片段：
>
> ```bash
> sudo tee /etc/sudoers.d/openlatch-drill >/dev/null <<'EOF'
> <user> ALL=(ALL) NOPASSWD: /usr/bin/true, /usr/sbin/ip, /usr/sbin/iptables, /usr/sbin/modprobe
> EOF
> sudo chmod 440 /etc/sudoers.d/openlatch-drill
> sudo visudo -cf /etc/sudoers.d/openlatch-drill   # 必须 parsed OK；路径以 which 实测为准
> ```
>
> 未配置时该套件**显式跳过**（不判失败）——CI/自动化务必核对 `Skipped: 0` 防假绿。
> 进程级演练对算力敏感（选举窗 800ms），数值门复核建议在独占/高性能硬件上执行；
> hosted 共享 runner（2 vCPU）不满足该前提，CI nightly 已据此暂停（保留手动触发）。
