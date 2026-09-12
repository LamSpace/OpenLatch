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
与判读细节见 [09 §演练判据](09-troubleshooting.md)；CI 上 nightly 自动执行并交付 artifacts。
