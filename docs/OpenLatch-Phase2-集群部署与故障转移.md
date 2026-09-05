# OpenLatch Phase 2 集群部署与故障转移指南

| 项目     | 内容                                        |
|----------|---------------------------------------------|
| 适用版本 | OpenLatch 1.x（Phase 2，协议 v2）           |
| 面向     | 运维部署者、集成应用开发者                  |
| 依据     | 《Phase2 详细设计说明书》v1.2 §6/§8/§9/§11   |

本指南覆盖把 Phase 1 单机服务升级为 3/5 节点 Raft 复制组的部署配置、客户端 Leader 发现行为、故障切换语义与升级/回滚顺序。

---

## 1. 部署拓扑

推荐 3 节点（容忍 1 节点故障）或 5 节点（容忍 2 节点）。每个节点是**同一份二进制**，一个 Raft 副本 + 一套完整客户端接入层。客户端可连接**任意节点**：

- 写请求（ACQUIRE）必须由 Leader 处理；连到 Follower 时服务端回 `NOT_LEADER` + Leader 提示，v2 客户端自动改连；
- 存量锁的释放/续租（RELEASE/RENEW）落在任意存活节点皆可——Follower 会经内部通道转发给当值 Leader 复制执行（详见 §3）；
- HELLO / PING / CLUSTER_VIEW 任意节点可应答。

## 2. 服务端配置

在 Phase 1 的 Properties 文件基础上新增 `openlatch.cluster.*` 键族。每节点一份配置，示例（节点 1/3）：

```properties
# 接入端口（客户端连接）
openlatch.server.port=9410

# 集群
openlatch.cluster.enabled=true
openlatch.cluster.node-id=1
# Raft 复制成员（内部通信），必须含本节点，各节点该值须一致
openlatch.cluster.peers=1@node1:9411,2@node2:9411,3@node3:9411
# 可选：各节点客户端接入地址映射，供 Leader 提示与 CLUSTER_VIEW 直接给出 host:port
openlatch.cluster.client-addresses=1@node1:9410,2@node2:9410,3@node3:9410
openlatch.cluster.raft-port=9411
openlatch.cluster.data-dir=/var/lib/openlatch
openlatch.cluster.election-timeout-ms=3000
openlatch.cluster.snapshot-threshold=1000000
```

| 键 | 默认 | 说明 |
|----|------|------|
| `enabled` | `false` | `false` 时同一二进制回退 Phase 1 单机行为（不启动 Raft、不监听 raft-port） |
| `node-id` | 必填（≥1） | 节点唯一 id；参与 sessionId 高位编码 |
| `peers` | 必填 | `id@host:raftPort` 列表，含本节点，全节点一致 |
| `client-addresses` | 空（可选） | `id@host:port` 接入地址映射；**不配置不影响组网**，仅 Leader 提示的 `leader_address` 降级为空串，客户端改以种子自报兜底发现 |
| `raft-port` | `9411` | 本节点 Raft 复制监听端口 |
| `data-dir` | `./data` | Raft 日志与快照目录（建议独立磁盘） |
| `election-timeout-ms` | `3000` | 选举超时上界（Raft 层语义透传） |
| `snapshot-threshold` | `1000000` | 快照触发条目数（S4 生效：已应用位点距上次快照越过该阈值即在应用线程产出快照；每节点保留最近 2 份，旧快照随快照周期清理） |
| `log-segment-bytes` | `0`（库默认） | Raft 日志 segment 上限字节（Raft 层语义透传，运维无需配置；测试用于在小数据量下驱动滚动与截断） |

**硬性部署要求**：
- 节点间 **NTP 时钟同步，漂移 ≤ 1s**（租约到期由 Leader 驱动，默认租约 30s ≫ 漂移容限）；
- `enabled=true` 而必填项缺失/非法时进程启动失败并指明配置键，不静默降级为单机。

## 3. 客户端接入与 Leader 发现

### 3.1 配置种子列表

```java
OpenLatchClient client = OpenLatchClient.builder()
        .seeds("node1:9410", "node2:9410", "node3:9410")  // 全部节点地址
        .requestTimeout(Duration.ofSeconds(5))
        .build();
```

**强烈建议提供全部种子**：客户端在 Leader 被硬杀后依赖种子轮询重连与强制发现定位新 Leader；只提供单一地址且该地址恰为宕机节点时无法自动恢复。（Phase 1 的 `.address("host:port")` 单地址入口仍可用，等价于一元种子表，适用单机/单点。）

### 3.2 故障转移行为

- **启动**：连任一种子 → HELLO 返回 `leader_hint`/`leader_address` → 自动直连 Leader；
- **运行中 Leader 切换**：
  - 客户端持有锁所连节点**存活但降级为 Follower**（如优雅让位、少数派侧存活）→ 该锁的续租/释放经此节点**转发**给新 Leader，**锁不丢**、连接不断；
  - 客户端连接因节点宕机而**断开** → 触发断连快速失败（挂起操作以"服务不可用"失败，持锁按 Phase 1 lostAt 宽限期判定丢失并回调），客户端重连时先试原地址、失败后轮询种子，落到存活节点后经提示改连新 Leader；
- **连续 3 次 `NOT_LEADER`**（如提示陈旧指向死节点）→ 触发对种子列表的并发 `CLUSTER_VIEW` 强制发现；
- **快速失败优先**：切换窗口内单次获取受请求超时（默认 5s）/等待总超时（默认 30s）约束，不无限等待，由应用决定重试。

### 3.3 一致性声明（务必阅读）

> OpenLatch 集群提供"**已确认授予的锁不丢、任何时刻同一 key 至多一个持有者**"的保证。**切换窗口内未完成复制的授予可能回滚**：旧 Leader 已回复"授予成功"但未达多数派的锁，在故障切换后会失效；客户端在续租/解锁时以 `INVALID_TOKEN`/`NOT_HELD`/`SESSION_EXPIRED` 感知，据此**重新竞争**（与 Redis 主从切换同理）。**等待队列不随 Leader 切换迁移**——排队的等待者位次在切换时重置为"向新 Leader 重新排队"，公平性为"单个 Leader 任期内的严格 FIFO"。

因此：应用侧对获取应视为**可能因 failover 而失效**——持锁任务应能处理锁丢失回调（`addLockLostListener`），并在回调中放弃当前临界区、重新竞争。

## 4. 升级与回滚顺序

无既有部署时（首个 Phase 2 发布）常规合入即可。滚动升级到集群形态：

1. **先升级全部服务端**（v2 服务端同时接受 v1/v2 客户端）；
2. **再升级客户端**到 v2（获得自动 Leader 发现；v2 客户端连接旧 v1 服务端会被握手拒绝，故必须先服务端）。

回滚：客户端回退 v1 仍可用（存量锁经转发车道无损）；服务端回退到 S2，则 S3 客户端收到不带 hint 的 `NOT_LEADER` 后落入种子轮询，功能降级但不破坏正确性。**注意（S4 起）**：一旦集群产出过快照（日志已按快照位点截断），数据目录不可回退到 S2 二进制恢复——回退需清目录全量重加，或保持 S4+。

## 5. 故障演练与验收（P2-14）

进程级杀 Leader 演练由 `-Pdrill` profile 触发（`@Tag("drill")`，默认构建排除）：

```bash
# 先产出 shaded 可执行 jar
mvn -s <settings> -pl openlatch-server -am package
# 运行 kill -9 Leader 恢复计时演练
mvn -s <settings> -pl openlatch-client verify -Pdrill -Dit.test=LeaderKillDrillIT
```

演练产出 `docs/failover-drill-<日期>.md`，覆盖验收清单 §11-2（杀 Leader 恢复 < 10s、存活会话锁不丢）与 §8 行为表双场景。S3 退出取证报告见 `docs/failover-drill-2026-08-31.md`（全量 4/4 通过，实测恢复 1.6–1.8s）。

> **已知限制（诚实标注）**：`kill -9` 真实崩溃路径下，恢复依赖 Raft 崩溃选举（非优雅让位）；在高并发/共享 CI 环境上偶发恢复超过 10s 目标（选举窗口 + 客户端种子轮询叠加）。确定性语义正确性以进程内 3 节点集成测试（`ClientClusterIT`，含"home=被杀 Leader"恢复、存活让位锁不丢、隔离判丢、等待重排）为准；kill -9 的 <10s 数值门**发布级复核建议在独占硬件上执行**（本机 8 核 quiet 态已 4/4 通过，余量 ≥5 倍；重载瞬态下仍观察到过超时），S3 退出门已据此关闭。

## 6. 成员变更运维（S4/P2-17）

Phase 2 提供**编程接口**（无命令行/管理端点），入口为 `ClusterRuntime` / `RaftSubsystem`（嵌入部署中即应用持有的运行时对象）。所有变更经当值 Leader 提交单步配置，返回成功即已提交。

### 6.1 铁律

- **先加后删**：新节点必须先纳入并**等待追赶完成**（摘要与集群一致），才能移除旧节点；
- **禁止同时变更多数派成员**：封装层机械拒绝"单次同时加删"与"单次净变更 > 1 个投票者"（`IllegalArgumentException`），旧/新多数派恒相交是单步变更的安全前提；
- 变更进行中对集群已确认的锁不产生影响（持有与租约已复制；变更期间到 Leader 的写照常提交）。

### 6.2 加节点（三段式：listener 加入 → 追赶 → 升票）

```java
// 1) 新节点先以自身配置启动（其 peers 表须含全部现有成员 + 自己，data-dir 为空）。
// 2) 在任一现有成员节点上，把新节点以监听者（listener）纳入——不改变投票集合：
leaderRuntime.subsystem().setMembers(
        List.of("1@node1:9411", "2@node2:9411", "3@node3:9411"),   // 目标投票者全集（不变）
        List.of("4@node4:9411"));                                   // 目标监听者全集
// 3) 等新节点追赶至一致（快照安装 + 增量回放；可经其 digest 或 CLUSTER_VIEW 观测）。
// 4) 升为投票者（净变更 +1，符合护栏）：
leaderRuntime.subsystem().setMembers(
        List.of("1@node1:9411", "2@node2:9411", "3@node3:9411", "4@node4:9411"),
        List.of());
```

### 6.3 删节点（出组 + 会话自动清理）

```java
// 在当值 Leader 节点上（一次调用完成两步）：
leaderRuntime.removeMember(3);  // 3 为被移除节点 id
```

被移除节点上登记的会话按详设 §5.2 规则 4 的失联清理车道以日志条目批量关闭（`SESSION_CLOSE`），其持有的锁随之释放、可被重新授予，存活副本一致收敛。移除前若该节点仍为 Leader，先经 `transferLeadership` 让位（见 §3.2/S3 演练），再执行本操作。

### 6.4 快照与截断（与变更相关的行为）

- 各节点独立按 `snapshot-threshold` 产出快照并保留最近 2 份；日志截断推进至本节点快照位点（缺席节点不阻塞截断）；
- 落后过多的节点与空目录新节点都经**快照安装 + 增量回放**追平，追赶期间写请求返回 `NOT_LEADER`、查询照常；
- 手动快照：`leaderRuntime.subsystem().triggerSnapshot()`（任意成员节点均可，用于发布窗口前的主动压缩或诊断）。

## 7. 容错演练清单（S4/P2-18 · P2-19）

进程级/命名空间级演练经 `-Pdrill` profile 触发（`@Tag("drill")`，默认构建排除），
产出报告均入库 `docs/`。执行前先产出 shaded jar：

```bash
mvn -s <settings> -pl openlatch-server -am package
```

| 演练 | 命令 | 报告 / 判据 |
|---|---|---|
| 滚动重启（两顺序，客户端错误率 <1%） | `mvn -s <settings> -pl openlatch-client verify -Pdrill -Dit.test=RollingRestartDrillIT` | `docs/rolling-restart-drill-<日期>.md`（实测 先从后主 0.80% / 先主后从 0.00%，§11-5；先主后从序存在存量 P1 停摆风险，见下方运维推荐序） |
| 网络分区（netns 真分区，需 passwordless sudo） | `mvn -s <settings> -pl openlatch-client verify -Pdrill -Dit.test=PartitionDrillIT` | `docs/partition-drill-<日期>.md`；无特权环境显式跳过（辅轨 `MinorityQuorumTest` 提供近似判据，§11-3；主轨已于 2026-09-06 真分区全绿：少数派授予/释放道全拒、锁存活、撤分区自动收敛） |
| 混沌（随机杀/重启 + 共享 key 竞争不变式） | `mvn -s <settings> -pl openlatch-client test -Dtest=ClientChaosIT` | 零双授冲突 / 停载后锁表空 / 副本摘要收敛（§11-6；常规回归，短租约有界窗口） |

进程级杀 Leader 计时演练（P2-14，§11-2）见 §5。

**运维推荐序（滚动重启，v1.5 收口补录）**：逐台重启请按**"先从不先主"**顺序（先重启全部 Follower，最后重启 Leader，令其在两节点多数派之上从容重加入）。"先主后从"序存在存量 Ratis 3.3.0 缺陷风险：旧 Leader 带脏条目重启归群的时序下，新 Leader 任期提交可能停摆（表现为写请求持续被拒 >200 秒不自愈、复制组无错误日志）。故障表征与差分归因、跟进计划见 `openspec/changes/phase2-release-closure/defects/leader-replication-stall-ratis-3.3.0.md`。
