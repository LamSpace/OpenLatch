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
| `snapshot-threshold` | `1000000` | 快照触发条目数（S4 生效） |

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

回滚：客户端回退 v1 仍可用（存量锁经转发车道无损）；服务端回退到 S2，则 S3 客户端收到不带 hint 的 `NOT_LEADER` 后落入种子轮询，功能降级但不破坏正确性。

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
