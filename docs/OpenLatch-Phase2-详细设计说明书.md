# OpenLatch Phase 2（Raft 集群）详细设计说明书

| 项目     | 内容                                                                                                                |
|----------|---------------------------------------------------------------------------------------------------------------------|
| 项目名称 | **OpenLatch**                                                                                                       |
| 文档类型 | 详细设计说明书（Phase 2 / Raft 集群）                                                                               |
| 依据文档 | 《OpenLatch 概要设计说明书》v1.0、《OpenLatch-总体实施计划与验证方案》v1.0、《OpenLatch-Phase1-详细设计说明书》v1.0 |
| 版本     | v1.5                                                                                                                |
| 日期     | 2026-09-06                                                                                                          |
| 作者     | Lam Tong                                                                                                            |
| 状态     | 已验收（Phase 2 发布收口，v1.1 为 S1 定案回写，v1.2 为 S3 实现回写，v1.3 为 S2 退出补回写，v1.4 为 S4 实现回写，v1.5 为发布收口回写，见 §4/§6/§7/§9/§12/§13；验收证据汇总见 `docs/Phase2-验收报告.md`） |

**修订记录**：v1.0（2026-08-23）初版待评审；v1.1（2026-08-30）S1 Raft 选型 PoC 完成，§2 定案 Apache Ratis（证据见 `docs/raft-selection-report.md` 与 `poc/raft-selection/`），§12 风险 4 关闭；v1.2（2026-08-31）S3 Leader 发现与故障转移实现回写——§6.2 hint 字段编号定稿（复用 `leader_hint=5` + 新增 `leader_address=6`，补三类写响应 hint 载体与 `ClusterView.status`）、§9 增 `client-addresses` 配置键、§6.1 v1 客户端口径收紧、§3.2/§13.3 P2-12 措辞对齐分车道裁决（证据见 `openspec/changes/s3-leader-discovery-failover/` 与 `docs/failover-drill-*.md`）；v1.3（2026-08-31）S2 退出评审回写补齐——§4.5 排队登记主体由"本地 `CoreEngine`"改判为 Leader 侧任期作用域独立 `WaitQueue`（S2 design D9 正确性修复）、§4.3"命令级时间覆盖参数"定夺不做（S2 design D2）、§13.2 各任务标记完成（证据见 `openspec/changes/archive/2026-08-30-phase2-s2-replicated-state-machine/`）；v1.4（2026-09-02）S4 快照与恢复、容错加固实现回写——§7.1 `SnapshotState` 增 `next_lease_token=3`（发号水位，切割点跨副本一致的硬条件）并落定重建入口形态（`CoreStateRestore` 值对象）、§7.2 落盘口径与手动触发落点、§7.3 安装流（Ratis 3.3 pause→reload 模型，取代既有 `loadSnapshot(Stream)` 假设）、§9 增 `log-segment-bytes` 键与"截断推进至快照位点"装配口径、§1.4 core"零改动"口径切换为"§7.1 授权纯新增"、§13.4 各任务完成标记（证据见 `openspec/changes/phase2-s4-snapshot-recovery/`：`s4-exit-checklist.md`、`s4-zero-touch-evidence.txt`、`observations-ratis-3.3.0-s4-snapshot.md` 与 `docs/rolling-restart-drill-2026-09-02.md`）；v1.5（2026-09-06）Phase 2 发布收口回写——§13.3 S3 四任务退出记账补齐（镜像 v1.3 对 S2 的补回写模式，证据为 S3 归档与 `docs/failover-drill-2026-08-31.md`）、§11-3 主轨分区演练绿（netns 真分区，`docs/partition-drill-2026-09-06.md`，判据口径修正：HELLO 在少数派可完成本地握手、判定以会话化 ACQUIRE 的 NOT_LEADER 与 RELEASE 道非 OK + 锁存活为准）、§10 混沌字面 ≥10 分钟 soak 达成（510 杀/509 重启/0 冲突/0 泄漏）、§9 新增线程池装配契约（Ratis 3.3.0 cached 池在空闲后优雅关停必挂的 P0 修复，`observations-ratis-3.3.0-soak-shutdown-hang.md`）、§12 遗留增 P1 leader 复制停摆（存量缺陷，差分实验证明非收口期引入，`defects/leader-replication-stall-ratis-3.3.0.md`，运维推荐"先从不先主"）（证据见 `openspec/changes/phase2-release-closure/`）。

---

## 1. 概述

### 1.1 目标

将 Phase 1 的单节点锁服务升级为 **3/5 节点 Raft 复制组**（概要设计 §5.1）：

1. 锁状态（持有关系、租约）经 Raft 日志复制到多数派，Leader 宕机不丢已授予的锁；
2. 客户端自动发现 Leader，Leader 切换后无需人工干预恢复服务；
3. 快照 + 日志追赶支持节点重启与新节点加入。

### 1.2 非目标（Phase 2 明确不做）

- **等待队列不复制**：FIFO 等待队列仅存在于当值 Leader 内存（见 §4.4 设计决策）；
- 多 Raft 组 / 分片（单 key 数量级不需要，留待后续）；
- 读优化（Lease Read / ReadIndex）：所有读写均经 Leader；
- 跨数据中心部署优化。

### 1.3 与概要设计的追溯矩阵

| 概要设计章节                                    | 本文对应章节 |
|-------------------------------------------------|--------------|
| §4.1 P1：Raft 复制、Leader 切换、快照恢复       | §3–§7        |
| §5.1 部署拓扑（Phase 2）                        | §3.1         |
| §5.3 Raft 库选型（Ratis / SOFAJRaft，PoC 定案） | §2           |
| §6.1 协议版本字段预留扩展                       | §6           |
| §7 集群 Leader 切换行为                         | §8           |
| §11 风险 2（Raft 选型）                         | §2.3         |

### 1.4 技术基线

同 Phase 1：Java 25、`io.github.lamspace`。新增模块不改变既有模块结构，集群能力全部落在 `openlatch-server` 内（新增包 `server.raft`）与协议扩展。

---

## 2. Raft 库选型（S1）

### 2.1 候选与定案

概要设计 §5.3 指定候选： **Apache Ratis** 与 **SOFAJRaft**。S1 PoC（P2-01～P2-04）已完成实测：两候选均通过 §2.4 全部门槛，按 §2.2 权重综合评分**定案 Apache Ratis**，S2 起实现基于 **ratis-server / ratis-grpc / ratis-client / ratis-metrics-default 3.3.0**（PoC 数据基于 3.2.2；3.3.0 已补冒烟验证）。决定性因素：全量 shading 的传输层共存（与主干 Netty/protobuf 零类路径交集）、ASF 维护模型、杀主恢复 548ms（JRaft 1508ms，均过线）。完整数据、评分与摩擦档案见 `docs/raft-selection-report.md`。

### 2.2 评估维度与权重

| 维度           | 关注点                                                                         | 权重   |
|----------------|--------------------------------------------------------------------------------|--------|
| 状态机集成方式 | 能否以"外部状态机 + 日志回放"方式接入现有 `CoreEngine`，而非被迫使用库内置存储 | 高     |
| 快照支持       | 快照触发策略、快照序列化是否可自定义（需序列化锁表）、追赶流程                 | 高     |
| 传输层         | 自带传输与 Netty 的共存成本（端口、线程模型、内存池冲突）                      | 高     |
| 性能           | 写入延迟（锁授予延迟预算见 §2.4）、批量提交能力                                | 中     |
| 成员变更       | 联合共识/单步变更的 API 完整度                                                 | 中     |
| 社区与维护     | 发布频率、issue 响应、文档质量                                                 | 中     |
| 许可证         | Apache-2.0（两者均满足，记录在案）                                             | 门槛项 |

### 2.3 PoC 任务书

两个候选各实现同一最小原型：

1. 把 Phase 1 的 `CoreEngine` 作为状态机接入（日志条目见 §4.2）；
2. 基准：3 节点本机部署，`单键获取+释放` 混合负载 5 分钟，记录授予延迟 P99 与吞吐；
3. 杀 Leader 计时：从进程终止到新 Leader 选出并可服务的时间；
4. 触发一次快照并重启 Follower，验证追赶；
5. 记录 API 侵入度（为接入改动的代码行数、被迫引入的抽象）。

### 2.4 判定标准（通过/不通过）

| 指标                            | 门槛                                    |
|---------------------------------|-----------------------------------------|
| 集群授予延迟 P99（本机 3 节点） | < 20ms（单机基线 5ms 之上允许复制开销） |
| Leader 故障到恢复服务           | < 10s（与实施计划 §5.2 验收一致）       |
| 快照加载 + 追赶                 | 10 万锁条目快照恢复 < 30s               |
| 状态机集成                      | 无需重写 `CoreEngine` 的锁语义代码      |

任一候选不满足门槛即淘汰；均满足则按权重综合评分定案。

**S1 实测结果（v1.1 回填，3 轮中位）**：Ratis P99 4.87ms / 杀主 548ms / 快照恢复 2514ms；JRaft 4.50ms / 1508ms / 1876ms；`CoreEngine` 零改动、全轮次零双授违例。**双方均过线，按权重定案 Ratis**（归因、评分与证据：`docs/raft-selection-report.md`）。

---

## 3. 集群架构

### 3.1 部署形态

```
应用进程 ──▶ 任意 OpenLatch 节点（接入）
               │
               ├─ 是 Leader：直接处理（日志复制后响应）
               └─ 非 Leader：返回 NOT_LEADER + leaderHint，客户端改连
                    │
       节点间：Raft 复制组（3/5 节点，日志 + 快照持久化于本地磁盘）
```

- 每个节点 = 一个 Raft 副本 + 完整接入层（复用 Phase 1 pipeline）；
- 客户端可连接任意节点，但 **写请求（ACQUIRE/RELEASE/RENEW）必须由 Leader 处理**；
- HELLO/PING 任意节点可答（会话注册除外，见 §5）。

### 3.2 节点内部结构（对 Phase 1 的增量）

```
server/raft/
├── RaftSubsystem           初始化/关停 Raft 库；生命周期与 OpenLatchServer 绑定
├── LockStateMachine        Raft 状态机适配器：日志条目 → CoreEngine 调用；快照序列化/加载
├── ReplicationGateway      Leader 侧：core 命令 → 日志提交 → 提交后响应客户端
│                           （亦是 Follower 转发道：RELEASE/RENEW 经此内部提交至 Leader）
├── SessionCoordinator      会话的集群登记（§5）
├── ClusterRequestHandler   集群写请求接入：预检/分车道裁决（§4.5）
└── LeaderTracker           当前 Leader 变更事件 → 通知接入层与客户端提示
```

> v1.2 实现落点：原规划的独立 `ForwardingProxy` 类未单列——Follower 侧 RELEASE/RENEW 的转发直接复用 `ReplicationGateway` 的内部提交通道（`SESSION_OPEN` 本就走此路），`ClusterRequestHandler` 摘除角色门即接入；`LeaderTracker` 为 S3 新增（`server.raft`），保留 Ratis `notifyLeaderChanged` 的新 Leader 身份并折算为 `{nodeId, address}` 提示单源。`ClusterRequestHandler`/`LeaderTracker` 均为详设结构在实现中的落点补全，不改变 §3.1 拓扑。

`CoreEngine`（openlatch-core）复用原则：集群化改变的是"谁调用它、何时响应客户端"，不改变锁语义本身。这是 Phase 1 将核心隔离为纯 Java 模块的直接收益。**v1.4 口径修正（S4）**：为支撑快照恢复，`openlatch-core` 获 §7.1 授权的<b>纯新增</b>（`core.snapshot.CoreStateRestore` 值对象 + `CoreEngine.restoreFrom`/`nextLeaseToken` + `LockEntry.restored` 工厂），既有方法体逐字节不变（证据 `openspec/changes/phase2-s4-snapshot-recovery/s4-zero-touch-evidence.txt`：core diff 122 insertions、0 deletions）——S2 退出时"git diff 为空"的口径在 S4 后不再成立，改以"既有方法体零改动"表述。

---

## 4. 复制状态机设计（S2）

### 4.1 复制边界

| 状态                                 | 是否复制                    | 理由                                        |
|--------------------------------------|-----------------------------|---------------------------------------------|
| 锁持有关系（key、mode、Owner、计数） | ✅ 复制                     | 正确性核心：failover 后锁不能"消失"或"双授" |
| 租约（token、到期时刻、租期）        | ✅ 复制                     | 同上；到期释放必须在新 Leader 上继续生效    |
| 会话注册表（sessionId → nodeId）     | ✅ 复制                     | 集群需知道会话归属哪个节点的连接            |
| FIFO 等待队列                        | ❌ 不复制（Leader 内存）    | §4.4 设计决策                               |
| 服务端限额/配置                      | ❌ 不复制（各节点本地配置） | 运维配置，非锁状态                          |

### 4.2 日志条目定义

在 `openlatch-protocol` 新增 `raft.proto`（服务端内部消息，客户端不收发）：

```proto
enum RaftEntryType {
  RAFT_ENTRY_UNKNOWN   = 0;
  SESSION_OPEN         = 1;
  SESSION_CLOSE        = 2;
  LOCK_ACQUIRE_ENTRY   = 3;
  LOCK_RELEASE_ENTRY   = 4;
  LEASE_RENEW_ENTRY    = 5;
  LEASE_EXPIRE_ENTRY   = 6;
  NOOP                 = 7;   // Leader 当选后用于确认提交位点
}

message RaftLogEntry {
  RaftEntryType type             = 1;
  int64         seq              = 2;   // Leader 分配的全局序号，用于幂等与诊断
  int64         wall_clock_ms    = 3;   // Leader 发起时刻，仅诊断
  bytes         command_payload  = 4;   // 复用 Phase 1 请求消息的序列化
}
```

`command_payload` 直接复用 Phase 1 的 `AcquireRequest` / `ReleaseRequest` / `LeaseRenewRequest` 加上 `session_id` 字段包装—— **状态机应用逻辑与 Phase 1 单机路径完全同一份代码**（都调用 `CoreEngine`）。

### 4.3 确定性与时间问题

Raft 要求状态机确定性回放。锁语义中唯一的非确定来源是 **时间**（租约到期、`nowMs()`）。处理：

1. **到期由 Leader 驱动**：Leader 的租约扫描线程发现到期时，追加 `LEASE_EXPIRE_ENTRY(key, leaseToken)` 日志；所有副本在回放该条目时才真正释放。到期判断只发生在 Leader，回放侧不自行判断时间——同一日志序列产生同一状态；
2. 回放/追赶期间，历史 `LEASE_EXPIRE_ENTRY` 照常重放；快照点之后的到期由新 Leader 的扫描线程继续驱动；
3. **时钟前提**：`wall_clock_ms` 只用于到期计算与诊断，要求节点间 NTP 同步、漂移 ≤ 1s。默认租约 30s ≫ 漂移容限，误差影响可忽略。该前提写入部署文档（§9）；
4. `CoreEngine` 注入的 `Clock` 在集群模式下使用"条目携带时刻"回放：回放 `LOCK_ACQUIRE_ENTRY` 时，租约到期 = 条目内记录的授予时刻 + 租期，而非回放时的物理时钟。**S1 PoC 修订（v1.1）**：经 `LockStateMachineCore.EntryClock`（apply 线程 thread-local 条目时刻注入）在两库 applier 上验证成立，`CoreEngine` 可保持零改动——命令级时间覆盖参数由"唯一增量改动"降级为 S2 可选的显式化改进，**S2 评审定夺：不做**（v1.3 回写，S2 design D2）——改 `CoreEngine`/`AcquireCommand` 公开签名违背 §1.4 零改动原则；EntryClock 的 thread-local 前提（applier 单线程、apply 无跨线程逃逸）写入 `LockStateMachine` 类级 Javadoc 为显式契约，并以多线程不串扰反例测试固化。

### 4.4 设计决策：等待队列不复制

- **决策**：等待队列仅存在于当值 Leader 内存。
- **理由**：
    1. 排队是瞬态优化状态，不是锁的正确性状态——丢失队列只影响等待者，不影响互斥；
    2. 复制队列会把"通知→重发"的往返耦合进日志，显著放大写入量与状态机复杂度；
    3. 概要设计 §7 已声明 Leader 切换时"锁操作快速失败重试"，等待者重试正是既定语义。
- **切换时行为**：旧 Leader 上的等待项随其日志中止被丢弃；客户端因连接仍在（节点存活但失去 Leadership）收到 `NOT_LEADER`，或连接已断而触发重连——两条路径都导向"向新 Leader 重新排队"，等待位次重置。公平性语义表述更新为"**单个 Leader 任期内的严格 FIFO**"。

### 4.5 Leader 上的请求路径

```
客户端 ── ACQUIRE ──▶ Leader 接入层
  → 预检查（本地状态快速判断：限额、key 校验）
  → 生成 RaftLogEntry 提交复制组
  → 多数派确认、状态机应用（调用 CoreEngine）
  → 按应用结果构造 AcquireResponse 回复客户端
```

- **授予/排队的结果由状态机应用时刻的状态决定**，预检查只是快速失败通道，最终以应用结果为准；
- 排队（QUEUED）同样写日志吗？ **不**——排队不是复制状态（§4.4）。Leader 在任期作用域的独立 `WaitQueue`（`server.raft`，与 `CoreEngine` 解耦）中登记等待者并立即回 `QUEUED`；仅授予/释放/续租/会话变更写日志。由此：
    - ACQUIRE 在"可授予"时走复制路径（延迟 ≈ 一次多数派提交）；
    - "需排队"时本地即时响应（延迟与单机一致）；
    - 状态机回放的 ACQUIRE 条目因此总是"授予"语义（提交前已在 Leader 本地预演判定可授予）；并发同键令预演在应用点失效时，Leader 在 apply 时刻本地补登记并回 QUEUED，Follower 忽略队列副作用（队列非复制状态，副本一致性不破坏）；Leader 在窗口内死亡则排队丢失，由客户端重排队覆盖（§8，队列可丢为既定语义）。

    **登记主体为何不是 `CoreEngine`（v1.3 / S2 design D9）**：等待项若登记进本地引擎，降级节点会残留陈旧等待项，回放同 key 授予条目时被"队列非空禁止越过在队者"规则拦成 DENIED/QUEUED 与其他副本分歧——复制状态（锁表）被**非复制状态**（队列）污染。故集群路径引擎永不登记等待项（apply 恒 `queueIfBusy=false`），排队/位次/同 `(sessionId,requestId)` 去重/深度限额由 `WaitQueue` 承载并在 WinLeadership 时清空重建——任期边界机械保证 §4.4 的"单个 Leader 任期内的严格 FIFO"。

**Follower 写请求分车道（v1.2 / S3/P2-12）**：非 Leader 节点对三类写按"是否依赖当值节点本地态"分道处理——

| 车道 | 请求 | Follower 处理 | 依据 |
|------|------|--------------|------|
| 拒绝+改道 | ACQUIRE | 立即 `NOT_LEADER` + Leader 提示（选举空窗 nodeId=-1），不产生条目、不动等待队列 | 排队登记与 `AWAIT_NOTIFY` 推送是 Leader 本地态（§4.4），Follower 受理无法保证通知送达 |
| 转发 | RELEASE / RENEW | 摘除角色门，经内部提交通道（与 `SESSION_OPEN` 同车道，即 §3.2 `ForwardingProxy` 机制本体）提交至当值 Leader 复制执行 | 纯 token/归属校验、无 Leader 本地态依赖；Leader 按 §5.2 规则 2 校验 `sessionId` 登记于复制状态 |

由此"Leader 切换后存活会话的存量锁可续可解"成立（§11 验收 2），且该转发对 v1 客户端同样生效。客户端连接拓扑对应设计为"稳态单连接（home 即 Leader），故障期双车道（home 转发出口 + Leader 获取道）"（`s3-leader-discovery-failover` design D6）。

---

## 5. 会话的集群化

### 5.1 问题

Phase 1 中"连接断开 → 立即清理该会话的锁"依赖连接与服务端同进程。集群下必须回答：会话注册在哪个节点、连接断开如何传播、节点宕机会话如何清理。

### 5.2 设计

1. **会话归属于接入节点**：客户端连接节点 N，HELLO 时 N 分配 `sessionId = (nodeId, localSeq)`（高位 nodeId 保证全局唯一），并向 Leader 提交 `SESSION_OPEN` 日志；
2. **写请求携带会话上下文转发**：客户端被重定向到 Leader 后，Leader 接入层校验 `sessionId` 已登记（复制状态中存在）再处理；
3. **连接断开传播**：节点 N 检测到连接断开 → 向 Leader 提交 `SESSION_CLOSE(sessionId)` → 状态机执行与 Phase 1 `sessionClosed` 相同的清理；
4. **接入节点宕机**：Raft 成员检测到节点 N 失联（心跳超时）→ Leader 对复制状态中所有归属 N 的会话执行批量清理。清理以日志条目（每会话一条 `SESSION_CLOSE`）落地，保证各副本一致；
5. **客户端视角**：连接的是接入节点；接入节点存活时，即使 Leader 切换，连接与会话保持有效（会话在复制状态中，新 Leader 认账）——这是"Leader 切换锁不丢"成立的前提。

### 5.3 会话续命

接入节点与客户端之间沿用 Phase 1 空闲检测（PING/IdleState）；集群不引入额外会话 TTL——连接活着 = 会话活着，语义与单机一致。

---

## 6. 协议扩展（S3 客户端侧）

### 6.1 向后兼容原则（v1.2 实现口径）

- `protocol_version` 升为 **2**；服务端握手接受区间 `[1,2]`（`OpenLatchServer.isClientVersionSupported`），区间外回 `INVALID_REQUEST` 并断连，不做隐式兼容；应答信封 `protocol_version` **回显请求版本**，故 v1 客户端看到与 Phase 1 同形的响应（新增字段为其未知字段，proto3 容忍）；
- **v1 客户端在集群模式的实际能力**（分车道，见 §4.5/§3.2）：其存量锁的 RENEW/RELEASE 经 Follower 转发车道正常送达 Leader（**可用**）；仅"新 ACQUIRE 恰好落在 Follower"时会收到 `NOT_LEADER`——v1 不理解 hint，表现为按普通错误码失败，需应用重试或（推荐）升级到 v2 客户端获得自动改连；单机模式行为与 Phase 1 逐字节一致；
- 不删除、不复用任何 Phase 1 字段与错误码；仅新增枚举值/字段/消息。Phase 1 已发布编号由 `OpenlatchProtoContractFreezeTest` 逐行钉死。

### 6.2 新增/变更（v1.2 定稿编号）

```proto
// Envelope.protocol_version：v1 固定 1；v2 起请求填 1 或 2，服务端回显请求版本
// MessageType 新增：CLUSTER_VIEW = 7（请求无 payload，任意节点作答）

// HelloResponse：复用 Phase 1 预留的 leader_hint 承载 nodeId，新增地址字段
int64  leader_hint    = 5;  // v2 启用（Phase 1 预留）：Leader 的 nodeId；集群模式必填——
                            // >0 为 Leader nodeId，-1=本节点暂不知晓，单机模式不填(0)
string leader_address = 6;  // v2：Leader 接入地址（host:port）；未配置地址映射时为空串

// 三类写响应：NOT_LEADER（Phase 1 预留码，v2 启用）随附提示载体（仅拒绝路径填充）
AcquireResponse     { int64 leader_node_id=6;  string leader_address=7;  }
ReleaseResponse     { int64 leader_node_id=3;  string leader_address=4;  }
LeaseRenewResponse  { int64 leader_node_id=3;  string leader_address=4;  }

// 新增消息（Envelope oneof 新增字段 = 19）
message ClusterView {         // 应答含 status 自述（OK+成员表 / 错误码+空表），v2 未发布前增补
  repeated NodeInfo nodes = 1;
  StatusCode status      = 2;  // 保证单机拒绝路径状态码线路可见
}
message NodeInfo {
  int64  node_id   = 1;
  string address   = 2;  // 未配置 client-addresses 时空串（客户端种子自报兜底）
  bool   is_leader = 3;
}
```

> v1.1 草稿曾拟 `HelloResponse` 另起 `leader_node_id=6/leader_address=7`；实现复用 Phase 1 既预留的 `leader_hint=5` 作 nodeId、`leader_address=6` 作地址，避免字段 5 空置与双 id。三类写响应的 hint 载体（v1.1 §6.3 提及"随附 leaderHint"但未定义字段）在此补齐。

### 6.3 客户端 Leader 发现与故障转移

```
启动：按种子节点列表（配置多个地址）任一建连 → HELLO
  → HelloResponse.leader_address
      有效 → 直连 Leader（接入连接保留用于 CLUSTER_VIEW 等查询）
      无效（选举中）→ 退避后重新 HELLO
运行中：
  收到 NOT_LEADER 响应 → 读取随附 leaderHint → 重连新 Leader，重放未完成请求
  连接断开 → Phase 1 重连状态机；重连目标先试原地址，失败后轮询种子列表
  连续 N 次（默认 3）请求得到 NOT_LEADER → 强制走一次种子列表发现
```

- 未完成请求的重放遵循 Phase 1 幂等规则（同 `requestId` 重发；新会话需要新 `requestId` 空间，重连后重置）；
- **快速失败优先**：切换窗口内请求不无限等待——请求超时（默认 5s）内未找到可用 Leader 即向调用方抛错，由应用决定重试（与概要设计 §7 一致）。

---

## 7. 快照与恢复（S4）

### 7.1 快照内容

| 内容         | 说明                                                                    |
|--------------|-------------------------------------------------------------------------|
| 全部锁条目   | key、mode、写持有者+计数、所有读持有者+计数、leaseToken、到期时刻、租期 |
| 会话注册表   | sessionId → nodeId                                                      |
| 日志位点     | 由 Raft 库管理                                                          |
| ~~等待队列~~ | 不快照（不复制，§4.4）                                                  |

序列化：复用 `raft.proto` 定义 `SnapshotState` 消息（锁条目 repeated），单一二进制文件；加载即反序列化后重建 `CoreEngine` 状态。

> **v1.4 实现落点（S4/P2-15）**：`SnapshotState` 在 S2 骨架（`locks`/`sessions`）基础上增补 `next_lease_token=3`——<b>发号水位</b>（S4 design D10）：凭证发号是历史累计量，被释放条目的已消耗凭证不出现在快照中，若仅按"继承最大凭证+1"起号，重建副本与未截断副本会对同一尾部日志发出不同 `leaseToken`，跨副本状态永久分叉（PoC"回灌无法复现 token 序列"的镜像）。水位不入 digest（`ShadowTable.toProto` 不变），由 `LockStateMachineCore.snapshotState()` 组合、`installSnapshot` 消费；缺字段（值 0）按 max(凭证)+1 兜底。重建入口实为 core 新增 `CoreStateRestore` 值对象 + `CoreEngine.restoreFrom`（§1.4 口径修正）——"包级私有"字面不可行（core/server 跨模块跨包），取"public 入口 + Javadoc 契约限定唯一调用方与用途"。重建内容四件套：锁表直写、会话登记、到期堆回填、发号水位落位；等待队列不恢复（集群引擎无等待项，design D9）。切割点不变性（≥100 组随机序列"前缀装快照+后缀回放"与全程直接回放 digest 一致）是该水位的机械判据。

### 7.2 触发与保留

- 触发：日志条目数阈值（默认 100 万条，可配置）或手动管理命令；
- 保留最近 2 份快照；快照写入期间状态机照常服务（先取条目锁内的一致性快照副本，再异步落盘）。

> **v1.4 实现落点（S4/P2-15）**：自动触发 = 装配层把 `snapshot-threshold` 接入 Ratis `auto-trigger`（S2 关闭的那行在 S4 打开）；保留 2 份 = `raft.snapshot.retention.file.num=2`（快照位点已应用⊆已提交，截断推进至快照位点对多数派安全，装配层恒置 `purgeUptoSnapshotIndex=true`——否则任一缺席节点卡死截断，§7.3-2 安装流永不触发）；手动管理命令落点为 `RaftSubsystem.triggerSnapshot()`（任意角色节点可调，语义同自动触发，S4 design D6；不引入管理端点）。"异步落盘"口径（design D4）：`applyLock` 内 `shadow.toProto()` 一致性副本 → <b>放锁后</b>写盘提交（tmp→原子 rename→MD5 伴随）；完全异步（先返位点后落盘）会使截断先于持久化而失快照，被否决——落盘期间应用线程的短暂停为既定代价，耗时由 §10 快照基准度量（实测 10 万条目：落盘 45ms）。

### 7.3 恢复流程

1. 节点启动：加载最新快照 → 重建锁状态 → 回放快照后日志；
2. 落后过多的节点：由 Raft 库安装 Leader 的快照后继续增量追赶；
3. 追赶期间节点可接入只读查询（`CLUSTER_VIEW`），写请求一律 `NOT_LEADER`。

> **v1.4 实现落点（S4/P2-16，design D5 spike 结论）**：Ratis 3.3.0 无 `loadSnapshot(SnapshotInfo, InputStream)`——安装流为库写快照文件到 `SimpleStateMachineStorage` → `stateMachine.pause()`（自行迁移生命周期 RUNNING→PAUSED）→ 库原子发布（tmp→`snapshot.T_I`，含 MD5 伴随）→ `StateMachineUpdater.reload()` 调 `reinitialize()`（状态机重扫盘上最新快照、`installSnapshot` 整体重建、位点钉到快照位点、生命周期回 RUNNING）→ 增量回放续起。要点：安装发布不经状态机引用（reinitialize 必须盘上重扫 `loadLatestSnapshot`）；MD5 伴随文件是有效性判定输入（takeSnapshot 必须 `computeAndSaveMd5ForFile`）；`initialize` 须经 `startAndTransition` 把生命周期推到 RUNNING。启动加载与安装共用同一 `loadSnapshot(SingleFileSnapshotInfo)` 通道。追赶窗口写请求 `NOT_LEADER` 由 S3 角色门天然覆盖（安装中恒为 Follower）。结论档案见 `openspec/changes/phase2-s4-snapshot-recovery/observations-ratis-3.3.0-s4-snapshot.md`。

### 7.4 成员变更

- 采用 Raft 库提供的成员变更（Ratis：`setConfiguration`；JRaft：`changePeers`）；
- 运维流程文档化：先加新节点并等待追赶完成，再移除旧节点；禁止同时变更多数派成员；
- 成员变更期间的会话清理：被移除节点上的会话按 §5.2 规则 4 清理。

---

## 8. 故障场景行为表

| 故障                           | 集群行为                                                         | 客户端可见                                                                                | 正确性依据                                                            |
|--------------------------------|------------------------------------------------------------------|-------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| 杀 Leader（接入节点 = Leader） | 选举新 Leader（< 10s）；新 Leader 批量清理死节点会话持有的锁     | 连接断开 → 重连种子节点 → 发现新 Leader；持有锁若绑定该连接则失锁（回调）；等待项重新排队 | 会话归属复制状态；互斥不因切换破坏                                    |
| Leader 角色转移（节点存活）    | 日志复制照常在新 Leader 继续                                     | 收到 `NOT_LEADER` → 自动改连；**连接未断的会话持有的锁保留**                              | 持有关系与租约已复制                                                  |
| 杀 Follower                    | 无服务影响（多数派仍满足，3 节点容忍 1）                         | 无感（除非连接的恰是该节点 → 走重连）                                                     | 多数派                                                                |
| 网络分区（少数派侧）           | 少数派无法选主、无法提交                                         | 少数派侧节点的写请求超时/拒绝                                                             | Raft 多数派；无双主授予                                               |
| 滚动重启                       | 逐台重启，任意时刻 ≥ 多数派存活                                  | 短暂 `NOT_LEADER`/重连，服务不中断                                                        | §7.4 运维流程                                                         |
| 快照点前后重启                 | 快照 + 日志回放恢复                                              | 无感（该节点非 Leader 时）                                                                | §7.3                                                                  |
| 切换瞬间的"预演授予"           | 旧 Leader 已回复客户端但未完成复制的授予 → 日志丢弃 → 该授予失效 | 客户端续租/解锁时得到 `INVALID_TOKEN`/`NOT_HELD` → 触发锁丢失处理，重新竞争               | 与概要设计 §7"failover 后锁可能失效"声明一致（与 Redis 主从切换同理） |

**一致性声明**（写入用户文档）：OpenLatch 集群提供"已确认授予的锁不丢、任何时刻至多一个持有者"的保证；切换窗口内未完成复制的授予可能回滚，客户端以续租/解锁的错误码感知并重新竞争。

## 9. 部署与配置

新增配置项（并入 Phase 1 的 Properties 体系）：

| 配置键                                  | 默认值    | 说明                                  |
|-----------------------------------------|-----------|---------------------------------------|
| `openlatch.cluster.enabled`             | `false`   | 关闭即 Phase 1 单机行为（同一二进制） |
| `openlatch.cluster.node-id`             | 必填      | 节点唯一 id                           |
| `openlatch.cluster.peers`               | 必填      | `id@host:raftPort` 列表               |
| `openlatch.cluster.client-addresses`    | 空（可选）| v1.2 新增：`id@host:port` 接入地址映射，供 Leader 提示与 `CLUSTER_VIEW` 作答。缺省不阻塞启动，`leader_address` 降级为空串、客户端以种子自报兜底 |
| `openlatch.cluster.raft-port`           | `9411`    | Raft 复制通信端口                     |
| `openlatch.cluster.data-dir`            | `./data`  | 日志与快照目录                        |
| `openlatch.cluster.snapshot-threshold`  | `1000000` | 快照触发条目数（S4 起接入自动触发；保留 2 份由装配层钉死，不入配置） |
| `openlatch.cluster.election-timeout-ms` | `3000`    | 依 Raft 库语义透传                    |
| `openlatch.cluster.log-segment-bytes`   | `0`       | v1.4（S4）新增：Raft 日志 segment 上限字节，依 Raft 库语义透传（0=库默认；运维一般不配，测试以小值驱动截断/安装流） |

部署要求：节点间 NTP 同步（漂移 ≤ 1s，§4.3）；`data-dir` 建议独立磁盘。

> **v1.5 装配契约（发布收口 / P0 修复）**：Ratis 的 proxy/server/client 三组线程池由装配层钉死为**非缓存固定池（size=4）**，不入配置。动因：Ratis 3.3.0 `RaftServerProxy.close` 以 fire-and-forget 将组关停派发进 proxy 池，而默认 cached 池 worker 空闲 60s 全部回收——空闲节点（>60s 无派发）的优雅关停任务可能永不被执行，关停线程挂库内 1 天超时（生产 SIGTERM 停机必中）。固定池常驻 worker 从构造上消除该前提；取证与回归见 `openspec/changes/phase2-release-closure/observations-ratis-3.3.0-soak-shutdown-hang.md` 与 `IdleNodeGracefulStopIT`（规约化入 `cluster-node-lifecycle`"长空闲后优雅关停有界"场景）。

## 10. 测试设计

| 层次       | 内容                                                                                                                            |
|------------|---------------------------------------------------------------------------------------------------------------------------------|
| 状态机单元 | 日志条目序列回放确定性：同序列两次应用状态逐字段一致；到期条目回放不依赖物理时钟                                                |
| 复制集成   | 3 节点嵌入式集群（Raft 库测试工具）：授予经多数派确认；停 1 节点仍可服务；停 2 节点不可授予                                     |
| 故障演练   | 实施计划 §5.2 验收清单 1–7 逐项自动化（杀 Leader 计时、分区隔离用进程组/网络命名空间隔离实现）                                  |
| 快照       | 10 万锁条目快照：大小、落盘耗时、加载耗时；加载后状态与集群一致（全量比对工具）                                                 |
| 客户端     | 种子发现、`NOT_LEADER` 重定向、切换窗口内超时行为、幂等重放                                                                     |
| 混沌       | 随机杀节点 + 持续负载（≥10 分钟），不变式检查器：任何时刻同 key 至多一个写持有者；无锁泄漏（停负载 + 等待一个租约期后锁表为空）。**v1.5 双档达成**：常规回归为 ~18s 短窗口（缺省语义）；字面 ≥10 分钟 soak 档经 `-Dopenlatch.chaos.soak-minutes` 启用，发布级单轮实测 `loadWallMs=601038 / kills=510 / restarts=509 / grants=2567 / conflicts=0`（2026-09-05，静息态） |

## 11. Phase 2 验收标准

即实施计划 §5.2 验收清单（7 项），此处给出判定口径：

1. ✅ 3 节点正常服务 —— 复制集成测试全绿；
2. ✅ 杀 Leader 恢复 < 10s、存活会话锁不丢 —— 故障演练计时与锁保留断言；
3. ✅ 少数派不能授予 —— 分区演练断言少数派全部写请求失败（主辅双轨：辅轨 `MinorityQuorumTest`，S4 归档；主轨 netns 真分区 `docs/partition-drill-2026-09-06.md`，会话化 ACQUIRE×4 判 NOT_LEADER、RELEASE 道非 OK、Leader 侧同凭证释放成功证锁存活，撤分区自动收敛；v1.5 收口绿）；
4. ✅ 快照恢复一致 —— 全量比对通过；
5. ✅ 滚动重启不中断 —— 演练期间客户端错误率 < 1%（仅切换窗口瞬时错误）；
6. ✅ 切换期无死锁无泄漏 —— 混沌测试不变式检查通过；
7. ✅ 文档声明一致 —— 用户文档含 §8 一致性声明原文。

## 12. 遗留与风险

| # | 项                                                                    | 处理                                                       |
|---|-----------------------------------------------------------------------|------------------------------------------------------------|
| 1 | 等待位次在 Leader 切换时重置（公平性降为"任期内 FIFO"）               | 已在 §4.4 声明；若用户反馈强烈，后续评估复制轻量排队元数据 |
| 2 | 到期由 Leader 驱动：Leader 长时间无写入时到期延迟 ≤ 扫描周期（500ms） | 可接受                                                     |
| 3 | 时钟漂移超出假设（> 1s）时租约误差放大                                | 部署文档强制 NTP；监控（Phase 3）可加节点时钟偏移指标      |
| 4 | Raft 库与 Netty 的线程/内存共存细节依赖 S1 PoC 结论                   | **已关闭（v1.1）**：Ratis thirdparty 全着色，与主干 protobuf 3.25.5 / netty 4.1.137 零类路径交集（`poc/raft-selection/friction-ratis.md`） |
| 5 | Ratis 3.3.0 leader 复制停摆：旧 leader 带脏条目重启归群的语境下，新 leader 任期 NOOP 可能永不提交（双稳态），写面 `LeaderNotReady` 阻塞 200+ 秒不自愈 | **在案（v1.5）**：存量 P1（差分实验证明非收口期引入；S4"R2 离群轮 24.22% 瞬态"旧判定由本行更正）。运维缓解：滚动重启推荐"先从不先主"（已入部署文档）；跟进：独立 change 立项——leader 自愈看门狗评估 / Ratis 3.3.1 升级跟踪。取证与跟进计划见 `openspec/changes/phase2-release-closure/defects/leader-replication-stall-ratis-3.3.0.md` |

## 13. 实施子任务拆分

**粒度定义**：同 Phase 1——每个子任务可独立交付、独立验证，编号（P2-xx）稳定。S1 的 PoC 子任务产出允许一次性（throwaway），选型定案后的实现才进主干。

### 13.1 S1：Raft 选型 PoC（已完成，v1.1）

| ID    | 子任务             | 内容与交付物                                                                                                 | 前置         | 验证                                  |
|-------|--------------------|--------------------------------------------------------------------------------------------------------------|--------------|---------------------------------------|
| P2-01 | PoC 环境搭建       | 本机 3 节点最小集群骨架（两库共用负载脚本与计时工具）                                                        | Phase 1 发布 | ✅ 两库均可组集群并完成选主（`poc/raft-selection/`） |
| P2-02 | Ratis 接入原型     | `CoreEngine` 挂入 Ratis 状态机；单键授予/释放走复制                                                          | P2-01        | ✅ 功能跑通；胶水 321 LOC（`friction-ratis.md`） |
| P2-03 | SOFAJRaft 接入原型 | 同上范围，JRaft 侧实现                                                                                       | P2-01        | ✅ 功能跑通；胶水 306 LOC（`friction-jraft.md`） |
| P2-04 | 对比评估与选型定案 | 按 §2.4 门槛逐项实测：授予延迟 P99、吞吐、选主计时、快照恢复耗时、API 侵入度；输出选型报告并回写本文档修订版 | P2-02、P2-03 | ✅ 报告 `docs/raft-selection-report.md` 含逐项数据；评审中；**S1 退出** |

### 13.2 S2：复制状态机（已完成，v1.3 回写）

证据：`openspec/changes/archive/2026-08-30-phase2-s2-replicated-state-machine/`（含 `zero-touch-evidence.txt`、`observations-ratis-3.3.0.md`）。

| ID    | 子任务                  | 内容与交付物                                                                            | 前置         | 验证                                       |
|-------|-------------------------|-----------------------------------------------------------------------------------------|--------------|--------------------------------------------|
| P2-05 | `raft.proto` 定义       | `RaftEntryType` / `RaftLogEntry`（§4.2）、`SnapshotState` 骨架（§7.1）                  | P2-04        | ✅ 生成代码编译通过；golden 文件冻结编号比对，Phase 1 wire format 零 diff |
| P2-06 | LockStateMachine 适配器 | 日志条目 → `CoreEngine` 应用；回放时钟用条目时刻（§4.3）；NOOP 条目                     | P2-05        | ✅ 确定性回放属性测试（≥100 组随机序列两次回放 digest 一致）；`openlatch-core` git diff 为空 |
| P2-07 | ReplicationGateway      | Leader：预检查 → 提交日志 → 应用后响应；QUEUED 本地即时响应不写日志（§4.5）             | P2-06        | ✅ MiniRaftCluster 授予/排队两路径用例通过（含停 1 Follower 追平、失主快速失败） |
| P2-08 | 会话集群化              | `SESSION_OPEN/CLOSE` 条目、`sessionId=(nodeId, localSeq)`、接入节点失联批量清理（§5.2） | P2-07        | ✅ 登记/断连清理/失联清理三路 digest 一致；failover 后新 Leader 认账存活会话 |
| P2-09 | 租约到期复制            | Leader 扫描驱动 `LEASE_EXPIRE_ENTRY`；回放按 token 幂等校验                             | P2-07        | ✅ failover 后到期继续生效（误差 ≤ 一个扫描周期）；ABA 交错空操作用例通过 |
| P2-10 | 3 节点复制集成          | 多数派确认、停 1 节点仍可服务、停 2 节点不可授予                                        | P2-08、P2-09 | ✅ §10 复制集成全绿，`mvn clean verify` 全 reactor 绿；**S2 退出** |

### 13.3 S3：Leader 发现与客户端故障转移（已完成，v1.5 回写）

证据：`openspec/changes/archive/2026-08-31-s3-leader-discovery-failover/`（含 `s3-exit-checklist.md`；实现回写见 v1.2）与 `docs/failover-drill-2026-08-31.md`。

| ID    | 子任务                         | 内容与交付物                                                                         | 前置  | 验证                                |
|-------|--------------------------------|--------------------------------------------------------------------------------------|-------|-------------------------------------|
| P2-11 | 协议 v2 扩展                   | `HelloResponse` leader 字段、启用 `NOT_LEADER`、`CLUSTER_VIEW`；服务端兼容 v1 客户端 | P2-10 | ✅ 协议测试全绿（`OpenlatchProtoContractFreezeTest`/`ProtocolCodecTest`/`HandshakeTest` v1·v2·v3 三分支）；v1 客户端行为回归零 diff |
| P2-12 | LeaderTracker 与 Follower 分车道 | `LeaderTracker`（保留 Ratis 新主身份→`{nodeId,address}` 提示单源，§3.2/§4.5）；Follower 分车道：ACQUIRE 回 `NOT_LEADER`+提示、RELEASE/RENEW 经转发道由 Leader 复制执行（§4.5） | P2-11 | ✅ 角色切换后的响应行为用例通过（`LeaderFailoverServerTest` 7 项：拒绝+hint==真主、选举空窗 hint=-1、跨 failover 续租/释放、未登记会话转发被拒）；提示一致性三消费方同源 |
| P2-13 | 客户端 Leader 发现             | 种子列表、HELLO hint 直连、`NOT_LEADER` 重定向、连续 3 次失败强制发现（§6.3）        | P2-12 | ✅ §6.3 流程逐分支用例通过（`LeaderDiscoveryTest` 5 项 + `ClientClusterIT` 5 项端到端：存活会话持锁不丢、等待者跨 failover 重排） |
| P2-14 | 杀 Leader 演练自动化           | 计时脚本 + 存活会话锁保留断言 + 等待者重排队断言                                     | P2-13 | ✅ 端到端恢复 < 10s（实测 1621–1806ms，7 有效样本）；全量演练 4/4；**S3 退出** |

### 13.4 S4：快照与恢复、容错加固（已完成，v1.4 回写）

证据：`openspec/changes/phase2-s4-snapshot-recovery/`（`s4-exit-checklist.md`、`s4-zero-touch-evidence.txt`、`observations-ratis-3.3.0-s4-snapshot.md`）；发布收口（主轨分区、soak 字面达标、P0/P1 处置）：`openspec/changes/phase2-release-closure/`。

| ID    | 子任务             | 内容与交付物                                                                              | 前置  | 验证                                   |
|-------|--------------------|-------------------------------------------------------------------------------------------|-------|----------------------------------------|
| P2-15 | 快照生成           | `SnapshotState` 序列化（含发号水位）、条目锁内一致性副本 + 锁外落盘、保留 2 份、手动触发（§7.1/§7.2） | P2-14 | ✅ 快照可加载（round-trip + 切割点不变性 109 组）；快照期间服务不受影响（`ClusterSnapshotTest` 低阈值并发负载零错误） |
| P2-16 | 快照加载与追赶     | 启动加载 + 日志回放；Ratis 3.3 pause→reload 安装流；10 万条目基准与全量比对工具（§7.3）    | P2-15 | ✅ 恢复 < 30s（实测 521ms）；全量比对一致（`StateComparisons.diff` 空）；`ClusterSnapshotRecoveryTest` 两恢复路径 |
| P2-17 | 成员变更           | `RaftSubsystem.setMembers/removeVoter` + `ClusterRuntime.removeMember`；"先加后删"运维文档；变更期间会话清理 | P2-16 | ✅ `ClusterMembershipTest`：加节点（listener→追平→升票→当选服务）、删节点会话清理、多数派护栏拒绝 |
| P2-18 | 分区与滚动重启演练 | 少数派不可授予断言；滚动重启客户端错误率统计；容错加固（失联判定）                          | P2-17 | ✅ 辅轨 `MinorityQuorumTest` 全绿（并暴露/修复失联判定误伤——§4.5 未述之实现缺陷，D12）；**主轨 netns 真分区绿**（v1.5 收口，2026-09-06：会话化 ACQUIRE×4 判 NOT_LEADER、RELEASE 道非 OK、Leader 侧同凭证释放成功证锁存活、撤分区自动收敛；真跑 6 轮暴露并修复 6 处演练工具缺陷，见 `docs/partition-drill-2026-09-06.md` 与 observations/D5 扩展记录）；滚动重启双顺序 0.80%/0.00%（`docs/rolling-restart-drill-2026-09-02.md`）——先主后从序存在存量 P1 停摆（§12 风险 5 在案，运维推荐序"先从不先主"） |
| P2-19 | 混沌测试与验收闭环 | 随机杀节点 + 持续负载 + 不变式检查器（§10）；§11 验收证据收集；用户文档一致性                 | P2-18 | ✅ `ClientChaosIT` 零双授冲突/零泄漏/摘要收敛；`s4-exit-checklist.md` 逐项；**v1.5 收口补全**：字面 ≥10 分钟 soak 单轮达成（510 杀/509 重启/0 冲突/0 泄漏）、§11-3 主轨绿、P0 关停缺陷修复+规格化、存量 P1 在案（§12 风险 5）；`docs/Phase2-验收报告.md` 七项逐项闭环 + DoD 五条款自检；**Phase 2 发布** |
