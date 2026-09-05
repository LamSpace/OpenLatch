# OpenLatch Phase 2 验收报告

| 项目     | 内容                                   |
|----------|----------------------------------------|
| 项目名称 | **OpenLatch**                          |
| 文档类型 | 阶段验收报告（Phase 2 / Raft 集群）     |
| 依据文档 | 《OpenLatch-Phase2-详细设计说明书》v1.5 |
| 版本     | v1.0（定稿；发布定夺见签署栏）           |
| 日期     | 2026-09-06                             |
| 作者     | Lam Tong                               |
| 状态     | 已签署                                 |

本报告逐项闭环《总体实施计划与验证方案》§5.2 Phase 2 验收清单（七项），并按 §5.4 完成定义（DoD）自检。证据以引用汇总，路径均相对仓库根。

## §11 验收标准逐项闭环

### 标准 1：3 节点集群正常服务——复制集成测试全绿

- 判据：授予/释放经 Leader 与多数派确认。
- 证据：`openspec/changes/archive/2026-08-30-phase2-s2-replicated-state-machine/`（S2 复制集成全绿，`mvn clean verify` 全 reactor）；S3 退出检查单全量构建记录（373 项 0 failures，`archive/2026-08-31-s3-leader-discovery-failover/s3-exit-checklist.md`）；2026-09-05 收口复跑 `clean verify` 全 reactor SUCCESS（6 模块，5:31，含 D8 关停修复与 soak 工具改动影响面）。
- 判定：✅

### 标准 2：杀 Leader 恢复 < 10s、存活会话锁不丢

- 判据：故障演练计时 + 锁保留断言。
- 证据：`docs/failover-drill-2026-08-31.md`（场景 A kill→首次成功授予 1621–1806ms，7 有效样本，判定线余量 ≥5.5 倍；死主会话失锁回调触发、停载零泄漏、存活让位持锁不丢）；收口当日复跑 `docs/failover-drill-2026-09-05.md`（1587ms，场景 B 杀 Follower 无感）；端到端面 `ClientClusterIT`（S3 归档）。
- 判定：✅

### 标准 3：少数派分区不能授予或释放任何锁

- 判据：分区演练断言少数派侧全部写请求失败——§11-3 字面含"不能授予或释放"两车道；HELLO 在少数派可完成本地握手（S3 规格允许 Follower 握手），不构成受理证据，判定以下两车道为准。
- 证据（辅轨）：`MinorityQuorumTest`（S4 归档，让主单侧+停余两节点：单节点 Leader 无法授予、复活后收敛且先期锁不丢）。
- 证据（主轨）：`docs/partition-drill-2026-09-06.md`（netns 桥接拓扑真分区：n3 ↔ (n1,n2) raft 面双向隔离、接入面保留；分区内多数派持续服务、少数派会话化 ACQUIRE ×4 判 NOT_LEADER、少数派携多数派真凭证 RELEASE 判非 OK、Leader 侧同凭证释放 OK（锁存活未被夺）、同键无分裂双授、撤分区自动收敛）。
- 判定：✅（主辅双轨）

### 标准 4：快照恢复一致

- 判据：触发后重启状态一致；新节点经快照+日志追赶加入。
- 证据：S4 归档（`archive/2026-09-03-phase2-s4-snapshot-recovery/s4-exit-checklist.md`）——`SnapshotBenchmarkTest` 10 万条目：4.48MB / 序列化 63ms / 落盘 45ms / 恢复 521ms（预算 30s）；`StateComparisons.diff` 空；切割点不变性 109 组随机序列；`ClusterSnapshotRecoveryTest` 双恢复路径；`ClusterMembershipTest` 加节点经安装流追平当选。
- 判定：✅

### 标准 5：滚动重启不中断（错误率 < 1%，持锁不丢）

- 判据：双顺序逐台重启，演练期客户端错误率 < 1%，切换窗口外零错误。
- 证据：`docs/rolling-restart-drill-2026-09-02.md`（先从后主 0.80% / 先主后从 0.00%，双序通过）；收口期复跑 `docs/rolling-restart-drill-2026-09-05.md` 暴露**先主后从序停摆**（26.02%/28.49%/5.13% 三轮，错误持续 200+ 秒不自愈）——差分实验（含摘除收口期全部产品改动的原二进制复跑）证明为**存量 P1 库层缺陷**：Ratis 3.3.0 新 leader 任期 NOOP 在"旧 leader 带脏条目重启归群"语境下可能永不提交（双稳态，选举/复制时序竞争调概率）。取证、机制链与跟进计划：`openspec/changes/phase2-release-closure/defects/leader-replication-stall-ratis-3.3.0.md`。
- 判定：⚠️ **条件通过**——09-02 双序绿证据成立；停摆缺陷按 DoD-3"P1 记录在案+跟进计划"承载，运维缓解（推荐序"先从不先主"）已入部署文档；发布定夺见"遗留与偏差记录"。

### 标准 6：切换期无死锁、无锁泄漏

- 判据：混沌不变式——任意时刻同 key 至多一写持有者；停载+一租约期后锁表空。
- 证据：常规回归档 `ClientChaosIT`（S4 归档 + 收口复跑 2026-09-05：27.07s，`kills=15 restarts=13 grants=81 conflicts=0`）；**字面 ≥10 分钟 soak 档**（详设 §10 时长口径，收口达成）：2026-09-05 静息态单轮 `loadWallMs=601038 kills=510 restarts=509 grants=2567 conflicts=0`，BUILD SUCCESS——首次 10 分钟复跑并全指标收敛，且以 1019 次优雅关停/重启全有界复核了 D8 关停修复。
- 判定：✅

### 标准 7：文档声明一致（Leader 切换行为与用户文档一致）

- 判据：用户文档含详设 §8 一致性声明原文。
- 证据：`docs/OpenLatch-Phase2-集群部署与故障转移.md` §3.3 原文在册（"已确认授予的锁不丢、任何时刻同一 key 至多一个持有者；切换窗口内未完成复制的授予可能回滚……"）；S3 退出时同步用户文档 §5 口径（归档检查单）。
- 判定：✅

## 缺陷修复记录（P0，收口期捕获并闭环）

- 缺陷：空闲节点（Raft proxy 线程池静默 >60s）执行优雅关停必永久挂起——Ratis 3.3.0 `RaftServerProxy.close` 将组关停 fire-and-forget 派发进 cached 池，worker 空闲回收后派发可能永不被执行，关停线程挂库内 1 天超时。生产语境：任何空闲集群的 SIGTERM 停机必中。
- 修复：装配层钉死 proxy/server/client 三组线程池为非缓存固定池（`RaftSubsystem.start()`，决策 D8），`cluster-node-lifecycle` 规格增"长空闲后优雅关停有界"场景。
- 回归：`IdleNodeGracefulStopIT`（drill 档，静默 65s→逐节点 stop 有界）绿；10 分钟 soak 复跑 510 杀/509 重启全有界（端到端复核）。
- 取证：`openspec/changes/phase2-release-closure/observations-ratis-3.3.0-soak-shutdown-hang.md` + `evidence/soak-hang-jstack-t+2{7,8}min.txt`。

## 基准数据汇总

| 组 | 指标 | 值 | 来源 |
|---|---|---|---|
| 选型 | 集群授予延迟 P99 / 杀主恢复 | 4.87ms / 548ms | `docs/raft-selection-report.md` |
| 杀主演练 | kill→可服务 | 1621–1806ms（09-01）/ 1587ms（09-05） | 两份 failover 报告 |
| 快照 | 10 万条目 大小/序列化/落盘/恢复 | 4.48MB / 63ms / 45ms / **521ms**（预算 30s） | `SnapshotBenchmarkTest` |
| 滚动重启 | 双序错误率 | 0.80% / 0.00%（09-02 绿）；停摆史见标准 5 | 两份 rolling 报告 |
| 混沌 | 字面 10 分钟 soak | 510 杀 / 509 重启 / 2567 授予 / **0 冲突 / 0 泄漏** | `ClientChaosIT` soak 档 |
| 分区 | 主轨真分区 | 少数派全非授予、撤除自动收敛 | `docs/partition-drill-2026-09-06.md` |

## 总体计划 §5.4 DoD 五条款自检

1. **交付物合入主干、`mvn verify` 全绿**：实现/测试/文档改动随本报告提交；`clean verify` 全 reactor SUCCESS（2026-09-05，6 模块）+ `-Pdrill` 辅轨（除在档 D9 停摆复现外全绿，含主轨真分区绿）。✅
2. **验收清单逐项有自动化用例或验收记录**：本报告七项逐项勾对。✅
3. **无已知正确性缺陷（P0 清零；P1/P2 在案+跟进）**：P0 关停挂起已修复+回归（上节）；**P1 leader 复制停摆在案**（`defects/leader-replication-stall-ratis-3.3.0.md`，跟进：独立 change 立项评估 leader 自愈看门狗 / Ratis 3.3.1 升级跟踪 / 运维推荐序已入部署文档）。✅（按条款口径）
4. **文档同步**：详设 v1.5（§13.3 S3 记账回写、混沌/分区/关停口径收口）；部署文档（滚动推荐序 + 故障表征）；公开 API 未新增（协议 v2 面 S3 已全 Javadoc）；代码侧 Javadoc 按 CLAUDE.md §5 同步（RaftSubsystem 装配契约、三个测试类）。✅
5. **基准记录在案、无显著退化**：上表六组；写延迟基线对比 Phase 1 单机 5ms 目标——集群 P99 4.87ms（PoC 数据）达标；快照/恢复/演练各值均在详设预算内。✅

## 遗留与偏差记录（如实）

1. **P1 leader 复制停摆（标准 5 ⚠️ 的展开）**：先主后从滚动重启存在双稳态；今日 4 轮复跑 3 轮命中、09-02 验收 1/1 绿——概率性，未定位到库内单点。运维缓解：滚动重启按"先从不先主"序（该序今日两轮全绿 0.46–1.92%）；根治立项跟进。**发布定夺留评审人签署。**
2. S4 归档检查单"R2 离群轮 24.22% 判定瞬态"的旧结论由本档案更正（同签名缺陷）。
3. 混沌常规回归仍为 ~18s 短窗口（语义与长窗等价）；字面 ≥10 分钟档为发布级单轮（本报告数据），非常规门禁。
4. 滚动重启历史观察（S4 起在案）：R2 24.22% 离群轮当时 4 轮未复现——见 1/2。
5. 演练工具层教训入档：`PartitionDrillIT` 非跳过路径首跑（收口期）连续暴露 6 处工具缺陷（br_netfilter 前提、宿主桥地址、JVM 绝对路径、判据协议语义、teardown 孤儿竞争、选举编排前置），均已修复并固化为幂等逻辑；主轨最终于 run-6 全绿（2026-09-06）。
6. 外观观察（主轨分区，run-6）：孤立节点 RELEASE 道错误码实测为 `NOT_HELD`（少数派以本地复制态判归属：新会话 sid≠持有者），与 `ReleaseResponse.status` 的 proto 注释"NOT_LEADER（仅转发失败路径）"码形不一致——判定语义正确（未释放、锁存活），错误码形状待随 §12-5 跟进 change 一并统一代码与注释。

## 发布宣告

- 版本：Phase 2（Raft 集群），协议 v2，模块 `openlatch-*` 1.0-SNAPSHOT → 发布评审通过后定版。
- 判据：《总体实施计划》§5.2 七项——六项 ✅、一项 ⚠️ 条件通过（P1 在案+运维缓解+跟进计划，DoD-3 口径）。
- 签署：评审人 ____________　日期 ____________
