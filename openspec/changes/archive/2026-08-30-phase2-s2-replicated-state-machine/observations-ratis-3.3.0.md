# S2 集成期观察项（Ratis 3.3.0）— 备 S4 评审

对应 tasks 6.4。来源：P2-06～P2-10 实现期实测（gRPC 传输、进程内 3/单机 5 节点、EmbeddedChannel 直驱）。

## 已验证为好的（S1 结论在集成级的再确认）

| 项 | 观察 |
|----|------|
| 提交后应用 | `StateMachineUpdater.applyLog` 仅应用 `applied < lastCommittedIndex` 条目，Leader/Follower 同一单线程 updater（design D10 的前提，集成测试"停 2 节点不可授予"反向锁证：存活单节点从未产生过授予成功应答） |
| 传输共存 | 与主干 netty 4.1.137 / protobuf 3.25.5 同 JVM 共存无冲突；shaded 类路径（`ratis-thirdparty-misc 1.1.0`）零交集复验通过 |
| 截断语义 | 降级/分区期间未提交条目被截断且从未进入状态机（无需 SM 回滚机制成立） |
| 客户端池 | 单 ClientId 在途串行复现（PoC 摩擦 #2）；池=4 足够本阶段负载 |

## S4 需带的风险与摩擦

1. **install pipeline 仍未实测**（S1 报告遗留，风险等级不变）：`notifySnapshotInstalled` 与 `SimpleStateMachineStorage` 的配合、Leader 主动 install 路径是 P2-16 的第一验证项。S2 状态机 `takeSnapshot()` 显式返回 `-1` + 装配层关闭 `Snapshot.setAutoTriggerEnabled`（默认开启、阈值 4096 会在集成压测中意外触发无实现快照——务必保留该关闭项直到 P2-15 落地）。
2. **`notifyLeaderChanged(newLeaderId)` 语义**：实测会在 `ServerState.setLeader(...)` 路径上对全体副本（含 Follower→Follower 的 newLeader 更名）触发，`newLeaderId` 可能为 `null`（步下）。任何"仅凭该事件计数任期"的实现都必须做 null 与"非本人更名"过滤——S2 网关据此实现了边沿触发（仅 `true→false` 终结 viaLeader 在途）。
3. **Ratis 客户端默认 `requestTimeout=1s`**：切主窗口内的正常重试易被计为提交失败。装配层已放宽至 10s（`RaftSubsystem`），S4/S3 若调整需在网关快速失败语义下重估。
4. **失联判定（D5）滞后实测 ≈ 探针周期 × (STALL_TOLERANCE+1)**：3 节点、election=500ms 时批量清理在 ~2-4s 收敛（集成用例 `lostAccessNodeTriggersBatchCleanup` 15s 时限内稳定）。S4 滚动重启演练若把 election 调小需同步复核误判。
5. **Java 25 告警**：`ratis-thirdparty` shaded netty 的 `sun.misc.Unsafe::allocateMemory` 终止期告警与 `System::loadLibrary` 受限方法告警（`--enable-native-access` 提示）——与 PoC 记录一致，不阻塞；升级 Ratis 时复验。
6. **`Division.getCommitInfos()`（D5 数据源）**：Leader 视角 per-peer commitIndex 由 `updateFollowerCommitInfos` 填充，空闲集群不前进——探针驱动方案的依据；若 Ratis 未来版本改语义需回归该用例。
7. **EmbeddedChannel 直驱经验**：接入层测试可完全绕开真实 socket 驱动集群异步桥（`runPendingTasks`+`readOutbound` 轮询）；S3 的 NOT_LEADER 重定向用例可沿用此基座（`ClusterHarness`）。

## 其它备忘

- `ClusterReplicationTest` 套件耗时 ~60s（12 用例×真实选主/追赶），进 `mvn verify` 默认门禁；若 CI 时长敏感，D8 预留 integration tag 拆分。
- PoC `installSnapshot` 的 token 空洞问题未带入 S2（快照回灌归 S4）；`SnapshotState` 已含 `lease_ms` 字段供重建回灌，S4 落地"包级私有重建入口"时需一并解决 token 序列复现（详设 §7.1 预留）。
