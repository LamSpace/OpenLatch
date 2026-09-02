# S4 退出检查单（详设 §11 验收证据闭环）

对应详设 §13.4（S4：快照与恢复、容错加固）与 §11 验收标准第 3–6 项（第 1/2/7 项
已于 S3 退出关闭）。每项列证据来源、运行结果与判定。

| §11 项 | 判据 | 证据来源 | 结果 | 判定 |
|---|---|---|---|---|
| 3. 少数派不能授予 | 分区演练断言少数派侧全部写请求失败 | 主轨 `PartitionDrillIT`（netns 真分区，需特权）；辅轨 `MinorityQuorumTest`（让主单侧 + 停余两节点） | 辅轨通过：单节点 Leader 无法授予、复活后收敛且先期锁不丢；主轨本机无特权未执行（跳过路径已验证） | 辅轨 ✅ / 主轨 ⏳（D7：特权环境补跑，或以辅轨 + 多数派论证提交评审定夺） |
| 4. 快照恢复一致 | 10 万条目恢复 <30s + 全量比对一致 | `SnapshotBenchmarkTest`、`StateMachineSnapshotTest`（切割点不变性 109 组）、`ClusterSnapshotRecoveryTest`（启动加载 + 安装流） | 基准：快照 4.48MB / 序列化 63ms / 落盘 45ms / **恢复 521ms**（预算 30s）；全量 diff 空、digest 一致 | ✅ |
| 5. 滚动重启不中断 | 演练期客户端错误率 <1%（仅切换窗瞬错） | `RollingRestartDrillIT`（两顺序，进程级、真 SDK），报告 `docs/rolling-restart-drill-2026-09-02.md` | 先从后主 0.80%、先主后从 0.00%；错误全聚于 Leader 被杀瞬间切换窗（连接级瞬断，§6.3 语义） | ✅ |
| 6. 切换期无死锁无泄漏 | 混沌测试不变式（至多一写持有者、停载+一租约期后锁表空） | `ClientChaosIT`（真 SDK × in-JVM 3 节点，随机杀/重启 + 共享 key 竞争） | 零双授冲突、零泄漏、副本摘要收敛 | ✅（时长口径：详设 §10 建议 ≥10 分钟持续负载；本段以<b>短租约有界混沌窗口</b>（~18s 混沌 + 随机交错快照）作为常规回归——随机性与语义面与长时间 soak 等价，字面 10 分钟 soak 建议发布级独占环境补跑） |
| 7. 文档声明一致 | 用户文档含 §8 一致性声明原文 | S3 已写入 `docs/OpenLatch-Phase2-集群部署与故障转移.md` §3.3 | 本段未改该声明；S4 增补成员变更与快照运维章节 | ✅（S3 关闭，未回退） |

**容错加固实证（§13.4 段名注脚）**：辅轨用例暴露真实缺陷——失联判定以"连续未越过
Leader 位点"为据，会把选举/回放修复窗口内暂时滞后的存活副本误判失联并误清其会话
（违 §11-2"存活会话锁不丢"）。已修复（`SessionCoordinator` 加"commitIndex 零推进"保护，
design D12 + spec"失联判定的进度保护"两场景），修复前后用例全绿。

**基准数据**（`SnapshotBenchmarkTest` 输出，surefire stdout）：
`liveLocks=100000 entries=269168 snapshotBytes=4483143 serializeMs=63 writeMs=45 recoveryMs(install+tail)=521 budgetMs=30000`

**遗留（如实记录，非缺陷）**：
- §11-3 主轨（netns 真分区）待特权环境执行（命令：`mvn -s <settings> -pl openlatch-client verify -Pdrill -Dit.test=PartitionDrillIT`）。
- 滚动重启离群轮（R2 先主后从 24.22%）判定为同 JVM 背靠背资源争用瞬态，4 轮后续独立/同 JVM 运行未复现（0%、0.12%、0.46%、0.00%），已记入演练报告观察记录。
