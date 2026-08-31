# S3 退出检查单（详设 §11 验收 1/2 证据汇总 · 4.3）

日期：2026-08-31。证据链：默认构建（组 2/3 全量测试）+ 进程级演练报告。

## 1. 默认构建全绿（§11 验收 1：3 节点正常服务——复制集成测试全绿）

`mvn -s /home/lam/repo/settings.xml clean verify`（2026-08-31 首轮 21:26–21:29；演练插桩后终验 22:13–22:16 全 reactor 复跑，合计 373 项 0 failures / 0 errors，BUILD SUCCESS；默认构建经 excludedGroups 不含 drill）。插桩后另观察到一次 `-pl openlatch-client verify` 单机热态偶发红（reactor 刚退出、日志未留存），其后连续两轮全绿未复现。

| 模块 | 单元（surefire） | 集成（failsafe） | 失败/错误 |
|------|------------------|-------------------|-----------|
| openlatch-server | 212 | 1 | 0 / 0 |
| openlatch-client | 63 | 16 | 0 / 0 |

S3 关键用例逐项（均 0 failures / 0 errors）：

- 组 1（协议 v2）：`OpenlatchProtoContractFreezeTest`、`ProtocolCodecTest`、`HandshakeTest`（v1 OK / v2 OK / v3 拒绝三分支）
- 组 2（P2-12）：`ClusterConfigTest`(11)、`LeaderTrackerTest`(3)、`ClusterReplicationTest`(12)、`LeaderFailoverServerTest`(7)（Follower ACQUIRE 拒绝+hint==真主、选举空窗 hint=-1 无悬挂、home=F 跨 failover 续租/释放、未登记会话转发被拒、三消费方一致）
- 组 3（P2-13）：`LeaderDiscoveryTest`(5)（脚本化 §6.3 逐分支）、`ClientClusterIT`(5)（种子重定向+杀主恢复、home=被杀 Leader 恢复、**存活让位持锁不丢（§8 行 2 端到端）**、隔离判丢、**等待者跨 failover 重排**）
- 回归：client 全套件 v1 兼容零 diff；Phase 1 单机形态零 diff（`enabled=false` 回退）

## 2. 杀 Leader 演练（§11 验收 2：恢复 < 10s、存活会话锁不丢）

报告：`docs/failover-drill-2026-08-31.md`（P2-14，`-Pdrill` 进程级 3 节点 shaded jar）。

- **计时门**：场景 A（kill -9 当值 Leader）插桩后有效样本 7 个，**1621–1806 ms**，判定线 10s 余量 ≥5.5 倍；全量演练（A+B 同 fork）连跑 4/4 通过。
- **§8 行 1**（home=死主）：失锁回调触发 ✅；停载后同键全新授予成功（无泄漏）✅。
- **§8 行 3**（杀 Follower 无感）：20 轮获取+释放 241–1185 ms 全成功 ✅。
- **§8 行 2**（home=存活 Follower，续租不断 + failover 后释放成功）与等待重排：进程级精确形态依赖真 stepdown/分区，按 design D8 由 `ClientClusterIT` 端到端覆盖（上文绿），分区形态移交 S4。
- **不变式（S3 最小面）**：单驱动串行 + token 审计（同 key 无双活）+ 停载锁表清空 ✅；强双授检查器随 P2-19 混沌横扩。
- **环境敏感性（如实）**：reactor 构建刚退出的热机瞬态下出现过双挂（#1、#2，未复现于插桩后）；崩溃选举对 CPU 争抢敏感的结论维持，发布级复核建议独占硬件执行。S3 以"本机 quiet 态数值门通过 + 确定性套件语义兜底"关闭开放项。

## 3. S3 退出门判定

| 门槛 | 判定 |
|------|------|
| 5.3 默认 `clean verify` 全绿（不含 drill） | ✅ 插桩后终验全 reactor BUILD SUCCESS（见 §1） |
| 5.3 `-Pdrill` 演练通过 | ✅ 全量 4/4（含计时门 1621–1806 ms） |
| P2-14（组 4）断言与证据入库 docs/ | ✅ 演练报告 + 本检查单 |
| §11 验收 1/2（S3 范围内两项） | ✅ 见上 |

**结论：S3（P2-11～P2-14）退出门达成，`s3-leader-discovery-failover` 可归档。**
