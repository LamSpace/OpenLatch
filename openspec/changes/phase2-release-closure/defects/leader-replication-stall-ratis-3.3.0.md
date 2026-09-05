# 缺陷档案：Ratis 3.3.0 leader 复制停摆（sticky LeaderNotReady）

- 登记：2026-09-05，`phase2-release-closure` 任务 6.1（`-Pdrill` 辅轨全套）现场捕获。
- 级别：**P1（可用性）**——非正确性缺陷（无锁丢失/无状态分叉证据，错误全为拒绝/超时路径），但触发后该 leader 任期内写面永久不可用直至人工介入。
- 归因：**存量**（非 `phase2-release-closure` 引入，差分实验见下）。S4 归档检查单记录的"R2 离群轮 24.22%，判定瞬态、4 轮未复现"与本档案为同一故障签名——该"瞬态"定判据本文更正。

## 症状

3 节点进程级集群，滚动重启"先主后从"序：首台重启（当值 Leader）后，集群进入**双稳态**——

- 收敛态：新 leader 的任期起始 NOOP 提交成功，秒级恢复，错误率 ≤0.9%（全落重启窗内）；
- **停摆态**：新 leader 当选成功（INFO 可见 `CANDIDATE to LEADER at term N`），但任期 NOOP 永不提交——对"重启归来且携带旧任期条目"的前 leader 节点，appender 走 INCONSISTENCY 回退（如 `setNextIndex 162 -> 145`）后无后续收敛日志；对另一存活 follower 亦无成功复制证据；`LeaderStateImpl.isReady()`（`startupLogEntry.get().isApplied()`）恒假，写门闸对一切 RW 请求持续抛 `LeaderNotReadyException` **200+ 秒不自愈**（两次观测 211.5s / 212.5s 截尾于演练结束），客户端错误率 5%~28%。

## 复现与频率矩阵（全部为本机 shaded jar，命令见附录）

| 轮次 | 二进制 | 先从后主 | 先主后从 |
|---|---|---|---|
| S4 2026-09-02（归档） | S4 原版 | 0.80% ✅ | 0.00% ✅（当日），后续含 24.22% ❌ 与 0/0.12/0.46% 等（5 轮混合） |
| 2026-09-05 套件 22:44 | S4+D8 钉池 | 0.89% ✅ | **26.02% ❌ 停摆** |
| 2026-09-05 差分#1 22:56 | S4+D8 钉池 | 1.92% ❌（错误全落窗内，强度超线，非停摆） | **28.49% ❌ 停摆** |
| 2026-09-05 差分#2 23:0x | **S4 原版（无钉池，git stash 复现）** | 0.46% ✅ | **5.13% ❌ 停摆**（尾 212.5s） |

差分#2 用 `git stash` 摘除 D8 六行钉池改动后重建 shaded jar——**停摆签名与 D8 无因果**；D8 亦未提升或恶化停摆概率（观察样本内）。先主后从今日 4/4 命中停摆（含 09-02 历史共 5 次签名），非停摆轮次亦存在于历史记录——概率随环境（机器热态，选举时序抖动）浮动，双稳态本质不变。

## 取证线索（供后续调查）

- 节点日志：`openlatch-client/target/drill-logs/roll-node{1,2,3}-*.log`（22:52–22:53 轮）；
- 选举互拒链：存活两节点在旧 leader 死亡后同刻超时 → 双双 CANDIDATE → 互相 REJECT 拉票（`n3<-n2#4:FAIL-t4`），旧 leader 重启归来后以更高 term 获票当选，随即陷入上述复制停滞；
- 关键源码位点（Ratis 3.3.0）：`LeaderStateImpl.isReady()`(450)、`startupLogEntry` 生命周期、`GrpcLogAppender` INCONSISTENCY 回退后的重试推进、`LeaderElection` 让位后 follower 追认路径（`FOLLOWER to FOLLOWER ... for candidate:n3` 停留在 term 3 的语义可疑）；
- 杀主演练（LeaderKillDrillIT，无"旧 leader 带脏条目归来"场景）今日绿（12.44s 双用例）——停摆的必要语境包含**旧 leader 重启回群**，与滚动重启耦合。

## 处置决策（本 change 内）

1. 不在收口 change 内修复（正确性无损；根因在库内，修复属独立调查——范围纪律）；
2. §11-5 验收行按实记录：09-02 双序绿 + 本档案复现史与差分归因；DoD §5.4-3 以"P1 在案+跟进计划"承载；
3. **跟进计划（发布前若采纳豁免路线则必须随附）**：独立 change 立项调查——候选方向（a）产品侧 leader 自愈看门狗（leader 角色下任期 NOOP 超时未提交 → `transferLeadership` 让位重选）；（b）升级通道跟踪 Ratis 3.3.1（社区在盘）与上游 issue 提报；（c）运维缓解：滚动重启纪律"先从不先主"（今日 R1 三模式全绿，0.46–1.92%）写入部署文档为推荐序；
4. 用户文档（部署文档 §5 滚动重启段）在 (3c) 落定前补"先从不先主"推荐序与故障表征一句话（低风险运维事实）。

## 附录：复现命令

```
mvn -s /home/lam/repo/settings.xml -pl openlatch-server -am package -DskipTests
mvn -s /home/lam/repo/settings.xml -pl openlatch-client -am verify -Pdrill \
  -Dit.test=RollingRestartDrillIT -Dtest=NoSuchUnitTest \
  -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false
```

报告自动追加至 `docs/rolling-restart-drill-<date>.md`；停摆轮判据：先主后从错误时刻尾值 ≈ 演练全程（收敛轮的尾值 ≈ 重启窗结束 <11s）。
