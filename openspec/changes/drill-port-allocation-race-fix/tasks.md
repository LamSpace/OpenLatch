# Tasks: drill-port-allocation-race-fix

判定基线：`mvn -s /home/lam/repo/settings.xml -pl openlatch-client verify -Pdrill -Dit.test=...`；**单轮次执行、轮间留冷却**（连跑压测在共享终端制造 CPU 饥饿假失败——调研期 load 4.3+ 时曾压出 LeaderKill 非产品性超时，此类伪证一律不计入判据）；红时先 `cp -r openlatch-client/target/drill-logs /tmp/…` 保全现场再清理。

## 1. 端口分配器重写

- [x] 1.1 `RollingRestartDrillIT.freePort()`：20000–29999 带外窗口随机取样 + 禁 SO_REUSEADDR 严格 bind 探针 + 100 次上限，方法 Javadoc 记机理与实证（bind(-98) 现场）
- [x] 1.2 `LeaderKillDrillIT.freePort()`：同款重写
- [x] 1.3 两文件 `startCluster` 的 access/raft 抽样加去重护栏（`raft==access` 重抽一次）
- [x] 1.4 回退早前试验项：`extendedPrimitivesSurviveRollingRestart` 客户端 A 的 `requestTimeout` 20s 恢复为 5s（方法 Javadoc 容忍度段同撤——无实证放宽不入账）
- [x] 1.5 `-pl openlatch-client -am test-compile` 绿

## 2. 回归验证（单轮次）

- [x] 2.1 `RollingRestartDrillIT` 整类全绿（3/3，Skipped: 0；两序末窗+45s 残留=0 复核当日报告）
- [x] 2.2 `LeaderKillDrillIT` 全绿（3/3，Skipped: 0；恢复窗 <10s）
- [x] 2.3 抽查修复生效物证：任一复跑轮 `grep -oE '"[0-9.]+"'` 或直接看 `drill-logs/` 端口命名——节点端口应全部落在 20000–29999

## 3. 遗留观察（不阻塞出阶段）

- [x] 3.1 runbook"证据保全"条目已交付（评审人桌面文件，Step 2 前缀注记 + Step 5 签名速查）
- [ ] 3.2 观察项：首写 `request N timed out` 若再现（当前领先解释=同竞态族其他显形，但缺现场实证），按保全纪律取节点日志+客户端时序升级排查，另立 change，不猜
- [ ] 3.3 观察项：freePort 同款习作的其余 7 处持有者（ClientProcessKillIT/IdleNodeGracefulStopIT/ClientChaosIT/ClientClusterIT/ClientSemaphoreClusterIT/ClientHandshakeTest/ScriptedServer）默认 verify 无观测失败，红一次改一处
