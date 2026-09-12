# Tasks: drill-metrics-port-collision-fix

判定基线：验证命令 `mvn -s /home/lam/repo/settings.xml`；套件级触发 `-pl openlatch-client verify -Pdrill -Dit.test=<类名>`（前置：server shaded jar 已在位）。

## 1. 夹具修复

- [x] 1.1 `LeaderKillDrillIT.startCluster` 节点配置模板追加 `openlatch.server.metrics.port=0`；`startCluster` Javadoc 端口面描述同步（"显式端口"→补管理端口临时化）
- [x] 1.2 `RollingRestartDrillIT.startCluster` 同款修复（relaunch 复用 cfg 文件，单点改动覆盖重启序）；Javadoc 同步

## 2. 回归验证

- [x] 2.1 前置体检：`pgrep -af openlatch` 无残留、`ss -tlnp | grep 9412` 空（上一轮泄漏子进程会直接复现冲突假象）
- [x] 2.2 `-pl openlatch-client -am test-compile` 过
- [x] 2.3 `LeaderKillDrillIT` 复跑全绿（Tests run: 3, Failures: 0, Skipped: 0），当日 failover 报告生成且恢复窗口 <10s
- [x] 2.4 `RollingRestartDrillIT` 复跑全绿（3/3, Skipped: 0），当日 rolling 报告两序"末窗+45s 预算后残留错误=0"；全程错误率与自愈事件如实入报告（复核数据，不判门禁）
- [x] 2.5 沙箱可跑性结论：记录开发环境（无桌面终端上下文）跑进程级演练是否稳定——若绿，修正"必须桌面复跑"的 runbook 前提与相关记忆；Partition 仍需 sudo/桌面不变

## 3. 交接待办（不在本变更实施）

- [ ] 3.1 向评审人交付两轮报告与改判草案输入（Phase 2 标准 5 / Phase 3 未执行项关闭）
- [ ] 3.2 部署文档注记转交 docs-restructure-and-report-retirement / bilingual-user-guide 观测页
