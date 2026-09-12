# Proposal: drill-metrics-port-collision-fix

## Why

评审人按 runbook 于 2026-09-12 桌面终端首跑全量 `-Pdrill`，LeaderKillDrillIT 3/3 失败（"30s 内未探测到 Leader"）、RollingRestartDrillIT 3/3 失败（"节点接入端口未在时限内就绪"），PartitionDrillIT/IdleNode/StallRepro 全绿。取证节点日志（`openlatch-client/target/drill-logs/`）：死亡节点一律终止于 `startup failed: 管理端口启动失败（端口 9412 可能被占用）: 地址已在使用`。根因是 Phase 3 T2 引入的默认启用指标管理端口（`MetricsConfig`：`openlatch.server.metrics.port` 默认 9412，绑定失败按 fail-fast 哲学整机启动退出），而两个演练的每节点 properties 模板只配业务/raft 端口、未覆盖 metrics 键——单机三节点抢绑同一 9412，后两节点必退，集群失多数派。绿/红分布与"是否共享宿主 9412"完全一致（Partition 各节点处独立 netns、命名空间隔离了 9412 故绿）。09-06 前的全绿历史在 T2 之前，T2 后 `-Pdrill` 恰为"未执行项"，回归潜伏至今；此前"沙箱环境不可靠"的归因系误判（clean-master A/B 只排除了当次改动，未排除 HEAD 自带缺陷）。

## What Changes

- `LeaderKillDrillIT.startCluster` 与 `RollingRestartDrillIT.startCluster` 的节点配置模板各加一行 `openlatch.server.metrics.port=0`（操作系统分配临时端口，零冲突且保留管理 HTTP 真实绑定路径；RollingRestart 的 relaunch 复用同一 cfg 文件，重启序天然获新端口）。
- 两处 `startCluster` 方法 Javadoc 同步（端口面描述补"管理端口临时化"）。
- 产品行为零改动：metrics 默认启用、占用即快速失败为 `metrics-observability` 既有定案规格，不动。
- 规格侧新增一条夹具纪律要求（metrics-observability ADDED）：同宿主多节点测试夹具 MUST 显式设置互异/临时管理端口，MUST NOT 依赖默认 9412 共存。
- 部署文档注记（同宿主多节点需为每节点配互异 `metrics.port` 或 0）转交 README/guide 建设期 change 承载（docs-restructure-and-report-retirement / bilingual-user-guide 的观测页），本变更不触文档。
- 验证即回归：修复后在开发环境复跑两套件全绿（Skipped=0、末窗后残留=0 分段判据），并顺带重估"沙箱不可跑进程级演练"的旧前提——两套件不依赖 sudo，若沙箱绿则后续回归可门禁化。

## Capabilities

### Modified Capabilities

- `metrics-observability`: 新增"同宿主多节点夹具管理端口隔离"要求（测试夹具纪律，不改服务端行为语义）。

## Impact

- 代码：仅 2 个测试类的配置文本块与 2 行 Javadoc；`src/main` 零触碰。
- 测试：LeaderKill/RollingRestart 两套件由"必挂"恢复可跑；Partition/Idle/Stall 不受影响；默认 `clean verify` 不涉（drill 组默认排除）。
- 证据：修复后复跑产出当日 `docs/failover-drill-*.md` 与 `docs/rolling-restart-drill-*.md`，构成 Phase 2 标准 5 复核改判的滚动重启数据源（形态 B 若命中按既有定夺如实记录，不判新增缺陷）。
- 记忆修正：`drill-process-its-unreliable-in-sandbox` 已改写为本根因（原"沙箱受限"结论证伪）。
