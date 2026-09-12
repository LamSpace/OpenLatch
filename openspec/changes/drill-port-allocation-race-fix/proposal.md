# Proposal: drill-port-allocation-race-fix

## Why

`drill-metrics-port-collision-fix` 落地后的复跑中，`RollingRestartDrillIT` 出现新失败形态：先主后从轮 `startCluster` 阶段 node3 接入端口 30s 未就绪，节点日志实证 `Failed to bind to address 0.0.0.0:34411 → bind(..) error(-98) 地址已在使用`。根因是演练夹具的端口分配竞态（TOCTOU）：`freePort()` 以"bind 临时 ServerSocket → 读出端口 → **关闭**"探测空闲口，而 OS 默认把 `ip_local_port_range`（本机 32768–60999）同时用作**出站连接的源端口池**——探针一关，端口立即回到该池；本 JVM 内客户端/gRPC 每轮演练数百次出站连接持续抽取源端口，在子进程真正 bind 前的数秒窗口内即可把刚"验过空闲"的口抢为源端口，子进程 EADDRINUSE 退出。探针落点恰在临时带内，竞态为系统性而非偶发；同族的晨间失败（9412 固定口互撞）与一例首写 `request 2 timed out`（独立重跑 12/12 不复现，现观察为同竞态族的其他显形，证据保全纪律已立）均指向夹具端口纪律缺口。PartitionDrillIT 恒用固定口 19411–19413（临时带外）从不发作，构成天然对照组。

## What Changes

- `LeaderKillDrillIT` 与 `RollingRestartDrillIT` 的 `freePort()` 重写：候选端口从 **20000–29999 固定窗口**随机抽取（`ip_local_port_range` 带外，OS 永不将其抽作出站源端口），探针以**禁用 SO_REUSEADDR** 的严格 bind 验可绑定（不放行 TIME_WAIT 残留），失败换下一候选（上限 100 次）；`startCluster` 内 access/raft 抽样重复时重抽一次（随机窗口撞车护栏）。
- 同改动早前试验性的"客户端 A requestTimeout 5s→20s"放宽**回退**：首写超时的领先解释已并入本竞态族，无实证支撑的预算放宽不入账；若放宽回退后再现 `request N timed out`，按证据保全纪律取日志另立排查（3.2 观察项）。
- 已知同款 `freePort()` 习作的其余持有者（`ClientProcessKillIT`/`IdleNodeGracefulStopIT`/`ClientChaosIT`/`ClientClusterIT`/`ClientSemaphoreClusterIT`/`ClientHandshakeTest`/`ScriptedServer`）本变更**不动**：无观测失败、多为单节点/轻连接场景，且属默认 verify 稳定轨道——登记为同族观察项，红一次改一处。

## Capabilities

### New Capabilities

- `fault-drill-harness`: 进程级演练夹具的端口分配纪律——为子进程 bind 预留的监听端口 MUST 取自 OS 临时端口带之外（探针-关闭-重绑与出站源端口抽取之间的 TOCTOU 竞态从根消除），探针 MUST 以禁用地址复用的严格 bind 验证可绑定性；同测内多端口抽样 MUST 去重。

### Modified Capabilities

- 无。

## Impact

- 代码：2 个测试类的 `freePort()` 重写 + `startCluster` 去重护栏 + Javadoc；`src/main` 零触碰。
- 测试：`RollingRestartDrillIT` 整类（3 用例）与 `LeaderKillDrillIT` 复跑全绿为退出判据；验证单轮次执行（连跑压测在共享终端制造 CPU 饥饿假失败，本 change 调研期实测 load 4.3+ 时曾压出非产品性超时，教训记入 tasks 判定基线）。
- 遗留观察（不阻塞本变更）：① 首写 `request 2 timed out` 一例在竞态族内未获直接节点日志实证（现场已失），复发时按保全纪律升级排查；② 同族 freePort 习作的其余 7 处持有者登记在册、改判据触发。
