# Proposal

## Why

台账 **W2** 余 7 处取口仍为 `new ServerSocket(0)`。根因已在 W2 坐实：探针取样恒落在内核
临时端口带内，探针关闭后端口回到临时池，真 bind 前的窗口里任何并发 `connect` 都可能被内核
派到同一口，导致 `bind EADDRINUSE`；`ClientClusterIT`/`ClientSemaphoreClusterIT` 已按
"带外取样 + `setReuseAddress(false)` + 换候选"修复。剩余 7 处中，2026-10-13 补登的
`ClusterHarness`（27 个 server 测试类复用）与 `ConsoleClusterTest`（3 节点 × 2 口）暴露面
最高，但 `TestPorts` 目前是 `openlatch-client` 测试源码里的包私有类，server/console 无法复用。

## What Changes

- 将 `TestPorts` 与 `TestPortsTest` 迁入 `openlatch-server` 测试源码的中性包
  `io.github.lamspace.openlatch.test`，类与方法改为 `public`；复用既有 `openlatch-server`
  test-jar（`pom.xml` 已配置并发布）作为跨模块共享工件，不新增模块。
- `openlatch-client`、`openlatch-console` 增加 `openlatch-server` `test-jar` 测试依赖；
  `ClientClusterIT`/`ClientSemaphoreClusterIT` 补 import。
- 7 处持有者改用 `TestPorts.freeNonEphemeral()` 并删除私有 `freePort()`：
  `IdleNodeGracefulStopIT`、`ClientChaosIT`、`ClientProcessKillIT`、
  `ClientHandshakeTest`、`ScriptedServer`、`ClusterHarness`、`ConsoleClusterTest`。
- `HANDED_OUT` 同 JVM 防重号、带有效性判据随类迁移；产品取口/转交语义不变。

## Capabilities

**无规格面**：纯测试夹具取口方式与测试依赖改造（`skip_specs: true`）。

### New Capabilities

- （无）

### Modified Capabilities

- （无）

## Impact

- 测试源码：上述 7 处、两个既有使用类、`TestPorts`/`TestPortsTest` 迁移。
- 构建：`openlatch-client/pom.xml`、`openlatch-console/pom.xml` 各加一条 test-jar 测试依赖；
  无新模块、无 `src/main` 改动、无发布元数据变化。
