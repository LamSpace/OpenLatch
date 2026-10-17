# Proposal

## Why

台账 **W2** 再次触发：CI run `39019655682`（PR #48 头 `f5460a9`）的 verify **内层第一轮**，`ClientSemaphoreClusterIT` 的三节点集群启动时 n3 的 gRPC 端口绑定失败——`java.io.IOException: Failed to bind to address 0.0.0.0:36599`，`bind(..) failed with error(-98): Address already in use`，`openlatch-client` 模块 FAILURE；内层重试第二轮绿。

失败端口（36599）落在内核临时端口带内，与 W2 已坐实的根因同族：`new ServerSocket(0)` 的取样**恒在内核临时带内**，探针-重绑窗口结构性敞开（本机 50 次采样 min=32869 / max=46657、带外 0）。W2 的动作列写明"任一处出现 `bind EADDRINUSE` → 按同款带外取样改造该处（**红一次改一处**）"；本次红的持有者是 `ClientSemaphoreClusterIT`（W2 原登记 7 处之一）。

## What Changes

- `ClientSemaphoreClusterIT`：私有 `freePort()`（`new ServerSocket(0)`）改用既有工具 **`TestPorts.freeNonEphemeral()`**（`harden-client-it-port-allocation` 落地、`ClientClusterIT` 已在用：取样带 `[20000, 29999]`、`setReuseAddress(false)`、带内换候选），并删除该类私有实现。
- **`TestPorts` 结构性补强（本次实测暴露的新缺陷）**：改用的首轮试跑即出现 `n1` 与 `n3` 拿到**同一个** Raft 口（`28544`，已在带外带内）——多节点装配会在任何服务端 bind **之前**连续取样 2n 次，探针只能证明"此刻可绑"，无法阻止同一口被交给两个节点。修法：本 JVM 内**预留已发放端口**（`HANDED_OUT` 集合），永不二次发放；探针失败时释放预留。
- 新增 `TestPortsTest#handedOutPortsAreNeverRepeated`（200 次取样全不同）作为概率性护栏。
- **不动其余持有者**（红一次改一处）：`ClientChaosIT`、`ClientProcessKillIT`、`IdleNodeGracefulStopIT`、`ClientHandshakeTest`、`ScriptedServer`、`ClusterHarness`、`ConsoleClusterTest` 保持登记。

## Capabilities

**无规格面**：纯测试夹具取口方式改造（`skip_specs: true`）。

### New Capabilities

- （无）

### Modified Capabilities

- （无）

## Impact

- 仅 `openlatch-client/src/test/java/io/github/lamspace/openlatch/client/ClientSemaphoreClusterIT.java`。
- 无 `src/main`、无协议/规格面；该类集群启动的端口改在带外取样，语义不变。
