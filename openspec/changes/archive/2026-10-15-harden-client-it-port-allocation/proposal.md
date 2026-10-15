# Proposal

## Why

**W2 的触发口径已在常规 CI 命中**：`ClientClusterIT.startCluster:115 » 启动失败（端口 43869 可能被占用）: Address already in use`（另一轮 43937），连续两次 CI 尝试同一处同一签名。台账对该口径的处置是既定的——"按 `drill-port-allocation-race-fix` 同款带外取样改造该处（红一次改一处）"。

机制的确定性证据（本机实测，50 次采样）：

```
PROBE legacy samples=50  min=32869  max=46657  inBand[20000,29999]=0
```

`ClientClusterIT.freePort()` 是 `new ServerSocket(0)` 式**探针取口**：内核从临时端口带（本机 32768–60999）挑一个给探针，探针关闭后该口**立即回到内核的临时池**——于是同一个 JVM 里紧接着的任何 `connect`（客户端后台重连、路由刷新探测、车道建连……）最可能拿到的正是**刚被回收的那个口**，随后服务端真 bind 就撞 `EADDRINUSE`。这不是"偶发运气差"，是**结构性**的：探针取到的口必然落在临时带内（10000/10000 命中），而 CI 的机器负载只决定命中率。

## What Changes

- 新增测试侧取样器 `TestPorts.freeNonEphemeral()`：**随机取样于 20000–29999（临时端口带之外）**、`setReuseAddress(false)`（不吃 TIME_WAIT 假阳性）、至多 100 次换候选——即 drill 侧已验证的同款治法。
- `ClientClusterIT.freePort()` 改为委托该取样器（**只改这一处**：台账口径"红一次改一处"；W2 余下的持有者保持登记，谁红改谁）。
- 新增单测 `TestPortsTest` 锁两条：①取样恒落在文档化的非临时带内；②**内核自动分配的口不得落在该带内**（OS 无关的有效性判据——若某机临时池与该带相交，测试即红，提示换带）。
- `WATCHLIST.md` W2 行：登记这两次 CI 出现、本次改造与"余下持有者不动"的口径。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

（无——测试侧取口策略，产品行为零改动，故 `skip_specs: true`。）

## Impact

- **代码**：仅测试树（`src/test`）；零 `src/main` 改动、零协议、零配置。
- **测试**：`ClientClusterIT` 的取口路径替换；新增取样器与其单测。
- **风险**：取样带若与某环境的临时池相交则策略失效——由 `TestPortsTest` 的第二条断言把它变成**显式红灯**而不是偶发 flake。
