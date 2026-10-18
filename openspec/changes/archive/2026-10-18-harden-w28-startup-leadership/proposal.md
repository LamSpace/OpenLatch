# Proposal

## Why

台账 **W28**：`ClusterReplicationTest#failoverKeepsExpiryDriving` 在 `2×nproc` 造载下，
首个 acquire 偶发返回 `NOT_LEADER`（2026-10-18 W26/W27 验证 4/10 轮命中，`:349`）。

根因在夹具启动同步点：`ClusterHarness.start(3, 500)` 只等 `hasLeader()`（存在任一
Leader 即返回），不等领导权**稳定**；500ms 选举超时在 CPU 重载下可让刚选出的 Leader
因心跳延迟立即降级（现场日志见 `change to CANDIDATE` / `LEADER → FOLLOWER`）。首请求
因此落在竞选窗，返回 `NOT_LEADER`；该失败发生在 W26/W27 改动路径之前，与两处 flake
无关。

## What Changes

- `ClusterHarness` 新增 `awaitStableLeader(quietMs, timeoutMs)`：同一 Leader 连续保持
  `quietMs` 才返回；期间换主则重新计时，超时抛带节点角色诊断的断言错误。
- `failoverKeepsExpiryDriving` 在捕获 Leader 与发起首个 acquire 前调用该等待（静置窗口
  取 2× 选举超时，即 1s；总预算 20s，与 `start` 的初始选主预算同口径）。
- 不改 `src/main`、不改协议、不改产品语义；不改变测试后续的 failover/到期断言。

## Capabilities

**无规格面**：纯测试夹具同步点（`skip_specs: true`）。

### New Capabilities

- （无）

### Modified Capabilities

- （无）

## Impact

- 测试源码：`ClusterHarness.java`、`ClusterReplicationTest.java`。
- 构建/发布：无 `src/main` 改动、无发布元数据变化、无新依赖。
