# Proposal

## Why

台账 **W25**：`ClientSemaphoreClusterIT#leaseSurvivesLeaderRedirectForSemaphoreHolders` 间歇报
`TransferLeadershipException: ... DIFFERENT_LEADER`（2026-10-17 本机复跑 2/20）。本次按"红先取证"
在 16 CPU 燃烧进程负载下复现 **1/20**（`run-15`），现场日志与 Ratis 3.3.0 源码确认机理：

1. 旧 Leader `n1` 向目标 `n2` 发起转交时，`n2` 日志落后一条；`n1` 在追平确认后发送
   `StartLeaderElection`（`lastEntry=(t:1,i:6)`）。
2. `n2` 竞选 term 2，但 `n1`/`n3` 以 `last:(t:1,i:7)` 拒绝——`n2` 在发出竞选前又落后了一条。
3. `n2` 竞选被拒，`n1` 因更高 term 降级，随后在 term 3 **重新当选**。
4. `RaftServerImpl.onGroupLeaderElected()` 以 `Result.SUCCESS` 完成 pending 转交请求；
   `PendingRequest.complete` 见 `currentLeader` 仍是 `n1`（不是目标 `n2`），遂转
   `DIFFERENT_LEADER` 并抛异常。

这是 Ratis 转交与并发选举竞态的**合法失败**，不是产品缺陷：夹具的意图只是"让领导权离开
`before` 节点"，不是"必须由某个指定节点接任"。当前 helper 把一次合法失败直接包装成断言失败，
导致低频假红。

## What Changes

- `ClientSemaphoreClusterIT#transferLeadership` 改为**有界重试**：
  - 捕获 `TransferLeadershipException` 后先重读 Leader；若领导权已离开 `before`，视为转交意图达成；
  - 否则短暂静置，重新选择当前非 Leader 节点作为目标，再试；
  - 最多 5 次；其他 `IOException` 仍立即失败，不掩盖真实故障。
- 调用点由"外部选目标"改为传入 `before`，目标在 helper 内按当前视图重选。
- 不改产品转交语义、不改测试的断言面；仅测试夹具健壮性。
- 归档时从 `WATCHLIST.md` 删除 W25 行（编号留空，不复用）。

## Capabilities

**无规格面**：纯测试夹具重试（`skip_specs: true`）。

### New Capabilities

- （无）

### Modified Capabilities

- （无）

## Impact

- 仅 `openlatch-client/src/test/java/io/github/lamspace/openlatch/client/ClientSemaphoreClusterIT.java`
  与 `WATCHLIST.md`。
- 无 `src/main`、无协议/规格面、无运行时行为变化。
