# Proposal

## Why

台账 W23 登记的两枚服务端"日志零增长"用例（`AdminClusterTest#adminQueries_produceNoLogEntries`、`StateMachineConditionTest.signalFamilyLeavesLogAndDigestUntouched`）在常规 CI 的 verify 作业内层重试轮中偶红，位置与签名均已复现：

- 本机空载复跑 **1/20 红**，签名 `expected: [3L, 4L, 2L] but was: [3L, 4L, 3L]`——只有一枚节点的 `lastApplied` 前进；
- 用"停 follower → 追加条目 → 重启（数据目录保留）"制造确定性追赶窗并把窗口拉开后，**3/3 必红**，而三次里 **leader 的 `lastApplied` 一次都没动**——前进的永远是追赶中的副本；
- 第二枚的 CI 红落在 `aliveAgreeWithLeader()` 这条**前置断言**本身（`Expecting value to be true but was false`），同法复现 **2/3 红**。

结论：**产品行为正确**（两族操作确实零日志贡献），红的是夹具把"副本异步追赶"误读成"产生了新条目"。缺的是基线取样前的"等副本追平"这一前置：断言隐含了"取样时全员已追平"，而复制是异步的。

## What Changes

- `AdminClusterTest#adminQueries_produceNoLogEntries`：在取基线前加**有界等待**——等待全部存活节点追平 leader 的 `lastApplied`（`ClusterHarness#awaitTrue`，20s 上限）。`after == before` 断言原样保留。
- `StateMachineConditionTest#signalFamilyLeavesLogAndDigestUntouched`：把裸的 `assertThat(h.aliveAgreeWithLeader()).isTrue()` 换为**同义的有界等待**（`awaitTrue`，20s 上限）；不收敛仍在超时处红。
- 不新增、不删除、不放宽任何判据；不触碰产品代码。

## Capabilities

**无规格面**：纯测试口径修正，产品行为契约不变（`skip_specs: true`）。

### New Capabilities

- （无）

### Modified Capabilities

- （无）

## Impact

- 仅测试文件：`openlatch-server/src/test/java/io/github/lamspace/openlatch/server/raft/AdminClusterTest.java`、`.../StateMachineConditionTest.java`
- 无 `src/main` 改动、无协议/规格面；套件时长基线不变（已追平时等待立即返回，只在真正未追平时有界等待）。
