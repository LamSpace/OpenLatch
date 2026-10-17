# Proposal

## Why

台账 **W26** 与 **W27** 是 `ClusterReplicationTest` 在造载下的两处夹具时序 flake，均与
产品语义无关：

- **W26** `failoverKeepsExpiryDriving`：`TestConn.disconnect()` 的 `SESSION_CLOSE` 经
  `sessionCoordinator().submitClose` **异步入日志**；用例紧接着在同一 key 上发起重授，
  若重授先到 Leader 而租约尚未按 `SESSION_CLOSE` 释放，预检见 busy ⇒ 返回 `QUEUED`，
  而非用例期望的 `OK`。
- **W27** `queuedPathDoesNotWriteLog`：以 `before = leader.lastApplied()` 为绝对基线断言
  "QUEUED/DENIED 零日志"。该位点是**全状态机**应用位点，造载窗口内可能有会话/元数据等
  后台条目前移，导致 `expected 5L but was 6L`；真正要锁的是"这两个请求没有写入用户条目"。

## What Changes

- `ClusterHarness` 新增夹具侧有界等待 `awaitKeyNotHeld`：轮询 Leader 复制投影直到指定 key
  不再 held，超时抛带诊断（各节点角色、held 读数、`lastApplied`）的断言错误。
- `failoverKeepsExpiryDriving` 在 `disconnect()` 后先等待 `fk2` 释放，再发起重授；
  保留原 `OK` 断言与后续 failover/到期断言。
- `ClusterHarness` 新增按 `RaftEntryType` 读取已提交日志区间的辅助方法
  （`RaftLog#get(index)` → `RaftLogEntry#type`）；`queuedPathDoesNotWriteLog` 改为断言
  窗口内**没有新增用户条目**（过滤会话/探针/到期扫描等后台条目），
  不再比较绝对 `lastApplied`；QUEUED/DENIED 的零用户日志原意不变。
- 全部改动限于 `openlatch-server` 测试源码，不改 `src/main`、不改协议、不改产品语义。

## Capabilities

**无规格面**：纯测试夹具同步点与日志断言改造（`skip_specs: true`）。

### New Capabilities

- （无）

### Modified Capabilities

- （无）

## Impact

- 测试源码：`ClusterHarness.java`、`ClusterReplicationTest.java`。
- 构建/发布：无 `src/main` 改动、无发布元数据变化、无新依赖。
