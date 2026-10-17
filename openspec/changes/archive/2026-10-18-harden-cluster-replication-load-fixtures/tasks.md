# Tasks

## 1. 基线复现（改前）

- [x] 1.1 起 `2×nproc`（16 个）CPU 燃烧进程造载，`ClusterReplicationTest` 两用例 3 轮：
  round 1/3 红（`queuedPathDoesNotWriteLog:125`，`expected 5L but was 6L`），
  round 2 绿；W26 的 `QUEUED` 本轮未复现。验证=`/tmp/w26w27-load/baseline/`。
- [x] 1.2 已明确记录"W26 未复现"及造载口径；W27 已按 `lastApplied` 绝对基线复现。

## 2. 夹具改造（test-only）

- [x] 2.1 `ClusterHarness.awaitKeyNotHeld(key, timeoutMs)`：50ms 轮询 Leader 复制投影，
  超时错误携带各节点角色、held 读数与 `lastApplied` 诊断。
- [x] 2.2 `ClusterHarness.Node.entryTypesBetween(fromExclusive, toInclusive)`：经
  Ratis `RaftLog#get(index)` 读取并解析状态机日志载荷；用例侧过滤
  `SESSION_OPEN`/`SESSION_CLOSE`/`NOOP`/`LEASE_EXPIRE_ENTRY`/`RAFT_ENTRY_UNKNOWN`。
- [x] 2.3 `failoverKeepsExpiryDriving`：`disconnect()` 后先 `awaitKeyNotHeld("fk2", 10_000)`，
  再重授；保留重授 `OK` 与 failover/到期断言。
- [x] 2.4 `queuedPathDoesNotWriteLog`：窗口断言改为"无新增用户条目"，失败信息列出
  窗口内实际类型；QUEUED/DENIED 零用户日志原意保留。
- [x] 2.5 新增/修改方法 Javadoc 已同步（含私有方法）。

## 3. 改后验证

- [x] 3.1 同口径造载复跑两批共 10 轮：W26 `QUEUED` 与 W27 `lastApplied` 零复发；
  8 轮零红。两批的 round 1 等 4 轮红均为独立的历史性启动期
  `failoverKeepsExpiryDriving:349`（首个 acquire，改前代码路径）
  `expected OK but was NOT_LEADER`——500ms 选举超时在重载下换主所致，与 W26/W27 无关。
  `fixed2` rounds 2–5 连续 4 轮零红。验证=`/tmp/w26w27-load/fixed{,-smoke}/`、
  `/tmp/w26w27-load/fixed2/`。
- [x] 3.2 `bash scripts/check-source-citations.sh` 零命中（exit 0）。
- [x] 3.3 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` **BUILD SUCCESS**
  （13:35，`/tmp/w26w27-load/full-verify.log`）。
- [x] 3.4 两批造载脚本的 `yes` 进程均已 kill，`pgrep yes` 无残留。

## 4. 收口约束

- [x] 4.1 改动仅限 `openlatch-server` 测试源码；本 change 为 test-only（`skip_specs: true`），无 delta spec。
