# Tasks

## 1. 基线证据

- [x] 1.1 回读 W26/W27 验证轮次的 W28 现场：`/tmp/w26w27-load/fixed/` 与 `fixed2/`
  的 `summary.txt`，确认 `failoverKeepsExpiryDriving:349 expected OK but was NOT_LEADER`
  与 `LEADER → FOLLOWER` 日志签名；必要时同口径再跑 1 轮确认。

## 2. 夹具改造（test-only）

- [x] 2.1 `ClusterHarness.awaitStableLeader(quietMs, timeoutMs)`：同一 Leader 连续保持
  `quietMs` 才返回（直接返回该 Leader，避免返回后重读的竞态）；换主即重置计时；
  超时错误携带各节点 id/角色/lastApplied 诊断。
- [x] 2.2 `failoverKeepsExpiryDriving`：捕获 `leader` 与首个 `acquire` 前调用
  `awaitStableLeader(1_000, 20_000)`；其余步骤与断言不变。
- [x] 2.3 新增/修改方法补齐 Javadoc（含私有方法），匹配现有风格。

## 3. 改后验证

- [x] 3.1 `2×nproc` 造载同口径复跑至少 5 轮，W28 签名零复发；记录每轮红绿与
  任何新签名。证据：`/tmp/w28-load/load/summary.txt`（16 进程 × 10 轮全绿）与
  `/tmp/w28-load/load-refined/summary.txt`（返回值收紧后 3 轮全绿）；13 轮均无
  `NOT_LEADER`。
- [x] 3.2 无载 smoke 跑 `ClusterReplicationTest` 至少 2 轮零红。证据：
  `/tmp/w28-load/smoke/round-{1,2}.log` 与 `refined-{1,2}.log`，2/2 + 2/2 绿。
- [x] 3.3 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆绿
  （13:40，`/tmp/w28-load/full-verify.log`）。
- [x] 3.4 `bash scripts/check-source-citations.sh` 零命中（exit 0）。
- [x] 3.5 造载进程收尾 kill，`pgrep yes` 无残留。

## 4. 收口约束

- [x] 4.1 改动仅限 `openlatch-server` 测试源码；test-only（`skip_specs: true`），
  无 delta spec；验证后按 test/docs 两个 commit 收口。
