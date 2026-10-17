# Tasks

## 1. 现场留档

- [x] 1.1 复现并保留证据：16 CPU 燃烧进程负载下 `ClientSemaphoreClusterIT#leaseSurvivesLeaderRedirectForSemaphoreHolders` 连跑 20 轮，`run-15` 红；现场日志 `/tmp/w25-load/run-15.log`、报告 `/tmp/w25-load/red-15/failsafe-reports/` 已保留，异常签名与 W25 一致。验证=日志中 `DIFFERENT_LEADER` 与 `current leader is n1` 可回读。
- [x] 1.2 根因定位：n1 向 n3 发送 `METADATAENTRY(c:6)`（`(t:1,i:7)`）后，n2 仍停在 `i:6`；n2 竞选 term 2 被 n1/n3 以 `last:(t:1,i:7)` 拒绝，n1 降级后在 term 3 重新当选；`RaftServerImpl.onGroupLeaderElected()` 以 `Result.SUCCESS` 完成 pending 请求，`PendingRequest.complete` 见当前 Leader 仍是 n1（非目标 n2）遂转 `DIFFERENT_LEADER`。验证=日志时序（`run-15.log:330-365`）与 Ratis 3.3.0 `TransferLeadership.java`/`RaftServerImpl.java` 源码一致。

## 2. 夹具改造

- [x] 2.1 `ClientSemaphoreClusterIT#transferLeadership` 改为有界重试：`TransferLeadershipException` → 重读 Leader；领导权已离开 `before` 即返回；否则静置后重选非 Leader 目标重试，最多 5 次；其他 `IOException` 立即失败。调用点传入 `before`。验证=编译通过、单用例空载连跑绿。
- [x] 2.2 重试路径留痕与强制验证：每轮重试输出一行 `[W25]` 诊断。40 轮负载轮**未自然触发**重试（`retries=0`），故临时注入一次 `TransferLeadershipException`（临时用例 `injectedTransferFailureIsRetried`），日志出现 `[W25] transferLeadership retry 1 (leader=2, target=1): injected ...` 且用例绿；注入代码在验证后已删除，最终 diff 仅保留重试与诊断行。验证=日志 `/tmp/w25-injected.log`。

## 3. 验证

- [x] 3.1 同口径验证：16 CPU 燃烧进程负载下连跑 40 轮，全部 `exit=0`、零 `DIFFERENT_LEADER`（逐轮日志 `/tmp/w25-verify/run-01..40.log`）。自然负载未触发重试，重试路径由 2.2 的注入验证覆盖。验证=逐轮 exit code 0/40、日志扫描零异常。
- [x] 3.2 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` **BUILD SUCCESS**（总耗时 13:39，日志 `/tmp/w25-full-verify.log`）；`bash scripts/check-source-citations.sh` 与 `--selftest` 均零命中。验证=构建与门禁输出。

## 4. 收口

- [x] 4.1 `WATCHLIST.md` 删除 W25 行（编号留空）、维护规约的销账清单补记；验证=数据行 16 行、每行 5 列，`rg W25` 仅命中维护规约销账句。
- [x] 4.2 change 归档到 `openspec/changes/archive/2026-10-17-harden-w25-transfer-leader-retry/`；`openspec validate --strict --all` 通过、`openspec list` 无活动 change。验证=命令输出。
