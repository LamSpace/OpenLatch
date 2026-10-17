# Tasks

## 1. 红先证据（判定已完成的留档）

- [x] 1.1 复现留档：空载 20 轮跑两枚用例，红 **1/20**，签名 `expected: [3L, 4L, 2L] but was: [3L, 4L, 3L]`；确定性诊断（停 follower→追加条目→重启，窗口拉至 1.5s）**3/3 红**且 leader `lastApplied` 三次均不动；第二枚同法 **2/3 红**在 `aliveAgreeWithLeader()`。验证=日志 `$CLAUDE_JOB_DIR/tmp/w23-repro-empty/`，临时打点已还原（`git status` 清洁）。
- [x] 1.2 `WATCHLIST.md` W23 行现状列写入判定（夹具口径 + 机理 + 证据）；验证=行内容与实测一致、表完整性复检（18 行、每行 6 竖线、id 单调）。

## 2. AdminClusterTest：基线追平前置

- [x] 2.1 在 `adminQueries_produceNoLogEntries` 取 `before` 前加 `harness.awaitTrue(...)`：全部存活节点 `lastApplied == leader.lastApplied`，上限 20s；`after == before` 断言原样保留。验证=该用例**空载 20/20 绿**，无新增睡眠。
- [x] 2.2 反向验证前置真实生效：修复前的重启 follower 追赶窗 + 窗口拉长使原断言 **3/3 红**（见 1.1），证明等待不是装饰；验后已还原。验证=诊断轮日志。

## 3. StateMachineConditionTest：摘要收敛前置

- [x] 3.1 把 `assertThat(h.aliveAgreeWithLeader()).isTrue()` 换为同义有界等待（`h.awaitTrue(h::aliveAgreeWithLeader, 20_000, "基线摘要追平")`）。验证=该用例**空载 20/20 绿**。
- [x] 3.2 反向验证：修复前用"裸断言 + 重启 follower 追赶窗"复现 **2/3 红**（见 1.1）；验后已还原。

## 4. 造载复测（台账口径）

- [x] 4.1 `2×nproc` 燃烧进程下，两枚用例各 20 轮：**0/20 红**。验证=日志 `w23-repro-fixed-load/`。

## 5. 收口

- [x] 5.1 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` **BUILD SUCCESS**；`scripts/check-source-citations.sh`（含 `--selftest`）零命中；验证=构建与门禁输出。
- [x] 5.2 `WATCHLIST.md` W23 行销账退役（编号留空、维护规约补记本 change 归档）；验证=**17 行**、每行 6 竖线、id 单调、全表引用回读通过。
- [x] 5.3 change 归档到 `openspec/changes/archive/2026-10-17-fix-server-log-assertion-convergence/`；验证=`openspec validate --strict --all` **17 passed / 0 failed**、`openspec list` 无活动 change。
