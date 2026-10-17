# Tasks

## 1. 红先证据（判定已完成的留档）

- [x] 1.1 复现与判定留档：整模块 surefire + `2×nproc` 燃烧进程复现 `awaitTimeoutCancelsThenThrowsTimeoutException` 红（`elapsed 0.928s`，与台账逐字同型）；打点变体（有界轮询测到达时延）在空载 / 16 / 32 燃烧进程共 20+ 轮测得 **CANCEL 帧每次必达、延迟 0–15ms**，整模块失败轮同轮变体 7ms。验证=日志 `$CLAUDE_JOB_DIR/tmp/w20-repro/`、`w20-module/`、`w20-module2/`、`w20-module3/`；临时打点已还原（`git status` 清洁）。
- [x] 1.2 `WATCHLIST.md` W20 行现状列写入判定（迟到，非未发出；含保留项）；验证=行内容与实测一致、表完整性复检（18 行、每行 6 竖线）。

## 2. 修用例

- [x] 2.1 `awaitTimeoutCancelsThenThrowsTimeoutException`：立即断言换成 3s / 20ms 有界轮询后断言非空，三条实体断言（非空、`op == CANCEL`、`await_request_id > 0`）原样保留；验证=类级 **20/20 绿**。
- [x] 2.2 反向验证：临时令脚本服务端**不记录** CANCEL，测试在 3s 轮询后**仍红**（`elapsed 4.230s`，`Expecting actual not to be null`）；验后已还原。验证=诊断轮输出。

## 3. 口径复测

- [x] 3.1 整模块 surefire（154 枚）+ `2×nproc` 燃烧进程 **3/3 绿**。验证=日志 `w20-fixed-module/`。

## 4. 收口

- [x] 4.1 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` **BUILD SUCCESS**；citation gate（含 `--selftest`）零命中；验证=构建与门禁输出。
- [x] 4.2 `WATCHLIST.md` W20 行销账退役（编号留空、维护规约补记本 change 归档）；验证=**17 行**（本分支，W23 待另一 PR 合并）、每行 6 竖线、id 单调、全表引用回读通过。
- [x] 4.3 change 归档到 `openspec/changes/archive/2026-10-17-fix-phaser-cancel-observation/`；验证=`openspec validate --strict --all` 通过、`openspec list` 无活动 change。
