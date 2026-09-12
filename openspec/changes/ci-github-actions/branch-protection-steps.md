# 分支保护操作步骤（评审人在 GitHub 网页执行，手工介入项）

前置：`.github/workflows/build.yml` 已合入 master 并至少产生一次成功 run（否则 status check 下拉中不可见）。

1. 打开 `https://github.com/LamSpace/OpenLatch/settings/branches`
2. "Add branch protection rule" → Branch name pattern: `master`
3. 勾选 **Require a pull request before merging**（可选，单人仓库可暂缓；若启用需同步关掉自合并权限的例外）
4. 勾选 **Require status checks to pass before merging** → 搜索并勾选：
   - `verify (unit + IT + javadoc)`（build job）
   - `source citation gate`（citation-check job）
5. 勾选 **Do not allow bypassing the above settings**（管理员同样受约束）
6. Create / Save

验证：开一个含编译错误的测试 PR → 两项检查红且合并按钮禁用 → 废弃该 PR。

额度备注（public 仓库）：hosted Linux 免费 2000 分钟/月；PR 门禁单轮估 10–20 min、drill nightly 60–90 min/天、benchmark 周任务 <10 min，正常开发节奏余量充足。若 drill 失败率高导致夜间额度浪费，按 tasks 2.4 暂停 schedule 仅留手动。
