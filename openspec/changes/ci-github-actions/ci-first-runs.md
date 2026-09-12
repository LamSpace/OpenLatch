# CI 首跑实录

## build.yml — run #1（自动触发,workflow 上线首验）

- 时间：2026-09-12 16:19–16:28（北京时间），runner 用时约 7.5 min
- 触发：push `bc15625..92efcbd` → master，head sha `92efcbd`
- 结果：**成功**（run_id 34682963512）
  - `verify (unit + IT + javadoc)`：成功——全 reactor `mvn -B -ntp clean verify`（temurin-25 + maven cache），javadoc 站点 artifact 已上传
  - `source citation gate`：成功——`--selftest` 夹具回归 + 全仓扫描零命中
- 结论：tasks 1.3 绿面达成；红面（含内部引用的测试 PR 被拒）待评审人放行后执行（在公开仓库建测试 PR 属外发动作）。
- 链接：`https://github.com/LamSpace/OpenLatch/actions/runs/34682963512`

## build.yml — run #2（runner 侧 flake 首例，在案观察）

- 时间：2026-09-12 16:5x–17:0x；触发 push `98cdd6c`（**仅 markdown 变更**）
- 结果：失败——`Full verify` 步骤；`source citation gate` 成功
- 归因：该提交的构建代码与成功的 #1 完全相同（diff 只有文档），#3（含真实代码变更）亦成功；
  本地同码全量绿——唯一变量为共享 runner 抖动命中时序敏感用例。工件自动上传机制经此验证工作
  （`test-reports-2` 570KB；下载需 token，匿名 401）。
- 处置：按 tasks 2.4 记档观察；nightly/首周若再现同类单发红，评估 `--fail-flaky` 类缓解或
  用例时序余量加固，**不逐例追**（PR 门禁可手动重跑即可）。
- 链接：`https://github.com/LamSpace/OpenLatch/actions/runs/34683503208`

## build.yml — run #3（③ 结构云端首验）

- 触发 push `e17983f`；结果：**成功**（build + citation-check 双 job）
- 意义：docs 三分/README 门户/报告落点改造后的全量门禁与反引用门禁在 CI 上首次通过。

## 待录

- drill.yml 首跑（需 Actions 网页手动 workflow_dispatch，或等 nightly `19:23 UTC`）：核对 PartitionDrillIT 非跳过 → 记入本文件（tasks 2.3）
- benchmark.yml 首跑（workflow_dispatch，tasks 3.2）
- 额度读数（tasks 4.3 收口时补）
