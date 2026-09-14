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

## drill.yml — run #1（定时首跑，失败在案）

- 时间：2026-09-13 05:29（北京时间）schedule 触发，job 用时 17.7 min
- 步骤结论：`Build reactor` ✅；`Run drills` ❌（exit 1）；**`Assert every drill suite executed` ✅——
  四套件的 failsafe 报告齐全且 Skipped=0，即 hosted runner 上 PartitionDrillIT 真实执行、
  特权/网段环境可用（tasks 2.3 的"非跳过"命题由本 run 回答：是）**；artifacts 步骤 ✅
  （`drill-evidence-1` 257KB——新 target/drill-reports glob 采集有效）。
- 失败面：某轨真断言/超时失败，具体套件匿名 API 不可达（logs/artifacts 需 token）。
- **分诊结论（2026-09-13）**：本地同构复跑（同 HEAD 系代码 + 同特权路径）全量 `-Pdrill`
  **五轨全绿 REPRO_EXIT=0**（stall 70.6s / idle 69.9s / partition 4.2s / LeaderKill 21.2s /
  Rolling 449.4s，全部 Skipped=0）→ 判定 **runner 侧争用**（hosted 4 核对时序敏感数值演练
  是部署指南既知弱项），与 build #2 同族，非产品缺陷、非 workflow 配置错误。
- 处置：按 tasks 2.4 观察至第二样本（下一 nightly）；若连续 flaky → 暂停 schedule 仅留手动；
  若单发 → 保留 schedule，red 视为运维信号非门禁信号（drill job 本就不挂合并阻断）。
- 附注（非阻塞）：Actions 提示 actions/checkout@v4 / setup-java@v4 依赖 Node 20 弃用路径与
  setup-java v5 迁移建议——后续小提交统一升级 @v5/v4→v5 与 checkout@v5，与本失败无关。
- 链接：`https://github.com/LamSpace/OpenLatch/actions/runs/34720084117`

## 红面验证（PR#1，tasks 1.3 红面达成，2026-09-13）

- 分支 `ci-citation-red-test`（注入 `CoreConfig.java:29` "设计说明书 §1" 样本行，a562f4b）→ PR#1 → 验证后 **closed（未合并）**。
- 结论：`source citation gate` = **failure**（拦截命中行）；`verify (unit + IT + javadoc)` = **success**——
  两 job 独立性实证；分支保护下红 check 使合并不可行，防线闭环。
- 附带发现（过程纠错如实记）：注入锚点最初按 `class CoreConfig` 书写，实际为 `record`——分类器服务
  抖动延后了执行，改用真实锚点后本地门禁先行命中，云端结果与本地预期逐项一致。

## build.yml — PR#2（docs/acceptance-signature，runner flake 第三例 → 防噪落地）

- 首跑 `verify` 红（md-only diff，本地同树 `209b182` 全量 verify 绿，16 分钟内实证）；`citation-check` 绿。
- 评审人 Re-run all jobs → **重跑双绿** → Merge（67deca2，仓库首个 PR 流程走通：门禁+分支保护+重跑三合一验证）。
- 三例同族（build #2 / drill #1 / PR#2）均为共享 runner 争用 → 本分支落防噪：build verify 失败自动重试一次 + 三 workflow 的 actions 升级（checkout@v7/setup-java@v6/upload-artifact@v7，消 Node 20 弃用告警）。

## drill.yml — run #2（2026-09-14 05:35 北京，第二夜即定性质）

- 步骤面：`Run drills` 红（17.0 min）；**`Assert every drill suite executed` 绿**（四套件报告齐全且 Skipped=0——防假绿与"未跳过"命题再证）；artifacts `drill-evidence-2` 上传成功。
- 失败面：LeaderKill **3/3 绿**、先从后主绿（1.68%，tail=0）；红在**最重的两条时序断言**——
  Partition "40s 内未在多数派侧探测到 Leader"、先主后从 `errors=156/626 (24.92%) tailErrors=116`
  （对照先从后主 2617 请求——吞吐坍缩至 1/4，驱动线程大面积阻塞在超时）。
- **定性（与 #1 合并结论）**：`ubuntu-latest` 2 vCPU 不满足本套件的硬件前提——netns/iptables/bind
  错误全日志零命中、`stallEvents=0`（产品看门狗未判停摆，复制在推进只是慢）、失败烈度与套件负载
  正相关、本地同码多次全绿——是**算力预算赤字**而非随机 flake（重试无效），且项目文档自身
  早已要求"<10s 数值门在独占硬件上执行"。
- **处置（tasks 2.4）**：暂停 `schedule`（workflow 内注释留档恢复条件），保留手动 dispatch；
  自动化恢复路径 = self-hosted runner（开发机，已证满足预算）或 ≥4 vCPU 档。

## benchmark.yml — run #1（2026-09-14 15:32 北京，tasks 3.2）

- scheduled 触发（队列延迟 ~5h，免费账户正常现象），**成功**；artifact `benchmark-baseline-1`（849B）在册。

## 额度读数（tasks 4.3）

- 上线两日累计 ~143 runner-min：build 105.5（16 run）/ drill 35.7（2 run，均 runner 不适配废弃）/ benchmark 1.6。
- public 仓库 hosted Linux 按合理用量免费不计账单；夜间 drill 暂停后主要开销回归 PR 门禁本身。
