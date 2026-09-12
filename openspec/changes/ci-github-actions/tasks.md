# Tasks: ci-github-actions

判定基线：workflow 语法本地以 `actionlint`（如可用）预检；最终真相 = GitHub 上首次 run；drill/benchmark 验证以 Actions 页 workflow_dispatch run 记录与 artifacts 为准。CI 内 mvn 命令一律不带 `-s`（runner 境外网络直连 Central）。

## 1. build.yml（push/PR 门禁）

- [x] 1.1 新增 `.github/workflows/build.yml`：`on: push(branches:[master]) + pull_request`；job `build`：runs-on ubuntu-latest、`concurrency: build-${{ github.ref }}` + cancel-in-progress；步骤 checkout → setup-java（temurin/"25"/cache:maven）→ `mvn -B -ntp clean verify` → 失败时 upload-artifact（`**/target/surefire-reports/**`、`**/target/failsafe-reports/**`，`if: failure()`）→ 成功时 upload javadoc（`**/target/site/apidocs/**`）
- [x] 1.2 同文件 job `citation-check`：checkout + `bash scripts/check-source-citations.sh`（模式集单一事实源在该脚本，见 javadoc-internal-citation-cleanup）
- [x] 1.3 （绿面）验证红绿两面：开一个临时 PR 在任一 `src/main` 注释加"设计说明书 §1" → citation-check 红、build 绿；删行 → 双绿；合并前回滚。run 链接记入本 change `ci-first-runs.md`

## 2. drill.yml（进程级演练重档位）

- [x] 2.1 新增 `.github/workflows/drill.yml`：`on: workflow_dispatch + schedule(cron: 非整点，如 "23 19 * * *")`；`concurrency: group: drill, cancel-in-progress: false`；`timeout-minutes: 150`；runs-on ubuntu-latest
- [x] 2.2 步骤：checkout → setup-java(temurin/25, cache:maven) → `mvn -B -ntp clean install -DskipTests`（产出 `openlatch-server-1.0-SNAPSHOT-executable.jar`——缺失时演练 assume-skip 假绿，此步不可省）→ `mvn -B -ntp verify -Pdrill` → `if: always()` upload-artifact：演练报告（现状 `docs/*-drill-*.md`、`docs/leader-stall-repro-*.md`；docs-restructure-and-report-retirement 落地后切 `**/target/drill-reports/**`，两侧对账）+ `**/target/drill-logs/**`
- [ ] 2.3 首次 workflow_dispatch：核对 hosted runner 上 PartitionDrillIT 非跳过（`sudo -n true` 可用 + netns 建立成功；若 assume-skip 或环境不满足，如实记录处置决定——保留进程内辅轨 MinorityQuorumTest 兜底、或申报 self-hosted runner——写入 `ci-first-runs.md`）
- [ ] 2.4 首周观察：连续 nightly 若出现 flaky 轮次，暂停 `schedule` 仅留 `workflow_dispatch`，处置结论记档（drill job 红灯不挂 badge，不作 PR 门禁）

## 3. benchmark.yml

- [x] 3.1 新增 `.github/workflows/benchmark.yml`：`on: workflow_dispatch + schedule(weekly)`；步骤：checkout → setup-java → `mvn -B -ntp clean install -DskipTests` → `mvn -B -ntp -pl openlatch-examples exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.BenchmarkMain`（注意 exec:java 不带 `-am`，带 `-am` 会落根聚合 pom 报 ClassNotFound）→ upload-artifact 报告（路径与 docs-restructure 落盘改点对账；系统属性覆盖输出到 `target/` 亦可）
- [ ] 3.2 首跑验证：run 绿且 artifacts 含当日 baseline 报告；run 链接记入 `ci-first-runs.md`

## 4. badge 与分支保护

- [x] 4.1 交付 badge markdown 片段（build / drill / license / Java 25 / Spring Boot 4 / protocol v3）写入本 change `badges.md`；README 写入与 docs-restructure-and-report-retirement 对账（其先落地则本任务顺带补行，其后落地则由其按 `badges.md` 原名嵌入）
- [ ] 4.2 分支保护操作步骤文档 `branch-protection-steps.md`：Settings → Branches → Add branch protection rule → `master`；Require status checks to pass → 勾选 `build`（建议连带 `citation-check`）；Do not allow bypassing。由评审人在 GitHub 网页执行（手工介入项），完成后本任务勾选即对账
- [ ] 4.3 收口：`ci-first-runs.md` 汇总三类 workflow 首跑链接 + 额度占用读数（Actions 页分钟数）
