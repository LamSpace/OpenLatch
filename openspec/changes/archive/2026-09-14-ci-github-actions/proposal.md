# Proposal: ci-github-actions

## Why

仓库已在 GitHub 公开（github.com:LamSpace/OpenLatch，评审人确认为 public），但无任何 `.github/` 配置——所有机械验证能力缺位：全量 `clean verify`（337+21+63 用例级、含 javadoc doclint 门禁）完全依赖本地人肉执行；Phase 2/3 验收报告把 `-Pdrill` 进程级演练明确记为"未执行项，留 CI/运维环境复跑"（本地无 TTY 沙箱不可靠，人工桌面跑无法成为常态纪律），该承诺没有承载物；已定夺"过时报告不入库"（评审决定），dated 演练/基准报告的常态归宿正是 CI run artifacts——本变更与之互为前提；javadoc-internal-citation-cleanup 产出的反引用门禁脚本需要 CI 承载才能防回潮。GitHub Actions 对 public 仓库提供免费 hosted Linux runner（具备 passwordless sudo 与完整内核能力，可承载 netns/iptables 分区演练），与本项目进程级故障演练的需求精确匹配。

## What Changes

- 新增 `.github/workflows/build.yml`：触发于 master push 与 pull_request；`actions/setup-java` temurin@25 + Maven 依赖缓存；job `build` 跑 `mvn -B -ntp clean verify`（CI 环境 MUST NOT 携带本机 `-s settings.xml`——阿里云镜像是本机网络事项，runner 直连 Central 即可）；job `citation-check` 调用 `scripts/check-source-citations.sh`；失败时上传 surefire/failsafe 报告为 artifacts，成功时上传 javadoc 站点。
- 新增 `.github/workflows/drill.yml`：`workflow_dispatch` 手动 + nightly `schedule`；`concurrency` 组串行化（PartitionDrillIT 固定端口 9411 与 netns 前缀 `oln`，并行必假红）；先 `clean install -DskipTests` 产出 shaded jar，再 `verify -Pdrill`；演练报告（现状 `docs/*-drill-*.md`，docs-restructure-and-report-retirement 落地后为 `**/target/drill-reports/**`）与 `**/target/drill-logs/**` 上传 artifacts；`timeout-minutes` 给足（全程估 40–70 分钟）。
- 新增 `.github/workflows/benchmark.yml`：`workflow_dispatch` + weekly schedule；构建后以 `mvn -pl openlatch-examples exec:java`（注意：exec:java 不可带 `-am`）跑 `BenchmarkMain`，报告以 artifact 交付。
- badge URL 契约：交付 build/drill 两个 workflow 的 status badge markdown 片段（README 写入动作由 docs-restructure-and-report-retirement 承载，或本变更在其之后落地时直接补行——两侧 tasks 均有对账项）。
- 分支保护：master 启用 required status checks（`build`，建议连带 `citation-check`）；GitHub 设置无法由仓库文件承载，交付操作步骤文档由评审人手工执行（唯一手工介入项）。
- 非目标：release/发布 workflow（评审定夺版本发布不算）；多 OS 矩阵（演练依赖 Linux netns）；Container registry 构建。

## Capabilities

### New Capabilities

- `continuous-integration`: push/PR 验证门禁（全量 verify + javadoc + 注释纪律 check）、drill/benchmark 重档位 job（nightly + 手动触发 + 并发串行 + artifacts 交付、报告不入仓库工作树）、分支保护策略。

### Modified Capabilities

- 无。

## Impact

- 文件：`.github/workflows/` 三个新 yml；零代码、零 pom 变更。
- 额度：public 仓库 hosted Linux 免费额度（3,000 min/月）内——PR 门禁估 10–20 min/轮，drill nightly 约 60–90 min/天，benchmark weekly 约 5 min；`concurrency` 与低频 schedule 防额度击穿。
- 依赖：`citation-check` 绿依赖 javadoc-internal-citation-cleanup 先行落地（否则上线即红，红正是门禁目的——顺序仍建议①→②）；drill job 的 artifact 路径与 docs-restructure-and-report-retirement 的落盘改点对账。
- 验收：首个 PR 触发 build/citation-check 全绿；故意加"设计说明书 §1"注释的测试 PR 被拒红；workflow_dispatch 跑通一次 drill 并在 Actions 页得到报告 artifacts（构成标准 5 复核的云端通道，与评审人桌面手工复跑互为冗余）；分支保护设置完成的对账记录。
