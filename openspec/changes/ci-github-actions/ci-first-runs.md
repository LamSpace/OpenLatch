# CI 首跑实录

## build.yml — run #1（自动触发,workflow 上线首验）

- 时间：2026-09-12 16:19–16:28（北京时间），runner 用时约 7.5 min
- 触发：push `bc15625..92efcbd` → master，head sha `92efcbd`
- 结果：**成功**（run_id 34682963512）
  - `verify (unit + IT + javadoc)`：成功——全 reactor `mvn -B -ntp clean verify`（temurin-25 + maven cache），javadoc 站点 artifact 已上传
  - `source citation gate`：成功——`--selftest` 夹具回归 + 全仓扫描零命中
- 结论：tasks 1.3 绿面达成；红面（含内部引用的测试 PR 被拒）待评审人放行后执行（在公开仓库建测试 PR 属外发动作）。
- 链接：`https://github.com/LamSpace/OpenLatch/actions/runs/34682963512`

## 待录

- drill.yml 首跑（需 Actions 网页手动 workflow_dispatch，或等 nightly `19:23 UTC`）：核对 PartitionDrillIT 非跳过 → 记入本文件（tasks 2.3）
- benchmark.yml 首跑（workflow_dispatch，tasks 3.2）
- 额度读数（tasks 4.3 收口时补）
