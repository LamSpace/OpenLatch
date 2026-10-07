# continuous-integration Specification

## Purpose
以 GitHub Actions 承载本仓库的例行守护：push/PR 上的轻量验证门禁（引用门禁自查 + 全反应堆 verify 含 javadoc 深度），与按需/周期的重档位 job（进程级演练手动随跑、benchmark 周跑）；把"防假绿"的断言（如演练套件缺失即红）固化在流水线内，使守护不依赖个人自觉。
## Requirements
### Requirement: push 与 PR 验证门禁

仓库 SHALL 以 GitHub Actions 定义验证 workflow：master push 与每个 pull request MUST 在 ubuntu-latest、temurin JDK 25 环境执行全量 `mvn -B clean verify`（单测、进程内 IT、javadoc doclint 门禁一并承载）；CI MUST NOT 依赖任何本机 Maven settings 文件（直连 Maven Central）。同 workflow SHALL 附带注释纪律检查 job（执行 source-comment-discipline 门禁脚本，命中即红）。验证失败时 MUST 将 surefire/failsafe 报告上传为 run artifacts；成功时 SHALL 上传生成的 javadoc 站点。master 分支 SHALL 配置分支保护，使 `build` 检查为合并必要条件。

#### Scenario: 失败 PR 被阻断

- **WHEN** 某 PR 触发门禁且任一模块测试或 javadoc 检查失败
- **THEN** 该 PR 状态检查呈红，surefire/failsafe 报告可从 artifacts 下载，分支保护阻断合并

#### Scenario: 注释纪律回潮即红

- **WHEN** 提交在 `src/main` 注释中引入内部文档引用
- **THEN** citation-check job 非零退出呈红，PR 被阻断

### Requirement: drill 与 benchmark 重档位 job

`-Pdrill` 演练套件与基准 MUST NOT 进入 PR 门禁（共享 runner 抖动不得转化为 PR 假红），但 SHALL 提供独立 workflow job：nightly `schedule` 与 `workflow_dispatch` 手动双触发；job 前置产出 server shaded jar；并发 MUST 以 `concurrency` 组串行化（分区演练占用固定端口与 netns 前缀，并行必然冲突误报）；超时给足。演练/基准报告与节点子进程日志 MUST 以 run artifacts 交付，MUST NOT 要求入库（与 documentation-structure 的"运行时报告不入工作树"口径一致）。

#### Scenario: nightly drill 产物可下载

- **WHEN** 夜间 drill job 完成
- **THEN** Actions run 页可下载各演练报告与 drill-logs，仓库工作树零新增文件

#### Scenario: 并行触发串行化

- **WHEN** nightly drill 运行期间又手动触发同 workflow
- **THEN** `concurrency` 组使二者不并行执行（排队或取消旧 run），无端口/namespace 冲突假红

