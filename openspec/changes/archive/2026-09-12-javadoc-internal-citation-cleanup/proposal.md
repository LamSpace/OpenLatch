# Proposal: javadoc-internal-citation-cleanup

## Why

六个发布模块的 `src/main` Javadoc 大面积引用内部过程文档与代号（按宽口径正则统计约 490 处、分布于 104 个文件）："设计说明书 §4.9"、"概要设计 §4.3 标准 3"、"Phase 3 详设 §2.4 / P3-05"、"（Phase 3 T2，spec\"只读统计观察面\"）"、"design.md D4"、"设计说明书 v1.2 §4.2 已同步为两值"等；模块 `pom.xml` 的 `<description>` 亦含"详设 §8，M4 交付""Phase 3 详设 §4 T3"。这些文档与代号是内部过程材料，外部读者无从访问，引用对他们是纯噪声；且 `maven-javadoc-plugin` 配置 `show=private`，私有成员注释一并进入生成的 javadoc——一旦随构建产出外发，内部溯源信息全部暴露；pom description 直接进 Maven Central 检索面。引用方言至少六种、遍布各模块，说明缺失的是纪律而非个别疏忽，一次性清理后必须有机械门禁防回潮。

## What Changes

- 清理 `openlatch-protocol/core/server/client/spring-boot-starter/console/examples` 全部 `src/main` Javadoc 内部引用：引用标记去除、语义句保留自洽；纯修订跟踪句（"v1.2 §4.2 已同步为两值"）整句删除；语义依赖引用支撑的改写为自洽表述（"Phase 1 不支持持读升级写"→ 直接陈述规则，不提阶段）。
- 重写 7 个模块 pom `<description>`：去章节号与里程碑代号（M2/M3/M4），仅留功能性一句话。
- `src/test` 同步改写内部流程叙事（"供验收报告标准 5 复核改判引用"之类），报告落盘路径等属功能事实保留（其入库形态由 docs-restructure-and-report-retirement 另行处理，本变更不动行为）。
- 新增 `scripts/check-source-citations.sh`：扫描 `src/main` 与 pom description 的引用模式集，命中输出 file:line 并非零退出；本变更内即由该脚本驱动清理验收，后续由 ci-github-actions 上 CI 常驻门禁。
- CLAUDE.md §5 增补纪律条款：对外注释禁止引用内部过程文档，注明门禁脚本路径。

## Capabilities

### New Capabilities

- `source-comment-discipline`: 对外源码注释与发布元数据不含内部过程文档引用；机械化检查脚本作为防回潮门禁；注释语义自洽性要求。

### Modified Capabilities

- 无（纯注释与元数据文本，不触碰任何行为规格）。

## Impact

- 代码：仅 Javadoc 文本与 pom description，外加一处已裁定的例外——BenchmarkMain `renderReport` 模板中 3 处字符串字面量（`"Phase 1 基准基线"` 标题、`"design D5 手写 harness"` 来源、`"详设 §10.5"` 尾注）改为功能性表述：门禁对 `src/main` 全行扫描（含字面量），此三处为全部清理面上仅有的非注释命中；生成的基线报告无消费者断言该文本，且硬编码阶段标题本属逐阶段手改的既有痛点。
- 构建：`doclint=all` + `failOnWarnings=true` 为清理后的 javadoc 语法正确性背书；回归全量 `clean verify`。
- 门禁：脚本新增本地可执行；CI 承载属 ci-github-actions（落地顺序：本变更先，CI 门禁接上即绿）。
- 非目标：不改注释深度与结构（既有契约级 Javadoc 规范不变）；不触碰 docs/ 内容、openspec 归档与 README。
