# source-comment-discipline Specification

## Purpose

约束对外发布面的可读性与内部信息边界：发布模块的源码注释/Javadoc 与 pom 发布元数据不得引用外部读者无从访问的内部过程文档与代号，并以机械化门禁脚本防回潮，使不访问内部文档的读者仅凭注释即可理解完整行为契约。

## Requirements

### Requirement: 对外注释与发布元数据不含内部过程文档引用

发布模块（openlatch-protocol、openlatch-core、openlatch-server、openlatch-client、openlatch-spring-boot-starter、openlatch-console、openlatch-examples）`src/main` 下全部 Java 源文件的 Javadoc 与行内注释，以及各模块 `pom.xml` 的 `<description>`，MUST NOT 引用内部过程文档与内部代号，包括但不限于：《详细设计说明书 / 设计说明书 / 概要设计 / 详设 / 实施计划 / 验收报告》及"§x.y"节号、"Phase 1/2/3"阶段代号与其任务号（P1-NN / P2-NN / P3-NN）与里程碑代号（M1–M4）、openspec change 名与决策编号（design D1 / spec"XX"）。注释 SHALL 自洽：不访问内部文档的读者仅凭注释即可理解完整行为契约；仅具修订跟踪价值的句子 MUST 整句删除而非保留悬空引用。

#### Scenario: 生成的 javadoc 不见内部溯源

- **WHEN** 以 `show=private` 配置生成 javadoc 站点
- **THEN** 产物全文（含私有成员注释）不命中上述内部引用模式

#### Scenario: pom 元数据只述功能

- **WHEN** 外部用户查看 Maven Central 上的模块描述
- **THEN** description 为功能性表述，无内部阶段/任务/文档代号

### Requirement: 防回潮机械门禁

仓库 SHALL 提供可独立执行的检查脚本（`scripts/check-source-citations.sh`）：扫描范围覆盖全部发布模块的 `src/main/**/*.java` 与模块 pom 的 `<description>`；命中引用模式集即非零退出，并逐条输出 `file:line` 与命中上下文。脚本 MUST 在无 CI 环境下本地可执行（供提交前自查），MUST 与 CI 门禁共用同一模式集（单一事实源）。注释纪律规范（CLAUDE.md）SHALL 记录该禁令与脚本路径。

#### Scenario: 新引入引用使门禁变红

- **WHEN** 提交在 `src/main` 注释中新增"设计说明书 §4.9"字样
- **THEN** 脚本非零退出并定位到该行

#### Scenario: 清理完成后门禁零命中

- **WHEN** 在本变更收尾状态执行脚本
- **THEN** 全仓 `src/main` 与 pom description 零命中，脚本退出码为 0
