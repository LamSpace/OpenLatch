# documentation-structure Delta Specification

## ADDED Requirements

### Requirement: docs 三分区与内容归宿

仓库 `docs/` SHALL 按内容性质三分：`docs/design/` 承载永久设计产物（概要设计、各阶段详细设计说明书、总体实施计划、选型决策报告）；`docs/quality/` 承载阶段验收快照报告；`docs/guide/` 承载用户向文档。dated 运行时报告（故障演练、基准基线）MUST NOT 进入 `docs/` 或仓库任何受版本控制的路径。`docs/README.md` SHALL 说明三区定位与历史 dated 报告的归档去向。

#### Scenario: 演练不污染仓库

- **WHEN** 在任意工作树执行 `-Pdrill` 全套与基准 harness
- **THEN** `git status` 保持 clean，报告仅出现于 `target/drill-reports/`（或 CI artifacts）

### Requirement: 运行时报告落点纪律

4 个进程级演练 IT 与 `BenchmarkMain` 的报告默认落点 MUST 为 `target/` 域（`drill-reports/` 同名追加语义保留），系统属性覆盖路径 MUST 保留；该落点为副作用输出，MUST NOT 影响任何用例断言逻辑。CI drill/benchmark job SHALL 以 run artifacts 交付这些报告作为常态分布渠道。

#### Scenario: 覆盖属性仍可用

- **WHEN** 以系统属性指定报告路径运行演练
- **THEN** 报告写入指定路径；未指定时写入 `target/drill-reports/`

### Requirement: 历史 dated 证据归档迁移

被验收报告/设计文档引用的 dated 演练与基准报告 MUST 从 `docs/` 迁出至引用方所属的 `openspec/changes/archive/<change>/evidence/` 目录（延续既有归档证据模式），引用文本的链接 MUST 改写至迁移后的可点位置；事实叙述与修订记录原文 MUST NOT 因迁移被改写。无引用方的 dated 报告直接删除（git 历史保真）。

#### Scenario: 验收报告链接不断裂

- **WHEN** 读者打开 Phase 2 验收报告中的演练证据链接
- **THEN** 链接指向 openspec 归档目录内实际存在的报告文件

### Requirement: README 门户正确性

README（EN 与 CN 对称维护）SHALL 反映当前交付全貌：不得残留已过期阶段声明（"Phase 1 / MVP""不含集群/TLS""S4 进行中"之类）；SHALL 含 badge 行、架构图、模块表、兼容性矩阵——兼容性矩阵 MUST 如实声明：要求 Java 25；`openlatch-spring-boot-starter` 仅支持 Spring Boot 4.x（依赖 Boot 4 独有构件，Boot 3.x 不兼容、官方不提供 starter，可手动装配 openlatch-client）；协议 v1/v2/v3 按 HELLO 版本区间协商、不做隐式兼容。语义表述 MUST 与实现一致：重启语义区分单机（全释放）与集群（快照/日志恢复）。文档索引 MUST 指向 `docs/guide/`；指向 `docs/design/` 的链接 SHALL 标注内部材料属性。

#### Scenario: Boot 3 用户得到诚实指引

- **WHEN** Spring Boot 3.x 用户查阅 README 兼容性矩阵
- **THEN** 明确得知 starter 不支持 Boot 3.x 及手动装配路径，不会在运行时撞 `spring-boot-starter-aspectj` 缺失

#### Scenario: 双语对称

- **WHEN** 对比 README.md 与 README_CN.md 章节结构
- **THEN** 两侧章节一一对应，无单侧独有的实质内容

### Requirement: poc 归档与选型证据锚定

`poc/` MUST NOT 存在于主干工作树；其内容 SHALL 经 git tag（`archive/raft-selection-poc`）锚定保留。`docs/design/raft-selection-report.md` 的"证据可复现"声明 MUST 改写为锚定该 tag 的复现指引（checkout 后 `summarize.py` 重算）；主规格中 capability `raft-selection-poc` MUST 随退役移除，选型结论以报告为唯一常驻记录。

#### Scenario: 选型证据可复现

- **WHEN** 审计者需要复算选型数据
- **THEN** `git checkout archive/raft-selection-poc` 后运行 `summarize.py` 即可从 results JSON 复现报告全部数字
