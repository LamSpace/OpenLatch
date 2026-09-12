# Proposal: bilingual-user-guide

## Why

项目面向用户的学习路径缺位：三阶段交付的用户面——客户端 SDK（OLock/ReadWriteLock/FairLock/OSemaphore/OCountDownLatch）、`@OpenLatch` 声明式锁、Raft 集群运维（种子发现、Leader 迁移、重启序与 supervisor、"先从不先主"推荐序的由来）、传输安全（TLS/mTLS、Token 轮换流程）、管理控制台、metrics/healthz——没有一处成体系的教程；唯一用户向文档《Phase2 集群部署与故障转移》为纯中文单篇，其余全部细节寄居在 README（因此臃肿至 260+ 行且仍在膨胀），外部用户拿不到"概念 → 上手 → 运维 → 排障"的渐进路径。评审定夺三阶段详设属内部材料不外传（javadoc-internal-citation-cleanup 的同一立场），用户文档必须独立成面。docs-restructure-and-report-retirement 已立 `docs/guide/` 骨架与 README 门户，门户需要有内容可指。

## What Changes

- 建立 `docs/guide/zh/` 与 `docs/guide/en/` 双语对照指南（11 节 + 附录）：00 简介与架构、01 核心概念（租约/看门狗/锁丢失/等待-通知-重发/FIFO 公平/超时纪律）、02 快速上手、03 客户端 SDK、04 Spring Boot starter（`@OpenLatch` SpEL、事务边界、AOP 自调用局限）、05 集群部署与运维（现部署指南扩充迁入）、06 安全（TLS/mTLS/Token 与轮换流程）、07 管理控制台、08 可观测性（metrics/healthz/管理端口）、09 故障排查与 FAQ（NOT_LEADER/NOT_HELD 码形、恢复窗口、锁丢失处理）、10 兼容性与协议版本（Boot 4 only、Java 25、协议协商）、附录术语表。
- zh 先行、en 镜像；页面对账纪律：任一侧缺页 MUST 在同侧索引显式标注，MUST NOT 静默不对称。
- 内容溯源纪律：技术表述以实现与已定案验收口径为准（README 现有正确内容下沉承接）；示例命令面向通用环境可复制执行（不含任何本机私有配置，如 `-s` 参数）；全部指南页纳入反内部引用检查（不引详设/验收报告/design DN，复用 javadoc-internal-citation-cleanup 模式集扩展至 `docs/guide/**`）。
- README（EN/CN）文档索引段从"细节内嵌"瘦身为"门户 + 指南链接"；README 保留快速上手最小闭环，其余细节以 guide 为唯一权威避免双写漂移。
- 非目标：docs/ 物理重组与 README 重写本体（docs-restructure-and-report-retirement）；站点化工具（MkDocs 等，v1 用 markdown 目录即够）；指南内出现任何未实现能力的承诺（控制台写操作等出范围项仅在 roadmap 处如实标注"未提供"）。

## Capabilities

### New Capabilities

- `user-documentation`: 双语指南分区布局与章节覆盖全集；页面配对与不对称标注；命令示例可复制性；指南与内部材料的引用隔离；README-guide 职责分工（门户 vs 权威细节）。

### Modified Capabilities

- 无（README 变更由 documentation-structure 规格约束，本变更承接其内容扩充）。

## Impact

- 文件：`docs/guide/zh|en/` 新增约 24 篇；README 双语索引段瘦身改写（与 docs-restructure 的 README 重写对账，后落地者做增量修改）；`scripts/check-source-citations.sh` 增加目录模式（`--paths docs/guide`）。
- 零代码变更；无测试影响。
- 验证：链接完整性脚本（guide 内链 + README↔guide）0 死链；引用门禁对 guide 目录 0 命中；技术正确性抽查（评审人指定 3 页 vs 实现行为对读——手工介入项）；EN 版由 zh 版逐节镜像生成后抽样对读。
- 依赖：在 docs-restructure-and-report-retirement 之后落地（目录骨架、README 重写、部署指南 git mv 均为其产出）。
