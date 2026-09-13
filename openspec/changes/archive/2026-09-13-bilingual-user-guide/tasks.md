# Tasks: bilingual-user-guide

判定基线：零代码变更，验证 = 链接完整性脚本 0 死链 + 反内部引用检查（guide 目录模式）0 命中 + 评审人抽查 3 页技术对读（手工介入项）。指南页命令示例一律通用形态（`mvn`/`java -jar`），不带任何本机私有参数。

## 1. 骨架与索引

- [x] 1.1 建 `docs/guide/zh/`、`docs/guide/en/` 页面骨架（00–10 + 术语表）与两侧 `index.md`；README 双语索引段改指（与 docs-restructure 落地的 README 新版本做增量修改，不动其门户结构）
- [x] 1.2 `scripts/check-source-citations.sh` 增 `--paths docs/guide` 模式（复用同一模式集）；guide 页写作时随写随查

## 2. zh 主体（四批）

- [x] 2.1 概念与上手批：00 简介架构、01 核心概念（租约/看门狗/等待-通知-重发/FIFO/锁丢失语义——从 README Semantics 段扩写并核对与实现一致）、02 快速上手（单机 server + client + starter 三最小示例）
- [x] 2.2 API 批：03 客户端 SDK（OLock/ReadWrite/FairLock/OSemaphore/OCountDownLatch 逐一：构造、语义、超时纪律、LockLost 回调义务、异步回调线程规则）、04 starter（`@OpenLatch` 属性全集、SpEL、锁在事务外层、自调用局限）
- [x] 2.3 运维批：05 集群（《Phase2 部署指南》扩充：拓扑图、种子发现、Leader 迁移与恢复窗口、滚动重启序与"先从不先主"由来及形态 B 残余的 supervisor 退避建议、快照恢复语义）、09 排障 FAQ（错误码表：NOT_LEADER/NOT_HELD/INVALID_REQUEST 等码形与处置，恢复窗口量级取自演练报告归档数据的中性重述）
- [x] 2.4 安全与观察批：06 安全（TLS/mTLS 配置矩阵、PEM 要求、业务 Token 轮换双活流程、9412/9413 明文边界如实声明）、07 控制台（部署与配置键全集）、08 可观测性（/metrics /healthz 端点与指标清单）、10 兼容性矩阵页、附录术语表

## 3. en 镜像

- [x] 3.1 按 zh 定稿逐节译制 00–04（含 README 语义承接句），术语与 en 业界惯例对齐（lease/watchdog/fencing 等以 JDK javadoc 惯用语为准）
- [x] 3.2 译制 05–10 + 附录；双语 index 互链；缺失页标注机制启用（若分批发行）

## 4. 收口

- [x] 4.1 链接完整性：guide 内链 + README↔guide 脚本全过；`git grep` 确认 guide 无指向 `docs/design/` 的必读依赖（标注内部材料的可指）
- [x] 4.2 `bash scripts/check-source-citations.sh --paths docs/guide` 0 命中
- [x] 4.3 抽查评审（手工介入项）：评审人已对读签认（2026-09-13），本变更出阶段
- [x] 4.4 README 瘦身终态：细节双写清零（README 独有而 guide 无的实质内容 = 0，快速上手最小闭环除外）
