# Tasks: docs-restructure-and-report-retirement

判定基线：验证命令一律 `mvn -s /home/lam/repo/settings.xml`；重组正确性 = 死链清零（`git grep` 旧路径无残留 + markdown 链接脚本检查）+ 全量 `clean verify` 绿；演练落点端到端以评审人桌面 `-Pdrill` 复跑或 CI drill job 首跑为退出判据（本环境无法完成进程级演练，不假验）。

## 1. docs 三分重组

- [x] 1.1 `git mv`：概要设计、Phase1–3 详设、总体实施计划与验证方案、raft-selection-report → `docs/design/`；Phase1/2/3 验收报告 → `docs/quality/`；《OpenLatch-Phase2-集群部署与故障转移》→ `docs/guide/zh/`（文件名保留，扩充属 bilingual-user-guide）
- [x] 1.2 全仓 `git grep` 旧路径引用（README、docs 内部互链、CLAUDE.md 等）逐处改写至新路径；修订记录/验收证据段仅改链接指向，不改事实文字
- [x] 1.3 新增 `docs/README.md`：三分区定位、历史 dated 报告归档政策（去向 openspec 归档 / git tag）、用户从 guide 进入的路径说明

## 2. 报告落点改点（唯一代码面）

- [x] 2.1 4 个演练 IT（`LeaderKillDrillIT`/`PartitionDrillIT`/`RollingRestartDrillIT`/`LeaderStallReproDrillIT`）报告目录常量 `../docs/` → `../target/drill-reports/`，`mkdir -p` 语义核对，系统属性覆盖保留；类级 Javadoc 同步（落盘句改路径、其余保留——注意本 change 与 javadoc-internal-citation-cleanup 顺序：若①先行，此处只改路径句）
- [x] 2.2 `BenchmarkMain` 默认输出 `docs/benchmark-baseline-<date>.md` → `target/benchmark/`（系统属性覆盖保留；Javadoc 同步）
- [x] 2.3 编译与回归：`-pl openlatch-client,openlatch-server -am test-compile` 绿 + 全量 `clean verify` 绿（断言零触碰，报告写为副作用）

## 3. 历史 dated 文件迁移与清除

- [x] 3.1 建迁移动作映射表（本 change 目录 `report-migration.md`）：对 10 个 dated 文件逐个 `git grep` 引用方 → 有引用者 `git mv` 至对应 `openspec/changes/archive/<change>/evidence/`（failover-08-31→s3；rolling-09-02→s4；09-05 两份→phase2-release-closure；09-06 failover/rolling/partition + leader-stall-repro→leader-stall-followup；benchmark 两份按引用方定档）；无引用者 `git rm`
- [x] 3.2 验收报告/详设中对应链接文本改写至 `evidence/` 新路径；`git grep "docs/failover-drill\|docs/rolling-restart\|docs/partition-drill\|docs/leader-stall-repro\|docs/benchmark-baseline"` 清零

## 4. poc 归档退役

- [x] 4.1 `git tag archive/raft-selection-poc`（当前 HEAD，poc 完整存在）；`git rm -r poc/` 提交
- [x] 4.2 `docs/design/raft-selection-report.md` 证据节改写：原始数据经 tag 锚定复现（checkout → `mvn -f poc/raft-selection/pom.xml` + `run-matrix.sh` + `summarize.py`）；friction/config-freeze 文档引用同步指 tag
- [x] 4.3 主规格退役：`openspec/specs/raft-selection-poc/` 随本 change 归档流程删除（delta 已具 REMOVED 八条）；根 pom 与文档中 `poc/` 提及清零（`git grep -w poc` 复核）

## 5. README 重写（EN + CN 对称）

- [x] 5.1 定位段重写：Phase 3 收口全貌（集群、扩展锁类型、控制台、TLS/mTLS、Token 认证、协议 v3）；删"Not in Phase 1"/"S4 进行中"等过期声明；badge 行按 ci-github-actions `badges.md` 原名嵌入
- [x] 5.2 新增：ASCII 架构图（client ↔ 集群 Leader/Follower(9410)/Raft(9411)/metrics(9412)/console(9413) ↔ console）；兼容性矩阵（Java 25｜Spring Boot 4.x only——`spring-boot-starter-aspectj` Boot 4 独有、Boot 3 不立项并给手动装配指引｜协议 v1/v2/v3 协商）
- [x] 5.3 语义修正："Restart = full release" 改单机/集群分立（集群：快照 + 日志恢复，锁不随重启全丢）；Known Limitations 与 Semantics 既有正确内容保留迁移
- [x] 5.4 链接区：Documentation → guide/（用户）+ design/（标注内部材料）；Benchmark 段 → 本地 `BenchmarkMain` 复现 + CI artifacts 指针（不链任何 dated 文件）
- [x] 5.5 CN 版逐节镜像 5.1–5.4；两侧 diff 对账章节齐平

## 6. 收口

- [x] 6.1 死链与残留检查：`git grep` 旧路径清零 + 简单 markdown 链接可达性脚本过
- [x] 6.2 `mvn -s /home/lam/repo/settings.xml clean verify` 全绿
- [x] 6.3 落点端到端：全 reactor `-Pdrill` 真实执行（四轨 Skipped=0、BUILD SUCCESS），报告落 `target/drill-reports/`×2 模块、演练期间 git status 零 docs 变更；CI drill job 以新 glob 首验随推送后的 workflow_dispatch 对账（ci-first-runs.md）
- [x] 6.4 对账：与 ①（注释句路径文字）、②（artifact path glob）、④（guide 骨架承接）三 change 的交叉项逐条勾清
