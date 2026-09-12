# Tasks: javadoc-internal-citation-cleanup

判定基线：验证命令一律 `mvn -s /home/lam/repo/settings.xml`（模块级 `-pl <m> -am test`，收口全量 `clean verify`）；"引用清零"的判定即 `bash scripts/check-source-citations.sh` 退出码 0。

## 1. 门禁脚本与模式集

- [x] 1.1 新增 `scripts/check-source-citations.sh`：扫描 `openlatch-*/src/main/**/*.java` 与 `openlatch-*/pom.xml` 的 `<description>`；模式集（ERE）：`设计说明书|概要设计|详细设计|实施计划|验收报告|详设|Phase ?[123]|P[123]-[0-9]{2}|§[0-9]|design(\.md)? ?D[0-9]|spec"`；命中输出 `file:line` 与整行上下文，汇总计数，非零退出 → 脚本内置 `--selftest`：脏夹具（含已知命中行）非零、净夹具零
- [x] 1.2 假阳性预演：对"phase"出现在普通词组（如 "phases of GC"）情形确认不误伤；必要时收紧词边界；模式集定稿记入脚本注释（单一事实源，供 CI 复用）
- [x] 1.3 全仓首跑，命中清单快照存本 change 目录 `citations-baseline.md`（作为清理台账与进度基线）

## 2. src/main 按模块清理

- [x] 2.1 `openlatch-protocol` + `openlatch-core`：逐处清理，每处先判句义——引用删除后语义自洽（重点核对 LockEntry/CoreEngine/Outcome/LeaseManager 的状态机与并发模型段，引用只是出处、本体描述须完整保留）；纯修订跟踪句整句删；"Phase 3 T1 后按家族承载"之类阶段叙事改写为现状陈述 → `-pl openlatch-core,openlatch-protocol -am test` 绿（javadoc 校验在 verify 阶段，此处先编译级）
- [x] 2.2 `openlatch-server` + `openlatch-client`：同纪律（重点 RequestDispatcher/ServerChannelInitializer/OpenLatchServer/ServerSessionHandler；client OLock/OReadWriteLock"Phase 1 不支持持读升级写"→"不支持持读升级写/持写降级读，一律通用排队"）；Watchdog"（概要设计 §6.3）"类直接删括号
- [x] 2.3 `openlatch-spring-boot-starter` + `openlatch-console` + `openlatch-examples`：同纪律（BenchmarkMain 的 `docs/benchmark-baseline-<date>.md` 落盘路径为功能事实保留；"详设 §9"引用删）
- [x] 2.4 7 个模块 pom `<description>` 重写为功能性一句话（例：console→"OpenLatch 管理控制台（只读观察 Web 应用）"；starter→"OpenLatch Spring Boot 4 自动装配与 @OpenLatch 声明式锁"）；`<name>` 不动

## 3. src/test 清理

- [x] 3.1 四个 DrillIT 与 SmokeIT 等测试类中内部流程叙事改写（"供验收报告标准 5 复核改判引用""P2-14 S3 退出门"→ 直陈判据语义）；报告落盘路径句保留（入库形态变更属 docs-restructure-and-report-retirement，本任务不动）；`src/test` 不进门禁范围，以清单清扫为准
- [x] 3.2 `git grep -E` 全仓 `src/test` 复核：除报告路径与夹具 README 外无文档代号残留

## 4. 收口

- [x] 4.1 `bash scripts/check-source-citations.sh` 退出 0（含 `--selftest`）
- [x] 4.2 `mvn -s /home/lam/repo/settings.xml clean verify` 全绿（doclint=all + failOnWarnings=true 背书 javadoc 完整性）
- [x] 4.3 CLAUDE.md §5 增补：对外源码注释与 pom description 禁止引用内部过程文档；门禁脚本路径与用法一句话
- [x] 4.4 抽查评审：从 `citations-baseline.md` 随机抽 30 处，逐条核对清理前后语义无损（人工介入项，评审人确认后本变更出阶段）
