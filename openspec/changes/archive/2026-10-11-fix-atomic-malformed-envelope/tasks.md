# Tasks

## 1. 红先：确定性复现

- [x] 1.1 新增脚本化夹具（`ScriptedServer`）：对 `ATOMIC_OP` 回一个 **异型应答信封**（`type` 回显 `ATOMIC_OP` 而载荷 oneof 缺 `atomic_op_response`）；断言 `addAndGet(5)` **不静默返回 0**——修前应红（当前读默认实例返回 0）。验证=未改实现前该用例**红**，且失败原因是"返回了 0"而非其他。
- [x] 1.2 同型夹具覆盖**有值引用族**（`OAtomicReference`）：应答缺 `atomic_op_response`；断言不静默交付默认值。验证=未改实现前**红**。
- [x] 1.3 有界收口侧：夹具持续回异型应答；断言最终以**显式异常**（超时口径）收场，MUST NOT 静默返回。验证=绿后该断言成立，且耗时受请求预算约束。

## 2. 实现：两处 presence 判定

- [x] 2.1 标量族：应答读取前增 `!answer.hasAtomicOpResponse()` 判定 → 缺码形按可重试失败处置（`continue`，同信封同 `op_seq` 重发）；同步方法级 Javadoc，写明"不得读默认实例"与跨会话守卫不放宽。验证=1.1、1.3 由红转绿；既有 `RemoteAtomicFactoryTest` 与原子族 IT 全绿。
- [x] 2.2 有值引用族同款（结构逐字对称，一并覆盖以免只修一半）；Javadoc 同步。验证=1.2 由红转绿。

## 3. 契约与文档

- [x] 3.1 接口级 Javadoc：`OAtomicLong` 族与 `OAtomicReference` 声明缺码形处置（按可重试失败重发、不静默、预算内不可判定则显式异常）。验证=`mvn -s /home/lam/repo/settings.xml -pl openlatch-client javadoc:javadoc` 通过（`show=private`）。
- [x] 3.2 双语排障指南：`docs/guide/{zh,en}/09-troubleshooting.md` 增一条排查项——"原子操作疑似静默错值（如 `addAndGet` 返回 0）"→ 该族现按缺码形重发、不再静默；并给出复核手段（`getStamped()`）。验证=中英双侧对应段落均已更新、语义一致。

## 4. 集成回归

- [x] 4.1 客户端全量套件（单测 + IT）复跑。验证=零失败、无 flaky。
- [x] 4.2 跨模块收口：全仓 `clean verify` 与引用门禁自查。验证=`mvn -s /home/lam/repo/settings.xml clean verify` 通过，`bash scripts/check-source-citations.sh` 零命中。
