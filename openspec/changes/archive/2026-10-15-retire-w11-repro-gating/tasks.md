# Tasks

> 设计工件按条件**故意跳过**：本 change 无跨模块面、无新依赖、无安全/性能/迁移复杂度、无可两选的技术决策（`design.md` 的 instruction 明示"create only if any apply"），故不产出。判据与理由记录于此，不再回溯。

## 1. 判据集转正（去掉标签）

- [x] 1.1 摘除三处 `@Tag("w11-repro")`——`RerouteConvergenceReproIT`（类级）、`SessionSwitchReproIT`（类级）、`ClientClusterIT#residentNonAuthoritativeHomeConvergesNonAcquireRequests`（方法级）——并清理由此不再使用的 `Tag` 导入。验证=`grep -rn "w11-repro" openlatch-client/src` 零命中。→ 三处标签与三处 `import ...Tag;` 均已摘除；残留命中仅为注释里的**历史叙述**（"原以 @Tag("w11-repro") 门控"）
- [x] 1.2 同步三处类/方法注释：不再是"红是预期／默认构建排除／标签与 profile 届时退役"，改为**默认构建内的契约回归网**——写明它守的是哪个面、由哪个 change 的红先判据转来。验证=注释只读自洽，且 `bash scripts/check-source-citations.sh` 零命中（不引内部过程文档/change 名）。→ 三处注释改为"来历与现状"段（守什么契约、谁改回去就红）；citation 门禁零命中（含新注释）

## 2. 构建门控退役

- [x] 2.1 `openlatch-client/pom.xml`：failsafe 的 `<excludedGroups>drill,w11-repro</excludedGroups>` 收窄为 `drill`；删除 `w11-repro` profile 及其注释。验证=默认构建报告里三个测试类的用例确实被执行（IT 计数较转正前 +9），且 `grep -n "w11-repro" openlatch-client/pom.xml` 零命中。→ 默认构建 IT 计数 **89 → 98**（+9）；三个类均出现 `Running ...` 且逐类 `Tests run: 5/3/9` 全绿；pom 内零命中
- [x] 2.2 对读确认 `drill` 门控未被牵连：`-Pdrill` 仍只跑 `@Tag("drill")` 用例，默认构建仍排除 drill。验证=两处配置对读（`excludedGroups` 与 drill profile 的 `groups`）。→ `excludedGroups=drill` 未动、`drill` profile 的 `<groups>drill</groups>` 原样；本 change 只摘掉另一个 profile

## 3. 验证（默认构建与稳定性）

- [x] 3.1 客户端模块默认构建：三处夹具的 9 枚用例全部被采集且全绿。验证=报告逐类 `Tests run` 输出 + 与转正前的 IT 计数差。→ `RerouteConvergenceReproIT` **5/5**（4.858s）、`SessionSwitchReproIT` **3/3**（3.630s）、`ClientClusterIT` **9/9**（51.29s，含转正那枚）；IT 合计 98（+9），0 失败
- [x] 3.2 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` 通过（0 失败 0 错误）。验证=反应堆汇总输出。→ **BUILD SUCCESS**，8/8；客户端 143 单测 + 98 IT（0 失败 1 跳过）
- [x] 3.3 稳定性：客户端 IT 连续 ≥2 轮全绿，**真集群那枚零红**。验证=各轮报告。若该枚在默认负载下不稳 → **回退该枚**（恢复其标签并另立观察项），MUST NOT 为让 CI 变绿而放宽其断言。→ 第 1 轮＝全反应堆内的 IT（`ClientClusterIT` 9/9，51.29s）；第 2 轮＝客户端 IT 全量重跑（98 全绿，`ClientClusterIT` 9/9，49.97s）。两轮真集群枚均零红，未触发放宽/回退条件
- [x] 3.4 门禁：`bash scripts/check-source-citations.sh` 零命中、`bash scripts/check-links.sh` 零死链。验证=命令退出码。→ 两脚本退出码均 0（零命中、死链 0）

## 4. 治理与记录

- [x] 4.1 `WATCHLIST.md` W11 行：把"`@Tag("w11-repro")` + `-Pw11-repro` 门控（默认构建排除）"改为**已转正**，记明判据集规模（三个类 9 枚）与本次退役动作。验证=行内容与实测一致（含行内数量）；表完整性复检（行 id 集合不变、每行 6 竖线）。→ 行内追加"判据集已转正（2026-10-15）"段（三处类别 + 九枚 + 撤标签/退役 profile）；表复检 17 行、行 id 集合不变、每行 6 竖线
