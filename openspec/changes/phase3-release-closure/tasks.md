# Tasks: phase3-release-closure

判定基线：验证命令一律 `mvn -s /home/lam/repo/settings.xml`；本变更为纯收口（零产品代码/协议/API 变更），实现口径按 `design.md` D1–D6。`.openspec.yaml` 设 `skip_specs: true`（无 spec 级行为变更）。

## 1. 基准复核与门禁复跑（先执行，结果供报告引用）

- [x] 1.1 复跑 `BenchmarkMain`（静息态单轮），记录环境信息（OS/CPU/JDK/采样口径，沿 `benchmark-baseline-2026-08-29.md` 格式）
  - verify：`mvn -s /home/lam/repo/settings.xml -pl openlatch-examples -am compile exec:java -Dexec.mainClass=io.github.lamspace.openlatch.examples.BenchmarkMain` 正常退出并输出指标
  - 执行记录（2026-09-10）：`-pl openlatch-examples`（不带 `-am`，exec:java 落根聚合 pom 会 ClassNotFound）；3 场景指标输出正常，`baseline written to docs/benchmark-baseline-2026-09-10.md`
- [x] 1.2 产出 `docs/benchmark-baseline-<date>.md`（环境表 + 指标表，结构对齐 Phase 1 基线）
  - verify：文件存在；场景行与 2026-08-29 基线逐项对齐（无场景缺失）
  - 执行记录：`docs/benchmark-baseline-2026-09-10.md` 落库；三场景（无竞争/16 线程/64 线程）齐备；harness 标题为静态"Phase 1"，落库时按阶段修订
- [x] 1.3 与 `benchmark-baseline-2026-08-29.md` 逐项对比，>20% 退化写明原因（design D3 口径：基准非发布门槛，噪声显著时以量级一致判定并注明）
  - verify：每场景有对比 delta；退化项有归因说明
  - 执行记录：对比段已入 `docs/benchmark-baseline-2026-09-10.md`「与 Phase 1 基线对比」；吞吐 +2.4%～+4.6%、延迟 ±10% 内，无 >20% 退化
- [x] 1.4 全量门禁复跑：`mvn -s /home/lam/repo/settings.xml clean verify` 全 reactor
  - verify：BUILD SUCCESS（记录耗时与模块计数入报告）；`-Pdrill` 按仓库纪律留 CI/运维环境（无 TTY 沙箱不可靠）
  - 执行记录（2026-09-10 22:22:03）：**BUILD SUCCESS（exit 0）**，07:29 min；8 模块全 SUCCESS（server 04:18 / client 02:28 / console 16.5s / starter 11.2s / protocol 7.1s / core 4.7s / examples 1.5s）；结果入 `docs/Phase3-验收报告.md`「构建门禁复跑」

## 2. Phase 3 验收报告（design D2：证据以引用汇总）

- [x] 2.1 建 `docs/Phase3-验收报告.md` 骨架（仿 `Phase2-验收报告.md`：头表 + 逐项闭环 + DoD 自检 + 缺陷/遗留 + 发布宣告尾注）
- [x] 2.2 总体计划 §5.3 Phase 3 六项逐项闭环（判据 / 证据路径 / 结果 ✅）
- [x] 2.3 详设 §8 Phase 3 六项逐项闭环（判据 / 证据路径 / 结果 ✅）——§8 与 §5.3 同轴，以映射表 + §8-6 兼容性专项呈现
- [x] 2.4 DoD §5.4 五条自检（含 1.3 基准对比结论引用）
- [x] 2.5 缺陷与遗留如实段：T4 tasks 3.6 集群夹具口径（D1）、环境限制项（L3 手工/`openssl s_client`/`-Pdrill`）、§9 出阶段遗留
- [x] 2.6 全文证据路径逐条核验可点开存在
  - verify：逐路径 `test -e`（或人工点开清单）全通过；无 ⏳ 悬置项
  - 执行记录：14 条引用路径（4 份 `t*-acceptance-evidence.md`、报告/详设/基线/选型报告、`specs/transport-security/spec.md`）逐条 `test -e` 全 OK

## 3. 详设收口（纯文档，design D4）

- [x] 3.1 `docs/OpenLatch-Phase3-详细设计说明书.md`：版本 v1.0 → v1.1、状态"待评审" → "已验收"、补修订记录条目（指向实现期各章勘误与本次收口）
  - verify：头部表格三处齐改；`git diff --stat` 显示仅触及头表与修订记录区，各章勘误正文零改动
  - 执行记录：`git diff --stat` = 5 insertions(+), 3 deletions(-)，仅头表 + 修订记录段
- [x] 3.2 通读确认无残留"待评审"、无契约语义变更引入
  - verify：`grep '待评审'` 唯一命中在修订记录 v1.0 历史描述内（符合预期）；正文零改动

## 4. README 文档面同步（纯文档，design D5）

- [x] 4.1 "文档"节补 Phase 2/3 详设与验收报告链接（`README.md` / `README_CN.md`）
- [x] 4.2 "已知局限（Phase 1）" 按现状改判：已由 Phase 2/3 交付覆盖的条目（如"单机内存锁、无集群"）移除或重标；仍成立项（读者批量授予、放弃等待不取消、客户端时钟回拨）保留
  - 执行记录：节名去 Phase 1 限定；移除「单机内存锁、无集群」（Phase 2 已交付）；读者批量授予按 Phase 3 评估结论重标（与 FairLock 公平性回归互斥，未采纳，§9-2）；补 Latch 条目存续局限一条
- [x] 4.3 中英双版节名与链接对齐
  - verify：两版对照 diff，节名一一对应；被移除条目确已被 Phase 2/3 交付覆盖（逐条核对）
  - 执行记录：中英节名 `已知局限`/`Known Limitations`、`基准基线`/`Benchmark Baseline`、`文档`/`Documentation` 一一对应，链接六项齐

## 5. T4 记账回填

- [x] 5.1 `openspec/changes/archive/2026-09-10-phase3-t4-security/tasks.md` 的 4.6 补记归档事实（提交 `c9b4313` 已含 archive + specs 同步）后勾选
- [x] 5.2 3.6 改记为"已评估·结构保证 + 单机覆盖，集群夹具另立小变更"（D1），不留空框
  - verify：无未解释的空框；3.6 口径与 `docs/Phase3-验收报告.md` §2.5 表述一致
  - 执行记录：3.6/4.6 均已勾选并附事实；`grep '^\s*- \[ \]'` 于 t4 tasks 零命中

## 6. 归档与发布宣告（design D6）

- [ ] 6.1 验收报告发布宣告尾注填版本/日期/commit
  - verify：尾注字段齐备
- [ ] 6.2 提交并归档本 change：`openspec archive phase3-release-closure`——**归档即 Phase 3 发布宣告**
  - verify：归档完成；git 主干含 `docs/Phase3-验收报告.md`；工作树干净
