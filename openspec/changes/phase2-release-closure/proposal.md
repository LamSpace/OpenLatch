# Proposal: phase2-release-closure

## Why

S4（`2026-09-03-phase2-s4-snapshot-recovery`）已勾完 Phase 2 全部 19 个子任务，但对照详设 §10/§11、总体实施计划 §5.2/§5.4（DoD）与 `s4-exit-checklist.md` 的"遗留"段逐条核对，收口段仍欠四件事，"Phase 2 发布"（P2-19 验证列）没有对应 artifact：

1. **§11-3 主轨分区证据未采集**：S4 design D7 自设口径"§11-3 证据以主轨为准（netns 真分区），辅轨标近似口径"，而本机 `sudo -n` 无权限致 `PartitionDrillIT` 只验证了跳过路径；D7 的备选裁决（辅轨 + 多数派论证豁免）至今未提交定夺——按既定口径这项验收严格说未闭环，是全部缺口中唯一的硬账。
2. **字面 ≥10 分钟混沌 soak 未跑**：详设 §10 建议"随机杀节点 + 持续负载（≥10 分钟）"，S4 tasks 5.1 按此措辞勾选，实际证据为 ~18s 短租约有界混沌窗口（检查单已如实注记）；`ClientChaosIT` 无时长参数，补跑需一处测试工具改动，tasks 措辞与证据口径存在缝隙。
3. **详设 §13.3 S3 记账缺失**：S1/S2/S4 表均逐项 ✅ + "已完成"段标，唯独 S3 四行未回写（证据链完整：`s3-exit-checklist.md` + 归档 + 演练报告 4/4），纯记账遗漏；文档状态行仍挂"待评审"。
4. **验收报告与发布宣告未入库**：Phase 1 有 `docs/Phase1-验收报告.md` 先例；Phase 2 的 §5.2 七项无逐项汇总证据文档、§5.4 DoD 五条无自检记录、基准数据散落四份检查单未汇总。

## What Changes

- **主轨分区演练（证据采集，零代码改动）**：按特权获取级联执行 `PartitionDrillIT`（netns 桥接拓扑、iptables 双向隔离少数派、`docs/partition-drill-*.md` 报告产出，格式沿 `failover-drill-2026-08-31.md`，失败轮次如实入库）。级联：宿主 sudoers drop-in（仅 `/usr/sbin/ip`、`/usr/sbin/iptables` 两行 NOPASSWD）→ 不可行则 docker `--privileged` 容器 → 均穷尽才走 D7 备选裁决（以"辅轨 + 多数派论证"豁免，理由记入验收报告，不静默降级）。
- **混沌 soak 参数化 + 补跑**：`ClientChaosIT` 新增 `-Dopenlatch.chaos.soak-minutes` 系统属性；缺省（未设）维持现行 ~18s 回归语义逐字节不变，设 `10` 执行字面 ≥10 分钟持续负载 + 不变式检查；静息环境单轮执行，结果（含时长与不变式判定）入库验收报告。
- **详设 v1.5 记账回写（纯文档）**：§13.3 四行补 ✅ 与证据链接、段标补"（已完成）"；§11 第 3 项与 §13.4 P2-18 行按主轨结果收口（主轨 ✅ 归档，或豁免裁决记录在案）；混沌行补 10 分钟 soak 数据引用；文档状态行"待评审"按实际评审结果更新。
- **《Phase 2 验收报告》**：`docs/Phase2-验收报告.md` 仿 Phase 1 结构——§5.2 七项逐项勾对（每项判据 + 证据路径 + 结果）、§5.4 DoD 五条自检、基准数据汇总（杀主 1621–1806ms / 快照 521ms·4.48MB / 滚动重启 0.80%·0.00% / soak 结果 / 分区主轨结果）、遗留如实记录（R2 离群轮观察等）；报告合入即 Phase 2 发布宣告。
- **缺陷应对预案（执行中已触发两次）**：
  - ① soak 首跑（3.1）暴露 P0 关停挂起（Ratis cached proxy 池零 worker 时 fire-and-forget 关停派发永不执行，空闲节点优雅停机必中，取证 `observations-ratis-3.3.0-soak-shutdown-hang.md`）——已撤销 `skip_specs`、补 delta spec、装配钉池修复并双验证（决策 D8）；
  - ② 6.1 第二腿捕获**存量** P1 leader 复制停摆（先主后从滚动重启双稳态：新 leader NOOP 永不提交、写门闸 200+ 秒不自愈；差分实验证明与修复 ① 无因果，S4"24.22% 瞬态离群"定判据随之更正，档案 `defects/leader-replication-stall-ratis-3.3.0.md`）——本 change 内不修复、按 DoD"P1 在案+跟进计划"处置，§11-5 验收行按实记录，发布与否为显式定夺（决策 D9）。

无 **BREAKING**：协议、公开 API、测试缺省语义均不动；产品运行时行为变化仅关停路径一处（D8 修复：由"可能永久挂起"变为"有界完成"，属缺陷修复非语义变更）。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

- `cluster-node-lifecycle`：「Raft 子系统生命周期绑定」增补"优雅关停有界且不依赖库线程池调度状态（装配钉死非缓存池）"条款，新增"长空闲后优雅关停有界"场景（D8）。

## Impact

- **产品代码**：`openlatch-server/.../raft/RaftSubsystem.java`（start() 钉死 proxy/server/client 线程池为非缓存固定池，D8）。
- **测试工具**：`openlatch-client/src/test/java/.../client/ClientChaosIT.java`（+soak 时长开关）；新增 `IdleNodeGracefulStopIT.java`（关停回归，drill 档）（均含 Javadoc 同步，CLAUDE.md §5）。
- **文档**：`docs/OpenLatch-Phase2-详细设计说明书.md`（v1.5）、`docs/Phase2-验收报告.md`（新增）、`docs/partition-drill-*.md`（演练产出）。
- **环境特权面**：宿主 `/etc/sudoers.d/openlatch-drill` drop-in（仅 `ip`/`iptables`；用户一次性交互授权）或 docker 特权容器（二选一，见 design D1）；演练自身的 netns/网桥/iptables 操作随 `teardownTopology()` 清理。
- **不受影响**：`openlatch-core`/`openlatch-server`/`openlatch-protocol` 全部产物、既有演练用例、CI 默认门禁（`-Pdrill` 仍为辅轨 profile，`PartitionDrillIT` 无权限时仍显式跳过）。
