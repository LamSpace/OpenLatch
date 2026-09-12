# raft-selection-poc Delta Specification

## REMOVED Requirements

### Requirement: 同一最小原型双候选实现

**Reason**: PoC 使命（jraft vs ratis 选型）已完成，Apache Ratis 定案且 Phase 2/3 收口。
**Migration**: 结论存于 `docs/design/raft-selection-report.md`；原型代码经 tag `archive/raft-selection-poc` 锚定，`git checkout` 该 tag 可取回。

### Requirement: CoreEngine 零改动接入

**Reason**: 同上，PoC 接入纪律随 poc 退役。
**Migration**: 主干集群实现（replicated-state-machine 等 capability）已独立承载该约束。

### Requirement: 门槛一——集群授予延迟 P99

**Reason**: 选型门槛判定已随报告定案封存。
**Migration**: 门槛数据存于 `docs/design/raft-selection-report.md` 与归档 results JSON。

### Requirement: 门槛二——杀 Leader 恢复计时

**Reason**: 选型门槛判定已随报告定案封存。
**Migration**: 同上；常态恢复计时由 cluster-node-lifecycle 的演练体系承载。

### Requirement: 门槛三——快照恢复与追赶

**Reason**: 选型门槛判定已随报告定案封存。
**Migration**: 同上；常态快照能力由 snapshot-recovery capability 承载。

### Requirement: API 侵入度记录

**Reason**: 侵入度对比是选型期一次性档案。
**Migration**: `poc/raft-selection/friction-{ratis,jraft}.md` 随 tag 保留。

### Requirement: 正确性不变式监视

**Reason**: PoC 期监视器使命结束。
**Migration**: 主干混沌/演练测试（ClientChaosIT、各 DrillIT）持续承载双主检测。

### Requirement: 选型报告与定案回写

**Reason**: 定案已回写完成（Phase 2 详设 v1.1 §2），报告为常驻记录。
**Migration**: 报告迁至 `docs/design/raft-selection-report.md`，复现指引改指 tag 锚定（见 documentation-structure）。
