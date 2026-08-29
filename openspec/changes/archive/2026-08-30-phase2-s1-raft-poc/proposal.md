# Phase 2 / S1：Raft 选型 PoC

## Why

Phase 2 要求将单节点锁服务升级为 Raft 复制组，但概要设计 §5.3 明确"不预先锁定"库选型（Apache Ratis vs SOFAJRaft），详设 §2 只给出了评估框架与判定门槛。S1（P2-01～P2-04）是 S2～S4 全部实现的前置：选错库将污染 `server.raft` 六个类的形态与传输层共存策略，必须在写主干代码前以实测数据定案。

## What Changes

- 新增一次性（throwaway）PoC 工程 `poc/raft-selection/`：独立 Maven reactor，**不加进根 pom `<modules>`**，不进主干构建与发布链路。
- 新增共享测试骨架 `poc-harness`：driver 进程（负载生成、计时、不变式监视、结果落盘）、节点最小行协议、`PocNodeAdapter` SPI、no-raft 伪节点基线模式。
- 新增两个候选适配器原型：`poc-ratis`（Apache Ratis）与 `poc-jraft`（SOFAJRaft），各自把 Phase 1 `CoreEngine` 挂为该库的状态机（`RaftLogEntry` proto 取详设 §4.2 的 PoC 本地拷贝），实现同一最小原型：3 节点本机集群、单键 acquire+release 混合负载 5 分钟、杀 Leader 计时、快照触发 + Follower 重启追赶。
- `CoreEngine` 零改动接入验证：以适配器侧 `EntryClock`（条目携带时刻注入，apply 线程 thread-local 覆盖）达成回放确定化，作为详设 §2.4 门槛"状态机集成：无需重写锁语义代码"的证据。
- 新增选型报告 `docs/raft-selection-report.md`：按详设 §2.4 四门槛逐项实测数据 + §2.2 权重综合评分 + 定案结论；并将结论回写详设修订版（v1.1 §2.1）。
- 明确非目标：QUEUED/AWAIT_NOTIFY 排队路径（Leader 本地行为，与选库无关）、Follower 写转发（ForwardingProxy 属 S3）、协议 v2 扩展（属 S3）。

## Capabilities

### New Capabilities

- `raft-selection-poc`: Raft 库选型 PoC 能力——共享测量骨架的行为要求（负载、计时、不变式、结果格式）、两候选适配器必须达成的同一功能面、四项判定门槛的测量口径与通过判据、选型报告的证据完整性要求。

### Modified Capabilities

（无。本变更为一次性评估工程，不改变 `core-lock-engine` / `lock-server` / `wire-protocol` / `client-sdk` / `spring-boot-starter` 的任何既有行为契约；对主干代码零侵入。）

## Impact

- **代码**：仅新增 `poc/raft-selection/`（独立 reactor）与 `docs/raft-selection-report.md`；主干六模块与 `openlatch-core` 零改动（PoC 依赖 `openlatch-core` 已发布构件或本地 install）。
- **依赖**：PoC 工程引入 `org.apache.ratis:ratis-server`（含 gRPC 传输）与 `com.alipay.sofa:jraft-core`（含 bolt 传输）及 protobuf 生成工具链；版本冲突（protobuf 3.25.5 / Netty 4.1.137 / Java 25）作为侵入度证据记录，不回流主干。
- **文档**：详设说明书回写 v1.1（§2.1 定案、§12 风险 4 关闭）。
- **风险登记**：若 JRaft 在 Java 25 上无法启动，记为门槛外风险项随报告呈报，不自动淘汰（评审定夺）。
