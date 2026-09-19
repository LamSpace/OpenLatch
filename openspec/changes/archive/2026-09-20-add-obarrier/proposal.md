## Why

ROADMAP 一档末项 `OBarrier`（对应 JDK `CyclicBarrier`）尚未启动。库内既有 `OCountDownLatch` 是一次性倒计数屏障——归零即终结、不可重组，无法覆盖"多方分阶段对齐、屏障反复复用"这一 JDK 并发编程的核心场景。`LatchEntry` 的等待队列、`AWAIT_NOTIFY` 推送桥、队满护栏与 v3/v4 两代协议门判例使机制复用度高；而"可复用相位"与"参与者死亡即时破障"两处新契约需要独立的世代状态机承载，故以新条目类与新家族落地（复用机制、不复用类，详见 design D1）。

## What Changes

- **协议 v5 门**：`protocol_version` 升至 5，握手接受 {1,2,3,4,5}；新增 `BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` 三组消息对与 `LockType.LOCK_TYPE_BARRIER=10`，v<5 会话发送 BARRIER 消息 `INVALID_REQUEST` 消息级拒绝、不断连（沿用 v3/v4 判例）。既有 v1–v4 行为逐项不变，无 **BREAKING**。
- **core 新增 `BarrierEntry` + `KeyFamily.BARRIER`**：装 `(parties, generation, arrived, awaiters, 了结记录, actionPendingOn)`；`await` 到场计次、集齐即 trip 并世代回卷可复用；每 key 世代号单调递增，应答回显世代，等待者重发以 `(会话, 请求)` 去重命中世代了结记录（钉死"旧世代重发被误认作新世代到场"竞态）；barrierAction 两阶段：最后到场者收"执行者"标记、本地执行动作后经 `BARRIER_ACTION_DONE` 提交，服务端方才放行其余等待者（忠实 JDK"动作完成前全体关闭"承诺）；**离场即破障**——在队等待者会话死亡、`await` 超时/中断、动作执行者死亡、显式 `breakBarrier()` 四条路径统一为当前世代即时 BROKEN（死亡即时破障增强于 JDK 的静默挂起；破障为世代局部，下一批到场自然开新世代，不提供 `reset()`）。
- **复制路径**：新增 `BARRIER_AWAIT_ENTRY`/`BARRIER_LEAVE_ENTRY`/`BARRIER_ACTION_DONE_ENTRY` 日志条目类型，一律经 Raft 提交后确定性应用；到场次序由 apply 序唯一定型，"谁是最后到场者"跨副本一致。
- **快照/恢复/观察三件套**：`SnapshotState` 新增 BARRIER 条目形态（parties/generation/arrived/等待队列/了结记录/执行者挂账）；ShadowTable 与 `AdminEntryView` 扩展屏障字段；控制台展示 parties、世代、剩余到场数与破障态；指标新增按结果维度的 BARRIER 操作计数。
- **客户端**：`OpenLatchClient` 新增 `newBarrier(key, parties)` 与携 `Runnable barrierAction` 重载；公开接口 `OBarrier`：`await()`、`await(timeout, unit)`、`breakBarrier()`、`isBroken()`（句柄本地最近所见读数，不发网络查询）、`getParties()`；`await(timeout)` 超时以 `BARRIER_LEAVE` 离场并即时破障当前世代（对齐 JDK"超时致全体 BrokenBarrierException"的强保证；离场即破障为统一契约，中断与显式 `breakBarrier()` 同径）；新增公开异常 `OBrokenBarrierException`；不提供 `reset()`（世代自动回卷覆盖其用途，契约注释显式声明）。
- **文档与测试**：双语指南新增循环屏障章节（增强/降级声明清单成文：死亡即时破障为增强、破障世代局部为语义差异、RTT 与无租约声明）；E2E 到场矩阵、相位复用重组、kill 会话破障、快照恢复保真；公平套件豁免（判例 `add-oatomic-long` tasks 8.4 形态）。
- **ROADMAP 勘正**：落地纪律第 1 条"下一个门为 v4"已被原子变量提案消耗，本提案随带更新为 v5 并将 OBarrier 行状态置"进行中"附提案链接。

## Capabilities

### New Capabilities

（无——沿用 v3/v4 先例，各面以增量需求落入既有能力。）

### Modified Capabilities

- `wire-protocol`: 协议版本升至 5 与 v5 专属语义门控；BARRIER 三组消息对与字段增量（纯增量，既有编号零变更）。
- `lock-server`: 会话握手版本区间 [1,4] → [1,5]。
- `core-lock-engine`: BARRIER 家族定型（跨家族互拒扩至四→五家族）；`BarrierEntry` 世代状态机、parties 非零主张断言、了结记录重发了结契约、死亡破障触发面、LEAVE 与自动破障判定、队列满护栏沿用。
- `replicated-state-machine`: BARRIER 三命令复制边界（trip/破障/动作了结的 apply 确定性；到场者身份与最后到场者判定由 apply 序承载）。
- `snapshot-recovery`: 快照内容完整性扩至 BARRIER 条目（世代、队列、了结记录、执行者挂账序列化，字段号纯增量）。
- `client-sdk`: `OBarrier` API 与工厂、等待-通知-重发闭环的世代口径、`OBrokenBarrierException` 与超时语义、无租约无看门狗声明、断线重连重放口径。
- `admin-observability`: `ADMIN_KEY_DETAIL`/列表呈现 BARRIER 条目（parties、世代、剩余到场、执行者挂账、无持有者形态）。
- `admin-console`: 五页面键详情展示 BARRIER 条目。
- `metrics-observability`: BARRIER 操作计数指标；`locks.held` 口径显式排除 BARRIER（同 LATCH/ATOMIC 判例）。
- `user-documentation`: 指南章节覆盖清单纳入循环屏障（核心概念、SDK、兼容性 v1–v5、术语表）。

## Impact

- **代码**：`openlatch-protocol`（两个 .proto）、`openlatch-core`（`BarrierEntry`/`CoreEngine` 分派/`CoreStateRestore`/`Outcome`/`KeyFamily`）、`openlatch-server`（`RequestDispatcher` v5 门、`LockStateMachineCore`、`ShadowTable`、admin/metrics、会话摘除→破障接线）、`openlatch-client`（工厂 + `OBarrier`/`RemoteBarrier` + 新异常）。
- **协议兼容**：v1/v2/v3/v4 既有行为逐项不变；新能力仅 v5 会话可见；不做隐式兼容纪律沿用。
- **测试夹具**：新增 `BarrierGatingTest`/`ClusterBarrierTest`/`BarrierEntryTest` 等；扩 `ClusterSnapshotRecoveryTest`/`SnapshotFamilyRoundTripTest`/`StateMachineDeterminismTest`/`ClientChaosIT` 对齐锚/`BenchmarkMain`；公平套件豁免记录进 tasks。
- **内存语义变化（需显式声明）**：破障与回卷为服务端裁决——在队者会话死亡或动作执行者死亡即时打破当前世代（增强：JDK 线程死亡静默无响应）；破障作用域限于当前世代（差异：JDK broken 粘滞至 reset）；barrierAction 执行时长不设服务端上限（对齐 JDK：动作挂起则世代挂起，其死亡由破障兜底）。
- 演进路线一档收尾：ROADMAP 状态列随提案合并更新为"进行中"，归档时 →"已落地"。
