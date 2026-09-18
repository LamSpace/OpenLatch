## Why

ROADMAP 一档首项 `OAtomicLong`（含 Integer/Boolean 形态）尚未启动。它是库内第一个"非锁"原语——复制型可变计数单元，填补"跨进程共享整数/布尔标志 + 无 ABA 的 CAS 协调"的能力空白；版本戳合一（`AtomicStampedReference` 形态）同时是"不做 `StampedLock`/`LongAdder` 分布式化"两笔裁决的正面回应。基建复用度高：条目状态机 + Raft 命令通道 + v4 协议门均有 Latch/Semaphore 判例可循。

## What Changes

- **协议 v4 门**：`protocol_version` 升至 4，握手接受 {1,2,3,4}；新增 `ATOMIC_OP`/`ATOMIC_OP_REPLY` 消息对与 `LockType` 三枚举（`ATOMIC_LONG=7`/`ATOMIC_INTEGER=8`/`ATOMIC_BOOLEAN=9`），v<4 会话发送 ATOMIC 消息 `INVALID_REQUEST` 消息级拒绝、不断连（沿用 v3 判例）。
- **core 新增 `AtomicEntry`**：装 `(kind, value:sint64, version:sint64)`；操作集 `GET/SET/GET_AND_SET/ADD/CAS/CAS_STAMPED`；每次成功写 version 恰 +1、GET 零迁移不推 version；无持有者、无租约、无等待队列；initial 非零主张定型断言（对齐 Semaphore/Latch 判例）；条目一经创建永不回收（`isEmpty()` 恒 false）。
- **复制路径**：新增 `ATOMIC_OP_ENTRY` 日志条目类型，读写（含 GET）一律经 Raft 提交后应用，确定性 apply；请求携会话内单调 `op_seq`，每 key 记最近已应用 `op_seq` 实现"超时重发不双加"的单槽去重。
- **快照/恢复/观察三件套**：`SnapshotState` 新增 ATOMIC 条目形态（kind/value/version/opSeq）；ShadowTable 与 `AdminEntryView` 扩展原子字段；控制台展示值与版本戳；指标新增按 kind+结果维度的 ATOMIC 操作计数。
- **客户端**：`OpenLatchClient` 新增 `newAtomicLong/newAtomicInteger/newAtomicBoolean` 工厂；公开接口 `OAtomicLong`（JDK 形六操作）+ stamped 增强形态 `compareAndSetStamped`/`getStamped`（ABA-free 承诺的公开载体）；`accumulateAndGet` 为客户端带版本 CAS 循环（重试上限）。
- **文档与测试**：双语指南新增原子变量章节（降级/增强声明清单成文）；E2E 并发 CAS 矩阵、版本戳单调性断言、kill 进程值不变、快照恢复保真、benchmark 热键基线。

## Capabilities

### New Capabilities

（无——沿用 v3 先例，各面以增量需求落入既有能力。）

### Modified Capabilities

- `wire-protocol`: 协议版本升至 4 与 v4 专属语义门控；ATOMIC 消息对与字段增量（纯增量，既有编号零变更）。
- `lock-server`: 会话握手版本区间 [1,3] → [1,4]。
- `core-lock-engine`: ATOMIC 家族定型（跨家族互拒扩至三→四家族）；`AtomicEntry` 操作语义矩阵、版本戳单调契约、initial 非零主张断言、条目永不回收与会话清理无操作声明。
- `replicated-state-machine`: `ATOMIC_OP_ENTRY` 复制边界（写与 GET 均经多数派提交；`op_seq` 单槽去重的回放确定性）。
- `snapshot-recovery`: 快照内容完整性扩至 ATOMIC 条目（kind/value/version/opSeq 序列化，字段号纯增量）。
- `client-sdk`: `OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean` API 与工厂、无租约无看门狗声明、超时裁决语义、stamped 形态与 `accumulateAndGet` 循环语义。
- `admin-observability`: `ADMIN_KEY_DETAIL`/列表呈现 ATOMIC 条目（kind、值、版本戳、无持有者/等待者形态）。
- `admin-console`: 五页面键详情展示 ATOMIC 条目。
- `metrics-observability`: ATOMIC 操作计数指标；`locks.held` 口径显式排除 ATOMIC（同 LATCH 判例）。
- `user-documentation`: 指南章节覆盖清单纳入原子变量（核心概念、SDK、兼容性 v4、术语表）。

## Impact

- **代码**：`openlatch-protocol`（两个 .proto）、`openlatch-core`（`AtomicEntry`/`CoreEngine` 分派/`CoreStateRestore`/`Outcome`）、`openlatch-server`（`RequestDispatcher` v4 门、`LockStateMachineCore`、`ShadowTable`、admin/metrics）、`openlatch-client`（工厂 + 三接口 + `RemoteAtomic*`）。
- **协议兼容**：v1/v2/v3 既有行为逐项不变；新能力仅 v4 会话可见；不做隐式兼容纪律沿用。
- **测试夹具**：新增 `AtomicGatingTest`/`ClusterAtomicTest` 等；扩 `ClusterSnapshotRecoveryTest`/`ClientChaosIT`/`BenchmarkMain`；公平套件不适用（无等待队列，豁免记录进 tasks）。
- **内存语义变化（需显式声明）**：值为跨会话共享常驻状态——不绑定 `(sessionId, threadId)`，任何会话死亡不回滚值；条目永不回收，key 是无上限资源使用者（maxKeys 护栏兜底）。
- 演进路线一档启动：ROADMAP 状态列随提案合并更新为"进行中"。
