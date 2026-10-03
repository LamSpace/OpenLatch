# Proposal

## Why

ROADMAP 二档（小载荷）的首个原语：在既有标量原子形态之上有值引用形态（`OAtomicReference`，对应 JDK `AtomicReference`），并随之开辟二档全部原语共用的**载荷通道基建**——proto 载荷字段对、`maxValueBytes` 全局钳制（默认 4KB、服务端裁决）、快照尺寸治理回归断言、控制台载荷截断展示。2026-09-16 定位裁决已采纳"允许小载荷协调数据结构"为能力边界，本 change 是该裁决的首个落地项；二档后续原语（`OBlockingQueue`/`OTopic`）直接复用本基建。

## What Changes

- 协议升至 **v6 门**：`LockType` 新增 `LOCK_TYPE_ATOMIC_REFERENCE = 11` 形态判别；`AtomicOpRequest`/`AtomicOpResponse` 以 `optional bytes` 显式 presence 字段增量承载载荷（operand/expected/initial 与 old_value/value 回执），复用 ATOMIC_OP 消息对与 `ATOMIC_OP_ENTRY` 复制条目——对落地纪律第 1 条"proto 消息对"作有据偏离（裁决与理由记入 design，OBlockingQueue/OTopic 仍自带消息对）。
- 载荷语义：不透明字节串，服务端 MUST NOT 反序列化/解释；null 与空字节串双形态可表达（显式 presence）；判定顺序、版本戳、同 `(session, op_seq)` 去重槽与标量形态逐项同构；**无 ADD/accumulate**（携带即值域拒绝）。
- 新增 `maxValueBytes` 钳制配置（`openlatch.server.limit.max-value-bytes`，默认 4096）：判定**仅在 Leader 入口/单机分发入口**执行，超限回 `INVALID_REQUEST` 且 MUST NOT 产生日志条目；引擎/条目侧不复核（节点本地配置参与 apply 判定会引入回放分歧）。钳制下调不追溯存量值。
- core 新建 `AtomicRefEntry`（复用机制而非扩类，判例 Barrier）：`KeyFamily.ATOMIC` 家族位，值域换 `byte[]`+null；同 key 跨形态（标量键上 ref 请求及反向）由既有类型互拒规则拦截。
- 复制路径：`applyAtomicOp` 按形态分派；`ApplyResult` 增量 bytes 回执字段（编号纯增量）；快照 `SnapshotLock` 增量 ref 字段与去重槽 bytes 化（`CoreStateRestore` 对应扩展），旧快照（无 ref 字段）加载兼容。
- 管理面/控制台/指标：`AdminKeyInfo`/`AdminKeyDetailResponse` 增量载荷大小与**截断预览**字段（观察面不做成载荷放大器）；`openlatch.server.atomic.total` kind 词表增 `reference`；SUMMARY 计数归入 `atomic_entries`。
- 客户端 SDK：公开 `OAtomicReference` 接口（`byte[]` 主形态 + UTF-8 `String` 便利形态）、`RemoteAtomicReference` 实现（op_seq 分配与可重试同序号重发继承 `RemoteAtomicBase` 现成车道）、`OpenLatchClient.newAtomicReference` 工厂（含初值主张重载）；握手版本声明升至 6。
- v6 门控：`session.protocolVersion() < 6` 携带 ref 形态 → 消息级 `INVALID_REQUEST` 拒绝、不断连（判例 LATCH v3 门/ATOMIC v4 门）；v1–v5 既有能力行为不变。
- 快照膨胀治理回归：新增"N 个 ref key × 恰限载荷 → 快照尺寸落界 + 安装后逐字段保真 + 追赶回放正常"的断言夹具，配合既有 `snapshot-threshold`/保留份数/1MiB 帧上限构成治理面。
- 双语指南、契约 Javadoc、ROADMAP 状态行与同 key 同类型约定登记（ATOMIC 家族含 REFERENCE 互斥行）。

## Capabilities

### New Capabilities

（无——全部落在既有能力面上。）

### Modified Capabilities

- `wire-protocol`: v6 门与握手常量；`LOCK_TYPE_ATOMIC_REFERENCE` 判别、ATOMIC 消息对 `optional bytes` 载荷字段对、形状互斥矩阵（ref 禁 ADD/标量槽位须空、非 ref 禁携 bytes）；`maxValueBytes` 入口钳制与超限 `INVALID_REQUEST` 且零日志产生；已发布编号冻结纪律延伸。
- `core-lock-engine`: `AtomicRefEntry` 状态机（值域 `byte[]`+null、判定顺序同构、去重槽 bytes 化应答、无 ADD 值域拒绝）；`CoreEngine` 原子门面按形态分派；同 key 跨形态互拒；值不绑定会话归属契约延伸。
- `replicated-state-machine`: `ATOMIC_OP_ENTRY` 承载 ref 形态命令与 apply 分派；`ApplyResult` bytes 回执字段；回放确定性与 digest 可比性含载荷；条目侧不复核 `maxValueBytes`。
- `snapshot-recovery`: `SnapshotLock` ref 字段与去重槽 bytes 化（编号纯增量）；`CoreStateRestore` 扩展与重建工厂；旧快照（无 ref 字段）加载兼容；快照尺寸有界回归。
- `lock-server`: 单机/集群两路 ATOMIC_OP 分发的 v6 门与入口钳制接线（超限不入日志）；结果→状态码映射延伸；ShadowTable 登记点扩展。
- `client-sdk`: `OAtomicReference` 公开契约（降级/增强清单：RTT、超时不确定窗、ABA 仅 stamped、载荷不透明、null/空串形态、服务端权威钳制、条目不回收）；工厂方法与 String 便利形态；握手升 6；同序号重发幂等。
- `admin-console`: 五页面 ref 行呈现（kind=reference、大小+截断预览）。
- `admin-observability`: `AdminKeyInfo`/`AdminKeyDetailResponse` 载荷大小与截断预览字段契约（预览转义规则、上限恒定不随配置膨胀）。
- `metrics-observability`: `openlatch.server.atomic.total` kind 词表增 `reference`（单一命名点）；`atomic_entries` 口径含 ref。
- `user-documentation`: 双语指南小载荷边界与 `OAtomicReference` 用法、v1–v6 兼容矩阵、术语表、快照尺寸治理运维注记。

## Impact

- **openlatch-protocol**：`openlatch.proto`/`raft.proto` 纯增量字段与枚举值；两份契约冻结测试扩至 v6 面；`ProtocolCodecTest` 载荷往返。
- **openlatch-core**：`LockType` 增形态枚举值、`AtomicRefEntry` 新类、`CoreEngine` 原子门面分派、`CoreStateRestore` 扩展、`Outcome` 注释口径更新（复用既有拒绝码，无新增 Outcome）。
- **openlatch-server**：`OpenLatchServer.PROTOCOL_VERSION` 5→6；`ServerConfig` 新配置项与校验；`RequestDispatcher`/`ClusterRequestHandler` 门控+钳制；`LockStateMachineCore` apply 分派；`ShadowTable`/`AdminRequestHandler`/`ServerMetrics` 扩展；快照生成/加载两侧。
- **openlatch-client**：`OAtomicReference`/`RemoteAtomicReference`/工厂；`ConnectionManager`/`RequestMultiplexer` 握手常量。
- **openlatch-console**：载荷预览呈现。
- **测试与基准**：core 单测、gating、确定性、快照往返、钳制边界、集群 E2E、客户端 IT、公平套件豁免注记、`BenchmarkMain` ref 相。
- **文档**：`docs/guide` zh/en 对称扩章；ROADMAP 状态行；契约 Javadoc（`check-source-citations.sh` 门禁）。
- **发布面**：protocol/client/starter 为 Maven Central 构件（1.0.0 已发布），本 change 仅协议向后增量、不动版本号与发布流程；无 **BREAKING** 变更。
