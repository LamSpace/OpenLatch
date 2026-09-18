## 1. 协议层（openlatch-protocol）

- [x] 1.1 `openlatch.proto`：`MessageType` 加 `ATOMIC_OP = 14`；`LockType` 加 `ATOMIC_LONG=7`/`ATOMIC_INTEGER=8`/`ATOMIC_BOOLEAN=9`；新增 `AtomicOp` 枚举与 `AtomicOpRequest`/`AtomicOpResponse` 消息；`Envelope.payload` 加 32/33 两槽（字段编号与 specs/wire-protocol 增量逐项一致）
- [x] 1.2 协议模块回归：既有字段编号/枚举值零变更断言（golden 契约冻结测试已扩至 v4 面，RED→GREEN 验证）；AtomicOp 编解码往返由既有 ProtocolCodecTest 全枚举形态覆盖
- [x] 1.3 握手版本常量与协商面升至 4（`OpenLatchServer.PROTOCOL_VERSION`、客户端 `ConnectionManager`/`RequestMultiplexer` 三处；`HandshakeTest` 新增 v4 接受用例 + 越界用例改 5，RED→GREEN）。v3 客户端×新服务端=全量既有测试（握手 ≤3）回归绿；新客户端×旧服务端不可在单仓实例化（旧 jar 不共存），以指南兼容矩阵注记承担

## 2. core 状态机（openlatch-core）

- [x] 2.1 新建 `AtomicEntry implements KeyEntry`：`(kind, value, version, initial, 去重槽(session,opSeq,应答四元组))`，六操作 `synchronized` 实现；`KeyFamily` 加 `ATOMIC`；`forceExpire` 抛 `IllegalStateException`、`removeSession` 无操作、`isEmpty()` 恒 false——逐方法 Javadoc 写明"值不绑定 (session,threadId)"契约
- [x] 2.2 `CoreEngine`：`familyOf`/`newEntry` 扩 ATOMIC 分支；`atomicOp(AtomicOpCommand)` 门面（会话校验但不 `touchIfPresent` 登记；key 校验、家族/形态定型 `REJECT_TYPE_MISMATCH`、`REJECT_ATOMIC_INIT`、int32 wrap 与布尔值域检查、GET 不建条目回 (0,0)、同槽重放）；`Outcome` 加 `REJECT_ATOMIC_INIT` 并更新 `Outcome` 类注释返回步骤
- [x] 2.3 `AtomicEntryTest`：操作矩阵逐格（六操作 × 成功/失败/初值主张/版本断言/溢出 wrap/布尔越界），版本戳恰 +1 与 GET 零迁移断言，同 `op_seq` 重发不双加与不重推版本，`CoreEngineAtomicTest` 跨家族互拒与 `sessionClosed` 不触值
- [x] 2.4 `CoreInspection`/明细只读观察面扩 ATOMIC 读数（kind/value/version/initial/去重槽占用）

## 3. 复制路径（openlatch-server/raft）

- [x] 3.1 `raft.proto`：条目类型加 `ATOMIC_OP_ENTRY`（编号纯增量）；载荷为序列化 `AtomicOpRequest` + session
- [x] 3.2 `LockStateMachineCore.applyAtomicOp`：解析→`CoreEngine.atomicOp`→`ApplyResult` 携带应答四元组 + `op` 回显；GET 条目 apply 零迁移；`ReplicationGateway` 回执挂接（无 notifyHead 路径）
- [x] 3.3 `ShadowTable`：`SLock`/`AdminEntryView` 扩 ATOMIC 字段与登记点；无租约不入到期堆断言
- [x] 3.4 应用确定性测试：同命令序列 Leader/新副本回放终态逐字节一致（摘要相等）；含 GET 条目日志全量回放零迁移摘要断言；未知/非法 ATOMIC 条目 error 路径

## 4. 单机分发与门控（openlatch-server/dispatch）

- [x] 4.1 `RequestDispatcher`：`ATOMIC_OP` 消息路由（单机直调引擎、集群经 `SessionCoordinator`/gateway 提交）；v4 门（`session.protocolVersion() < 4` → `INVALID_REQUEST` 消息级拒绝、不断连，判例对齐 LATCH 门）；ACQUIRE 携带 ATOMIC `lock_type` 的消息合法性拒绝
- [x] 4.2 结果→状态码映射：`REJECT_ATOMIC_INIT`/`REJECT_TYPE_MISMATCH`/形态越界 → `INVALID_REQUEST`；`REJECT_SESSION` → `SESSION_EXPIRED`；CAS 类失败 = `OK + applied=false`
- [x] 4.3 `AtomicGatingTest`（判例 `LatchGatingTest`）：v<4 拒绝不断连、v3 既有能力回归不变、ACQUIRE 携原子类型被拒；`MessageLegalityTest` 扩 ATOMIC 请求合法性矩阵
- [x] 4.4 指标：`ServerMetrics` 注册 `openlatch.server.atomic.total`（kind/op/status 维度，单一命名点）；`locks.held` 排除 ATOMIC；`ServerMetricsVocabularyTest` 扩表至十一项

## 5. 快照与恢复（openlatch-server/snapshot）

- [x] 5.1 `SnapshotState` 新增 ATOMIC 条目形态（key、kind、initial、value、version、去重槽三元组；字段号纯增量）与 `CoreStateRestore` 双向序列化/重建工厂（`AtomicEntry.restored(...)`）
- [x] 5.2 恢复测试：快照→重启→后续日志回放的 ATOMIC 值/版本戳/去重槽逐字段保真；重启后命中槽重发不双加（并入 `ClusterSnapshotRecoveryTest` 或新夹具）

## 6. 管理面与控制台

- [x] 6.1 `AdminRequestHandler`：SUMMARY 单列 ATOMIC 条目数、LIST_KEYS 行（持有者空/租约 0/等待 0）、KEY_DETAIL 形态/初值/值/版本戳
- [x] 6.2 console 五页面扩呈现（列表行、详情卡"无持有语义"占位、概览计数）；`AdminClusterTest`/`AdminProtocolTest` 扩断言

## 7. 客户端 SDK（openlatch-client）

- [x] 7.1 公开接口 `OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean` + `StampedLong` record：JDK 形六操作 + `getStamped`/`getVersion`/`compareAndSetStamped` + `accumulateAndGet`；契约 Javadoc 按 Lock 接口级撰写（降级/增强清单：RTT、超时不确定窗、ABA 仅 stamped 消除、wrap 域、条目不回收）
- [x] 7.2 `RemoteAtomicLong`（+int/bool 形态实现）：请求构造与 `op_seq` 会话内单调分配；同 key 在途写互斥；可重试失败（`NOT_LEADER`/断连/超时）同 `op_seq` 自动重发至总时限，界限内不可判定抛 `OpenLatchTimeoutException`；`accumulateAndGet` 有界 CAS_STAMPED 循环
- [x] 7.3 `OpenLatchClient` 三工厂方法（含非零初值主张重载）+ 握手版本声明升 4；无看门狗/租约/`LockLost` 路径接入断言
- [x] 7.4 客户端单测：工厂返回类型、参数校验（key 空/初值主张越界）、`accumulateAndGet` 循环收敛与界限抛出

## 8. 集群 E2E 与混沌

- [x] 8.1 `ClusterAtomicTest`（判例 `ClusterSemaphoreLatchTest`）：多客户端并发 CAS 矩阵——Σ成功 CAS == Δvalue、版本戳终值 == 成功写次数且无间隙、Integer wrap/Boolean 值域终态、`accumulateAndGet` 双端并发求和
- [x] 8.2 kill 进程裁决 E2E：持有写入的会话进程被杀 → 值纹丝不动（"不绑定归属"的可执行证明）；以专用场景落地：`ClusterAtomicTest.committedValueSurvivesLeaderKillAndRestart`（协议层换主锚）+ `ClientClusterIT.committedAtomicValueSurvivesLeaderKillWithoutDoubleApply`（SDK 层不丢不双加锚）——`ClientChaosIT` 随机混合负载本尊不改（其面向锁路径，原子针对性场景独立成测更稳）
- [x] 8.3 兼容矩阵 E2E：新客户端×v3 服务端明确失败、v3 客户端×新服务端全量回归绿
- [x] 8.4 公平套件豁免记录（本条即豁免正文）：`FairOrderingSuite*`（单机/集群两形态）对 ATOMIC 家族**不适用**——原子条目无等待队列、无挂起、无队首通知（全部操作即时线性化应答），FIFO 公平性语义无对应物可测；此系设计使然而非缺漏（design.md D7"Leader 队列驱动零触碰"、client-sdk 规格"MUST NOT 进入等待-通知-重发闭环"）。混沌/演练对齐以专用场景承担（`ClusterAtomicTest` 杀主重启锚、`ClientClusterIT` SDK 换主锚）；ROADMAP 落地纪律第 5 条据此勾选注记，归档时同步状态列

## 9. 基准与文档

- [x] 9.1 `BenchmarkMain` 扩原子相：热键 ADD 吞吐、CAS 争用放大、GET 延迟基线（运行 `-pl` 不带 `-am`，报告标题按阶段手改）
- [x] 9.2 双语指南：核心概念"原子变量与版本戳"节、SDK 章节三接口用法与语义降级/增强声明清单、兼容性 v1–v4、术语表（版本戳/去重槽/初值主张）、回滚窗口运维注记；zh/en 对称
- [x] 9.3 ROADMAP.md 本行状态 → 已落地（归档时）前先行更新为"进行中"并附提案链接；同 key 同类型约定登记（ATOMIC 家族行）
- [x] 9.4 全量验证：`mvn -s /home/lam/repo/settings.xml clean verify` 全绿 + `bash scripts/check-source-citations.sh --selftest` 后自查零命中
