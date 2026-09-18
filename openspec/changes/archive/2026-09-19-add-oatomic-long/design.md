## Context

动机见 proposal.md — Why；各面行为规范见 specs/ 十份增量。设计层面的现状约束：

- 库内既有三家族（LOCK/SEMAPHORE/LATCH）的条目全部围绕"持有者—租约—等待队列"生命周期建模（`KeyEntry` 契约），ATOMIC 是第一个无此三要素的家族——契约的多数为"空实现/不可达"，实现时须逐方法声明而非静默空转；
- 客户端面无任何纯读 RPC，状态读数一律搭命令回执便车（Latch 剩余计数经 countDown 应答读出）——`GET` 是破例者；
- Raft apply 确定性纪律（回放同函数同终态）与"应答由 apply 导出"的既有模式（`ApplyResult`）是本设计可依赖的骨架；
- v3 消息门（`RequestDispatcher` 握版本判定、拒绝码线路可见）与 Latch 专属消息对（不挤占 ACQUIRE）是判例来源。

## Goals / Non-Goals

**Goals:**
- 三形态原子变量在单机与集群两部署形态下语义逐项一致（同一 core 状态机，无分叉）；
- 写操作在"超时重发"这一实际最常见故障窗内不双加、不双 CAS——由协议内去重槽而非调用方义务保证；
- 版本戳能力贯穿到用户面（`getStamped`/`compareAndSetStamped`），ABA-free 承诺可被用户实际兑现；
- 全部新增对 v1–v3 客户端零扰动（纯增量协议 + 门控）。

**Non-Goals:**
- 不做载荷通道/`maxValueBytes` 治理（二档前提项，随 `OAtomicReference` 落地）；
- 不做 follower 读、read-index、批处理管道等读性能路径（GET 走 Raft 后若日志增长成为实际压力，作为被动观察项登记在册再评估）；
- 不做 `REMOVE`/条目回收操作（见 D4）；
- 不提供 `compareAndExchange*`/`weakCompareAndSet*` 家族（JDK 面全量对齐非本季目标）；
- spring-boot-starter 无声明式原子注解（`@OpenLatch` 语义为互斥获取，与原子操作无对应物）。

## Decisions

### D1：GET 走 Raft「零迁移」条目，不做 Leader 本地读

`ATOMIC_OP_ENTRY(op=GET)` 与写操作同路径提交，apply 时只读条目状态、不触碰值/版本戳/去重槽。备选「Leader 直读 ShadowTable + 任期守卫」省日志但引入换主窗陈旧读裁决——需要给 `LeaderTracker` 加读安全判定，属新机制；本库单写者模型下正确性优先，读放大留给演进。契约后果（GET 的线性一致性免费获得）与日志后果（读多写少键型膨胀）同时写进指南。

### D2：`op_seq` 单槽去重——每 key 记最近已应用写操作的 `(session, op_seq, 应答四元组)`

超时重发不双加的最小充分机制：客户端为每会话每 key 维护单调 `op_seq`，写请求必携；命中同槽直接重放存储的应答（版本戳不重复推进）。应答四元组入槽且进快照（见 D6），保证重启/换主后重发仍可判。备选「滑动窗口去重」能挡更早的重发，但客户端超时重发只发生在"最近一次"，单槽已覆盖实际故障窗，窗口化属推测性复杂度。**已知边界（契约声明）**：同会话对同 key 在重发前又成功发出新 `op_seq` 写操作后，旧请求的迟到重发不被去重、会再次生效——SDK 保证在途写操作互斥（同 key 同时至多一个在途写），该边界对用户不可达，仅裸协议客户端需自知。

### D3：形态判别用 `LockType` 三值，而非独立 kind 枚举

与"LockType 判别即家族定型来源"既有模式对齐（`familyOf(lockType)` 单 switch 扩展点）；三形态共享一个 `ATOMIC_OP` 消息对与一个 `AtomicEntry` 类（`kind` 字段决定值域检查与 int32 wrap 掩码），比三条消息对/三个条目类省一整个维度。备选「独立 AtomicKind 枚举」被否：`LockType` 已是家族判别的事实源，平行第二枚举制造 `familyOf` 双头。

### D4：条目永不回收，`isEmpty()` 恒 false

判例 Latch（归零条目存续承载一次性护栏）。ATOMIC 无任何天然"空"点——0 值是合法状态而非可回收标志；删除会引入"删后 GET 回 0"的复活竞态，且与"值不绑定会话"叠加后无任何安全删除时机。key churn 场景的泄漏面由既有 `maxKeys` 限额护栏兜底，指南显式声明"原子 key 是无上限资源使用者"。

### D5：公开接口面 = JDK 形六操作 + stamped 增强三件（`getStamped`/`getVersion`/`compareAndSetStamped`）+ `accumulateAndGet`

ROADMAP 把 `AtomicLong` 与 `AtomicStampedReference` 写成"合一"，若版本戳只到内部实现层则承诺名不副实；各接口内嵌不可变读数 record `Stamped`（`OAtomicLong.Stamped`/`OAtomicInteger.Stamped`/`OAtomicBoolean.Stamped`，字段为对应标量值+版本戳）。`accumulateAndGet` 为客户端 CAS_STAMPED 循环（服务端不提供函数下发执行——可序列化 `LongBinaryOperator` 是假象，服务端执行任意代码越出协调面定位）。`incrementAndGet`/`addAndGet` 用服务端 `ADD`（单 RTT 热键路径）而非本地 CAS 循环包装。

### D6：快照与状态机携带 `kind`/`initial`/去重槽；`Outcome` 新增 `REJECT_ATOMIC_INIT` 单值

初值断言沿用"非零主张"判例（Semaphore total/Latch total），`REJECT_ATOMIC_INIT` 与 `REJECT_SEMAPHORE_TOTAL`/`REJECT_LATCH_TOTAL` 同族同映射（`INVALID_REQUEST`）。「asserted initial = 0 与不主张不可区分」被接受为无害：0 即缺省初值，主张与缺省同值无观察差异。去重槽进快照是重发可判性的存续前提。

### D7：命令通路命名与分派落点

`MessageType.ATOMIC_OP`（14）→ `RequestDispatcher` v4 门 → `AtomicOpCommand`（core 命令记录，携 session/key/op/kind/操作数/expected/expectedVersion/initial/opSeq）→ 单机路径直调 `CoreEngine.atomicOp`；集群路径 `raft.proto` 新增 `ATOMIC_OP_ENTRY` → `LockStateMachineCore.applyAtomicOp` → 同 `CoreEngine.atomicOp` → `ApplyResult` 携带应答四元组 + `op` 回显 → `ReplicationGateway` 完成回执。ATOMIC 操作不产生 `notifyHead`，Leader 队列驱动零触碰。

## Risks / Trade-offs

- [GET 全走 Raft 日志，读热键型膨胀日志与快照] → apply 零迁移保证状态不腐；`atomic.total{op="get"}` 计数 + 日志规模指标进 WATCHLIST 式被动观察，指南建议用写回执携带的 `(value, version)` 免读；
- [单槽去重在裸协议客户端乱序重发下失守，值双加] → SDK 侧同 key 在途写互斥锁死可达路径；契约 Javadoc 与指南双处声明边界；CAS 家族天然自愈（expected 不符即失败）使最常见写型不受影响；
- [int64→int32 wrap 掩码实现错致 Integer 形态值域泄漏] → 形态值域矩阵逐格进 `AtomicEntryTest` 与 E2E 断言（终值域内校验）；
- [条目永不回收遇 key 爆炸性生成] → maxKeys 护栏拒绝新建 + 控制台 ATOMIC 条目数可见（admin SUMMARY 单列），泄漏可观测可告警；
- [v4 门只挡消息不挡误用]：v<4 会话收 `INVALID_REQUEST` 语义与既有门控一致，客户端 SDK 握手声明 4 后旧服务端收 4 断连——版本兼容矩阵测试覆盖（新客户端×旧服务端组合报明确错误）。

## Migration Plan

纯增量发布：服务端先升（接受 1–4 握手，v4 面闲置无成本）→ 客户端 SDK 发布带 `OAtomicLong` 版本（握手升 4，对旧服务端握手失败时以既有"未知版本断连"路径显式暴露，文档标注最低服务端版本）。回滚：服务端回退旧版本后 v4 会话握手中断、客户端显式失败——ATOMIC 状态随回滚丢失（旧版本不认识 ATOMIC 条目形态，快照含 ATOMIC 时旧版恢复失败），回滚窗口内须先确认无生产 ATOMIC key 或接受值清零，指南运维节写明。

## Open Questions

（无——D1–D7 已覆盖提案期全部裁决点；GET 读优化与条目回收作为信号触发的被动项，按 WATCHLIST 规约在有观察数据后另行立项。）
