# Design: phase3-t1-extended-lock-types

## Context

动机见 proposal.md - Why。本设计锚定在既有代码的两个事实上：

1. **双等待队列架构（Phase 2 design D9）**：单机模式下等待者驻留 `LockEntry.waiters`，队内裁决（重发命中、通知队首）全在 core；集群模式下 `LockStateMachineCore.applyAcquire` 恒以 `queueIfBusy=false` 调用引擎且引擎恒无等待项——位次裁决在 Leader 内存 `WaitQueue`（server 层），授予裁决进日志。T1 的两个新原语必须**同时穿透这两条路径**且判定语义等价。
2. **设计说明书 §2.1 协议缺口**：`SemaphoreEntry.permitsTotal` 与 `LatchEntry.count` 均无设定通道（文档只有获取/扣减消息，没有初始化语义）。本设计予以补齐，并需回写说明书勘误。

既有约束：core 零依赖、`LeaseManager` 堆记录为 `(expiresAtMs, key, token)` 三元组（类型无关）、`CoreEventListener.notifyHead(sessionId, requestId, key)` 单回调、协议字段编号只增不改。

## Goals / Non-Goals

**Goals：**
- 三类新原语在单机与集群两模式下语义等价交付；
- 互斥/读写锁既有行为**零变化**（P3-01 回归底线）；
- v1/v2 客户端逐字段不变（门控只做拒绝，不碰既有路径）。

**Non-Goals：**
- 不动读者批量授予优化（详设 §9-2，与公平性套件互斥评估）；
- 不提供 latch 重置、跨家族 key 复用、注解式 Semaphore/Latch；
- 不做管理协议/指标/安全（T2–T4 各自的 change）。

## Decisions

### D1 初始值定型通道：首次请求者定型（增补协议字段）

**Latch**（P3-05 实施时精化）：初值定型经**双通道**——`LatchCountDownRequest { key, count, total }` 与 `LatchAwaitRequest { key, total }` 均携带可选 `total` 断言：非零时条目不存在则创建、存在则须匹配（`0` 为不主张，条目不存在即拒绝）。精化动因：若仅 await 可定型，"创建者 await 之前 worker 的 countDown 先到"即丢失扣减（初始化竞态）；`countDown(0, total=n)` 提供非阻塞显式初始化，与 Java"创建器构造屏障"的用法同构。`count = 0` 且 `total = 0` 的 countDown 非法（无扣减无断言）。**Semaphore**：`AcquireRequest` 增补 `permits_total = 7`（文档 §2.1 只有 `permits = 6`）——建条目时必填 > 0，既有条目上非零值须匹配，0 为纯请求。

*备选*：B. `ACQUIRE(type=LATCH)` 做初始化——搅浑 ACQUIRE 语义、违背 §2.1"走独立通道"原意，否；C. `countDown` 负值表示初始化——语义最脏，否。A 与"条目由首次请求定型"的既有约定同构，代价仅为文档勘误。"先加入者定规则"的隐晦性由 `count = 0` 纯加入通道兜底：不知 N 的等待者仍可安全 await。

### D2 KeyEntry 接口边界：最小生命周期契约 + CoreEngine 家族分派

`KeyEntry` 只收编调用方真正需要的行为：`key()`、`isEmpty()`、`typeFamily()`、`removeSession(...)`、`forceExpire(...)`/租约字段读取。`acquire/release/renew` 语义留在各实现类，`CoreEngine` 按 `cmd.lockType()` 所属家族在 `computeIfAbsent` 的 factory 处定型分派（锁家族 → `LockEntry`，构造与现状逐行相同）。`LockTable` 泛型参数改 `KeyEntry`，`LeaseManager`/`SessionRegistry` 零改动（本就类型无关）。

*备选*：通用命令大接口（`acquire(KeyCommand)`）——Semaphore 无"写侧重入"、Latch 无租约，强行抽象产出满是 `UnsupportedOperationException` 的接口，否。`CoreEngine.release/renew` 收到 key 存在但家族不符的请求 → 类型不匹配拒绝路径（D3）。

### D3 错误码口径：core 细分、协议不新增

`Outcome` 新增 `REJECT_TYPE_MISMATCH`；`ReleaseStatus` 新增 `OVER_RELEASE`（归还有余检查，先于 NOT_HELD 判定）。协议层两者统一映射 `INVALID_REQUEST`，不新增 StatusCode——新原语是 v3 门控的增量，错误细分留在 core 侧可观测（日志/T2 指标标签），协议面保持最小。锁家族的既有 Outcome/ReleaseStatus 优先级链一字不动。

### D4 Semaphore 判定规则序与双模式等价

条目内规则序（对齐 `LockEntry` 规则编号风格）：**① 重入**（同 Owner 已持有 → 累加，先于一切位次检查，否则持有者排队期间无法重入即死锁）**② 队首/空队 + 许可足量 → GRANTED ③ queueIfBusy=false → DENIED ④ 入队（携带请求许可数）**。

- **单机**：等待者驻留 `SemaphoreEntry.waiters`（`Waiter` 增 permits 字段或 Semaphore 自有 waiter 记录）；通知规则 = "可用许可 ≥ 队首请求数"才通知队首。
- **集群**：`WaitQueue.Node` 增 `permits` 字段；Leader 预检（入日志前）复用"队首才提案"门控，与锁的 `hasWaiters` 门控同构；重入识别走影子表（持有者请求不受队首门控拦截，走既有锁路径的推广）。apply 后队首推进以回执中的剩余许可与 Node.permits 比较。

两处判定 MUST 由同一张场景矩阵表驱动测试钉住等价性（见 tasks P3-03/P3-07）。

### D5 Latch 生命周期：归零即空、随框架回收

归零瞬间对全部 awaiter 广播（逐个 `listener.notifyHead`，接口零改动）；广播不离队——awaiter 各自重发 await 命中"已归零"规则时离队。条目存续由**参与者集**（await/countDown 触达的会话）支撑：全部参与会话关闭且无等待者后条目回收。这同时解决两个问题：未归零且暂时无 awaiter 的屏障不被误 GC（等待扣减的载体必须存活），而归零屏障的护栏窗口 = 参与者存续期（参与者散尽即回收，"新屏障 = 新 key"约定推论，换取零状态泄漏——每请求一个 latch 是 CDL 常态用法，永久保留不可接受）。*备选*：`isEmpty = count==0 && awaiters 空`——未归零屏障被 GC，错误；归零永久保留/TTL——泄漏或新增配置面，否。Latch 无租约：`LatchEntry.leaseToken()` 恒 0、永不入到期堆（`CoreEngine.expireDue` 实际因不入堆而零分支成本）。

### D6 集群日志与消息路由

`raft.proto` EntryType 新增 `LATCH_COUNT_DOWN_ENTRY`（编号只增）；Semaphore 的 acquire/release 复用 `LOCK_ACQUIRE_ENTRY`/`LOCK_RELEASE_ENTRY` 载荷（内嵌的 `AcquireRequest`/`ReleaseRequest` 新字段自动透传），影子表按家族扩展。`LatchAwait` 完全不进日志：Leader 本地裁决（计数已 0 → 直接 OK；否则 `WaitQueue` 登记 → QUEUED，归零广播时 `purgeKey` 式全体出队通知）。`RequestDispatcher` 增加 LATCH_COUNT_DOWN/LATCH_AWAIT 两个 handler；v3 门控在会话握手的 `client_protocol_version` 上判定，消息级 `INVALID_REQUEST` 不断连（对齐 §5 认证失败断连的反例：这里不是安全事件）。

### D7 快照扩展

`SnapshotState` 的条目消息新增 `family` 判别字段与 Semaphore/Latch 状态字段（新增字段号，既有编号冻结不变——锁条目序列化字节零扰动）。`StateComparisons` 全量比对扩到三类条目；latch 的 awaiter 不入快照（与锁等待队列同口径）。

### D8 公平性回归套件形态

独立类 `FairOrderingSuite`（core 直驱档）+ server 端到端档 + 集群档，三档共享"错开到达时刻、断言授予序列 == 排队序列"的矩阵驱动写法（复用 `TestSupport` 手工时钟，杜绝 sleep 竞态）；`FAIR` 与 `REENTRANT` 两种类型参数化跑同一矩阵（语义等价的机器验证）。**常开判据**：类名/分组落 surefire 默认 include，MUST NOT 进 `-Pdrill` 分段（对齐 Phase 2 验收的常开要求）。

## Risks / Trade-offs

- **双模式判定漂移**（Semaphore 单机内核 vs Leader 门控两处实现）→ D4 的同矩阵表驱动测试 + P3-07 集群用例矩阵收口；
- **Latch 归零回收 vs 一次性语义**：晚到纯加入者看到拒绝而非放行 → 以文档明示 + `count = 0` 拒绝对错误可观测（INVALID_REQUEST 而非静默挂起）；Java 用户迁移误用是主要风险，examples 加一段 latch key 命名约定（如带轮次后缀）；
- **`WaitQueue.Node` 携带 permits 后既有锁路径扰动** → Node 对锁恒填 1，锁语义零分支差异，既有集群测试守住；
- **v3 门控误伤 v1/v2** → 门控仅在"新类型/新消息"判定式上生效，HandshakeTest/MessageLegalityTest 既有断言全保留为回归；
- **P3-01 重构面**（泛型签名波及编译期）→ 独立提交、只动抽象不动语义，回归底线测试先行跑通再合任何功能。

## Migration Plan

全增量、无数据迁移：既有集群快照（v2 时代生成）因条目消息新增可缺省字段而天然可读（proto3 默认值 = 锁家族）。回滚：P3-01～P3-07 按子任务独立提交，任一档可独立 revert；服务端回滚到 v2 时代二进制时，v3 客户端的新原语请求按握手版本门控自动失效（锁不受影响）。

## Open Questions

（无——探索阶段三项待定夺已按 D1/D2/D3 定夺；latch 回收窗口若上线后有用户反馈，走 §9 遗留项流程另行评估。）
