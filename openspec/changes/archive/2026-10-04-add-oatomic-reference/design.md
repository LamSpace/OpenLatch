# Design

## Context

动机与范围见 proposal.md（Why / What Changes）。本设计面对的现状与约束：

- ATOMIC 家族已有完整闭环：`AtomicEntry` 六操作判定顺序（形态互拒→值域→初值断言→GET 短路→同槽去重→执行）、`ATOMIC_OP_ENTRY` 复制通道、去重槽入快照（`SnapshotLock` 10–18）、ShadowTable/管理面/指标接线（v4 落地）。
- 接入层已有"先于提交的合法性拒绝"通道：`ClusterRequestHandler.handleAtomicOp` 在 `gateway.submit` 前做 v4 门与 `validateAtomicRequest` 形状校验，非法请求零日志产生；单机 `RequestDispatcher` 同型。
- 治理把手现成：`openlatch.cluster.snapshot-threshold`（按已应用条目数触发）+ 保留 2 份；Netty 入站帧上限 1MiB；`ServerConfig` 已有 `openlatch.server.limit.*` 配置段与启动校验。
- 约束：库产物禁用 `--enable-preview`；已发布 proto 编号冻结（golden 测试锁证）；回放确定性纪律（apply 结果不得依赖节点本地可变状态/配置——`EntryClock` 把时钟钉进条目正是同一命题的先例）；对外源码注释禁引内部过程材料（`check-source-citations.sh`）。

## Goals / Non-Goals

**Goals:**

- `OAtomicReference` 对 JDK 语义保真到协调面允许的最大程度：null/空串两态、字节内容 CAS、版本戳 stamped ABA-free、去重幂等。
- 开辟二档共用的**载荷通道基建四件套**：proto 载荷字段口径（`optional bytes` 显式 presence）、`maxValueBytes` 入口钳制、快照尺寸治理回归断言、控制台截断预览——`OBlockingQueue`/`OTopic` 直接复用。
- 标量原子三形态与 v1–v5 既有行为零扰动（共享消息对/条目家族的回归门）。

**Non-Goals:**

- 服务端不解释载荷：无 codec/编解码框架、无反序列化（Java serialization 明确排除——安全与定位双重理由）。
- 不做全局载荷总量配额（跨 key 聚合限额）：每 key 限额即为裁决边界。
- 不做握手回显 `maxValueBytes`（客户端预检便利，可后续纯增量引入；本期服务端权威拒绝即可）。
- 不动 Starter/AOP 面（注解语义面向锁；引用形态无等待/持有语义可织入）。
- `compareAndExchange`/`weakCompareAndSet` 族不入面（JDK 侧亦极少被正确使用，且弱 CAS 的"偶然失败"语义在 RTT 面失去意义）。

## Decisions

### D1 载荷为不透明字节，SDK 主形态 `byte[]` + UTF-8 `String` 便利

服务端全程 `bytes` 通道，MUST NOT 解释内容——对象序列化格式一旦入契约便是永久兼容负担（版本、类演化、gadget 攻击面），且与"协调数据结构"定位错位（KV/序列化正解是 Redis/Hazelcast，不做清单同源）。`String` 便利形态纯客户端编解码（UTF-8），两形态在服务端视角同一字节。备选"引入 Java serialization codec"否决（安全红线 + 兼容深渊）；备选"仅 byte[]"否决的理由是 UTF-8 字符串是小载荷协调场景的压倒数用法，一行编解码的 API 糖不构成抽象。

### D2 null 语义：`optional bytes` 显式 presence，null 与空串两态可区分

JDK `AtomicReference` 的 null 是一等公民：`get` 可回 null、`set(null)` 可清空、`compareAndSet(null, x)` 可建立空态。proto3 裸 `bytes` 无法区分缺省与零长度；选 `optional bytes` 显式 presence（`hasX()` 判定），null/"" 无损表达。备选"零长度即 null（空串不可表）"否决：把可一行 wire 决策解决的失真留给永久契约，且 `""` 与 null 的区分正是原子引用常见判据（"未初始化 vs 初始化但为空"）。代价（生成的 hasX 分支、快照 presence 序列化）是一次性的、局部的。

### D3 复用 ATOMIC 消息对与 `ATOMIC_OP_ENTRY`，不新建 REFERENCE_OP 通道

v6 全部增量落在：`LockType.ATOMIC_REFERENCE=11` 形态判别 + `AtomicOpRequest` 9–11 / `AtomicOpResponse` 7–8 / `ApplyResult` 18–19 / `SnapshotLock` 28–31 的纯增量字段。这是对落地纪律"每个新 wire 类型随协议版本门引入（proto 消息对）"的**有据偏离**，记档如下：纪律的意图是"新语义必过版本门、编号只增"，三者均满足（v6 门在、无编号复用）；而另起 `REFERENCE_OP` 消息对/条目类型会把判定顺序、去重槽、重放、apply 接线、ShadowTable、门控、指标七层各复制一份，纯重复实现，违反"复用机制而非扩类"的 Barrier 先例方向。标量/引用两域在同一条目类不混存（D4），消息层的字段互斥矩阵（D5 之形状面）由入口合法性拒绝兜底。后续 `OBlockingQueue`/`OTopic` 仍按纪律自带消息对——它们的语义面与 ATOMIC 无复用点。

### D4 新建 `AtomicRefEntry`，不扩 `AtomicEntry`

两形态值域不同（`long` 域 vs `byte[]`+null）、值域折算不同（int32 wrap/布尔值域 vs 无折算）、快照字段不同；单类双值域会让每个标量读数方法背上 presence 分支、快照序列化两栖化，阅读成本高于复制一条同构判定顺序的成本。`KeyFamily.ATOMIC` 共享保证既有跨家族/跨形态互拒规则直接生效（家族定型判据不变，形态枚举扩值）。备选"扩 AtomicEntry 塞第二值域"否决（理由同上）；备选"独立 KeyFamily"否决（同 key 同类型约定会被架空——引用与标量本就互斥，无需第二家族位）。

### D5 `maxValueBytes` 钳制点唯一在接入层，引擎/条目 MUST NOT 复核

判定发生在 `ClusterRequestHandler`（`gateway.submit` 前）与单机 `RequestDispatcher`（引擎调用前），超限 `INVALID_REQUEST` 且零日志产生——这保证进入复制面的载荷恒已钳制，apply 侧无需也无法依赖本地配置。核心论证：配置是节点本地部署态、不随复制面分发；若条目 apply 复核，`maxValueBytes` 漂移的两节点对同一日志一拒一受，**回放分歧**（比超限入日志更坏——后者只是膨胀，前者是正确性）。先例同构：`EntryClock` 消灭 apply 对物理时钟的依赖，本条消灭对配置态的依赖。配套契约：钳制下调不追溯存量（既有大载荷条目照常可读、可作 CAS 期望值），guide 运维注记。形状互斥矩阵（引用形态携非零标量槽位、标量形态携 bytes、引用携 ADD）同点裁决，复用 `validateAtomicRequest` 既有通道扩形。

### D6 配置落点：`ServerConfig` 扩段，不入 `CoreConfig`

`openlatch.server.limit.max-value-bytes`，默认 4096（2026-09-16 裁决钉定），校验区间 [1, 512KiB]（1MiB 帧上限留信封/多字段共存边际；超限配置启动快速失败，走 `ServerConfig.validate` 既有通道）。不入 `CoreConfig` 是 D5 的代码级体现：core 不感知载荷上限，引擎签名/条目状态机无尺寸参数。

### D7 编号与快照字段规划（冻结面）

`AtomicOpRequest`: `operand_bytes=9, expected_bytes=10, initial_bytes=11`；`AtomicOpResponse`: `old_value_bytes=7, value_bytes=8`；`ApplyResult`: `atomic_old_value_bytes=18, atomic_value_bytes=19`；`SnapshotLock`: `atomic_ref_initial=28, atomic_ref_value=29, atomic_ref_slot_old_value=30, atomic_ref_slot_value=31`。去重槽骨架（`slot_session=13/slot_op_seq=14/slot_applied=15/slot_version=18`）与 `version=12` 两形态共享复用；引用条目的标量值字段（10/11/16/17）恒缺省。`CoreStateRestore` 以并行 `AtomicRefState` 记录承载（自包含十字段，仿 `AtomicState`/`BarrierState` 并列先例），重建工厂 `AtomicRefEntry.restored(...)` 同款。管理面：`AdminKeyInfo` 11/12、`AdminKeyDetailResponse` 21/22。

### D8 控制台/观察面预览：恒定长度截断 + 转义

`atomic_payload_preview` 由服务端构造：至多 64 字节前缀，可打印 UTF-8 原样、其余 `\xHH` 转义；`atomic_payload_size` 给真实字节数。预览长度恒定不随 `maxValueBytes`（可到 512KiB）膨胀——管理面自身不做成载荷放大器，与 `MAX_PAGE_SIZE=200` 同一防放大纪律。全量载荷 MUST NOT 入任何管理应答。

### D9 指标与计数：kind 词表扩 `reference`，SUMMARY 并入 `atomic_entries`

`openlatch.server.atomic.total{kind="reference"}`（单一命名点扩值，Vocabulary 测试扩表）；op 维度零新增。SUMMARY 不单列——引用条目在 ATOMIC 家族内，`atomic_entries` 计数自然涵盖（避免"每形态一计数线"的词表爆炸；家族级计数与 per-kind 指标两口径互补已是既有格局）。

### D10 客户端车道全复用

`RemoteAtomicReference extends RemoteAtomicBase`：op_seq 会话内分配、同 key 在途写互斥、可重试失败（NOT_LEADER/断连/超时）同 `op_seq` 自动重发至总时限的既有裁决直接继承，仅请求构造器换 bytes 通道。超限拒绝与低版本门拒绝走"不可重试→显式异常"既有分类（超限若重试只会反复撞墙；协议不支持重试无意义）。

## Risks / Trade-offs

- **载荷 × 条目数 → 快照与日志体积增长**（触发阈值按条目数计，对字节量不敏感）→ 缓解：每 key 上限 + "恰限批量写 → 快照尺寸落界"回归断言（snapshot-recovery 新增要求）+ guide 运维注记；总量配额明确 Non-Goal，出现真实压力信号再按 WATCHLIST 纪律登记观察。
- **回滚窗口**：v6 前二进制无法理解 `lock_type=11` 条目与快照 ref 字段（未知枚举 proto3 保留、apply 走 error/误定型路径，行为不良但确定）→ 缓解：与 v4/v5 同型的"回滚后引用 key 不可用、回滚前清场或接受损坏面"注记入 guide 兼容章；不改代码（回滚支持从未是契约）。
- **大载荷 × RTT 的超时不确定窗**：4KB 写撞换主窗口时重发流量放大 → 缓解：去重槽幂等已有保证，上限 4KB 默认即为此设（协调面用途不需要更大）；可调上限 512KiB 的膨胀风险归运维注记。
- **同消息对双域字段的形状矩阵复杂度**：`validateAtomicRequest` 分支翻倍、误用面变大 → 缓解：违例一律入口 `INVALID_REQUEST` 零扰动 + MessageLegality 矩阵测试逐格覆盖；换取 D3 的七层复用收益。
- **条目常驻 + 载荷 = 泄漏驻留成本**（客户端忘删 key）→ 既有"条目不回收"契约的放大版，本就无服务端回收路径（不做清单口径）→ 缓解：guide 明示驻留成本与人工清理运维手册段落，控制台预览列辅助发现。

## Migration Plan

纯增量发布，无数据迁移：协议 v5→6（旧客户端全兼容，v1–v5 行为零扰动）；配置项有默认值（不配即 4KB）；快照新增字段旧二进制不读不坏（未知字段容忍）但**回滚须按兼容章注记执行**（引用 key 先清或接受不可用）。部署序：服务端先升（v6 能力静默待命）→ 客户端按需升。
