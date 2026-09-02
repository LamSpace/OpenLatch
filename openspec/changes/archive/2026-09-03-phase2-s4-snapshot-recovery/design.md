# Design: phase2-s4-snapshot-recovery

## Context

S2/S3 已把快照的"数据侧"备好：`ShadowTable` 携带 `toProto()/load()/digest()`（Javadoc 点名 S4 共用）、`raft.proto` 的 `SnapshotState` 骨架自 P2-05 冻结编号、`ClusterConfig` 解析并校验 `data-dir`/`snapshot-threshold`（S2 仅透传）、`StateComparisons` 全量比对工具随 test-jar 发布、`ClusterHarness.restartNode` 支持保留数据目录重启。缺口侧：`LockStateMachine.takeSnapshot` 为返回 `-1` 的桩，`RaftSubsystem` 显式 `setAutoTriggerEnabled(false)`，无加载/安装路径，无成员变更封装。

S1 PoC 提供一份**部分证伪**的参照实现：Ratis 适配器自管磁盘写快照文件、启动加载本地快照，追赶验证靠重启（S1 实测快照恢复 2514ms）——但**从未走过 Ratis 的快照安装流**（Leader→落后节点），且其"回灌重放"重建路线被其自身代码显式拒绝（快照含历史释放空洞时无法复现 token 序列，poc `LockStateMachineCore.installSnapshot`）。

约束：core 模块纯 Java 零协议依赖（Phase 1 隔离收益，§3.2）；协议零扰动（`SnapshotState` 为服务端内部消息，不入 Envelope）；§11 验收第 3–6 项收口在本段。

## Goals / Non-Goals

**Goals:**
- 快照生成/加载/安装三线全通：重启恢复、严重落后追赶、空目录新节点加入三条路径都以"快照 + 尾部日志"收敛，判据为跨副本全量比对一致。
- 恢复正确性锚定在 core 引擎状态可继续演化（继承锁可释放/续租/到期），而非仅影子表可比。
- §2.4 门槛（10 万条目恢复 <30s）与 §11 演练证据（分区、滚动重启、混沌）闭环。

**Non-Goals:**
- 不做管理端点/CLI（手动触发仅到 `RaftSubsystem.triggerSnapshot()` 方法面）；不做快照加密/压缩/增量快照；不做读优化与多 Raft 组（沿 §1.2）；不改客户端与 wire protocol。

## Decisions

### D1 重建入口取代回灌重放（§7.1 落定）

PoC 的回灌路线（`engine.acquire()` 逐条重放快照条目）在主干不可行：引擎凭证单调自增发号，快照条目携带的是历史实际凭证，中间有释放空洞时重放无法复现 → 快照凭证校验必炸。故按 §7.1 采用**直接重建**：core 模块新增不可变值对象 `CoreStateRestore`（字段用 core 原生类型：key、`LockType`、leaseToken、expiresAtMs、leaseMs、`(sessionId, threadId, count)` 持有列表、会话 id 集合）+ `CoreEngine.restoreFrom(CoreStateRestore)`。
**否决"包级私有"字面形态**：`CoreEngine`（`...openlatch.core` 包，openlatch-core 模块）与消费方 `LockStateMachineCore`（`...server.raft` 包，openlatch-server 模块）跨模块跨包，包私有不可达；取"public 入口 + Javadoc 契约限定唯一调用方与唯一用途"（JDK `@apiNote` 风格，符合 CLAUDE.md §5 契约级注释要求）。`SnapshotState` proto → `CoreStateRestore` 的翻译放 `LockStateMachineCore`，core 不引入协议依赖。

### D2 重建范围 = 引擎四件全套（对照 `CoreEngine` 字段清单）

仅恢复锁表会留下三个隐性正确性洞，重建 MUST 覆盖：
1. `lockTable`：写持有者+计数/读持有者+计数/凭证/到期直写（集群引擎 waiters 恒空，无队列恢复问题）；
2. `leaseTokenCounter`：跳至 max(快照凭证)，否则新授予与继承凭证撞号，继承锁的 release/renew 会误判；
3. `LeaseManager` 堆：逐继承条目 offer——否则该节点日后当选 Leader 时 `expireDue()` 看不见快照锁，§4.3-2"快照点之后的到期由新 Leader 继续驱动"断链（spec"到期扫描覆盖继承租约"场景即此）；
4. `SessionRegistry`：逐逻辑会话 `sessionOpened()` 造内部 sid 并登记，`LockStateMachineCore.sidMap` 重建 logical→internal 映射（内部 sid 不外露、随机值无害）。
重建在 `applyLock` 内整体执行，与 `shadow.load()` 原子推进；不触碰 `EntryClock` thread-local 契约（重建直写到期时刻，无时钟语义参与）。

### D3 走 Ratis 原生快照存储，不抄 PoC 自管磁盘

PoC 完全自管磁盘（自造目录、信封、latest 引用），代价是：无日志截断协同（"回放快照后日志"无从谈起）、无安装流、保留策略自写。主干改用 3.3.0 的 `SimpleStateMachineStorage`（经 `getStateMachineStorage()` 暴露给库）：库负责命名（`snapshot.T_I`）、下发、目录管理、`retention.file.num=2` 清理与截断协同；SM 负责写文件本体（自管 `.tmp`→原子 rename→MD5 伴随，安装端按 MD5 校验）与 latest 引用更新（`updateLatestSnapshot`）。`initialize` 读 `storage.loadLatestSnapshot()` 重建后 `setLastAppliedTermIndex`；自动触发 = 打开 S2 关闭的那行、`snapshot-threshold` 接 `setAutoTriggerThreshold`。文件内容 = `SnapshotState` 纯字节，term/index 由库命名与 `SingleFileSnapshotInfo` 承载——**不沿用** PoC 的 `SnapshotFile{term,index,shadow}` 信封（重复造位点）。

### D11 截断推进与 segment 粒度（安装流的先决条件）

库默认 `purge.upto.snapshot.index=false`：截断位点取全体 peer 提交位之 min，任一长期缺席节点即卡死截断——日志无上界增长，且"Leader 已无历史可发、必须装快照"的位点差永不形成，§7.3-2 安装流成死路。装配层恒置 `true`（快照位点恒为已应用⊆已提交，对多数派安全）。另新增配置键 `openlatch.cluster.log-segment-bytes`（0=库默认，语义透传同 `election-timeout-ms`）：测试以 4KB segment 在小条目量级制造滚动+截断；生产默认不动。退出时回写详设 §9 配置表。

### D4 "异步落盘"口径 = 锁内一致性副本 + 锁外写盘

Ratis 的 `takeSnapshot` 与 `applyTransaction` 同线程（StateMachineUpdater）。§7.2"先取条目锁内的一致性快照副本，再异步落盘"落定为：`applyLock` 内 `shadow.toProto().toByteArray()`（O(n) 序列化，10 万条目数十 ms 级），**放锁后**写盘提交。完全异步（先返 index 后台落盘）被否决：返回 index 驱动日志截断，落盘未完成即崩溃则快照与日志双失——正确性不可交换。updater 线程在写盘期间的短暂停作为既定代价，落盘耗时列入 §10 基准度量（阈值 100 万条保证触发罕见，稳态 P99 不受影响）。此 stall 语义写入 `LockStateMachine` 类级 Javadoc。

### D5 安装流（已 spike 验证，结论固化）

原设想沿用的 `loadSnapshot(SnapshotInfo, InputStream)` 在 3.3.0 **不存在**——spike（P2-16 前半天完成，判据固化为 `ClusterSnapshotRecoveryTest#severelyLaggingFollowerInstallsSnapshotFromLeader`，全绿）确认 3.3.0 安装流为：库侧把 Leader 下发的快照块直写本节点 `SimpleStateMachineStorage` 目录 → `stateMachine.pause()`（SM 自行迁移生命周期 RUNNING→PAUSING→PAUSED）→ 库原子发布（rename tmp→`snapshot.T_I`，含 MD5 伴随文件）→ `StateMachineUpdater.reload()` 调 `reinitialize()`（SM 重扫盘上最新快照、`installSnapshot` 整体重建、位点钉到快照位点、生命周期回 RUNNING）→ 增量回放自动续起。要点三条（详见 `observations-ratis-3.3.0-s4-snapshot.md`）：安装发布不经 SM 引用，reinitialize 必须盘上重扫；MD5 伴随文件是有效性判定输入；`initialize` 须经 `startAndTransition` 把生命周期推到 RUNNING（S2 停留 NEW 无碍，S4 reload 断言使其成为硬要求）。追赶窗口写请求 `NOT_LEADER` 由 S3 角色门天然覆盖（安装中本节点恒为 Follower），用例已断言。

### D6 成员变更：AdminApi 封装 + 复用失联清理车道

`RaftSubsystem` 封装 `setConfiguration`（新节点以 listener 加入、追赶收敛后升 voter；移除即出组）。被移除节点的会话清理**不新建机制**：走 §5.2 规则 4 的"归属 N 的会话批量 `SESSION_CLOSE`"已交付车道（S2/P2-08），成员变更出组即触发同一失联判定路径。"先加后删、禁止同时变更多数派"为运维约束，文档化 + 封装层对同批多成员变更做入参校验拒绝。

### D7 分区演练双轨，netns 轨为主判据

§11-3 措辞是"分区演练断言少数派全部写请求失败"。主轨：`ip netns + veth + iptables` 真分区脚本（3 节点独立 netns，切断少数派↔多数派双向，保留观察通道取日志），断言少数派无主/写入超时或拒绝、恢复联通后自动收敛且全量比对一致；脚本放演练层（不进 surefire，需特权，产出 `docs/partition-drill-*.md`，格式沿 `failover-drill-2026-08-31.md`，含失败轮次如实记录）。辅轨：in-JVM 近似用例（`transferLeadership` 让主到 n1 后停 n2/n3，n1 少数派写全部失败）进常规集成测试作零权限回归。§11-3 证据以主轨为准，辅轨标"近似口径"。

### D8 core"零改动"口径切换与回写

S2 退出证据为 `git diff openlatch-core 为空`；S4 后口径改为"**§7.1 授权的纯新增**：既有方法体逐字节不变；既有文件仅追加式新增成员（`CoreEngine` 的重建入口 + 发号水位读点 + 守卫位、`LockEntry` 的重建工厂），其余为新增文件（`core.snapshot.CoreStateRestore`）"，退出时以 diff 审查记录佐证（`s4-zero-touch-evidence` 形态延续）。详设 §1.4/§2.4/§13.4 相应措辞与 §9 新增键（`log-segment-bytes`，见 D11）在退出评审时回写 v1.4（沿 v1.2/v1.3 回写先例）。

### D10 快照必须携带发号水位（实施期发现，正确性硬条件）

仅凭 `max(继承凭证)+1` 起号不成立：凭证发号是"每次新授予消耗一格"的历史累计量，被释放条目的已消耗凭证不出现在快照中——重建副本的计数器落后于未截断副本，此后同一尾部日志各自发出的 `leaseToken` 逐笔不同，**跨副本状态永久分叉**（客户端持 Leader 的凭证对安装副本重放的锁不匹配，release/renew 全判 INVALID_TOKEN）。这正是 PoC"回灌无法复现 token 序列"的镜像问题：回灌复现不了历史值，跳界复现不了未来值，唯一出路是把水位本身变成快照内容。落点：`SnapshotState` 增 `next_lease_token = 3`（服务端内部消息、S4 前无历史快照存在，编号纪律允许纯新增；golden 契约文件同步追加）；水位不入 `ShadowTable.toProto()`（digest 输入不变，否则在役 Leader 计数器持续前进会使跨副本摘要永不相等），由 `LockStateMachineCore.snapshotState()` 组合、`installSnapshot` 消费；缺字段（值 0）按 `max(凭证)+1` 兜底。属性测试"快照切割点不变性"（≥100 组，任意切割点"装快照+放后缀"与全程回放 digest 一致）是该水位的机械判据。

### D9 10 万条目基准构造走 apply 直灌

经 `LockStateMachineCore.applyEntry` 序列化入口直灌（每条目独立解析+应用，贴近真实 apply 路径），不经网络（客户端路径造 10 万锁会把测试拖入吞吐问题域）。度量：`SnapshotState` 字节数、落盘耗时、加载+回放耗时；比对：`StateComparisons.diff` 全量结构级（恢复节点 vs 在役副本）+ `awaitDigestsAgree` 快速判定。

### D12 失联判定加"零推进"保护（P2-18 辅轨发现的真实缺陷，容错加固本体）

辅轨用例（单节点 Leader 复活愈合后）暴露：既有判据"连续 M 周期未越过 Leader 位点"把**选举/回放修复窗口内暂时滞后的存活副本**误判失联——commitInfo 缓存天然滞后（可达一个刷新周期）而 Leader 位点随探针持续前移，健康副本可在 3×周期内被记满停滞，其存活会话遭批量清理，直接违 §11-2"存活会话锁不丢"。修复（SessionCoordinator.probeTick）：停滞计数仅在该 peer commitIndex **零推进**时累计，有推进即复位——真失联节点的定义特征正是零推进，停止节点的清理时延不变（S2 失联三路 digest 用例保持绿）。新增 spec Requirement"失联判定的进度保护"两场景锁行为。此项即 §13.4 段名"容错加固"的实证注脚。

## Risks / Trade-offs

- ~~**安装流行为与预期不符**~~ **已消解（D5 spike 全绿 + 观察档案 §1-2）**：3.3.0 为"库写文件、SM 钩子 pause/reinitialize"模型，与初设的 `loadSnapshot(Stream)` 假设不同但更强（SM 无须管理流），判据已固化为集群用例。
- **`toProto()` 在 applyLock 内的序列化 stall 放大**（百万条目阈值下状态大）→ D4 已限定锁内仅序列化；基准记录实测，若超预算再议分块（本期不做）。
- **日志截断与安装竞态**：Leader purge 过快、快照安装中断的中间态 → 依赖 Ratis tmp→commit 原子语义 + 保留 2 份；混沌演练（P2-19）覆盖随机杀节点×快照窗口交错。
- **netns 脚本环境不可用**（无 root CI）→ 双轨设计，主轨标注运行环境要求，证据缺失时以辅轨+多数派论证提交评审定夺。
- **重建入口被误用**（单机路径/运行中调用）→ Javadoc 契约限定"仅快照加载、构造后应用前调用"；测试断言运行中调用被拒或文档化未定义行为前加防御性状态位（实现期定，倾向轻量校验抛 `IllegalStateException`）。
- **回滚**：S4 集群数据目录含截断日志，回退 S2 代码不可恢复 → Phase 2 未发布，无既有部署；发布说明标注"快照启用后不可回退至 S2 二进制"。

## Open Questions

- 混沌不变式检查器的采样通道（各副本 dump 轮询 vs 客户端持有簿记核对）——不影响规格与任务拆分，P2-19 实现期定夺。
