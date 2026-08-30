# Design：Phase 2 S2 复制状态机

## Context

S1 PoC（`poc/raft-selection/`，定案 Ratis 3.3.0）已验证一条关键接缝：`LockStateMachineCore`（apply 内核，238 LOC）+ `EntryClock`（apply 线程 thread-local 条目时刻）+ `ShadowTable`（影子表 digest）+ sidMap（逻辑 sessionId → 本地 engine sid），在 `CoreEngine` 零改动下通过了确定性回放与全部 §2.4 门槛（报告 `docs/raft-selection-report.md`）。主干现状：Phase 1 写路径完全同步（Netty eventLoop → `RequestDispatcher.dispatch` → `CoreEngine` → 同线程写回），`OpenLatchServer` 构造期直接 `new CoreEngine(..., new SystemClock(), new NotifyEventBridge(sessions))`。S2 的核心矛盾是把"应答时机"从"函数返回"改成"多数派应用后"，同时保持单机路径与 `CoreEngine` 契约不动。详设 §4.5、§5.2、§9、§13.2 已给出行为框架；本设计补三个框架未写死的决定（并发预演失效、在途请求跨线程桥、失联检测）。

## Goals / Non-Goals

**Goals:**
- 复制状态（锁表、租约、会话注册表）三副本 digest 恒等；回放确定性可被随机序列属性测试证伪
- Leader 写路径端到端：提交前预检查快速失败、QUEUED 零日志、授予应答与多数派应用结果一致（详设 specs/replicated-state-machine）
- `enabled=false` 单机行为与 Phase 1 逐用例一致（specs/cluster-node-lifecycle）

**Non-Goals:**
- Follower 转发/拒绝与客户端 Leader 发现（S3：P2-11～14）；ForwardingProxy 不落地，S2 集成测试直连 Leader
- 快照生成/安装/追赶（S4：P2-15～17）；`SnapshotState` 仅骨架进 proto
- 进程级杀节点与分区演练（P2-14/P2-18 用 PoC driver 底子另建）；S2 全部故障用例为 JVM 内嵌集群停节点
- 读优化（Lease Read/ReadIndex）与多 Raft 组（详设 §1.2）

## Decisions

### D1：PoC 内核"转正"进主干，而非重写
`LockStateMachineCore` / `EntryClock` / `ShadowTable` 的语义已在两库 applier 上双验证，重写只会引入回归风险；转正 = 迁入 `io.github.lamspace.openlatch.server.raft` 包、剥离 PoC 专用线协议（`RaftPoc.proto`）改用主干 `raft.proto`、按 CLAUDE.md §5 补齐全量 Javadoc（含 private 成员，`show=private` 构建门槛）。PoC 模块保持不动、不入根 reactor，作历史证据。
**备选**：主干从零实现——否，PoC 的 sidMap、影子表双写核算等细节是踩坑所得（`friction-ratis.md` 9 条），从零写等于重踩。

### D2：条目时刻沿用 EntryClock，不做命令级时间参数
详设 §4.3.4 的"命令级时间覆盖参数"正式降级为**不做**：改 `CoreEngine`/`AcquireCommand` 公开签名违背 §1.4"零改动"卖点，且 EntryClock 双库验证成立。thread-local 的前提（applier 单线程、apply 无跨线程逃逸）写入 `LockStateMachine` 类级 Javadoc 作为显式契约，并在属性测试中固化（多线程并发 apply 时条目时刻互不串扰的反例测试）。
**备选**：`RenewCommand` 等加 `nowMs` 覆盖参——否，收益仅是"显式化"，代价是 core 公开 API 破坏 + §2.4"无需重写锁语义代码"门槛表述弱化。

### D3：并发同键"预演失效"以应用结果收口，队列副作用 Leader-only
§4.5 的预演是快速通道而非裁决：回放侧统一以 `queueIfBusy=false` 调 `CoreEngine`，应用结果为"需排队"时——Leader 经 apply 回调在本地队列登记等待者并应答 QUEUED；Follower 忽略该副作用（队列非复制状态，§7.1 快照与 digest 均不含队列，副本一致性不破坏）。这要求 `LockStateMachineCore.applyEntry` 的返回值携带完整应用结果（PoC 已有 `ApplyResult`），gateway 以其完成 future。
**备选**：Leader 按 key 串行化提交使预演恒真——否，需引入 per-key 锁与排队提交队列，复杂度高于"以应用结果为准"这条既定原则（§4.5 第一条），且对跨 Leader 切换的竞态无改善。
**修订（实现期，见 D9）**："本地队列"的承载结构为 Leader 侧独立 `WaitQueue`，不是 `CoreEngine` 内部队列。

### D4：跨线程应答桥 = CompletableFuture + eventLoop 回写，Leadership 丧失即异常完成
`ReplicationGateway` 结构：eventLoop 线程 submit（非阻塞）→ future 挂起 → Ratis apply 线程完成 future → listener 将应答 `channel.eventLoop().execute(writeAndFlush)` 弹回原 eventLoop。`NotifyEventBridge` 的 AWAIT_NOTIFY 同理经 session 所在 eventLoop 回写。future 注册表随 Leadership 丧失事件全量异常完成（可重试错误），杜绝悬挂——这是 specs"Leadership 丧失时在途请求快速失败"的实现承载。
**备选**：eventLoop 上 `future.join()` 阻塞等待——否，独占 Netty 线程饿死其余连接，违反 Phase 1 pipeline 契约。

### D5：接入节点失联检测 = Leader 轮询 per-peer commitIndex + NOOP 探针（spike 已完成）
详设 §5.2 规则 4 假定"Raft 成员检测节点 N 失联"，但 Ratis 无现成 peer-lost 回调。**spike 结论（P2-08 前置，2.5 小时）**：公开 API `RaftServer.getDivision(gid).getCommitInfos()` 在 Leader 角色下逐 peer 返回 `(RaftPeer, commitIndex)`（`LeaderStateImpl.updateFollowerCommitInfos` 填充），可按 peer 归属；空闲集群 peer commitIndex 天然停滞（无日志可复制），故 Leader 侧失联判定用**低频 NOOP 探针**驱动：探针条目提交后轮询各 peer commitIndex，连续 M 周期未越过最新探针位点 → 判失联 → 批量补发 `SESSION_CLOSE`。参数：探针周期 = `election-timeout-ms`，M = 3（误判需持续 3 个选举周期无法追平——该 peer 即使恢复也早已丢主，清理幂等无害）。滞后上界 ≈ 4×选举超时（3s 默认 ≈ 12s），锁安全由租约到期兜底（§12 风险 2 同理）。
**备选**：①`DivisionInfo.getFollowerMatchIndices()`——返回无 peer id 的裸数组，不可归属，弃；②`getRoleInfoProto()`——`LeaderInfoProto` 不含 per-follower 列表（仅 `FollowerInfoProto` 是 follower 角色视图），弃；③节点间自建心跳通道——多一套传输与故障面，弃。

### D6：RaftSubsystem 与 §9 配置并入 P2-07 交付
详设 §13.2 未给 `RaftSubsystem` 与 `openlatch.cluster.*` 指派任务 ID；gateway 无法在无节点装配的情况下集成测试，故 P2-05 仅做 proto，P2-07 交付"配置解析 + 子系统装配 + gateway"三件套，`enabled=false` 时装配代码零触碰（懒初始化路径不加载 Ratis 类）。

### D7：Ratis 3.3.0 依赖风险前置到第一批 CI
选型报告明言"3.3.0 仅冒烟级验证，集成级风险随 S2 暴露"。P2-06 的转正 PR 即为根 pom 引入 `ratis.version=3.3.0`（root `dependencyManagement`，四件套对齐 PoC：ratis-server/grpc/client/metrics-default）+ server test-jar 的首个 CI 变更，让 shading、JVM 25 告警、test-jar 可用性等在 S2 第 1 周而非联调周爆雷。着色共存前提 S1 已验证（`friction-ratis.md`），主干仅新增依赖、不引 transport 冲突面。

### D8：测试基座 = MiniRaftCluster 进程内 + digest 比对工具入 testFixtures
集成层用 `ratis-server` test-jar 的 `MiniRaftCluster`（3 节点同 JVM，支持 division/节点停机；3.3.0 test-jar 已核实含 `MiniRaftCluster`），故障语义覆盖"停 1 可服务 / 停 2 不可授予"够用；进程级 kill 留给 S3/S4 演练复用 PoC driver 模式。全量比对工具从 `ShadowTable.digest()` 抽为 server testFixtures 公开 API——S4 快照比对（P2-16）与 S2 退出门共用一份实现。

### D9：集群模式等待队列独立于 CoreEngine（修订详设 §4.5 的"本地 CoreEngine 登记"）
**实现期发现的正确性问题**：若等待项登记在 Leader 本地 `CoreEngine`（详设 §4.5 原文），降级节点的引擎里会残留陈旧等待项；此后它在回放同 key 的授予条目时被 LockEntry"队列非空禁止越过在队者"规则拦成 DENIED/QUEUED，与其他副本分歧——复制状态（锁表）被**非复制状态**（队列）污染，digest 比对与快照正确性同时失效。修复方案：集群模式的等待队列落在 Leader 侧独立结构 `WaitQueue`（`server.raft`，任期作用域），引擎在集群路径**永不登记等待项**（apply 恒 `queueIfBusy=false`，本地排队路径不进引擎）：
- 排队/位次/同 `(sessionId,requestId)` 去重/已通知队首重发窗口（`headReplyTimeoutMs` 清扫）/深度限额（`maxQueueDepthPerKey`）由 WaitQueue 承载，语义对齐 Phase 1 锁内规则；
- WaitQueue 在 WinLeadership 时清空重建——公平性契约"**单个 Leader 任期内的严格 FIFO**"（§4.4）由任期边界机械保证，降级节点残留随下次当选一并清除；
- 唤醒通知（AWAIT_NOTIFY）在 apply 回调中由 WaitQueue 判定队首后经本地注册表推送；跨节点通知转发挂 S3 `ForwardingProxy` 内部通道（S2 集成测试直连 Leader，本地投递即覆盖）。
**备选**：①降级时全量日志回放重建引擎（丢弃陈旧队列）——正确但代价 O(log)，且 S2 无快照截断兜底，否；②给 `CoreEngine` 加"摘除全部等待项"入口——违背 `openlatch-core` 零改动门槛（§2.4），否。详设 §4.5 措辞需在 S2 退出评审时回写对齐（记入 tasks 6.4 观察项）。

### D10：Ratis 应用时机实证——提交后应用（决定 D4 无需回滚机制）
读 ratis-server 3.3.0 源码（`StateMachineUpdater.applyLog`）：状态机仅应用 `applied < lastCommittedIndex` 的条目，Leader 与 Follower 共用同一单线程 updater，客户端回执在 apply future 完成后送达。推论：①"应用后应答"天然满足"多数派确认后"（spec 场景"授予应答等于应用结果"的成立基础）；②Leader 状态机不会持有未提交条目，降级/截断无需 SM 回滚（PoC 的隐式前提在主干显式化）；③EntryClock 的"applier 单线程"前提即该 updater 线程，D2 契约在类 Javadoc 声明。3.3.0 实测行为以集成测试"停 2 节点不可授予（无任何授予成功回执）"反向锁证。

### D11：内部提交通道 = RaftClient 池（复用 PoC 摩擦结论）
gateway/SessionCoordinator 向 Leader 提交条目走 Ratis `RaftClient`（async，自动 leader 发现与 NOT_LEADER 重路由）——PoC 实测同一 ClientId 在途请求被 Leader 串行化（吞吐 ~500/s 上限），沿用以轮转小池（4 客户端）摊开；单连接内请求顺序不要求保序（位次瞬态、裁决在 log 序），跨连接并发由 log 全序仲裁。内部通道同时解决"HELLO 落在 Follower 时 SESSION_OPEN 如何进 Leader 日志"（ratis-client 重路由），S2 无需 ForwardingProxy。

### D12：集群 HELLO 语义 = 登记提交确认后回响应
`enabled=true` 时 HELLO 分配 `(nodeId<<32)|localSeq` 后提交 `SESSION_OPEN` 并等待应用回执（Ratis 提交+apply，本机 3 节点 P99 < 20ms）再回 `HelloResponse`——保证客户端握手成功后首个写请求必不早于其会话注册进 log，消除"未登记会话写请求被拒"的正常路径竞态（该拒绝分支仅保留给伪造/过期 sid）。引擎状态变更入口收敛为唯一漏斗"log apply"（`engine.sessionOpened/acquire/release/renew` 在集群节点上只被 apply 线程调用），租约凭证计数器因此跨副本确定性重放成立——D9 的队列外置与本条共同构成该不变式。

## Risks / Tradeoffs

- [Ratis 3.3.0 集成级未验证面（日志段回收、install pipeline、test-jar 行为）] → D7 前置暴露；install pipeline 风险真实影响在 S4，S2 记录观察项不阻塞
- [MiniRaftCluster 同 JVM 共享时钟与线程池，"停 2 节点"的租约到期用例可能受选举扰动噪声] → 到期用例把扫描周期与租约参数放大（分钟级租约 + 手工推进条目时刻），或将选举超时调大隔离噪声
- [apply 结果回调（D3）使 Leader 本地队列登记与日志应用存在两步窗口，窗口内 Leader 死亡则排队丢失] → 与 §4.4 既定语义一致（队列可丢、重排队），spec"等待队列不随切换迁移"已显式接受
- [peer 失联判定滞后于真实断连（D5），期间宕机会话的锁未被清理] → 锁本身有租约兜底（到期自动释放），会话清理只是加速路径；spike 后在 design 记录实测滞后上界
- [EntryClock thread-local 若未来 apply 路径引入线程池跳转（如 gateway 侧误用）会静默回落系统时钟，破坏确定性] → 属性测试 D2 项固化反例；`LockStateMachine` Javadoc 声明"条目时刻仅 apply 线程内有效"契约
- [root pom 引入 test-jar 依赖使 server 模块 test scope 变重，CI 时长增加] → 集成测试挂 `-Pintegration` profile 或 JUnit tag，单元层保持秒级

## Migration Plan

无数据迁移（新功能默认 `enabled=false` 关闭）。回滚 = 配置回退；主干 jar 滚动替换后单机行为不变。合入序：P2-05/06（proto + 转正，含 D7 依赖变更）→ P2-07（装配 + gateway）→ P2-08/09 并行 → P2-10（集成门）。

## Open Questions

（实现期已全部收口：D5 spike 定案 commitInfos+NOOP 探针与 M=3 防抖；位次分配器归属定为 WaitQueue（D9）。）
