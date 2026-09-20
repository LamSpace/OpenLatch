## 1. 协议层（openlatch-protocol）

- [x] 1.1 `openlatch.proto`：`MessageType` 加 `BARRIER_AWAIT=15`/`BARRIER_LEAVE=16`/`BARRIER_ACTION_DONE=17`；`LockType` 加 `LOCK_TYPE_BARRIER=10`；`StatusCode` 加 `BARRIER_BROKEN=12`；新增三对 `Barrier*Request/Response` 消息（字段编号与 specs/wire-protocol"v5 消息与字段增量"逐项一致）；`Envelope.payload` 加 34–39 六槽；admin 三消息扩屏障字段（`barrier_entries=11`、KeyInfo 8–10、KeyDetail 16–20，`family` 词表注记加 barrier）。验证：协议模块编译生成代码，逐字段与本增量对照
- [x] 1.2 协议模块回归：golden 契约冻结测试扩至 v5 面（既有编号零变更断言，RED→GREEN）；`ProtocolCodecTest` 全枚举形态扩三组屏障消息编解码往返
- [x] 1.3 握手版本常量升至 5（`OpenLatchServer.PROTOCOL_VERSION`、客户端 `ConnectionManager`/`RequestMultiplexer` 三处；`HandshakeTest` 新增 v5 接受用例 + 越界用例改 6，RED→GREEN）。v4 及以前客户端×新服务端=既有全量握手回归绿；新客户端×旧服务端不可单仓实例化，指南兼容矩阵注记承担（§9.2）

## 2. core 状态机（openlatch-core）

- [x] 2.1 新建 `BarrierEntry implements KeyEntry`：装 `(parties, generation, 当前世代到场账簿, awaiters 队列, 动作挂账, 了结记录)`；`await/barrierLeave/actionDone/removeSession` 四操作 `synchronized` 实现判定顺序（specs/core-lock-engine"到场合拢与世代状态机"+"离场即破障"逐条对应）；`forceExpire` 抛 `IllegalStateException`（无租约不可达）；`KeyFamily` 加 `BARRIER`；`Outcome` 加 `REJECT_BARRIER_PARTIES` 并更新 `Outcome` 类注释判定顺序段。逐方法契约 Javadoc 按 Lock 接口级撰写（世代竞态、幂等口径、破障触发面）
- [x] 2.2 `CoreEngine`：`familyOf`/`newEntry` 扩 BARRIER 分支；`barrierAwait/barrierLeave/barrierActionDone` 三门面（会话校验 + `touchIfPresent` 登记，判例 latchAwait；parties 非零主张建条目；`breakBarrier` 路径无条目幂等无操作）；会话清理路径 `removeSession` 对 BARRIER 条目接破障传播（在队者摘除 → 当前世代 BROKEN → 全体等待者进 notify 列表）
- [x] 2.3 `BarrierEntryTest` 逐格矩阵：到场/合拢/回卷、同 `(会话,请求)` 幂等不双计、旧世代重发命中了结记录（TRIPPED/BROKEN 各一）、了结窗口外按新到场、队列满拒且不计次、parties 断言三分支（缺失/相符/冲突）、动作待决与 `actionDone`（正指定/非指定拒绝/重复幂等）、离场即破障四触发面（超时 leave/中断 leave 同径/死亡 removeSession/breakBarrier 无在队身份）、破障后新世代自愈、`CoreEngineBarrierTest` 跨家族互拒（LATCH×BARRIER 双向）与触及集登记断言（RED→GREEN）
- [x] 2.4 `CoreInspection`/明细只读观察面扩 BARRIER 读数（parties/generation/arrived/动作挂账/了结形态），观察零扰动断言（读不推进世代与到场）

## 3. 复制路径（openlatch-server/raft）

- [x] 3.1 `raft.proto`：`RaftEntryType` 加 `BARRIER_AWAIT_ENTRY=10`/`BARRIER_LEAVE_ENTRY=11`/`BARRIER_ACTION_DONE_ENTRY=12`（编号纯增量）；载荷为序列化对应请求消息 + sessionId，模式沿 `LATCH_COUNT_DOWN_ENTRY`
- [x] 3.2 `LockStateMachineCore.applyBarrier*` 三分支：解析→`CoreEngine.barrier*`→`ApplyResult` 携应答（generation/位次/executor 标记/了结状态）；破障经 `SESSION_CLOSE` 应用路径传播；`ReplicationGateway` 回执挂接与 notify 事件桥接（AWAIT_NOTIFY 复用，不新建推送）
- [x] 3.3 `ShadowTable`：`AdminEntryView`/`SLock`/`toProto`/`load` 扩屏障复制态（账簿以逻辑会话 id 投影，内部 sid 不出节点）；实现勘误：`expireUpTo` 无租约家族豁免名单补 BARRIER（原子提案曾修同类到期误扫缺陷，屏障首版遗漏，由快照切割点不变性属性测试暴露并修复）
- [x] 3.4 应用确定性测试：交叠世代（合拢→回卷→破障→再合拢）命令序列 Leader/新副本回放终态摘要逐字节一致；执行者死亡 `SESSION_CLOSE` 全副本破障一致；到场重放在任何副本不双计（RED→GREEN）

## 4. 单机分发与门控（openlatch-server/dispatch）

- [x] 4.1 `RequestDispatcher`：三 BARRIER 消息路由（单机直调引擎、集群经 `SessionCoordinator`/gateway 提交）；v5 门（`session.protocolVersion() < 5` → `INVALID_REQUEST` 消息级拒绝、不断连，判例 LATCH v3 门/ATOMIC v4 门）；ACQUIRE 携带 `LOCK_TYPE_BARRIER` 消息合法性拒绝（`MessageLegalityTest` 扩矩阵）
- [x] 4.2 结果→状态码映射：`REJECT_BARRIER_PARTIES`/`REJECT_TYPE_MISMATCH`/非执行者回报 → `INVALID_REQUEST`；`REJECT_SESSION` → `SESSION_EXPIRED`；世代破障了结 → `BARRIER_BROKEN`；`REJECT_QUEUE_FULL` 沿用 LATCH await 现行映射不新增
- [x] 4.3 `BarrierGatingTest`（判例 `LatchGatingTest`/`AtomicGatingTest`）：v<5 三消息各拒绝不断连、v4 既有能力回归不变、ACQUIRE 携屏障类型被拒；三消息 × v5 会话放行冒烟
- [x] 4.4 指标：`ServerMetrics` 注册 `openlatch.server.barrier.total`（`op`：await/leave/action_done × `status`，单一命名点）；`ServerMetricsVocabularyTest` 扩表至十二项；`locks.held` 不含 BARRIER 断言

## 5. 快照与恢复（openlatch-server/snapshot）

- [x] 5.1 `SnapshotState` 新增 BARRIER 条目形态（key、parties、generation、当前到场账簿、动作挂账、了结记录三元组；字段号纯增量）与 `CoreStateRestore` 双向序列化/`BarrierEntry.restored` 重建工厂
- [x] 5.2 恢复测试：快照→重启→后续日志回放的世代号/账簿/挂账/了结记录逐字段保真（含"历经破障与回卷交叠"的脏世代序列，specs/snapshot-recovery 两新场景）；重启后旧世代重发按了结记录裁决、不双计（并入 `ClusterSnapshotRecoveryTest` 或新夹具）；`SnapshotFamilyRoundTripTest` 扩 BARRIER 形态往返

## 6. 管理面与控制台

- [x] 6.1 `AdminRequestHandler`：SUMMARY 单列 BARRIER 条目数、LIST_KEYS 行（持有者空/租约 0/等待者=当前世代在队数 + parties/generation/arrived）、KEY_DETAIL 屏障五字段含 `barrier_last_final` 词表（tripped/broken/none）、Follower 视角 `wait_queue_leader_only` 断言
- [x] 6.2 console 五页面扩呈现（列表行 parties/世代/到场、详情卡"无持有语义"占位 + 动作挂账与了结形态、概览计数）；`AdminClusterTest`/`AdminProtocolTest` 扩断言

## 7. 客户端 SDK（openlatch-client）

- [x] 7.1 公开接口 `OBarrier` + 新公开异常 `OBrokenBarrierException extends OpenLatchException`：`await()`/`await(timeout,unit)`/`breakBarrier()`/`isBroken()`/`getParties()`；契约 Javadoc 按 Lock 接口级撰写增强/降级清单（死亡即时破障为增强、世代局部无粘滞、非受检与 boolean 形态、在途不自动重放、动作异常放大破障、`isBroken` 本地读数）
- [x] 7.2 `RemoteBarrier`：等待编排——QUEUED→挂等 `AWAIT_NOTIFY`→同 requestId 重发幂等了结（OK/BARRIER_BROKEN 分流）；执行者路径——本调用栈跑 action→`ACTION_DONE`→应答终结本等待，动作异常→改发 `LEAVE`；超时/中断→`LEAVE` 后各自语义（false/`InterruptedException`）；在途传输失败/换会话→放弃抛 `OpenLatchException` 不重放（判例 countDown 至多一次）；通知丢失兜底=请求超时上限自发重发一次（幂等无害，判例 latch）
- [x] 7.3 `OpenLatchClient` 三工厂方法（`newBarrier(key)`/`newBarrier(key, parties)`/`newBarrier(key, parties, action)`，parties≥1 参数校验）+ 握手版本声明升 5；无看门狗/租约路径接入断言（await 全程零 `LEASE_RENEW`）
- [x] 7.4 客户端单测：工厂形态与参数校验、`isBroken` 本地性、`await(timeout)` false 与破障联动（假服务端注入 `BARRIER_BROKEN`/`OK`/`QUEUED` 各分支）

- [x] 7.5 等待-通知闭环时序缺陷修复（由屏障高频循环复现暴露）：Latch/Barrier 等待循环在收到 `QUEUED` 应答后 MUST NOT 立即摘除通知登记——登记保持至本等待收场（放行/破障/拒绝/超时分支各自摘除，`finally` 兜底），否则所有 `AWAIT_NOTIFY` 落空、每世代退化为请求超时兜底重发；回归钉为 `ClientBarrierLoopIT`（单客户端双线程/双客户端两形态、20 世代、唤醒时界断言）

## 8. 集群 E2E 与混沌

- [x] 8.1 `ClusterBarrierTest`（判例 `ClusterSemaphoreLatchTest`）：多客户端 parties 合拢矩阵（Σ到场=parties、恰一执行者、`executor=true` 方完成前他方不放行）；相位复用后重组（连续 K 世代 + 世代边界并发压测旧重发×新到场竞态，钉了结记录窗口）；队列满护栏（打满 `maxQueueDepthPerKey` → 拒绝且不计次）；Leader 切换在途世代重发不双计（kill/restart 锚，判例 `committedValueSurvivesLeaderKillAndRestart`）
- [x] 8.2 kill 进程裁决破障 E2E（ROADMAP 头号验收点）：在队参与者进程被杀 → 同世代全体即时 `OBrokenBarrierException`（harness 级会话摘除 + 真实连接断开两形态）；动作执行者被杀 → 待决世代破障、挂账清除；破障后新到场自愈进新世代（集群层/SDK 层锚各一，SDK 层判例 `ClientClusterIT` 换主锚，`ClientChaosIT` 本尊不改——判例 ATOMIC"针对性场景独立成测更稳"）
- [x] 8.3 兼容矩阵 E2E：v5 客户端×v4 服务端发屏障消息明确失败面（服务端侧可测：`BarrierGatingTest` 承担）、v4 及以前客户端×新服务端全量回归绿
- [x] 8.4 公平套件豁免记录（本条即豁免正文）：`FairOrderingSuite*` 对 BARRIER 家族**不适用**——合拢为全体广播事件、无 FIFO 授予序可测；到场顺序与执行者指定由 Raft apply 序承载，确定性证明由 §3.4 回放摘要测试与 §8.1 合拢矩阵承担。此系设计使然而非缺漏（design.md D6、specs/core-lock-engine 无队首推进需求）。混沌/演练对齐以 §8.1/§8.2 专用场景承担；ROADMAP 落地纪律第 5 条据此勾选注记，归档时同步状态列

## 9. 基准、文档与闭环

- [x] 9.1 `BenchmarkMain` 扩屏障相：parties=N 合拢延迟基线（到场扇入→放行往返）、世代回卷连续复用吞吐、带动作世代两阶段开销；运行 `-pl` 不带 `-am`（`exec:java` 落根聚合 pom 的既有坑），报告标题按阶段手改
- [x] 9.2 双语指南：核心概念"循环屏障与世代"节、SDK 章节 OBarrier 用法 + JDK 迁移对照与增强/降级声明清单（逐条对 specs/client-sdk 与 §7.1）、兼容性 v1–v5 矩阵（含"新客户端×旧服务端"注记与回滚窗口"降级前清在途 barrier"）、故障排查 `BARRIER_BROKEN` 语义、术语表（世代/了结记录/执行者/离场即破障）；zh/en 对称
- [x] 9.3 ROADMAP.md：OBarrier 状态列 → 已落地并附归档链接（提案合并时先行"进行中"）；落地纪律第 1 条"下一个门为 v4"勘正为 v5；同 key 同类型约定登记（BARRIER 家族行，LATCH×BARRIER 互斥注记）
- [x] 9.4 全量验证：`mvn -s /home/lam/repo/settings.xml clean verify` 全绿 + `-Pdrill` 演练（复跑纪律：单轮次防压测伪证、红先保 drill-logs）+ `bash scripts/check-source-citations.sh --selftest` 校验后自查零命中（发布模块注释无内部过程引用）
