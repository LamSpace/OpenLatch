## 1. 协议层（openlatch-protocol）

- [x] 1.1 `openlatch.proto`：`MessageType.QUEUE_OP = 18`、`Envelope.payload` 加 `queue_op_request = 40`/`queue_op_response = 41`、`LockType` 加 `LOCK_TYPE_QUEUE = 12`/`LOCK_TYPE_DELAY_QUEUE = 13`、新增 `QueueOp` 枚举与 `QueueOpRequest`（1–9）/`QueueOpResponse`（1–7）消息、`AwaitNotify.request_id_ref` 注释口径扩 QUEUE_OP、管理面 `AdminSummaryResponse.queue_entries = 12`、`AdminKeyInfo` 13–16、`AdminKeyDetailResponse` 23–28（字段编号与 design D11/specs wire-protocol 逐项一致；`StatusCode` 零新增，双"满"语义以注释钉死）
- [x] 1.2 `raft.proto`：`RaftEntryType.QUEUE_OP_ENTRY = 13`、`QueueOpPayload`、`ApplyResult` 加 20–22、`SnapshotLock` 加 32–34、新增 `SnapshotQueueElement`/`SnapshotQueueSlot` 消息（编号与 design D11 一致，已发布编号零触碰）
- [x] 1.3 协议回归：两份契约冻结测试（`OpenlatchProtoContractFreezeTest`/`RaftProtoContractFreezeTest`）扩至 v7 面——既有编号零变更断言 + 新增编号钉定（RED→GREEN）；`ProtocolCodecTest` 扩队列往返：`element_bytes` 三形态 presence（缺省/零长度/非空）逐格可判别、恰限 4KB 元素字节级保真、`QueueOpRequest/Response` 全字段往返
- [x] 1.4 握手版本常量升至 7（`OpenLatchServer.PROTOCOL_VERSION` 6→7、客户端 `ConnectionManager`/`RequestMultiplexer`；`HandshakeTest` 扩 v7 接受用例 + 区间回显用例 + 越界用例改 8，RED→GREEN）

## 2. core 状态机（openlatch-core）

- [x] 2.1 `KeyFamily.QUEUE` 家族位、`LockType` 扩两形态、`CoreEngine.familyOf`/`newEntry`/`familyOfKey` 穷尽扩支、`Outcome` 新增 `REJECT_QUEUE_CAPACITY` 并入既有 `INVALID_REQUEST` 映射口径（类注释通道清单同步）
- [x] 2.2 新建 `QueueEntry implements KeyEntry`：形态/容量定型、`ArrayDeque<Element(payload, expiresAtMs)>`（QUEUE 按到达序、DELAY 按到期升序同到期 FIFO）、put/take 双轨 awaiters、每会话去重槽 `Map<session,(op_seq,op,回执)>`、TAKE/DRAIN 交付字节入槽；`synchronized` 判定顺序按 specs 逐规则（会话→key→形态→容量断言→去重→执行）；逐方法 Javadoc 写明"条目不复核载荷/容量/批量限额""未到期不可见但计入 SIZE""同到期 FIFO 为语义增强""元素不绑定会话——SESSION_CLOSE 零触碰"契约
- [x] 2.3 `CoreEngine.queueOp` 门面（建条目竞态重试环、读类 op 不建条目回零值、`isEmpty()` 恒 false 不回收、唤醒收集双轨分派——入队唤 take 队首且 DELAY 须队首已到期、出队唤 put 队首）、`removeSession` 队列分支（摘挂起与槽、元素零触碰）、`CoreInspection`/明细观察面扩队列读数（容量/深度/首到期/总驻留字节/首元素预览源）；`QueueOpCommand`/`QueueOpResult`/`QueueOpType` 命令结果类型
- [x] 2.4 `QueueEntryTest` 判定矩阵逐格（FIFO/容量边界恰限/满挂起与立即 DENIED/OVERLOADED 等待深度/同 op_seq 重放不双插不偷吃且同字节/跨会话槽互不遮蔽/槽覆盖纪律/DRAIN 空列表与部分摘取/DELAY 未到期不可见+越过禁令+同到期 FIFO+SIZE 驻留口径/断言冲突与跨形态互拒/会话关闭元素零触碰与槽等待摘除）+ `CoreEngineQueueTest`（跨家族互拒矩阵、key 不存在读不建条目、手工 `Clock` 到期推进）——RED→GREEN
- [x] 2.5 `CoreStateRestore.QueueState`（形态/容量/元素列表/槽表自包含记录）与双向序列化、`QueueEntry.restored(...)` 重建工厂、`CoreEngine.restoreFrom` 队列分支；恢复往返单测（含未到期元素与 TAKE 交付槽逐字段保真）

## 3. 复制路径（openlatch-server/raft）

- [x] 3.1 `LockStateMachineCore.applyQueueOp`：`QueueOpPayload` 解析入引擎门面（apply 侧恒以立即式判定——挂起属 Leader 本地，回弹续挂由分发层回执处理，判例"应用侧引擎调用恒不登记等待项"）；`ApplyResult` 20/21/22 装配（DENIED 回弹零迁移断言：不改元素不写槽）
- [x] 3.2 `ShadowTable`：`SLock`/`AdminEntryView` 扩队列字段（capacity/depth/headExpiry/totalPayloadBytes/首元素 size+预览源）与各 apply 登记点；`expireUpTo` 家族豁免续列（队列无租约恒 0 判例 LATCH/BARRIER/ATOMIC）；`closeSession` 队列分支（镜像保留、元素零触碰）；`heldIndex` 不收队列 key
- [x] 3.3 `WaitQueue` 双轨改造：`Node` 增 role（由 op 导出）、`enqueue` 带轨重载、队首推进分轨（`onElementReady`/`onCapacityFreed`，DELAY 唤取者须队首可消费）、`sweepNotified`/`deferHead`/`isHead`/`totalWaiters`/位次按轨兼容；锁/Semaphore/Latch/Barrier 既有套件全绿作为零扰动回归门
- [x] 3.4 `ClusterRequestHandler.handleQueueOp`：v7 门 → 形状校验 → 元素/容量/drain N 入口钳制（钳定 N 入日志）→ 可满足性查 ShadowTable → 不可满足且 blocking 则 Leader 本地挂起回 QUEUED 不提交（判例 `latchAwaitLocal`）→ 提交后 DENIED 回执回弹原位重挂（位次保持、不推错误）→ 状态码映射与应答装配（presence 回显、capacity 镜像回显）；`ReplicationGateway` 应用事件处双轨唤醒推送与 `sweepWaitQueue` 队列臂
- [x] 3.5 `QueueReadyDriver`（判例 `LeaseExpiryDriver` 骨架）：Leader-only、`ready-tick-ms` 周期、每 tick 反查有 take 挂起的 key × 镜像队首到期 → 仅推送 `AWAIT_NOTIFY`（MUST NOT 提交条目；notified 标记防重推）、`onLeadershipGained` 立即首扫、非 Leader 短路、close 幂等；`ClusterRuntime` 装配与任期边界接线；`OpenLatchServer.startScheduler` 单机同型扫描臂（引擎手工时钟测试道）；单测：到点唤醒闭环、重复推送防护、FOLLOWER 零定时器、换主首扫
- [x] 3.6 应用确定性测试：多会话混合 PUT/TAKE/DRAIN/DELAY 同一命令序列 Leader/新副本回放终态逐字节一致（元素序/到期时刻/槽表/digest 含交付字节）；到期折算跨副本逐毫秒一致（新副本于不同物理时刻回放）；DENIED 回弹条目零迁移摘要等值；配置漂移用例——`maxValueBytes`/`max-queue-capacity`/`max-drain-bytes` 调小的副本回放照常落态与 Leader 终态一致（分歧不可能性的可执行证明）

## 4. 分发、门控与钳制（openlatch-server/dispatch + net）

- [x] 4.1 `ServerConfig` 加三配置项（`openlatch.server.limit.max-queue-capacity` 默认 1024 校验 [1,65536]、`openlatch.server.limit.max-drain-bytes` 默认 262144 校验 [1,512KiB]、`openlatch.server.queue.ready-tick-ms` 默认 200 校验 ≥10）+ `validate` 快速失败 + 启动日志关键限额列出；`ServerConfigTest` 越界逐格
- [x] 4.2 `RequestDispatcher` 单机 `dispatchQueueOp`：形状校验与钳制同型（引擎调用前）、条目双轨挂起与 `fireNotify` 唤醒环、手工时钟延时臂
- [x] 4.3 `MessageLegalityTest` 扩形状矩阵：ACQUIRE/其余消息携 12/13 拒、非 PUT 携元素拒、PUT 缺元素（presence）拒、非 DELAY 携 `delay_ms` 拒、非 DRAIN 携 `max_elements` 拒、读携 `op_seq` 拒、`blocking` 携于 DRAIN/PEEK/SIZE 拒、`lock_type` 非队列值携 QUEUE_OP 拒——各格 `INVALID_REQUEST` 不断连零日志
- [x] 4.4 `QueueGatingTest`（判例 `LatchGatingTest`/`AtomicGatingTest`）：v6 会话 QUEUE_OP 拒不断连、v≤5 既有回归不变；钳制边界矩阵（恰限/超一字节元素、容量恰限/超一、drain N 钳定入日志）× 两部署形态；拒绝零日志断言（提交前后日志位点不动）；钳制下调存量照常可读/消费、新写拒
- [x] 4.5 指标：`ServerMetrics` 加 `QUEUE_TOTAL`（op 词表 `put/take/drain/peek/size` 单一命名点）与 `ELEMENTS_DEPTH_MAX` gauge（与既有等待队深 `queue.depth.max` 两口径注释互引防混读）、`waiters` Gauge 涵盖双轨；`ServerMetricsVocabularyTest` 扩表；两满分轨（DENIED/OVERLOADED）与超限拒绝走 `INVALID_REQUEST` 线的归线断言

## 5. 快照与恢复（openlatch-server/snapshot）

- [x] 5.1 `SnapshotLock` 队列字段生成/加载双向（32–34：元素序列化序即队列序、QUEUE 形态到期字段恒缺省、DELAY 到期逐毫秒保真、槽表确定性序含交付字节）；`StateMachineSnapshotTest`/`SnapshotFamilyRoundTripTest` 扩队列态
- [x] 5.2 恢复测试：空队/普通/恰限大元素/未到期延时四态 + TAKE/DRAIN 交付槽 → 快照 → 重启 → 逐字段保真；重启后未到期元素仍不可见、到期后可消费、命中槽重发仍交付同一份字节、双轨挂起经客户端重挂与新 Leader 首扫恢复；v6 旧夹具（无队列字段）加载兼容回归
- [x] 5.3 快照膨胀治理回归夹具队列版：N 个队列 key × 满容量 × 恰限元素（含 DELAY 到期字段）+ 交付槽，多轮 put/take 回填推进 → 断言尺寸 ≤ N×(capacity×maxValueBytes + 槽交付预算 + 常数)×安全系数且不随操作轮次增长 → 安装新副本逐字段保真、追赶回放正常（与 v6 引用夹具并列为二档守门用例）

## 6. 管理面与控制台

- [x] 6.1 `AdminRequestHandler`：SUMMARY `queue_entries`（两形态合并计数）；LIST_KEYS/KEY_DETAIL 装配队列字段（容量/深度/总驻留字节/首元素 size+恒定 64B 截断转义预览/首到期时刻读数；非队列家族恒零值）；全量元素零外发断言；Follower 双轨等待为零 + `wait_queue_leader_only`；`AdminProtocolTest`/`AdminClusterTest` 扩用例（含恰限大元素截断、观察零扰动——反复查询不动元素/深度/槽）
- [x] 6.2 console 五页面扩呈现：概览 `queue_entries`、列表行形态/容量/深度/首元素预览（DELAY 注到期）、详情卡队列区段（驻留字节/双轨等待位次分列、无持有区段"无此语义"如实）；页面断言测试扩用例

## 7. 客户端 SDK（openlatch-client）

- [x] 7.1 公开接口 `OBlockingQueue` + 子接口 `ODelayQueue`：byte[] 主形态 + UTF-8 String 便利族（put/offer/offer(timeout)/take/poll/poll(timeout)/drainTo/peek/size/remainingCapacity/capacity + offerDelayed；方法面与 specs 逐项一致）；契约 Javadoc 按 Lock 接口级撰写降级/增强/差异清单（每操作一 RTT、超时不确定窗与槽幂等、**死亡不吞元素**、同到期 FIFO 增强、元素非 null 且与 `OAtomicReference` null 语义"看似相仿实则相反"的对照声明、`offerDelayed` 之于 JDK 签名改名、tick 级唤醒精度、回弹续挂对调用者透明、条目常驻驻留成本、需 v7 握手）
- [x] 7.2 `RemoteBlockingQueue`/`RemoteDelayQueue`：op_seq 会话内分配与同 key 在途写互斥继承 `RemoteAtomicBase` 车道（复用或抽共基——不动锁车道）；队列等待跟踪环（QUEUED→挂起→AWAIT_NOTIFY→同 `requestId` 重发→DENIED 回弹继续挂→OK/中断/本地超时终态）；超限/形状/低版本/容量断言拒绝归"不可重试→显式异常"分类（不重试不截断）
- [x] 7.3 `OpenLatchClient.newBlockingQueue`/`newDelayQueue` 工厂（capacity ≤0 本地 `IllegalArgumentException` 快速拒绝；capacity 作定型主张随写携带）；握手版本声明升 7
- [x] 7.4 客户端单测：工厂参数校验、形状装配断言（presence/delay/max_elements/op_seq 分配与读类填 0）、UTF-8 两形态等价、`ScriptedServer` 道：伪 QUEUED→推送→重发 OK 闭环、DENIED 回弹继续挂、中断摘除

## 8. 集群 E2E、混沌与演练

- [x] 8.1 `ClusterQueueTest`（判例 `ClusterSemaphoreLatchTest`/`ClusterAtomicTest`）：8 产 8 销并发 put/take/drain——Σ放入 = Σ取出+Σ摘取+终深度、无重无漏字节级保真、同 key FIFO 逐一成立、深度恒 ≤ capacity、drain 与 put/take 交错
- [x] 8.2 唤醒与回弹专项 E2E：入队仅唤 take 轨/出队仅唤 put 轨（位次连续断言）；双 take 抢单元素——一者授予、一者应用点回弹重挂、下一可见性事件先唤重挂者、客户端全程不见错误；换主窗口挂起重挂（`migrateWaitsTo` 车道）后续消费闭环
- [x] 8.3 延时形态 E2E：集群真实墙钟短延迟（offerDelayed 数百 ms→tick 内唤醒→take 取回，断言精度契约内）；单机手工时钟道（未到期不可消费→推进→可消费）；未到期期间无多余日志条目断言
- [x] 8.4 kill 进程裁决 E2E：投递者进程被杀 → 元素存续且照常消费（契约可执行化）；挂起等待者被杀 → 仅其等待摘除、队首推进不误；换主窗口 put/take 锚已建但间歇红挂 WATCHLIST W10 禁用留证（`ClientClusterIT#committedQueueElements...`）
- [x] 8.5 公平同款落点说明：锁参数化套件形态不适配非 acquire 车道，等价保证改由 `QueueEntryTest.putTrackHeadGate*`（core 确定性）+ `ClusterQueueTest.wakeTracksGrantInParkOrder`（集群位次/授予序）承载
- [x] 8.6 客户端 IT 与混沌对齐：`ClientQueueIT`（阻塞中断、offer/poll 超时、断连快速失败、String 便利族、超限显式异常零生效）；`ClientChaosIT` 混合负载口径注记（针对性场景由 8.1/8.2/8.4 承担、混沌不扩队列面，判例 v6 ATOMIC——本条即对齐记录正文）；v6 会话兼容拒绝由 4.4 承担（跨版本 jar 不并置判例沿用）
- [x] 8.7 演练对齐（2026-10-04 用户非沙箱终端单轮复跑全绿：client 侧 8/8——PartitionDrillIT 真跑、LeaderKill 3/3、RollingRestart 3/3（先从后主 errors 19/2645=0.72%、先主后从 14/2606=0.54%，errorTimes 全部落入 restartWindows、tailErrors=0、stallEvents=0——**W8 形态 B 本轮同序未复现**，印证负载敏感间歇定性）；server 侧停摆采样 8 轮 STALL=0/RECOVERED=8/ABORTED=0。报告落两模块 `target/drill-reports/`）：`-Pdrill` 全量单轮次复跑（非沙箱终端、进程级演练真实生效；红先保 `target/drill-logs/` 再清理；W8 已知选举风暴形态 B 红按既有判据归因、不记作本 change 回归）

## 9. 基准与文档

- [x] 9.1 (扩相已实现：handoff/fanout/drain/delay 四相；基线报告由 `mvn -pl openlatch-examples exec:java` 出盘，本轮以全量 verify 编译门为准，正式基线随发布前 benchmark 轮出) `BenchmarkMain` 扩队列相：热键 put/take 吞吐、容量争用唤醒延迟分布、drainTo 批量摊薄效应、DELAY tick 精度实测、快照生成时长对比（运行 `-pl` 不带 `-am`；报告标题按阶段手改；基线落 `target/benchmark/` 供 W9 观察判据引用）
- [x] 9.2 双语指南 zh/en 对称扩章：01-concepts 有界队列与延时可见性（双"满"、绑定 key、应用点折算）、03-client-sdk `OBlockingQueue`/`ODelayQueue` 用法与差异清单、05-cluster-deployment 队列治理三注记（日志条目率与 W9、三限额语义、队列存续期下调 `maxValueBytes` 先清大元素警示、回滚前清队列 key）、07-admin-console 队列呈现、08-observability 词表与两口径之辨、09-troubleshooting（DENIED vs OVERLOADED 判读、挂起无响应唤醒链排查）、10-compatibility v1–v7 矩阵、术语表（定型容量/出队谓词/到期折算/双轨等待/应用点回弹/drainTo）
- [x] 9.3 ROADMAP/WATCHLIST 收口（2026-10-04 归档轮：二档行翻"已落地"附归档链接；三档"延时触发"行注记覆盖兑现；W9 登记且记录位置钉死归档路径与 benchmark 基线、W10 携形态 A/B 取证结论在账）：本行状态 → 已落地（归档时）；三档"延时触发"行注记"已由 `OBlockingQueue` 延时形态覆盖兑现"；WATCHLIST 登记 W9——热点队列 key 日志条目率膨胀观察（读写皆经提交 + drain 摊薄为既有缓解；触发口径与动作按 W6/W7 同型），记录位置指向本 change 基准报告
- [x] 9.4 全量验证（2026-10-04 收口：`mvn -s …/settings.xml clean verify` 全反应堆 BUILD SUCCESS——8/8 模块，protocol 20/core 174/server 507/client 68+IT84/starter 26/console 23/examples 编译+javadoc 门全绿、0 失败 0 错误 2 跳过=W10 禁用锚+既有跳过；`check-source-citations.sh --selftest` OK + 全源零命中。定性记录：收口轮曾见 2 次非队列侧瞬态——既有原子锚 connect 后首写 SESSION_EXPIRED 一次、failsafe fork 启动崩一次，单模块重跑均绿，签名并入 W10 观察族（客户端车道×选举/会话登记启动竞态，判例 W1/W2 体裁），非 v7 触达面。8.7 `-Pdrill` 已由用户非沙箱终端单轮复跑全绿（见该条记录））：`mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆 BUILD SUCCESS（v1–v6 既有套件全绿 = 零扰动回归门）+ `bash scripts/check-source-citations.sh --selftest` 校验后全源自查零命中 + 8.7 演练复跑记录；红点归因纪律按既有判据（已知接受残余不改产品码）
