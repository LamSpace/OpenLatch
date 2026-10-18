# 决策账本(Decision Ledger)

本文件是项目裁决的单一事实源:每次影响定位/协议/次序/边界的决定在此追加一行。
原语与契约的细节永远沉淀在各自的提案与规格里(见 `openspec/`);被动观察项见 `WATCHLIST.md`(配对文件)。
最后更新:2026-10-18。

## 决策记录

- **2026-10-18 OpenLatch 1.1.0 发布裁决**：`1.1.0` 已发布并打 tag `v1.1.0`（merge commit `712ce7f`）。Maven Central 范围沿用 1.0.0 裁决：parent POM + `openlatch-protocol` + `openlatch-client` + `openlatch-spring-boot-starter`；`openlatch-core`/`server`/`console`/`examples` 不进中央仓库，服务端与控制台 executable jar 经 GitHub Releases 分发。协议 v11：服务端 1.1.0 接受 v1–v11 客户端；客户端 1.1.0 自报 v11、要求服务端 ≥1.1.0（升级顺序：先服务端后客户端）。发布内容=v6–v11 六个新原语（OAtomicReference、OBlockingQueue/ODelayQueue、OTopic、OCondition、OPhaser、OTimer）、客户端虚拟线程化、ScopedValue 重构与换主窗/缺码形护栏修复。发布验证=本地 `-Prelease -Dgpg.skip=true clean install`、CI `verify`+`citation gate`、Central 四坐标及 sources/javadoc/签名核验、GitHub Release 双 jar。依据=2026-10-18 发布执行；记录位置=本行与 GitHub Release `v1.1.0`。
- **2026-10-18 Ratis 升级评估对账裁决（W4；数据拉取 2026-10-17）**：评估结论=**暂不升级，W4 维持**。事实：①3.3.1 已于 2026-09-22 发布，五 artifact 与 tests jar 齐备，Java 8 字节码基线不变；②形态 A/B 均未修复——3.3.0→3.3.1 的 `LeaderStateImpl`/`LeaderElection`/`VoteContext`/`LogAppenderBase`/`LogAppenderDaemon`/`GrpcLogAppender` 源码逐字一致；3.3.1 的 27 项变更不触本路径（RATIS-2500 为 InstallSnapshot 通知环，非本环），最接近 A 的 RATIS-2523 仍 Open；③3.4.0 未发布（JIRA released=False、无工件/tag），63 项在册无 A/B 针对性修复（RATIS-2311 为让位等待改善，RATIS-2735/2738 为搬迁重构，RATIS-2761 未列 fixVersion）；④B 无上游工单，RATIS-1403 以 Not A Problem 拒绝“拒绝投票不改状态”路线。裁决：看门狗与 supervisor 契约保留；若另立 3.3.1 维护性升级 change，须先做存储/恢复兼容性 spike 与混合版本滚动，且**必带 W8 现场复验**——两序进程级滚动演练单轮静置多轮，判据 tailErrors=0/stallEvents=0；B 为间歇形态，单轮绿不得据此移除看门狗或改判 W8。依据=2026-10-17 只读评估；记录位置=`WATCHLIST.md` W4/W8 行与本条。
- **2026-10-17 DENIED 归因面评估裁决（W24）**：评估结论=**当前不立实现 change**，W24 维持在册并细化触发口径。事实：①`AcquireResponse` 的 `DENIED` 无载荷，客户端一律坍缩为固定文案 `LockDeniedException`，同步 `tryLock()` 仅 boolean；②集群快失败路径的影子表有持有者 `(sessionId, threadId)` 与到期读数，但按设计"可旧不可错"；应用点 `DENIED` 权威却无字段（`ApplyResult`），单机路径经 `AcquireResult` 同样无字段；③`DENIED` 至少有"他人持有/队列非空/信号量池不足/应用点并发竞态"四类成因；④客户端断连 `failAll` 不记录"可能已生效"，会话切换即丢弃旧 `sessionId`。裁决：触发后按需求选最小面——诊断→纯客户端不确定性记录＋异常细分（不碰协议）；重试时点→`DENIED` 附 `lease_expires_at_ms`；API 级精确归因→`DENIED` 附持有者 `sessionId`/`threadId`＋客户端保留近期会话谱系比对（仅 async 面；`sessionId` 为判据，`threadId` 不可单独使用）；纯附加 `optional` 字段技术上可无需升版，但须按"新面随版本门"纪律显式裁决。否决 B（持有者存活标志：Leader 本地注册表只知本节点连接，跨集群无会话存活表，易误导）；暂缓 E（客户端实例身份/跨会话授予重放：改身份与幂等语义，仅当 A 不足且成硬需求时再评估）。任何方案均不改立即式语义。依据=2026-10-17 只读勘察；记录位置=`WATCHLIST.md` W24 行与本条。
- **2026-10-11 周期核对整体回退裁决**：`fix-lane-reroute-convergence` 的 home 驻留期周期核对及其两个连带 change（`fix-await-notleader-reroute`、`fix-phaser-timer-await-reroute`）**整体回退**，客户端回到本批改动之前的已知良好态（代码/主规格/双语文档逐字还原，三份归档保留为历史）。证据：`ClientClusterIT#committedAtomicValueSurvivesLeaderKillWithoutDoubleApply` 隔离复跑——改动前 0/8、引入后 2/6、CI 两次尝试同签名 2/2、**停用该核对 0/10**；失败形态含 `addAndGet` **返回 0**（proto3 默认实例读作 OK——W10"伪成功"同族）与服务端 `SESSION_EXPIRED`，均在启动窗首个原子写。**微观机制未达**（失败率对负载敏感，插桩二十轮未捕获失败轮），故按"假绿严格劣于显式失败"（D3 判据）行使回退优先：回退后该窗恢复为显式失败。**本行取代**同日两条等待类语义裁决（其重道预算与使能路径随回退一并失效）。重做须先造出**确定性复现**再红先，且主动改道不得与在途写换会话相撞。
- **2026-10-11 等待类原语 NOT_LEADER 语义裁决（范围扩展）**：同日既有裁决的范围由"闩/屏障 `await`"扩至**相位器/定时器的等待面**——判据不变（服务端已答复 `NOT_LEADER` ⇒ 零生效 ⇒ 跨会话重放安全），且这两族的等待环**本就**在瞬态分支放行跨会话重发（代码自述"纯等待无复制态副作用：跨会话重建信封安全"）,`NOT_LEADER` 的未受理证据比瞬态更强。护栏边界：两族"同一会话以原 `rid` 重发"的分片保活纪律不受影响（跨会话重道必然换会话与新 `rid`,见各自等待环）；仍**不含**变异类（发布/注册/装载/撤销——其 `NOT_LEADER` 是"换新请求重试"的显式信号,可能已受理,不得盲目重发）。原裁决中"相位器/定时器等待面本次不动"一句由本行取代。
- **2026-10-11 等待类原语 NOT_LEADER 语义裁决**：修订 v9——闩/屏障 `await` 收到 `NOT_LEADER` **应答状态**时,由其时的"终态显式抛出"改为**零生效重道**（摘除本机等待登记 → 重取路由 → 重发）,该重发受**独立短预算**约束（上限取 2 个 Leader 核对周期）,预算耗尽或路由在预算内未变化时仍以 `NOT_LEADER` 显式失败——既保住 v9/D3"不悬挂至等待总预算、不假绿"的关切,又消除"真主存在而本方路由陈旧"的即时失败。范围限闩/屏障 `await`;相位器/定时器的等待面同型但本次不动;变异类（发布/注册/装载/撤销）不受影响,其 `NOT_LEADER` 仍为"换新请求重试"的显式信号（可能已受理,不得盲目重发）。
- **2026-10-10 在册事项优先级裁决**:执行序(轴=触发接近度×后果严重度×解锁成本)=W11 > W8(已触发但按动作列改道并入 W4) > W4 > W13 > W12 > W3 > W2 > W1 > W9 > W6 > W14 > W7 > W15 > W16;依据=WATCHLIST 各行触发口径与 v4–v11 设计裁决,本序不改变任何行的触发门槛(未触发=不动),重排须在本表补行;同日 W5 发布挂账随 2026-10-01 发布清账自 WATCHLIST 删行(编号留空不重用)。
- **2026-10-10 结构化并发评估裁决(工程改进第四项)**:评估结论=库内**无结构化并发落点**(R1 空集:客户端 RPC 单 future、屏障/闩/相位器/定时器的 `register + send` 刻意有序、服务端 topic 扇出在 Raft apply 线程内**串行**、复制扇出在 Ratis 内——均非 fork+join);定性=R1(内部采纳,非对外契约,双语指南零触碰)。`StructuredTaskScope` JDK 25=第五预览(JEP 505)、26=第六预览(JEP 525)且 API 仍改名,`--enable-preview` 对已发布库无例外窗口。转正触发器=JEP final **且** API 连续 ≥2 发布稳定(严于单纯"转正"),届时基线若落 JDK 27 接受 `maven.compiler.release` 25→27 抬升。该事项自本表工程改进行**退役入 WATCHLIST**(W16);路线图表格的物理退役与 `ROADMAP.md → DECISIONS.md` 改名由后续独立治理 change 承担。与第三项耦合:`EntryClock` 作用域继承面仅经 `StructuredTaskScope.fork`,采纳即令第三项边界矩阵绊线按设计翻红(落地 change 须显式对账)。
- **2026-10-09 上下文传播评估裁决(工程改进第三项)**:评估结论=客户端**无 ambient 上下文需求**——会话身份全程显式传递(`SessionContext` 经 `Route` 捕获 + `Thread.currentThread().threadId()` 显式读),ROADMAP 原措辞设想的"线程本地袋"不存在;全仓库唯一真 `ThreadLocal` 在服务端 `EntryClock.APPLY_NOW`(apply 期入口)。落点据此收敛为**该 ThreadLocal 的 ScopedValue 机制替换**(纯内部、行为零变化,`skip_specs`)。实证:JEP 506 final 版 ScopedValue 绑定**仅**经 `StructuredTaskScope.fork` 继承(`Thread.start`/`ofPlatform`/`ofVirtual`/线程池一律不继承),故传播边界与 ThreadLocal 逐格相同、不引入语义变宽;`StructuredTaskScope` 仍预览(禁令排除)故不追求跨线程继承。第 52 行原"客户端内部会话/追踪上下文传递"定位据实校正为服务端机制替换。
- **2026-09-16 定位裁决**:采纳"B——允许小载荷协调数据结构"(每 key 限额、默认 4KB、服务端钳制)为能力边界;落地次序按"A——纯协调面先行"。来源:JDK 并发原语扩展的可行性分析(五轴:定位契合/机制复用/协议成本/语义保真/竞品先例)。
- **2026-10-01 发布裁决**:1.0.0 发布并打 tag `v1.0.0`;Maven Central 发布范围收窄至客户端 SDK 链(protocol/client/starter),core/server/console 退出中央仓库、服务端与控制台改经 GitHub Releases 可执行 jar 分发。WATCHLIST W5 发布挂账就此清账。
- **2026-10-07 三档立项裁决(OTimer)**:OCondition/OPhaser 同型复刻——"延时触发(定时单次标记)"于需求信号未至时提前立项(用户裁决"按 OCondition/OPhaser 先例登记决策行立项走 B";放行依据即本行)。立项前置核验收窄第 43 行既有"覆盖即可"结论:队列延时形态覆盖**延时交接**(一元素一消费者),但**广播单次标记**(一次到期全体共见)与**改期/撤销**两类 Timer 语义不可表达——该缺口即本原语的存在理由。协议升至 v11 门;架构裁决=**到期派生零条目**(装载/撤销入日志沿"迁移入日志"纪律,fire 为账簿与判定时刻的纯函数——"到期不是迁移,是时间的兑现",与租约"到期是状态迁移故入日志"构成判例族两端)、等待/取消/查询零日志(v9/v10 类目直接延伸)、DISARM 以既有 DENIED 首次可达面了结等待(StatusCode 连续三代零新增)、死亡不撤钟(绑定 key,队列"死亡不吞元素"同轴、与屏障破障/phaser 摘除并列死亡三形态);三 Non-Goal=周期重挂/任务回调/绝对时刻装载均不做(用户裁决,理由钉入 design)。
- **2026-10-06 三档立项裁决(OPhaser)**:OCondition 落地同型复刻——OPhaser 于需求信号未至时提前立项(用户裁决"基于预研落地 OPhaser";三档"需求信号出现再评估"的规则以本行登记为放行依据,预研随 change 转正)。协议升至 v10 门;架构裁决=变异全入日志(注册/到场/离场逐条 `PHASER_OP_ENTRY`——"到场是状态迁移"沿 v5 Barrier,落地纪律第 2 条对本原语不豁免)、等待/取消/查询零日志(v9 双拓扑延伸:等待是订阅不是状态);三 Non-Goal=onAdvance 钩子/终止态粘滞/父子分层派生均不做(用户裁决,理由钉入 design);死亡语义=隐式摘除配额且已到场事实不撤销(与 Barrier"死亡即破障"刻意对照)。
- **2026-10-05 三档立项裁决**:OCondition 于需求信号未至时提前立项(用户裁决"先立项看看,尽量完善";三档"需求信号出现再评估"的规则以本行登记为放行依据,预研随 change 转正)。协议升至 v9 门;架构裁决=半入日志(await 的释放折叠经既有 LOCK_ACQUIRE_ENTRY 恰一条条目、SIGNAL/SIGNAL_ALL/LEAVE 零日志判例 v8 豁免类目化"signal 是事件不是状态")。OPhaser 维持三档不动。
- 库产物面**不得**启用 `--enable-preview`(W5 发布挂账已于 2026-10-01 随 1.0.0 发布清账,禁令延续至发布后仍适用);预览 API 转正前不入任何对外契约。
- 每个新 wire 类型随协议版本门引入(先例:v3 携带 FAIR/SEMAPHORE/LATCH)。

## 不做清单(冻结,重启需在此登记决策)

| 项 | 一句话理由 |
|---|---|
| `ConcurrentHashMap`/`ConcurrentMap` 分布式化 | 转 KV 存储:Raft 写放大 + 快照膨胀 + 一致性域完全变味;该需求正解是 Redis/Hazelcast |
| 远程执行(`ExecutorService`/`ForkJoinPool` 化) | 计算网格 = 另一产品线 |
| `CopyOnWrite*`/跳表系/并发 Deque | 全量数据复制,协调面契合度极低 |
| `LongAdder`/`Accumulator` 分布式化 | 条带红利在 RTT 瓶颈处消失;批量合并破坏即时可见契约 |
| `StampedLock` | 乐观读 validate 多一次 RTT、红利被抹平;版本戳已并入一档 CAS |
| `Exchanger`/`SynchronousQueue`/`TransferQueue` | 会合+载荷面窄;`OBlockingQueue`(含 0 容量形态)覆盖该需求 |
| `ThreadLocalRandom` | 进程本地语义;跨节点发号由 `OAtomicLong` 覆盖 |
| `VarHandle`/`Unsafe`/`java.lang.ref.*`/`LockSupport`/AQS 家族 | JVM 内部件,无 API 面映射 |

## 每个原语的落地纪律(提案立项时逐项列入任务)

1. proto 消息对 + 协议版本门(门随立项逐次递增,判例 v3–v7;OTopic 立项为 v8);
2. core 条目状态机 + Raft 命令路径 + 恢复(`CoreStateRestore` 同款);
3. ShadowTable/管理控制台/指标三件套登记;
4. 双语用户指南(docs/guide)与契约 Javadoc——**语义降级与增强处必须显式声明**;
5. E2E 对齐既有公平性/混沌/演练套件 + 同 key 同类型约定登记;
6. 提交前 `bash scripts/check-source-citations.sh` 自查。

## 维护规约

- 决策记录为**追加式历史账本**:每次定位/协议/次序/边界裁决补一行,记其依据,不回改已落行的历史;
- 定位边界变更(载荷限额伸缩、不做清单解冻)必须先在"决策记录"节补行,再动相关登记;
- 与 WATCHLIST 分工:`WATCHLIST.md` = 被动触发观察(触发 → 动作),本文件 = 主动裁决(决策 → 依据);
- 本文件不写长文——细节沉淀在提案与规格里。
