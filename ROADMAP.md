# 演进路线(Roadmap)

本表是主动演进方向与次序的决策清单,与 `WATCHLIST.md`(被动观察项)配对。
裁决、状态、次序的单一事实源在本文件;原语的契约细节永远沉淀在各自的提案与规格里。
最后更新:2026-10-09。

## 决策记录

- **2026-10-09 上下文传播评估裁决(工程改进第三项)**:评估结论=客户端**无 ambient 上下文需求**——会话身份全程显式传递(`SessionContext` 经 `Route` 捕获 + `Thread.currentThread().threadId()` 显式读),ROADMAP 原措辞设想的"线程本地袋"不存在;全仓库唯一真 `ThreadLocal` 在服务端 `EntryClock.APPLY_NOW`(apply 期入口)。落点据此收敛为**该 ThreadLocal 的 ScopedValue 机制替换**(纯内部、行为零变化,`skip_specs`)。实证:JEP 506 final 版 ScopedValue 绑定**仅**经 `StructuredTaskScope.fork` 继承(`Thread.start`/`ofPlatform`/`ofVirtual`/线程池一律不继承),故传播边界与 ThreadLocal 逐格相同、不引入语义变宽;`StructuredTaskScope` 仍预览(禁令排除)故不追求跨线程继承。第 52 行原"客户端内部会话/追踪上下文传递"定位据实校正为服务端机制替换。
- **2026-09-16 定位裁决**:采纳"B——允许小载荷协调数据结构"(每 key 限额、默认 4KB、服务端钳制)为能力边界;落地次序按"A——纯协调面先行"。来源:JDK 并发原语扩展的可行性分析(五轴:定位契合/机制复用/协议成本/语义保真/竞品先例)。
- **2026-10-01 发布裁决**:1.0.0 发布并打 tag `v1.0.0`;Maven Central 发布范围收窄至客户端 SDK 链(protocol/client/starter),core/server/console 退出中央仓库、服务端与控制台改经 GitHub Releases 可执行 jar 分发。WATCHLIST W5 发布挂账就此清账。
- **2026-10-07 三档立项裁决(OTimer)**:OCondition/OPhaser 同型复刻——"延时触发(定时单次标记)"于需求信号未至时提前立项(用户裁决"按 OCondition/OPhaser 先例登记决策行立项走 B";放行依据即本行)。立项前置核验收窄第 43 行既有"覆盖即可"结论:队列延时形态覆盖**延时交接**(一元素一消费者),但**广播单次标记**(一次到期全体共见)与**改期/撤销**两类 Timer 语义不可表达——该缺口即本原语的存在理由。协议升至 v11 门;架构裁决=**到期派生零条目**(装载/撤销入日志沿"迁移入日志"纪律,fire 为账簿与判定时刻的纯函数——"到期不是迁移,是时间的兑现",与租约"到期是状态迁移故入日志"构成判例族两端)、等待/取消/查询零日志(v9/v10 类目直接延伸)、DISARM 以既有 DENIED 首次可达面了结等待(StatusCode 连续三代零新增)、死亡不撤钟(绑定 key,队列"死亡不吞元素"同轴、与屏障破障/phaser 摘除并列死亡三形态);三 Non-Goal=周期重挂/任务回调/绝对时刻装载均不做(用户裁决,理由钉入 design)。
- **2026-10-06 三档立项裁决(OPhaser)**:OCondition 落地同型复刻——OPhaser 于需求信号未至时提前立项(用户裁决"基于预研落地 OPhaser";三档"需求信号出现再评估"的规则以本行登记为放行依据,预研随 change 转正)。协议升至 v10 门;架构裁决=变异全入日志(注册/到场/离场逐条 `PHASER_OP_ENTRY`——"到场是状态迁移"沿 v5 Barrier,落地纪律第 2 条对本原语不豁免)、等待/取消/查询零日志(v9 双拓扑延伸:等待是订阅不是状态);三 Non-Goal=onAdvance 钩子/终止态粘滞/父子分层派生均不做(用户裁决,理由钉入 design);死亡语义=隐式摘除配额且已到场事实不撤销(与 Barrier"死亡即破障"刻意对照)。
- **2026-10-05 三档立项裁决**:OCondition 于需求信号未至时提前立项(用户裁决"先立项看看,尽量完善";三档"需求信号出现再评估"的规则以本行登记为放行依据,预研随 change 转正)。协议升至 v9 门;架构裁决=半入日志(await 的释放折叠经既有 LOCK_ACQUIRE_ENTRY 恰一条条目、SIGNAL/SIGNAL_ALL/LEAVE 零日志判例 v8 豁免类目化"signal 是事件不是状态")。OPhaser 维持三档不动。
- 库产物面**不得**启用 `--enable-preview`(W5 发布挂账已于 2026-10-01 随 1.0.0 发布清账,禁令延续至发布后仍适用);预览 API 转正前不入任何对外契约。
- 每个新 wire 类型随协议版本门引入(先例:v3 携带 FAIR/SEMAPHORE/LATCH)。

## 状态图例

`未启动` | `进行中` | `已落地` | `挂起` | `不做`

## 一档 — 纯协调面(无载荷依赖,立即可行)

| 候选原语 | JDK 对应 | 核心语义 | 复用基建 | 关键验收点 | 状态 |
|---|---|---|---|---|---|
| `OAtomicLong`(含 Integer/Boolean 形态) | `AtomicLong` + `AtomicStampedReference`(版本戳合一,解决 ABA) | `get`/`incrementAndGet`/`addAndGet`/`getAndSet`/`compareAndSet`/`accumulateAndGet`;CAS 附每 key 单调版本戳 | 条目状态机 + Raft 命令通道(近似 SemaphoreEntry 去许可语义) | 并发 CAS 矩阵 E2E;版本戳单调性断言;值不绑定归属 `(session,threadId)` 需显式声明 | 已落地([提案归档](openspec/changes/archive/2026-09-19-add-oatomic-long/)) |
| `OBarrier` | `CyclicBarrier` | 多方集合、可复用、`barrierAction` 由最后到场者执行;**参与者会话死亡 → 即时破障**(整队列失败,强于 JDK,契约注释必须显式声明) | Latch 机制复用(等待队列/推送/队满护栏)+ 新建 `BarrierEntry` 世代状态机(提案裁决:复用机制而非扩类) | kill 进程裁决破障 E2E;相位复用后重组;队列满护栏 | 已落地([提案归档](openspec/changes/archive/2026-09-20-add-obarrier/)) |

## 二档 — 小载荷(前提:载荷通道;由本档首项开辟)

**载荷通道基建项**(随二档首个原语一并落地,已随 `add-oatomic-reference` 落地(归档见 openspec/changes/archive/2026-10-04-add-oatomic-reference/)):proto 载荷消息对、`maxValueBytes` 全局钳制配置(默认 4KB)、快照/压缩尺寸治理参数、控制台载荷展示(截断)。

| 候选原语 | JDK 对应 | 核心语义 | 关键验收点 | 状态 |
|---|---|---|---|---|
| `OAtomicReference` | `AtomicReference` | 引标量形态之上的有值引用:引用值 get/set/版本 CAS,≤`maxValueBytes` | 超限 `INVALID_REQUEST`;快照膨胀回归测 | 已落地([提案归档](openspec/changes/archive/2026-10-04-add-oatomic-reference/)) |
| `OBlockingQueue` | `BlockingQueue` + `DelayQueue` 变体 | 有界队列 `put`/`take`/`offer`/`poll`/`drainTo`;延时形态 = 元素带到期时刻(服务端定时器基建先例:租约到期) | FIFO 公平套件同款;元素生命周期绑定 key 而非会话——持有者死亡不吞元素,需契约声明 | 已落地([提案归档](openspec/changes/archive/2026-10-04-add-oblocking-queue/)) |
| `OTopic` | `Flow.Publisher`/`SubmissionPublisher` | 广播发布/订阅,协调级消息限额;**弱背压为显式契约**(慢消费者缓冲满的丢弃/断连策略在提案中裁决) | 多订阅者 fan-out E2E;取消订阅回收服务端订阅条目(防泄漏) | 已落地([提案归档](openspec/changes/archive/2026-10-05-add-otopic/)) |

## 三档 — 需求信号出现再评估

| 候选原语 | JDK 对应 | 备注 | 状态 |
|---|---|---|---|
| `OCondition` | `Condition` | 锁 key 下的服务端等待集(`await`/`signal`/`signalAll`,唤醒后重新竞争)。等待队列与推送机制现成,但虚假唤醒承诺、signal 权限、与租约到期交互需专章设计(三专章裁决见提案 design:D1 折叠与双拓扑、D3 权限分轨、D6 租约与换主分层)——已随 v9 落地兑现 | 已落地([提案归档](openspec/changes/archive/2026-10-06-add-ocondition/)) |
| `OPhaser` | `Phaser` | Barrier 的泛化(动态注册/分层派生),Barrier 落地后自然延伸。2026-10-06 提前立项裁决见决策记录;分层派生与 onAdvance 钩子、终止态为显式 Non-Goal(泛化的可判定子集,理由见提案 design);v10 三 Non-Goal 差异与死亡摘除契约已在双语指南 01 §12/03 章声明 | 已落地([提案归档](openspec/changes/archive/2026-10-06-add-ophaser/)) |
| 延时触发(定时单次标记) | `Timer` | 2026-10-07 立项裁决(见决策记录):队列延时形态仅覆盖延时交接,广播单次标记与改期撤销不可表达——单立 OTimer 兑现;v11 到期派生零条目裁决、DISARM 代终结 DENIED 线、死亡不撤钟三契约已在双语指南 01 §13/03 章声明;与 ODelayQueue 的选型分界矩阵入指南 03 | 已落地([提案归档](openspec/changes/archive/2026-10-07-add-otimer/)) |

## 工程改进(非对外 API 面,各立小 change)

| 事项 | JDK 对应 | 备注 | 状态 |
|---|---|---|---|
| 影子到期误摘 PHASER 修复(`ShadowTable.expireUpTo`) | —(非对外 API 面,镜像投影缺陷) | v11 落地审查发现的理论窗:`expireUpTo`(LEASE_EXPIRE_ENTRY 应用点的影子清扫)无租约跳过清单含 LATCH/BARRIER/QUEUE/ATOMIC/TIMER(v11 已带)而**独缺 PHASER**——phaser 镜像条目 `expiresAtMs` 恒 0,任一租约到期扫描会把它误摘→管理面 phaser 键瞬时失踪(引擎账簿无恙,后续镜像事件即自愈,故为观察面瞬时缺陷非状态腐化);一行修复+夹具:跳过清单补 `isPhaserType`,以"phaser 键与到期锁共存"回归红先钉住 | 已落地([change](openspec/changes/archive/2026-10-07-fix-shadow-sweep-phaser/),来源:PR #15 备注与 add-otimer 归档过程记录) |
| 客户端虚拟线程化 | `Thread.ofVirtual` | 阻塞 API 桥接与看门狗线程模型评估(`maven.compiler.release=25` 已就位);评估结论=桥接面 JDK25 下零钉扎、看门狗现形态(共享定时器驱动零按锁线程)保持不动,唯一实体线程点 OTopic 派发虚拟线程化 | 已落地([change](openspec/changes/archive/2026-10-07-client-virtual-threads/)) |
| 上下文传播 | `ScopedValue`(25 已转正) | 评估结论=客户端无 ambient 上下文需求(会话身份全程显式传递);全仓库唯一真 ThreadLocal 在服务端 `EntryClock`,以其 ScopedValue 机制替换兑现。JEP 506 final ScopedValue 仅经 `StructuredTaskScope.fork` 继承,边界与 ThreadLocal 逐格相同——纯内部重构、行为零变化(决策见"决策记录"2026-10-09 行) | 已落地([change](openspec/changes/archive/2026-10-09-refactor-entry-clock-scoped-value/)) |
| 结构化并发 | `StructuredTaskScope` | 25 仍预览且发布禁止 `--enable-preview`——挂起至转正 | 挂起 |

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

- 原语提案立项即更新本表(状态 → `进行中` 并附提案链接),归档时 → `已落地`;
- 定位边界变更(载荷限额伸缩、不做清单解冻、档位升降)必须先在"决策记录"节补行,再动表格;
- 与 WATCHLIST 分工:W 表 = 被动触发观察(触发 → 动作),本表 = 主动方向(决策 → 次序);
- 本表只增删行、不写长文——细节沉淀在提案与规格里。
