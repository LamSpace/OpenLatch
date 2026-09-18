# 演进路线(Roadmap)

本表是主动演进方向与次序的决策清单,与 `WATCHLIST.md`(被动观察项)配对。
裁决、状态、次序的单一事实源在本文件;原语的契约细节永远沉淀在各自的提案与规格里。
最后更新:2026-09-16。

## 决策记录

- **2026-09-16 定位裁决**:采纳"B——允许小载荷协调数据结构"(每 key 限额、默认 4KB、服务端钳制)为能力边界;落地次序按"A——纯协调面先行"。来源:JDK 并发原语扩展的可行性分析(五轴:定位契合/机制复用/协议成本/语义保真/竞品先例)。
- 库产物面**不得**启用 `--enable-preview`(发布未定,见 WATCHLIST W5);预览 API 转正前不入任何对外契约。
- 每个新 wire 类型随协议版本门引入(先例:v3 携带 FAIR/SEMAPHORE/LATCH)。

## 状态图例

`未启动` | `进行中` | `已落地` | `挂起` | `不做`

## 一档 — 纯协调面(无载荷依赖,立即可行)

| 候选原语 | JDK 对应 | 核心语义 | 复用基建 | 关键验收点 | 状态 |
|---|---|---|---|---|---|
| `OAtomicLong`(含 Integer/Boolean 形态) | `AtomicLong` + `AtomicStampedReference`(版本戳合一,解决 ABA) | `get`/`incrementAndGet`/`addAndGet`/`getAndSet`/`compareAndSet`/`accumulateAndGet`;CAS 附每 key 单调版本戳 | 条目状态机 + Raft 命令通道(近似 SemaphoreEntry 去许可语义) | 并发 CAS 矩阵 E2E;版本戳单调性断言;值不绑定归属 `(session,threadId)` 需显式声明 | 已落地([提案归档](openspec/changes/archive/2026-09-19-add-oatomic-long/)) |
| `OBarrier` | `CyclicBarrier` | 多方集合、可复用、`barrierAction` 由最后到场者执行;**参与者会话死亡 → 即时破障**(整队列失败,强于 JDK,契约注释必须显式声明) | `LatchEntry` 状态机扩展(可复用相位)+ 等待队列推送 | kill 进程裁决破障 E2E;相位复用后重组;队列满护栏 | 未启动 |

## 二档 — 小载荷(前提:载荷通道;由本档首项开辟)

**载荷通道基建项**(随二档首个原语一并落地):proto 载荷消息对、`maxValueBytes` 全局钳制配置(默认 4KB)、快照/压缩尺寸治理参数、控制台载荷展示(截断)。

| 候选原语 | JDK 对应 | 核心语义 | 关键验收点 | 状态 |
|---|---|---|---|---|
| `OAtomicReference` | `AtomicReference` | 引标量形态之上的有值引用:引用值 get/set/版本 CAS,≤`maxValueBytes` | 超限 `INVALID_REQUEST`;快照膨胀回归测 | 未启动 |
| `OBlockingQueue` | `BlockingQueue` + `DelayQueue` 变体 | 有界队列 `put`/`take`/`offer`/`poll`/`drainTo`;延时形态 = 元素带到期时刻(服务端定时器基建先例:租约到期) | FIFO 公平套件同款;元素生命周期绑定 key 而非会话——持有者死亡不吞元素,需契约声明 | 未启动 |
| `OTopic` | `Flow.Publisher`/`SubmissionPublisher` | 广播发布/订阅,协调级消息限额;**弱背压为显式契约**(慢消费者缓冲满的丢弃/断连策略在提案中裁决) | 多订阅者 fan-out E2E;取消订阅回收服务端订阅条目(防泄漏) | 未启动 |

## 三档 — 需求信号出现再评估

| 候选原语 | JDK 对应 | 备注 | 状态 |
|---|---|---|---|
| `OCondition` | `Condition` | 锁 key 下的服务端等待集(`await`/`signal`/`signalAll`,唤醒后重新竞争)。等待队列与推送机制现成,但虚假唤醒承诺、signal 权限、与租约到期交互需专章设计 | 未启动 |
| `OPhaser` | `Phaser` | Barrier 的泛化(动态注册/分层派生),Barrier 落地后自然延伸 | 未启动 |
| 延时触发(定时单次标记) | `Timer` | 由 `OBlockingQueue` 延时形态覆盖即可,不单立 | 挂起 |

## 工程改进(非对外 API 面,各立小 change)

| 事项 | JDK 对应 | 备注 | 状态 |
|---|---|---|---|
| 客户端虚拟线程化 | `Thread.ofVirtual` | 阻塞 API 桥接与看门狗线程模型评估(`maven.compiler.release=25` 已就位) | 未启动 |
| 上下文传播 | `ScopedValue`(25 已转正) | 客户端内部会话/追踪上下文传递评估,替代线程本地袋 | 未启动 |
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

1. proto 消息对 + 协议版本门(下一个门为 v4,握手版本判例沿用 v3);
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
