# 决策账本(Decision Ledger)

本文件是项目裁决的单一事实源:每次影响定位/协议/次序/边界的决定在此追加一行。
原语与契约的细节永远沉淀在各自的提案与规格里(见 `openspec/`);被动观察项见 `WATCHLIST.md`(配对文件)。
最后更新:2026-10-10。

## 决策记录

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
