# user-documentation Specification

## Purpose
TBD - created by archiving change bilingual-user-guide. Update Purpose after archive.
## Requirements

### Requirement: 双语用户指南分区与章节覆盖

仓库 SHALL 提供 `docs/guide/zh/` 与 `docs/guide/en/` 双语对照用户指南，章节覆盖 MUST 完整：简介与架构、核心概念（租约、看门狗续租、锁丢失、等待-通知-重发、FIFO 公平、超时纪律、原子变量与版本戳、有值引用与小载荷边界、循环屏障与世代、有界队列与延时可见性——容量定型、双"满"语义、元素绑定 key 不绑定会话、到期时刻应用点折算、广播发布/订阅与弱背压——至多一次、drop-newest 两级缓冲、`topic_seq` 仅同 Leader term 内有序、订阅绑定会话（死亡即退订，与队列"死亡不吞元素"刻意相反）、"一 key 一形态"对 topic 为契约声明而非机制互斥、条件变量与等待集——await 全量释放折叠与唤醒重获取（返回时持锁、重入从 1 起）、signal 须持有归属（服务端权威）、命名寻址跨进程等价（对 JDK 句柄身份的适配）、虚假唤醒允许与 guard loop 调用方义务、"等待是承诺、signal 是事件"的换主分层语义、**Phaser 与相位——动态注册与每会话配额、到场与等待解耦（到场相位回显/等待了结返回当前相位）、"到场 ≥ 注册"合拢与相位号单调不复用、参与者会话死亡→隐式配额摘除且已到场事实不撤销（与屏障"死亡即破障"对照呈现）、注册归零=空转可复活不终止（与 JDK 归零终止对照）、onAdvance 钩子/终止态/父子分层三不提供为显式 Non-Goal**）、快速上手、客户端 SDK（各锁类型、同步原语、原子变量族 `OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean`、有值引用 `OAtomicReference`、循环屏障 `OBarrier`、队列 `OBlockingQueue`/`ODelayQueue`、广播 `OTopic`、条件变量 `OCondition`（经 `OLock.newCondition(name)`）与 **Phaser `OPhaser`（经 `OClient.newPhaser`）**的 API 及语义边界——含每操作一次网络往返与 `drainTo` 批量摊薄、值与元素不绑定会话归属、超时不确定窗口与自动重发裁决、ABA 与 stamped 形态的区分、条目常驻不回收、载荷不透明且服务端权威钳制（默认 4KB）、null 与空串可区分（引用形态）与元素/消息体不可为 null（队列与 topic 形态，多语义差异须并置声明防混读）、无算术操作面、屏障的到场合拢/世代回卷/动作两阶段放行/离场即破障与相对 JDK 的异常形态差异、队列的阻塞/立即双形态与中断语义、延时元素的可见性规则与同到期 FIFO 增强、`offerDelayed` 对 JDK 签名的改名差异、topic 的发布回执仅表"已受理入 fan-out"、换主窗不重放与自动重订阅、跨换主重试可双投与消费侧幂等义务、`droppedCount()` 的 gap 推断口径与 term 基线重置、监听器单订阅串行与 dispatcher 线程、订阅的常驻登记与缓冲成本、条件的 guard loop 标准惯用法示例、await 超时/中断仍"返回时持锁"与返回值 best-effort 语义、`signal()`/`signalAll()` 非持有者的双层 `IllegalMonitorStateException`、await 全量释放与唤醒后重入 1 级的计数算术、等待者无租约与持有者死亡不代为 signal（推荐 timed await 自救）、换主自动重挂但窗内 signal 不补偿、读写锁形态不支持条件（`UnsupportedOperationException`）、await/signal 的 RTT 成本模型、**phaser 的配额严格归属（`arriveAndDeregister` 仅扣本会话配额、零配额拒绝——JDK 匿名 party 未定义行为的显式化）、`awaitAdvance` 旁观等待无需注册配额、查询为 Leader 本地非线性化读数（即刻过期是契约）、无 onAdvance 时以返回值自实现动作的惯用法、等待谓词跨换主自愈与条件 signal 丢失窗的对照防混读、register/arrive/await 各操作的 RTT 成本模型**）、Spring Boot starter（`@OpenLatch`、SpEL、锁先于事务、AOP 自调用局限）、集群部署与运维（拓扑、种子发现、Leader 迁移、滚动重启序与 supervisor 配置、载荷下的快照尺寸治理与版本回滚窗口注记、队列维度的治理注记——条目日志率与 W6/W7 同型观察、`max-queue-capacity`/`max-drain-bytes`/`ready-tick-ms` 限额语义、队列存续期下调 `maxValueBytes` 的先清大元素警示、回滚前队列 key 清空或接受不可用、topic 维度的治理注记——`max-subscribers-per-key`/`max-subscription-buffer` 限额语义与 drop 率告警口径、topic 零持久态使回滚 v7 窗口天然干净（无清 key 要求，与队列回滚注记对照呈现）、fan-out 放大面（订阅数×发布率）的容量评估提示、条件维度的治理注记——零新配置（条件等待人数并入 `max-queue-depth-per-key` 合计口径的说明）、条件无持久态（等待集进程易失、快照零足迹）使回滚窗口无 key 可清、唯一回滚约束为 v9 会话收敛（区间外握手即拒属既有纪律）、**phaser 维度的治理注记——`max-parties-per-phaser` 限额语义与 registered 水位告警口径、phaser 有持久账簿（相位/配额/到场）与快照新字段使回滚为屏障/队列同型第四口径（降级前确认无在途 phaser 流量或接受 phaser key 在旧二进制不可用，与 topic/condition"天然干净"并列防混读）、到场条目率随 parties×相位频率增长的 W14 观察口径、W11 换主窗对 phaser 直发车道的作用面与等待侧自愈的非对称声明**）、安全（TLS/mTLS、业务 Token 与轮换流程、管理 Token 独立性）、管理控制台（含有值引用条目的大小+截断预览、队列条目的容量/深度/驻留字节/首元素预览呈现、topic 条目的订阅数+订阅者列表呈现、锁详情的条件等待区段（条件名/会话/请求/线程/等待时长，与等待队列并列不重复计数）与 **phaser 条目的三计数+配额明细+等待明细呈现（Follower 账簿可读而等待明细如实空的"双速呈现"标注）**，全量元素列表与已交付消息内容不外发；topic 键仅 Leader 视角可见、条件明细仅 Leader 视角非零**与 phaser 等待明细仅 Leader 视角非零（账簿各节点一致）**的口径说明）、可观测性（/metrics、/healthz、管理端口；`atomic.total` kind 词表含 `reference`、`queue.total` op 词表、`topic.total` op 词表与 `REJECT_SUBSCRIBERS` 取值、`condition.total` op 词表与 `NOT_HELD` 权限线（含 await 计数归 `acquire.total` 不设 await 线的折叠口径说明）、**`phaser.total` op 词表与两超限 `OVERLOADED` 的 op 分轨判读（register 线=配额、await_advance 线=深度）**、`elements.depth.max`/`queue.depth.max`/`topic.subscribers.max`/`condition.waiters.max`/`phaser.parties.registered.max` **五口径之辨**、`topic.dropped.total` 的告警阈值建议、`condition.waiters.max` 水位与换主窗 signal 丢失（W13）的告警口径、**`phaser.parties.registered.max` 逼近限额与 `{register,OVERLOADED}`/`{await_advance,OVERLOADED}` 增速的告警建议**）、故障排查与 FAQ（错误码语义如 NOT_LEADER/NOT_HELD（**双义判别：释放校验与 signal 权限——以请求类型区分**）/BARRIER_BROKEN/REJECT_SUBSCRIBERS、载荷超限 INVALID_REQUEST、元素满 DENIED 与等待满 OVERLOADED 的判读、队列挂起无响应的唤醒链排查、topic"换主后收不到消息"的判别路径（home 重连日志 → SUBSCRIBE 重放 → 服务端 drop 计数与客户端 gap 推断三分法）、await 不醒的判别三分法（signal 权限被拒——`condition.total{NOT_HELD}` 线 → 换主窗 signal 丢失——比对 await 重挂日志与换主时点 → 缺 guard loop 的虚假唤醒谓词复查失误——审查调用方守卫）、**相位不推进的判别三分法（到场计数未达注册——QUERY/管理面比对 arrived 与 registered 及配额明细找缺席会话 → 等待方车道窗——W11 型直发车道显式超时判别（区别于条件：phaser 唤醒谓词在复制态、重挂自愈，不推进的根因在"到场不足"非"事件丢失"）→ 应用侧注册/配额簿记错误——arriveAndDeregister 透支拒绝的计数核对）**、恢复窗口、双活审计）、兼容性与协议版本（Java 25、Spring Boot 4.x only、协议 v1/v2/v3/v4/v5/v6/v7/v8/v9/v10 协商与"不做隐式兼容"、v8 topic、v9 condition 与 **v10 phaser** 需双端升级且服务端先行、v9 客户端连 v10 服务端照常、v7 客户端连 v8 服务端照常、v8 客户端连 v9 服务端照常的兼容方向声明）、术语表（含版本戳、去重槽、初值主张、世代、了结记录、执行者、离场即破障、载荷钳制、截断预览、定型容量、出队谓词、到期折算、双轨等待、应用点回弹、广播 term、topic_seq、弱背压、drop-newest、gap 推断、订阅路由键、条件等待集、命名寻址、释放折叠、搬运、signal 事件性、返回时持锁、guard loop 义务、**到场相位、应到集合、注册配额、隐式摘除、空转复活、双速呈现**）。指南 MUST 是用户细节的唯一权威载体，README 仅保留最小上手闭环与索引。

#### Scenario: 新用户完成学习闭环

- **WHEN** 用户从 README 进入 guide，按 00→02 顺序执行
- **THEN** 无需打开 `docs/design/` 任何文件即可完成"理解概念→跑通单机→部署集群"全路径

#### Scenario: 双语页面对账

- **WHEN** 任一语言的指南页在其对侧缺失
- **THEN** 该侧索引页显式标注缺失（不允许静默不对称）

#### Scenario: 原子变量语义降级处有言明

- **WHEN** 审阅指南与契约 Javadoc 中 `OAtomicLong` 相关段落
- **THEN** 相对 JDK 的每项语义差异（网络往返延迟、不确定超时窗口的效果边界、ABA 仅在 stamped 形态消除、溢出 wrap 域、条目永不回收）均有显式声明，无只描述成功路径的乐观表述

#### Scenario: 有值引用语义差异处有言明

- **WHEN** 审阅指南与契约 Javadoc 中 `OAtomicReference` 相关段落
- **THEN** 相对 JDK `AtomicReference` 的每项差异均有显式声明——载荷为不透明字节（服务端不解释、无反序列化，对象需应用层自行编码）、尺寸由服务端入口权威钳制且超限拒绝零生效、钳制下调不追溯存量值、null 与空串两态可区分、无 `compareAndExchange`/`weakCompareAndSet` 族、条目永不回收的驻留成本、协议需 v6——并给出快照/日志尺寸增长的运维注记与回滚窗口警示（回滚至 v6 前二进制时引用条目不可用）

#### Scenario: 循环屏障语义差异处有言明

- **WHEN** 审阅指南与契约 Javadoc 中 `OBarrier` 相关段落
- **THEN** 相对 JDK `CyclicBarrier` 的每项差异均有显式声明——增强面（参与者会话死亡即时破障而非静默挂起、破障跨进程可观测）与差异面（破障为世代局部无 JDK 粘滞语义、无 `reset()`、受检异常改非受检与 boolean 形态、`await` 超时会破障且连带他方、在途到场不跨会话自动重放、动作异常经离场路径放大为全体破障、`isBroken()` 为句柄本地读数）逐条列明，并给出与 JDK 代码迁移时的对照注记

#### Scenario: 队列语义差异处有言明

- **WHEN** 审阅指南与契约 Javadoc 中 `OBlockingQueue`/`ODelayQueue` 相关段落
- **THEN** 相对 JDK `BlockingQueue`/`DelayQueue` 的每项差异均有显式声明——增强面（投递者进程死亡不吞元素、同到期时刻 FIFO、容量跨进程定型）、差异面（元素不可为 null 且为不透明字节、无 `iterator/contains/remove(Object)` 面、`offerDelayed` 改名之于 JDK `offer(e,timeout,unit)` 语义混读风险、`poll/offer(timeout)` 为客户端本地计时、延时到点唤醒精度为服务端 tick 级、阻塞消费经等待-通知-重发闭环且应用点竞态回弹续挂、条目常驻不回收与驻留成本、容量/载荷/drain 预算由服务端权威钳制）逐条列明，并给出与引用形态 null 语义"看似相仿实则相反"的防混读对照与回滚窗口警示（回滚至 v7 前二进制时队列条目不可用）

#### Scenario: topic 语义差异处有言明

- **WHEN** 审阅指南与契约 Javadoc 中 `OTopic` 相关段落
- **THEN** 相对 JDK `Flow.Publisher`/`SubmissionPublisher` 的每项差异均有显式声明——至多一次交付且发布回执仅表受理、弱背压 drop-newest 两级缓冲（显式不采纳 onOverflow-close 及其会话死亡连带的理由）、无 `request(n)` 强背压面、`topic_seq` 仅同 Leader term 内有序且跨 term 重置（gap 推断口径随注）、订阅绑定会话死亡即退订（与队列"死亡不吞元素"的对照防混读句随行呈现）、换主窗不重放与 SDK 自动重订阅的透明边界、跨换主重试可双投与消费侧幂等义务、监听器串行与 dispatcher 线程模型、订阅常驻登记与缓冲成本、"一 key 一形态"为契约声明（撞 key 探测尽力而为）、协议需 v8——并给出 `REJECT_SUBSCRIBERS`/`dropped.total` 的运维告警口径与"零持久态回滚干净"的窗口注记

#### Scenario: 条件语义差异处有言明

- **WHEN** 审阅指南与契约 Javadoc 中 `OCondition`/`OLock.newCondition` 相关段落
- **THEN** 相对 JDK `Condition` 的每项差异与每项保真均有显式声明——保真面（虚假唤醒允许且 guard loop 为调用方义务、signal 权限的 `IllegalMonitorStateException` 双层、超时/中断"返回时持锁"、唤醒后重入 1 级的计数算术、空集 signal 无操作、持有者死亡不代为唤醒）与差异面（命名寻址替代句柄身份且跨进程等价、await 权限服务端降级而 signal 服务端权威的分轨及理由、await 至少两次 RTT 与 signal 一次的即时回执成本模型、换主后等待项自动重挂而窗内 signal 不补偿的分层语义、读写形态不支持、无 `awaitNanos`/`awaitUntil`/`awaitUninterruptibly`/异步对偶、条件等待人数并入 key 等待合计）逐条列明，并给出与 topic"signal 是事件/等待是承诺"对照的防混读句与"无人 signal 则永睡——推荐 timed await"的运维提示

#### Scenario: phaser 语义差异处有言明

- **WHEN** 审阅指南与契约 Javadoc 中 `OPhaser` 相关段落
- **THEN** 相对 JDK `Phaser` 的每项差异均有显式声明——保真面（动态注册/离场、到场与等待解耦、按已见相位等待、到场相位回显与相位单调、`getUnarrivedParties` 口径、旁观者 awaitAdvance 无需配额）、差异面（无 onAdvance 钩子及其返回值自实现替代惯用法、无终止态——归零空转可复活与 JDK 粘滞终止相反及理由、无父子分层派生及 RTT 论证、`arriveAndDeregister`/`bulkRegister` 配额严格归属本会话——JDK 匿名 party 未定义行为显式化拒绝、每次调用至少一次 RTT 的成本模型与查询非线性化声明、无 `register()` 即时本地计数红利）与增强面（跨进程参与者同相合拢、等待谓词跨换主自愈——与条件 signal 丢失窗的对照防混读句随行、死亡→隐式摘除不空转且与屏障"死亡即破障"的对照表呈现）逐条列明，并给出 `max-parties-per-phaser` 限额与 W14 条目率观察的运维注记、回滚窗口第四口径警示（回滚至 v9 前二进制时 phaser key 不可用）

#### Scenario: 回滚窗口注记按原语分轨

- **WHEN** 审阅指南部署章的回滚窗口注记
- **THEN** 队列（回滚前清大 key 或接受不可用）、topic（零持久态天然干净）、条件（零持久态、无 key 可清、唯一约束为活跃 v9 会话收敛——区间外握手即拒的既有纪律显形）与 **phaser（有持久账簿与快照字段——屏障/队列同型：回滚前确认无在途 phaser 流量或接受 phaser key 在旧二进制不可用，与 topic/condition"天然干净"并列为第四口径、互不混读）**四行并列呈现，升级序（服务端先行）对 v8/v9/**v10** 统一表述
### Requirement: 指南内容纪律

指南技术表述 MUST 与实现及定案验收口径一致（含"重启语义单机/集群分立"“锁可能丢失必须处理回调”等既有 README 正确表述的承接）；示例命令 MUST 可在通用环境复制执行（不依赖任何私有配置）；指南 MUST NOT 引用内部过程文档与代号（《详细设计说明书》、验收报告、Phase 代号、design DN 等——内部引用检查模式集扩展至 `docs/guide/**`）；对未提供的能力（如控制台写操作）MUST 如实标注"未提供"而非省略或暗示存在。

#### Scenario: 门禁覆盖指南

- **WHEN** 某指南页引入"详设 §5.2"字样
- **THEN** 反内部引用检查（含 guide 目录模式）非零退出

#### Scenario: 排障答案与实现一致

- **WHEN** 用户按 FAQ 处理 NOT_LEADER 响应
- **THEN** 指南描述的语义与协议实现/错误码定义逐字一致
