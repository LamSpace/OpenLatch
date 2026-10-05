# user-documentation Specification

## Purpose
TBD - created by archiving change bilingual-user-guide. Update Purpose after archive.
## Requirements
### Requirement: 双语用户指南分区与章节覆盖

仓库 SHALL 提供 `docs/guide/zh/` 与 `docs/guide/en/` 双语对照用户指南，章节覆盖 MUST 完整：简介与架构、核心概念（租约、看门狗续租、锁丢失、等待-通知-重发、FIFO 公平、超时纪律、原子变量与版本戳、有值引用与小载荷边界、循环屏障与世代、有界队列与延时可见性——容量定型、双"满"语义、元素绑定 key 不绑定会话、到期时刻应用点折算、广播发布/订阅与弱背压——至多一次、drop-newest 两级缓冲、`topic_seq` 仅同 Leader term 内有序、订阅绑定会话（死亡即退订，与队列"死亡不吞元素"刻意相反）、"一 key 一形态"对 topic 为契约声明而非机制互斥）、快速上手、客户端 SDK（各锁类型、同步原语、原子变量族 `OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean`、有值引用 `OAtomicReference`、循环屏障 `OBarrier`、队列 `OBlockingQueue`/`ODelayQueue` 与广播 `OTopic` 的 API 及语义边界——含每操作一次网络往返与 `drainTo` 批量摊薄、值与元素不绑定会话归属、超时不确定窗口与自动重发裁决、ABA 与 stamped 形态的区分、条目常驻不回收、载荷不透明且服务端权威钳制（默认 4KB）、null 与空串可区分（引用形态）与元素/消息体不可为 null（队列与 topic 形态，多语义差异须并置声明防混读）、无算术操作面、屏障的到场合拢/世代回卷/动作两阶段放行/离场即破障与相对 JDK 的异常形态差异、队列的阻塞/立即双形态与中断语义、延时元素的可见性规则与同到期 FIFO 增强、`offerDelayed` 对 JDK 签名的改名差异、topic 的发布回执仅表"已受理入 fan-out"、换主窗不重放与自动重订阅、跨换主重试可双投与消费侧幂等义务、`droppedCount()` 的 gap 推断口径与 term 基线重置、监听器单订阅串行与 dispatcher 线程、订阅的常驻登记与缓冲成本）、Spring Boot starter（`@OpenLatch`、SpEL、锁先于事务、AOP 自调用局限）、集群部署与运维（拓扑、种子发现、Leader 迁移、滚动重启序与 supervisor 配置、载荷下的快照尺寸治理与版本回滚窗口注记、队列维度的治理注记——条目日志率与 W6/W7 同型观察、`max-queue-capacity`/`max-drain-bytes`/`ready-tick-ms` 限额语义、队列存续期下调 `maxValueBytes` 的先清大元素警示、回滚前队列 key 清空或接受不可用、topic 维度的治理注记——`max-subscribers-per-key`/`max-subscription-buffer` 限额语义与 drop 率告警口径、**topic 零持久态使回滚 v7 窗口天然干净（无清 key 要求，与队列回滚注记对照呈现）**、fan-out 放大面（订阅数×发布率）的容量评估提示）、安全（TLS/mTLS、业务 Token 与轮换流程、管理 Token 独立性）、管理控制台（含有值引用条目的大小+截断预览、队列条目的容量/深度/驻留字节/首元素预览呈现与 topic 条目的订阅数+订阅者列表呈现，全量元素列表与已交付消息内容不外发；topic 键仅 Leader 视角可见的口径说明）、可观测性（/metrics、/healthz、管理端口；`atomic.total` kind 词表含 `reference`、`queue.total` op 词表、`topic.total` op 词表与 `REJECT_SUBSCRIBERS` 取值、`elements.depth.max`/`queue.depth.max`/`topic.subscribers.max` 三口径之辨、`topic.dropped.total` 的告警阈值建议）、故障排查与 FAQ（错误码语义如 NOT_LEADER/NOT_HELD/BARRIER_BROKEN/REJECT_SUBSCRIBERS、载荷超限 INVALID_REQUEST、元素满 DENIED 与等待满 OVERLOADED 的判读、队列挂起无响应的唤醒链排查、**topic"换主后收不到消息"的判别路径（home 重连日志 → SUBSCRIBE 重放 → 服务端 drop 计数与客户端 gap 推断三分法）**、恢复窗口、双活审计）、兼容性与协议版本（Java 25、Spring Boot 4.x only、协议 v1/v2/v3/v4/v5/v6/v7/v8 协商与"不做隐式兼容"、v8 topic 需双端升级且服务端先行、v7 客户端连 v8 服务端照常的兼容方向声明）、术语表（含版本戳、去重槽、初值主张、世代、了结记录、执行者、离场即破障、载荷钳制、截断预览、定型容量、出队谓词、到期折算、双轨等待、应用点回弹、广播 term、topic_seq、弱背压、drop-newest、gap 推断、订阅路由键）。指南 MUST 是用户细节的唯一权威载体，README 仅保留最小上手闭环与索引。

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
### Requirement: 指南内容纪律

指南技术表述 MUST 与实现及定案验收口径一致（含"重启语义单机/集群分立"“锁可能丢失必须处理回调”等既有 README 正确表述的承接）；示例命令 MUST 可在通用环境复制执行（不依赖任何私有配置）；指南 MUST NOT 引用内部过程文档与代号（《详细设计说明书》、验收报告、Phase 代号、design DN 等——内部引用检查模式集扩展至 `docs/guide/**`）；对未提供的能力（如控制台写操作）MUST 如实标注"未提供"而非省略或暗示存在。

#### Scenario: 门禁覆盖指南

- **WHEN** 某指南页引入"详设 §5.2"字样
- **THEN** 反内部引用检查（含 guide 目录模式）非零退出

#### Scenario: 排障答案与实现一致

- **WHEN** 用户按 FAQ 处理 NOT_LEADER 响应
- **THEN** 指南描述的语义与协议实现/错误码定义逐字一致

