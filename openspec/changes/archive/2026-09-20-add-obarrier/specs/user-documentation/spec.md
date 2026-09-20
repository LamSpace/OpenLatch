## MODIFIED Requirements

### Requirement: 双语用户指南分区与章节覆盖

仓库 SHALL 提供 `docs/guide/zh/` 与 `docs/guide/en/` 双语对照用户指南，章节覆盖 MUST 完整：简介与架构、核心概念（租约、看门狗续租、锁丢失、等待-通知-重发、FIFO 公平、超时纪律、原子变量与版本戳、循环屏障与世代）、快速上手、客户端 SDK（各锁类型、同步原语、原子变量族 `OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean` 与循环屏障 `OBarrier` 的 API 及语义边界——含每操作一次网络往返、值不绑定会话归属、超时不确定窗口与自动重发裁决、ABA 与 stamped 形态的区分、条目常驻不回收、屏障的到场合拢/世代回卷/动作两阶段放行/离场即破障与相对 JDK 的异常形态差异）、Spring Boot starter（`@OpenLatch`、SpEL、锁先于事务、AOP 自调用局限）、集群部署与运维（拓扑、种子发现、Leader 迁移、滚动重启序与 supervisor 配置）、安全（TLS/mTLS、业务 Token 与轮换流程、管理 Token 独立性）、管理控制台、可观测性（/metrics、/healthz、管理端口）、故障排查与 FAQ（错误码语义如 NOT_LEADER/NOT_HELD/BARRIER_BROKEN、恢复窗口、双活审计）、兼容性与协议版本（Java 25、Spring Boot 4.x only、协议 v1/v2/v3/v4/v5 协商与"不做隐式兼容"）、术语表（含版本戳、去重槽、初值主张、世代、了结记录、执行者、离场即破障条目）。指南 MUST 是用户细节的唯一权威载体，README 仅保留最小上手闭环与索引。

#### Scenario: 新用户完成学习闭环

- **WHEN** 用户从 README 进入 guide，按 00→02 顺序执行
- **THEN** 无需打开 `docs/design/` 任何文件即可完成"理解概念→跑通单机→部署集群"全路径

#### Scenario: 双语页面对账

- **WHEN** 任一语言的指南页在其对侧缺失
- **THEN** 该侧索引页显式标注缺失（不允许静默不对称）

#### Scenario: 原子变量语义降级处有言明

- **WHEN** 审阅指南与契约 Javadoc 中 `OAtomicLong` 相关段落
- **THEN** 相对 JDK 的每项语义差异（网络往返延迟、不确定超时窗口的效果边界、ABA 仅在 stamped 形态消除、溢出 wrap 域、条目永不回收）均有显式声明，无只描述成功路径的乐观表述

#### Scenario: 循环屏障语义差异处有言明

- **WHEN** 审阅指南与契约 Javadoc 中 `OBarrier` 相关段落
- **THEN** 相对 JDK `CyclicBarrier` 的每项差异均有显式声明——增强面（参与者会话死亡即时破障而非静默挂起、破障跨进程可观测）与差异面（破障为世代局部无 JDK 粘滞语义、无 `reset()`、受检异常改非受检与 boolean 形态、`await` 超时会破障且连带他方、在途到场不跨会话自动重放、动作异常经离场路径放大为全体破障、`isBroken()` 为句柄本地读数）逐条列明，并给出与 JDK 代码迁移时的对照注记
