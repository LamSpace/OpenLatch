# Spot-Check Sheet: javadoc-internal-citation-cleanup (task 4.4)

- 抽样方式：自 `citations-baseline.md` 全部 648 处命中随机抽 30 处（seed=20260912，可复现）。
- 核对项：清理前后语义无损——引用删除后注释自洽；契约/状态机/判据本体完整；无悬空标点。
- 状态：待逐条核对并人工评审签字后，本变更出阶段。

| # | 位置 | 清理前（baseline 原文） | 清理后（工作区） |
|---|------|--------------------------|-------------------|
| 1 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OLock.java:23 | * JUC 风格的同步锁句柄（详设 §6.3）。 * JUC 风格的同步锁句柄。 |
| 2 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:107 | * 获取车道（Leader 车道，design D6）：{@code null} 即稳态单连接——home 即 * 获取车道（Leader 车道）：{@code null} 即稳态单连接——home 即 |
| 3 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:946 | * 并尝试收口因此清空的待退车道（design D6"存量清零→连接收口"）。 * 并尝试收口因此清空的待退车道（"存量清零→连接收口"）。 |
| 4 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1413 | /** 是否启用 TLS（Phase 3 T4，spec"客户端 TLS 与认证消费"）；默认 false=明文。 */ /** 是否启用 TLS；默认 false=明文。 */ |
| 5 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1604 | * 设置业务令牌（服务端业务认证开启时 HELLO 校验；spec"客户端 TLS 与 （整句删除） |
| 6 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:43 | * 等待跟踪器（详设 §6.5）：管理排队中的获取请求全生命周期。 * 等待跟踪器：管理排队中的获取请求全生命周期。 |
| 7 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:53 | *   ├─ AWAIT_NOTIFY → 以同一 requestId 重发（服务端幂等，§4.8） *   ├─ AWAIT_NOTIFY → 以同一 requestId 重发（服务端幂等） |
| 8 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientSecurity.java:26 | * 客户端安全装配工具（Phase 3 详设 §5.1/§5.2，spec"客户端 TLS 与认证消费"）： （整句删除） |
| 9 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:223 | * 全参构造（S3 多车道，design D6）：车道以显式初始目标建连。 * 全参构造（多车道形态）：车道以显式初始目标建连。 |
| 10 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/HeldLockRegistry.java:28 | * 本地持锁簿记（详设 §6.1/§6.3）。 * 本地持锁簿记。 |
| 11 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/SeedDiscovery.java:48 | * 种子扇出发现（详设 §6.3"连续 N 次 NOT_LEADER → 强制走一次种子列表发现"， （整句删除） |
| 12 | openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:35 | * 看门狗：持锁期间的自动续租调度（详设 §6.6）。 * 看门狗：持锁期间的自动续租调度。 |
| 13 | openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClient.java:109 | * 构造单节点客户端（不连接；明文、无业务令牌——T4 前的既有形态）。 * 构造单节点客户端（不连接；明文、无业务令牌）。 |
| 14 | openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClientPool.java:34 | * 节点连接池（Phase 3 T3 design D6，spec"节点连接管理与故障降级"）： * 节点连接池： |
| 15 | openlatch-core/src/main/java/io/github/lamspace/openlatch/core/KeyFamily.java:29 | *   <li>{@link #SEMAPHORE}——许可门闸家族（{@code SemaphoreEntry} 承载，Phase 3 T1 引入）；</li> （整句删除） |
| 16 | openlatch-core/src/main/java/io/github/lamspace/openlatch/core/LockType.java:48 | * 顺序的保证由 FIFO 队列本体承载（详设 §2.2）。 * 顺序的保证由 FIFO 队列本体承载。 |
| 17 | openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockEntry.java:53 | * <p><b>并发模型</b>（设计说明书 §4.9）：条目内所有状态迁移都在 * <p><b>并发模型</b>：条目内所有状态迁移都在 |
| 18 | openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/SemaphoreEntry.java:435 | * 明细只读快照（Phase 3 T3，spec"明细只读观察面"）：条目锁内拷贝 * 明细只读快照：条目锁内拷贝 |
| 19 | openlatch-core/src/main/java/io/github/lamspace/openlatch/core/result/Outcome.java:63 | * （Phase 3 T1 详设 §2.3 / design D1）；条目状态零扰动，server 层 * 的 {@code permitsTotal}，或既有条目上非零主张与定型值不符； * 条目状态零扰动，server 层 |
| 20 | openlatch-core/src/main/java/io/github/lamspace/openlatch/core/result/Outcome.java:71 | * design D1）；条目状态零扰动，server 层映射协议 {@code INVALID_REQUEST}。 * 扣减/挂起），或既有屏障上非零主张与定型值不符；条目状态零扰动， * server 层映射协议 {@code INVALID_REQUEST}。 |
| 21 | openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/WatchdogExample.java:28 | * 示例 4：看门狗续租与锁丢失回调（详设 §9）。 * 示例 4：看门狗续租与锁丢失回调。 |
| 22 | openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:40 | * 逐请求鉴权（连接即身份，详设 §5.3）。 * 携带，一次完成于握手；会话生命周期内不做 * 逐请求鉴权（连接即身份）。 |
| 23 | openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:53 | /** 配置键前缀（详设 §5.2）。 */ /** 配置键前缀。 */ |
| 24 | openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:51 | * @param snapshotThreshold 快照触发条目数（S4 起接入 Ratis 自动触发阈值） * @param snapshotThreshold 快照触发条目数（作为 Ratis 自动快照触发阈值） |
| 25 | openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ServerConfig.java:28 | * 服务器配置。配置键与默认值对齐设计说明书 §5.7。 * 服务器配置。 |
| 26 | openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:175 | *                     受理至写回，含 Raft 提交等待，spec"耗时与到期计数口径"） *                     受理至写回，含 Raft 提交等待） |
| 27 | openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaderTracker.java:25 | * Leader 提示的单源视图（详设 §3.2 {@code LeaderTracker}，s3 design D3）。 * Leader 提示的单源视图。 |
| 28 | openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:312 | // 超额归还：参数与持有不符，回执 INVALID_REQUEST（P3-07 码形接正）。 // 超额归还：参数与持有不符，回执 INVALID_REQUEST。 |
| 29 | openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:260 | * <p><b>多数派护栏</b>（spec"成员变更运维"，机械拒绝而非仅文档约定）： * <p><b>多数派护栏</b>（机械拒绝而非仅文档约定）： |
| 30 | openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchProperties.java:35 | * 装配（design D4）。时长类属性支持标准 Duration 写法（如 {@code 5s}、 * 装配。时长类属性支持标准 Duration 写法（如 {@code 5s}、 |
