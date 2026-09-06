# Tasks: phase3-t2-metrics

判定基线：每组末尾的验证命令一律 `mvn -s /home/lam/repo/settings.xml`（模块级 `-pl ... -am test`，收口为全量 `clean verify` 与 `-Pdrill`）；新增 Java 源文件按 CLAUDE.md §5 配齐 Javadoc。

## 1. P3-08a 配置与端点骨架

- [x] 1.1 server pom 增 `micrometer-core`/`micrometer-registry-prometheus`/`netty-codec-http`（版本入根 pom dependencyManagement，随 Netty BOM）；新增 `MetricsConfig`（仿 `ClusterConfig`：`load(path)` 同文件读 `openlatch.server.metrics.enabled`（默认 true）/`openlatch.server.metrics.port`（默认 9412，允许 0），`validate()` 快速失败）
- [x] 1.2 `MetricsHttpServer`：独立 Netty 两线程 daemon；精确路由 `/metrics`→`PrometheusMeterRegistry.scrape()`、`/healthz`→200 "ok"、其余 404；`port()` 回读实际端口；绑定失败抛 `IllegalStateException`
- [x] 1.3 `OpenLatchServer` 装配：构造期建 registry 与 `MetricsConfig` 注入、`start()` 按 `enabled` 绑定管理端口（冲突即整体启动失败）、`stop()` 解除；`main()` 增 `MetricsConfig.load`；启动日志打印管理端口
- [x] 1.4 验证：端点骨架单测（port 0 起停 + 三路径 HTTP 断言 + 非法配置快速失败）；`-pl openlatch-server -am test` 全绿（既有测试不受扰）

## 2. P3-08b 词表、单机埋点与 gauge 数据源

- [x] 2.1 `ServerMetrics`：§3.2 十项指标名/标签唯一命名点 + 记录方法（null 注册表即 no-op）；L1 词表单测——`PrometheusMeterRegistry` 直挂，断言 D2 映射后的全部线路名（`openlatch_server_acquire_total`、`openlatch_server_acquire_duration_seconds_bucket/_count/_sum` 等）
- [x] 2.2 `CoreEngine.stats()` 只读观察面（`CoreStats` 值对象：按家族 held 数（LATCH 除外）、等待者总数、单 key 最大队深、会话数）；core 新增统计单测（稳态精确断言 + 与在途变更并发无异常）；**回归底线：既有 core 测试全绿**
- [x] 2.3 `RequestDispatcher` 埋点：acquire/release/renew 计数（`status` 取应答状态码名，各受理请求恰好一次）、duration 计时（dispatch 起止，`result` granted/queued/denied）；`startScheduler()` 累加 `expireDue()` 返回值到 `lease.expired.total`
- [x] 2.4 单机 gauge 绑定：`locks.held{type}`/`waiters`/`queue.depth.max`/`sessions` 以回调式 `Gauge.builder(..., supplier)` 挂 `CoreEngine.stats()` 与 `ServerSessionRegistry.size()`
- [x] 2.5 验证：L1 单机埋点用例（驱动 dispatcher 消息面逐项断言 registry.find）；`-pl openlatch-server,openlatch-core -am test` 全绿

## 3. P3-08c 集群埋点与角色指标

- [x] 3.1 `ShadowTable.heldIndex` 改 `ConcurrentHashMap`——**核实结论（实现期）：无需改造**——`heldIndex` 本就是 `ConcurrentHashMap`（类注释既定跨线程弱一致读口径），digest 计算域仅 `locks`+`sessions`（`toProto()` 不触碰投影），`StateMachineDeterminismTest` 全绿佐证；为 gauge 家族聚合给 `HeldRef` 增加 `lockType` 定型字段（不入 digest 线形？——`HeldRef` 非 proto 结构，digest 零扰动）
- [x] 3.2 `ClusterRequestHandler` 埋点：受理→应答写回（`writeSync`/`respondAsync`/`commitFailure` 全出口）计数一次 + duration 含提交等待；`NOT_LEADER`/提交失败按状态码计入
- [x] 3.3 集群 gauge 与计数：`LockStateMachineCore` 应用 `LEASE_EXPIRE_ENTRY` 实际释放处 +1；`WaitQueue.totalWaiters()/maxQueueDepth()`；Leader 模式 gauge 读 `heldEntries()`/`WaitQueue`；`is_leader{node_id}` 依 `LeaderTracker.snapshot()`，仅集群启用注册
- [x] 3.4 验证：`ClusterRuntime` 装配链注入测试 + 集群 dispatch 面 L1 用例（含 follower NOT_LEADER 计数）；`-pl openlatch-server -am test` 与 `-Pdrill` 演练全绿

## 4. P3-09 指标断言测试与联调证据

- [x] 4.1 L2 单机脚本 IT：固定脚本（granted/queued 同 key 竞争/denied 非排队拒绝/release OK+NOT_HELD/renew 失败/短租约到期/latch 与 semaphore 混布）→ 基线差值法经 HTTP 抓 `/metrics`，用 Prometheus textparser（`MetricsText` 测试解析器）逐项断言；附 `/healthz`、404、Content-Type、metrics 开/关应答逐字段一致用例
- [x] 4.2 L2 并发冒烟：授予/释放压力进行中连续抓取 10+ 次，全部 200 且可解析、无异常
- [x] 4.3 L3 集群 IT：三节点各 port 0 管理口——`is_leader` 1/0/0 分布、Leader `locks.held`/`waiters` 与影子表/队列一致、follower `acquire.total{status="NOT_LEADER"}` 增长、到期各副本恰 +1（在途重试不虚增）
- [x] 4.4 `t2-acceptance-evidence.md`：docker 起真 Prometheus scrape 9412→目标指标落地记录（config 样例 + 抓取输出）；§3.2 清单 ↔ 用例编号对照表
- [x] 4.5 验证：`-pl openlatch-server -am test`、全量 `clean verify`、`-Pdrill` 全绿；§7 T2 判据逐项对照

## 5. P3-10 客户端指标与 starter 注入

- [ ] 5.1 client pom 增 optional `micrometer-core`；`ClientMetrics` 门面（null 注册表全 no-op）+ `Builder.meterRegistry(...)`；埋点三收口：`RequestMultiplexer`（requests{type,status}/duration，status 含 timeout/unavailable）、`ConnectionManager`（reconnect.total）、`OpenLatchClient.loseEntry`（locks.lost.total）
- [ ] 5.2 客户端单测（`SimpleMeterRegistry`）：默认关闭零记录；固定脚本计数逐项吻合；断线失锁用例（复用既有故障注入夹具）计数 +1
- [ ] 5.3 starter：`ObjectProvider<MeterRegistry>` 条件注入（有 bean 注入 builder、无 bean 静默跳过）；`ApplicationContextRunner` 两态用例
- [x] 5.4 验证：client/starter 全量测试绿；**T2 退出**——§8-3 证据齐（清单逐项用例 + 联调记录在案）

## 6. 收口

- [x] 6.1 全仓 `mvn -s /home/lam/repo/settings.xml clean verify` + `-Pdrill` 全绿
- [x] 6.2 详设回写勘误：§3.4 埋点位置（双路径共用 `ServerMetrics`）、§3.2 增命名映射口径注记；README 部署段知会 9412 管理端口
- [ ] 6.3 提交并归档（delta 同步主规格：`metrics-observability` 新建 + `core-lock-engine`/`client-sdk`/`spring-boot-starter` 增量）
