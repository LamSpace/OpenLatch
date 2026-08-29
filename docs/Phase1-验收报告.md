# OpenLatch Phase 1 验收报告

| 项目 | 内容 |
|------|------|
| 依据 | 《OpenLatch-Phase1-详细设计说明书》v1.1 §11 验收标准（追溯《概要设计》§4.3） |
| 里程碑 | M1–M4（变更 `m4-starter-examples-docs` 收尾） |
| 验证命令 | `mvn clean verify`（全仓，含单测与集成/故障注入 IT、javadoc `show=private` 闸门） |
| 验证时间 | 2026-08-29 16:2x（构建总耗时 62s，BUILD SUCCESS） |
| 环境 | Java 25.0.3 (Oracle LTS) / Apache Maven 3.9.16 / Linux 7.0.0-30-generic / 8 CPU |

**总计：182 项自动化测试，0 失败 0 错误 0 跳过。**

| 模块 | 单元（surefire） | 集成（failsafe） |
|---|---|---|
| `openlatch-protocol` | 11（`ProtocolCodecTest`：round-trip、未知字段容忍、type/payload 不匹配） | — |
| `openlatch-core` | 27（`CoreEngineMutexReentrantTest`、`CoreEngineReadWriteFairnessTest`、`CoreEngineLeaseTest`、`CoreEngineConcurrencyTest`、`CoreEngineSessionIdempotencyLimitTest`） | — |
| `openlatch-server` | 50（分发表逐消息、握手、帧编解码、通知桥、断连清理、配置、会话） | 1（`SmokeIT`：可执行 jar 独立启动 + HELLO→ACQUIRE→RENEW→RELEASE） |
| `openlatch-client` | 52（多路复用、等待跟踪、同步包装、看门狗、重连、builder、握手） | 11（`ClientConcurrencyIT`、`ClientReadWriteIT`、`ClientWatchdogIT`、`ClientFaultInjectionIT`、`ClientProcessKillIT`） |
| `openlatch-spring-boot-starter` | 21（兼容性冒烟、条件装配矩阵、切面行为、事务序） | 9（`OpenLatchStarterIT` ×6、`OpenLatchStarterDisabledIT` ×2、`OpenLatchStarterShutdownIT` ×1） |
| `openlatch-examples` | 0（示例经人工运行验证，见下） | 0 |

## §11 验收标准逐项闭环

### 标准 1：互斥、重入计数、读写互斥、FIFO 公平、租约到期释放全部有自动化测试覆盖

**结论：达成。**

- 证据：§10.1 全部用例组对应 `openlatch-core` 5 个测试类 27 项全绿（手工时钟，无 sleep）；
- 端到端复核：`ClientConcurrencyIT`（≥16 线程互斥无丢失更新）、`ClientReadWriteIT`（读写矩阵）、`ConcurrencyExample` 实测 FIFO 授予顺序 == 入队顺序（16/16，max concurrent holders = 1）。

### 标准 2：故障注入——持锁断连→租约释放；等待断连→快速失败

**结论：达成。**

- 证据：`ClientFaultInjectionIT`（持锁断连后一个租约期内服务端自动释放、他人可获取；等待中断连 future 快速失败且队列无残留）与 `DisconnectEndToEndTest`（服务端会话清理）全绿；
- 补充：`WatchdogExample` 活证据——杀服务端后续租失败，锁丢失回调按 `lostAt` 裁决触发。

### 标准 3：客户端所有请求路径带超时，无死等

**结论：达成。**

- 证据：`§6.4/§6.7` 的每请求超时（默认 5s）与等待兜底（默认 30s）经 `RequestMultiplexerTest`、`OpenLatchClientBuilderTest`、`OLockSyncWrapperTest` 断言；
- 端到端：`ClientProcessKillIT`（进程级杀服务端：全部请求在超时内失败、重启后自动重连恢复服务，测试内无挂起线程）通过。

### 标准 4：SB 应用仅加依赖与注解即可使用（口径修订：SB 4.0.x+，见详设 §8.4）

**结论：达成。**

- 证据：`OpenLatchStarterIT` 以内嵌真实服务器 + 最小 Boot 上下文（`@SpringBootConfiguration` + starter 依赖，零装配代码）验证互斥执行、SpEL 按参数求值、获取失败抛 `LockAcquisitionTimeoutException`、READ/WRITE 矩阵，9 项全绿；
- 顺序锁定：`OpenLatchTransactionOrderTest` 事件序断言 commit 先于 unlock（锁在事务外层）；
- 人工接入活证据：`SpringAnnotationExample` 单命令运行通过（异 key 并发、同 key 串行、立即式拒绝三场景输出见运行记录）。

## 示例运行记录（P1-31 判据）

2026-08-29 逐一 `mvn -pl openlatch-examples compile exec:java -Dexec.mainClass=…`，全部 exit 0、正常退出无残留线程：

| 示例 | 关键输出 |
|---|---|
| `QuickStartExample` | tryLock 持锁期间 false / 释放后 true |
| `ConcurrencyExample` | enqueued == granted 顺序（FIFO true），max concurrent holders = 1 |
| `ReadWriteExample` | 双读者屏障汇合；写持读拒、读持写拒 |
| `WatchdogExample` | 持有 6s（3× 租约）无丢失；杀服务端后 `lostAt` 裁决回调触发 |
| `SpringAnnotationExample` | SpEL 异 key 并发（+1088/+1088）、同 key 串行（+1398→+1709）、waitTime=0 拒绝 |
| `BenchmarkMain` | 基线落档（下节） |

## 基准基线（P1-32）

[benchmark-baseline-2026-08-29.md](benchmark-baseline-2026-08-29.md)：无竞争 tryLock 往返 ~11.3k ops/s（P50 0.04ms / P99 0.10ms）；16 线程竞争 ~11.4k ops/s（授予延迟 P99 ~2.0ms）；64 线程 ~11.3k ops/s（P99 ~8.7ms）。按 §10.5 仅作防退化参考，不设发布门槛。

## 遗留与偏差记录

| # | 事项 | 处置 |
|---|---|---|
| 1 | Spring Boot 定案 **4.0.3**（原 3.5.x 计划，Java 25 认证所迫，正向验证替代负向实验） | 详设 §8.4/§1.2/§11-4 已回写；标准 4 口径改 SB 4.0.x+ |
| 2 | Boot 4 依赖形状出入：AOP 聚合为 `spring-boot-starter-aspectj`；`@DynamicPropertySource` 迁至 spring-test | 详设 §8.1 注记；design D8 |
| 3 | 注解锁类型复用 client `LockType`（原 `LockMode`）；切面获取路径单一化为 `acquireAsync`/`releaseAsync`（`leaseTime` 在 OLock 冻结表面无入口）；释放守卫以状态码分类实现 | design D2/D7 定案与实施修正 |
| 4 | M4 修正既有缺陷：`openlatch-server` uber-jar 由替换 main 制品改为 `-executable` 分类器附带，`slf4j-simple` 转 optional——消除下游 classpath 污染（Boot 日志系统冲突实证） | `SmokeIT`/`ClientProcessKillIT` 定位路径同步；影响 README 部署命令 |

**结论：Phase 1 §11 四条验收标准全部闭环，M4 交付完成，Phase 1 达到发布就绪状态。**
