## 1. P1-27 Spring Boot 兼容性验证（D1）

- [x] 1.1 父 pom `dependencyManagement` 引入 `spring-boot-dependencies:4.0.3` BOM（import 声明序置于 netty-bom/protobuf 条目之后，保住现有版本 pin）；starter pom 补 `openlatch-client` + `spring-boot-autoconfigure` + `spring-boot-starter-aop` 依赖（拉取失败则按 D8 降级为 spring-aop + aspectjweaver 直依赖）
- [x] 1.2 写最小装配冒烟：一个 `@AutoConfiguration` 空壳 + `AutoConfiguration.imports` + `ApplicationContextRunner` 加载测试，Java 25 × Boot 4.0.3 下 `mvn -s /home/lam/repo/settings.xml verify -pl openlatch-spring-boot-starter` 跑通
- [x] 1.3 定案结论回写详设 §8.4（含 §11-4"SB 3.2+" → "SB 4.0.x+"口径修订），提交记录验证过程与结论
- [x] 1.4 验证：冒烟测试全绿；§8.4 修订落档

## 2. P1-28 自动装配

- [x] 2.1 `OpenLatchProperties`：§8.2 表逐字段（enabled/server-host/server-port/request-timeout/default-wait-timeout/reconnect-initial-backoff/reconnect-max-backoff），Duration 类型，全部带 Javadoc
- [x] 2.2 `OpenLatchAutoConfiguration`：`@EnableConfigurationProperties`；`OpenLatchClient` Bean（builder 映射属性，`@ConditionalOnMissingBean`，`destroyMethod = "shutdown"`）；`@EnableAspectJAutoProxy` 依赖 starter-aop 自动配置，不重复声明
- [x] 2.3 注册 `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
- [x] 2.4 验证：`ApplicationContextRunner` 条件测试全绿——Bean 存在、用户 Bean 让位、属性覆盖断言到 `client.config()`、服务器不可达时上下文照常启动（`connectAsync()` 未就绪不阻塞）

## 3. P1-29 注解与切面

- [x] 3.1 `@OpenLatch` 注解（key/type 取 `LockType`（D2）/waitTime/leaseTime/timeUnit）+ starter 模块 pom 开启 compiler `<parameters>true`
- [x] 3.2 `OpenLatchAspect`（`@Order(0)`，`@Around("@annotation(openLatch)")`）：SpEL 编译缓存（方法+表达式）、`StandardEvaluationContext` 每调用新建、`DefaultParameterNameDiscoverer` 注入 `#paramName`；求值非空校验失败抛 `OpenLatchException`
- [x] 3.3 获取路径单一化修正（实施回流，见 design D7）：一律经 `acquireAsync`/`releaseAsync`，READ/WRITE 直接以 `LockType` 入 `AcquireSpec`，不再建 OLock 句柄缓存（`leaseTime` 在 OLock 冻结表面无入口）
- [x] 3.4 waitTime 三分支映射 `AcquireSpec.waitMs`（`<0 → -1`、`=0 → 0`、`>0 →` 毫秒数，总超时计时由客户端内部承担，切面本地界双保险）；获取失败抛 `LockAcquisitionTimeoutException` 且业务不执行；`leaseTime` 透传经 `AcquireSpec`
- [x] 3.5 释放守卫（D3 于异步路径的等价实现）：`INVALID_TOKEN`/`NOT_HELD`/`SESSION_EXPIRED`/断连失败静默跳过记 debug；其余真实失败业务成功时抛出、业务已失败时记日志让位
- [x] 3.6 验证：mock `OLock` 切面单测全绿——三分支映射、SpEL 隔离/空值、释放守卫（锁丢失路径）、事务序断言（`@Transactional` + `@Order(0)`，事件序 commit 先于 unlock）

## 4. P1-30 Starter 集成测试

- [x] 4.1 starter 测试夹具：内嵌 `OpenLatchServer`（临时端口，仿 `ClientTestServers`）+ 动态端口注入 `openlatch.server-port` 的 `@SpringBootTest(webEnvironment = NONE)` 基座
- [x] 4.2 互斥执行用例：≥16 线程并发调用 `@OpenLatch` 方法，临界区计数器断言无丢失；连接就绪以 `connectAsync()` 门闩
- [x] 4.3 SpEL 用例：`key = "#id"` 不同参数互不阻塞、同参数串行；裸 client 抢注 key 后注解调用抛 `LockAcquisitionTimeoutException`
- [x] 4.4 READ/WRITE 用例：双 READ 并发执行、WRITE 持有时 READ 排队
- [x] 4.5 `enabled=false` 端到端用例：注解不生效、方法直执
- [x] 4.6 验证：§10.3 对应 starter 部分全绿；`mvn -s /home/lam/repo/settings.xml verify` 全模块通过

## 5. P1-31 examples

- [x] 5.1 `openlatch-examples/pom.xml`：依赖全模块 + spring-boot-starter + slf4j-simple（runtime）+ exec 插件；共享夹具类（内嵌服务器、租约档位、线程工具）
- [x] 5.2 `QuickStartExample`：内嵌 server + lock/tryLock/unlock 最小闭环
- [x] 5.3 `ConcurrencyExample`：16 线程竞争同 key，打印入队/授予顺序（FIFO 可观察）
- [x] 5.4 `ReadWriteExample`：读写并发矩阵演示
- [x] 5.5 `WatchdogExample`：长任务续租日志 + 持有期间 `server.stop()` 触发锁丢失回调演示
- [x] 5.6 `SpringAnnotationExample`：完整 Boot 应用（main + application.yaml + `@OpenLatch` SpEL），内嵌 server `@Bean`
- [x] 5.7 验证：六个 main 逐一 `mvn -pl openlatch-examples exec:java` 运行通过、正常退出无残留线程（**P1-31 判据**）

## 6. P1-32 基准基线

- [x] 6.1 `BenchmarkMain`（D5 手写 harness）：热身 + 固定时长采样；无竞争 tryLock 往返吞吐、16/64 线程竞争吞吐、授予延迟全量样本排序取 P99；结果打印并写 `docs/benchmark-baseline-<date>.md`（含机器/JDK/服务器档位说明，三批取中位）
- [x] 6.2 验证：基线报告提交入库；**不作 CI 断言**（§10.5）

## 7. P1-33 文档与验收闭环

- [x] 7.1 README.md / README_CN.md（当前 0 字节，双语从零）：是什么与边界（单机内存、Phase 1 不含项）→ Quick Start（server 制品部署 + 编程式 + starter 注解式，明示 `-parameters` 要求与 SB 4.0.x 适用版本）→ 配置参考（§5.7/§6.7/§8.2 三表）→ 语义与警示（租约/看门狗/锁丢失监听义务、SIMPLE 自锁、无升降级、锁在事务外层、自调用绕代理、重启全释放）→ 已知局限（§12 四条）→ 基准基线链接
- [x] 7.2 新增两模块公开 API Javadoc 复查：对着 spec 逐条核契约描述与实现一致（构建闸门 `show=private` 已保证无缺失）
- [x] 7.3 `docs/Phase1-验收报告.md`：§11 四行标准 × 证据（测试类清单 + `mvn clean verify` 输出摘要 + 示例运行记录），逐项闭环
- [x] 7.4 验证：§11 验收表逐项闭环；全仓 `mvn -s /home/lam/repo/settings.xml clean verify` 绿；**M4 退出，Phase 1 发布就绪**
