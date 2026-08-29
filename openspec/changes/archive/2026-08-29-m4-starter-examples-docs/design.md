## Context

M3 已交付稳定的客户端公开 API（`OpenLatchClient.builder()` / `OLock` / `LockType` / `connectAsync()` / `shutdown()`），服务端可编程内嵌（`OpenLatchServer(ServerConfig).start()`，端口 0 即临时端口，测试夹具 `ClientTestServers` 已有先例）。`openlatch-spring-boot-starter` 与 `openlatch-examples` 目前是零源码占位 pom；两个 README 为 0 字节；`-parameters` 未在任何模块开启；javadoc 校验（`show=private` + `failOnWarnings`）对全模块生效。环境事实：本机 Java 25.0.3；本地仓库备有 Spring Boot 4.0.3 / Framework 7.0.5 / aspectjweaver 1.9.25.1 / jmh 1.37 全家桶而**无** 3.5.x；Maven Central 网络可达。行为契约见 `specs/spring-boot-starter/spec.md`，动机见 proposal.md - Why。

## Goals / Non-Goals

**Goals:**

- starter 是 client 的**薄装配层**：只做配置映射、Bean 生命周期与注解拦截，不新增任何锁语义，不改 client 公开 API。
- 兼容性定案（P1-27）产出可回写详设 §8.4 的明确结论，而非模糊带过。
- 六个示例与基准 harness 全部机械可运行（单命令、自包含），"逐示例运行通过"不依赖人工部署服务器。
- 文档承担契约义务：`-parameters` 前置、锁丢失监听义务、锁在事务外层、已知局限，README 与 Javadoc 双轨。

**Non-Goals:**

- 不做 Boot 3.x 兼容层、不做多客户端实例、不做 `openlatch.*` 表外属性（§8.2 表即全集）。
- 不实现显式"取消排队"、不引入幂等窗口（M2/M3 既定遗留，与本里程碑无关）。
- 基准不设 CI 门槛（§10.5：记录基线、防退化、不作发布门槛）。
- examples 不发布到 Maven Central，不追求库级 Javadoc 美观（但过 javadoc 构建闸门）。

## Decisions

### D1：Spring Boot 定案 4.0.3，正向验证代替负向实验（P1-27 定案）

详设 §8.4 的决策阶梯为"3.5.x 不完全支持 Java 25 → 优先升级 4.x"。事实侧：SB 3.5 官方认证矩阵止于 Java 24，本地仓库只备货 4.0.3，SB 4.0（Framework 7）官方支持 Java 25。故不构造 3.5.x 负向实验，直接以**最小自动装配 + 上下文加载冒烟**在 4.0.3 上正向验证；通过即定案，回写 §8.4 与 §11-4 口径（"SB 3.2+" → "SB 4.0.x+"）。

- 理由：正向验证的结论与负向验证等价（都是"4.0.3 可用"），成本一半；且 §8.4 的阶梯本就优先 4.x。
- 备选（否决）：3.5.x 实测不行再切 4.x——多一轮构建与报告，结论不变；starter 单独 `--release 17` 编译——双版本复杂度，且 Boot 3 的 CGLIB/ASM 在 Java 25 上未经认证，风险反而更高。
- 后果（明示）：按 Boot 4 编译的 starter 不向下兼容 Boot 3 应用（autoconfigure 类搬移），README 与详设须写清适用版本。

### D2：注解锁类型直接复用 client 公开枚举 `LockType`，不新增 `LockMode`

详设 §8.3 签名写 `LockMode type()`，但 client 已公开语义一一映射的 `LockType`（REENTRANT/SIMPLE/READ/WRITE，`LockType.java:32`）。starter 本就依赖 client，再造一个枚举只换来一处 switch 映射和"两个枚举可能漂移"的维护面。`@OpenLatch` 的 `type()` 直接取 `LockType`，回写详设 §8.3 措辞。

- 理由：单账本原则的 API 版——锁类型的事实源是协议/client，注解只是透传。
- 备选（否决）：新增 `LockMode` 作防腐层——Phase 1 无跨版本防腐需求，YAGNI；若未来 client 枚举演进再引入不迟（注解属性加宽是兼容变更）。

### D3：切面释放加"本地持锁"守卫（§8.3 增补）

流程改为 `finally { if (lock.isHeldByCurrentThread()) lock.unlock(); }`。业务执行期间锁被看门狗裁决丢失时，本地登记已移除，裸 `unlock()` 会抛 `IllegalMonitorStateException` 掩盖业务异常；丢失事件已经 `LockLostListener` 通道通知，无需在释放路径重复报错。回写详设 §8.3。

- 理由：锁丢失是 Phase 1 声明的常态事件（§7），切面必须让"业务结果"优先于"善后噪声"。
- 备选（否决）：裸 unlock——业务异常与失锁竞态叠加时调用方拿到错误异常类型，排障误导。

### D4：`enabled=false` 只关切面，client Bean 照建

§8.1/§8.2 的开关语义是"注解不生效（切面不注册）"。编程式用户仍注入 `OpenLatchClient` 使用；要彻底排除自动装配有 Spring 标准 exclude 机制，无需自造。

- 理由：开关只管自己承诺的那件事；"注解失效但 Bean 还在"可测、可解释。
- 备选（否决）：连 client Bean 一起关——破坏"仅加依赖即可编程式使用"的降级路径，且 `@ConditionalOnProperty` 套两个 Bean 徒增条件矩阵。

### D5：基准用手写 harness，不引入 JMH

§9 明文"JMH 或手写热身+计时"。三项指标（无竞争 tryLock 往返吞吐、16/64 线程竞争吞吐、授予延迟 P99）均由网络往返主导，几十字节代码的热身+固定时长采样+全量延迟数组排序足够；JMH 需注解处理器 + `benchmarks.jar` 独立打包，把 examples 模块变成一套构建工程，而其产出仅是"存档基线、不进 CI 门槛"。产物：`BenchmarkMain` + `docs/benchmark-baseline-<date>.md` 提交入库。

- 理由：测量目标简单、结论用途是防退化参考，不值得 JMH 的构建复杂度。
- 备选（否决）：JMH——统计学更严谨（误差条、fork 隔离），但 Phase 1 基线不承担发布判定；如 Phase 2 要做跨版本严格对比再引入。

### D6：示例一律自包含内嵌服务器（端口 0）

六个示例的 main 均进程内起 `OpenLatchServer`（临时端口）再演示，单命令 `mvn -pl openlatch-examples exec:java -Dexec.mainClass=…` 可逐一验证；`SpringAnnotationExample` 是完整 Boot 应用，服务器以 `@Bean` 内嵌，兼作验收标准 4 的活证据。README 的 Quick Start 则讲真实部署路径（`java -jar` 起 server 制品 + 连接），两轨并行不互扰。

- 理由："逐示例运行通过"必须机械可重复，不能要求评审者先部署服务器。
- 备选（否决）：示例连外部服务器——每次运行验证变成人工流程，示例 CI 化无门。

### D7：装配结构

```
openlatch-spring-boot-starter/
├── pom.xml                         ← openlatch-client + spring-boot-autoconfigure
│                                      + spring-boot-starter-aspectj；compiler <parameters>true
├── main/resources/META-INF/spring/
│   └── o.s.b.autoconfigure.AutoConfiguration.imports
└── io.github.lamspace.openlatch.spring/
    ├── OpenLatchProperties          ← §8.2 表逐字段，Duration 原生绑定
    ├── OpenLatchAutoConfiguration   ← client Bean（destroyMethod="shutdown"）
    │   @ConditionalOnMissingBean；enabled 经 @ConditionalOnProperty(prefix="openlatch",
    │   name="enabled", matchIfMissing=true) 仅作用于切面 Bean
    ├── OpenLatch                    ← @Target(METHOD)，key/type(LockType)/waitTime/leaseTime/timeUnit
    └── OpenLatchAspect              ← @Order(0) 保证锁在事务（LOWEST_PRECEDENCE）外层
```

SpEL：`ConcurrentHashMap` 按"声明方法 + 表达式"缓存解析后的 `Expression`（解释模式，不启用 SpEL 编译器）；`StandardEvaluationContext` 每次调用新建（非线程安全），参数名经 `DefaultParameterNameDiscoverer` 注入 `#paramName`，同时提供 `#p0/#a0` 位置引用。

**获取路径单一化（P1-29 实施修正）**：切面一律走 client 公开异步内核 `acquireAsync(AcquireSpec)`/`releaseAsync(key, token, threadId)`，不再经 `OLock` 句柄缓存——`OLock` 冻结表面不携带 `leaseTime` 入口（§6.3），若"默认租约走 OLock、自定义租约走异步内核"则形成双份等待/超时/释放映射（aspect 复刻 `RemoteLock` 逻辑，违背单账本精神）。异步内核映射：`waitTime<0 → waitMs=-1`（客户端内部以 `defaultWaitTimeout` 兜底总超时）、`=0 → waitMs=0`、`>0 → waitMs=毫秒数`（客户端内部计时）；切面 `future.get()` 仅加"总超时 + requestTimeout + 1s"的本地双保险界。`leaseTime>0` 直入 `AcquireSpec.leaseMs`（服务端钳制），`=0` 用服务端默认。`releaseAsync` 在 `fullyReleased` 时自行注销看门狗与本地簿记（client 内部闭环），重入嵌套（跨 bean 同 key 注解）由服务端计数逐层消化。READ/WRITE 直接以 `LockType.READ/WRITE` 入 spec，无需 `newReadWriteLock` 门面。获取失败（总超时/DENIED）统一抛 `LockAcquisitionTimeoutException`（client 类型，starter 不包新异常）。

### D8：父 pom BOM 与依赖形状

`spring-boot-dependencies:4.0.3` 以 import 方式进父 pom `dependencyManagement`，**声明序放在 netty-bom 与 protobuf 显式条目之后**——Maven 的 import 冲突取先声明者，保住现 pin（netty 4.1.137.Final、protobuf 3.25.5）不被 Boot 的托管版本覆盖。starter 的 AOP 聚合依赖实为 **`spring-boot-starter-aspectj`**（P1-27 实施发现：Boot 4 模块清单无 `starter-aop`，AOP 聚合更名——§8.1 风险项"Boot 4 装配细节出入"的落地，功能等价）。Boot 4 另一处搬移（P1-30 发现）：`@DynamicPropertySource`/`DynamicPropertyRegistry` 从 boot-test 迁至 Framework 7 的 `org.springframework.test.context`（spring-test），starter 测试据此补 spring-test 依赖。examples 模块依赖 protocol/core/server/client/starter 全部 + slf4j-simple（runtime，示例日志可见）。

### D9：测试策略

- **条件装配（P1-28）**：`ApplicationContextRunner` 纯单测，不起服务器——Bean 存在性、用户 Bean 让位、`enabled=false` 无切面 Bean、属性绑定断言到 `client.config()`。
- **切面行为（P1-29）**：mock `OLock`（接口现成）单测三分支、SpEL 失败、释放守卫、事务序——`@Order(0)` 断言用事件序记录（`beforeCommit` 记点 vs 切面记 unlock）。
- **starter 集成（P1-30）**：内嵌真服务器（临时端口）+ `@SpringBootTest(webEnvironment = NONE)`，`openlatch.server-port` 注动态值；连接就绪门闩用 `connectAsync()`，不 sleep。四组用例对着 spec：并发互斥（≥16 线程临界区计数器）、SpEL 参数隔离、获取失败抛异常（裸 client 先抢注 key）、READ 并发/WRITE 排斥矩阵。

## Risks / Trade-offs

- **[Boot 4 autoconfigure 机制与详设 §8.1 假设有出入]**（如 `AutoConfiguration.imports` 之外的注册细节变化）→ P1-27 的最小冒烟先于一切 starter 代码，出入即在此暴露并回写 §8.4，不铺完代码再返工。
- **[Spring AOP 自调用绕过代理，嵌套 `@OpenLatch` 方法不加锁]** 标准 Spring AOP 语义，非本项目缺陷 → README"语义与警示"明示（与 `@Transactional` 同类限制），不引入 `expose-proxy` 之类的伪修复。
- **[持 SIMPLE 锁的注解方法被同线程再次调用会自锁至租约到期]** §4.4 既定语义 → Javadoc 与 README 双重警示，示例不演示该误用。
- **[`-parameters` 只在本仓库模块生效，用户工程未开启]** → 属性名解析失败时抛明确异常（spec：非空字符串校验路径覆盖），README 首屏 Quick Start 明示编译要求；不改根 pom 全局开启（protocol/core 等无此需求，避免无关模块产物变化）。
- **[基准数字受开发机噪声影响，无统计显著性]** → 基线定位为"存档防退化参考"（§10.5），报告注明机器与 JDK 版本、跑三批取中位；门槛判定明确不做。
- **[示例内嵌服务器与 README 外部部署路径有认知差]** → 示例源码注释与 README 各自说明"这是演示夹具，生产请独立部署"。

## Migration Plan

无数据迁移。发布顺序：P1-27 定案 → starter（28/29/30）→ examples（31）→ 基准（32）→ 文档与验收（33）。回滚点：D1 若正向验证意外失败（Boot 4 装配跑不通），回到 §8.4 阶梯下一档（starter 单独 `--release 17` 或改测 3.5.x），影响面封闭在 starter 模块 pom 与 spec 的版本口径。

## Open Questions

（无。）P1-32 定档：热身 4s，3 批 × 5s，竞争档位 16 与 64 都跑，延迟蓄水池每线程 32768；首轮数据（无竞争 ~11.3k ops/s、竞争吞吐同量级、授予延迟 P99 随队深 2→9ms）已落 `docs/benchmark-baseline-2026-08-29.md`。
