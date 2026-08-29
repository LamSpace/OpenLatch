# Proposal: m4-starter-examples-docs

## Why

M1–M3 已交付协议、纯 Java 核心、单节点服务器与客户端 SDK，但概要设计 §4.3 成功标准 4（Spring Boot 应用"仅加依赖与注解即可使用"）尚无落点，§9 六个示例与 §10.5 基准基线未交付，两个 README 仍为空文件。M4 是 Phase 1 的收尾里程碑：交付 starter 与 examples 两个占位模块的实际内容（详设 §8/§9、子任务 P1-27~P1-33），并完成 §11 验收证据闭环，Phase 1 方可发布。

## What Changes

- **Spring Boot 兼容性定案（P1-27）**：以正向验证替代负向实验——最小自动装配在 Java 25 × Spring Boot 4.0.3 上跑通即定案，回写详设 §8.4；验收标准 §11-4 的"SB 3.2+"口径相应修订为"SB 4.0.x+"（starter 按 Boot 4 编译，不向下兼容 Boot 3 应用）。
- **实现 `openlatch-spring-boot-starter`（P1-28~P1-30，详设 §8）**：
  - `OpenLatchProperties`（`openlatch.*`，§8.2 表逐字段）+ `OpenLatchAutoConfiguration`（`AutoConfiguration.imports` 注册、`@ConditionalOnMissingBean` 的 client Bean、destroy 回调 `shutdown()`、`enabled` 开关）；
  - `@OpenLatch` 注解与 `OpenLatchAspect` 切面：SpEL key（编译缓存 + `-parameters` 参数名解析）、`(key, type)` 锁句柄缓存、`waitTime` 三分支（`<0` lock / `=0` tryLock / `>0` 限时）、`@Order` 保证锁在事务外层、锁丢失后释放守卫；
  - 条件装配测试 + 真实服务器上的 starter 集成测试（互斥执行、SpEL 按参数求值、获取失败抛异常、READ/WRITE）。
- **实现 `openlatch-examples`（P1-31，详设 §9）**：六个示例（QuickStart / Concurrency / ReadWrite / Watchdog / SpringAnnotation / BenchmarkMain）全部自包含可独立运行（内嵌服务器、临时端口）。
- **基准基线（P1-32，详设 §10.5）**：手写热身+计时 harness，产出无竞争吞吐、竞争吞吐、授予延迟 P99 三项指标，基线报告存档入库（不作 CI 门槛）。
- **文档与验收闭环（P1-33）**：README.md / README_CN.md 双语从零撰写（quick start、配置参考、语义与警示、已知局限）；公开 API Javadoc 全覆盖（构建闸门 `show=private` 已强制）；§11 四条验收标准逐项收集证据。
- 现有四模块（protocol/core/server/client）**无公开 API 变更**——starter 仅消费 M3 已冻结的 API。

## Capabilities

### New Capabilities

- `spring-boot-starter`: Spring Boot 自动装配与声明式锁的可观察行为契约——配置属性绑定与默认值、client Bean 生命周期（构建不阻塞启动、上下文关闭释放持锁）、`enabled` 开关语义、`@OpenLatch` 的获取/释放/异常/超时语义、SpEL key 求值规则、锁在事务外层、READ/WRITE 注解映射。

### Modified Capabilities

（无。`wire-protocol`、`core-lock-engine`、`lock-server`、`client-sdk` 的需求均不因 M4 改变：starter 是 client 公开 API 的消费方；examples 与文档是交付物而非行为契约，其可运行性判据落在 tasks 的验证列。）

## Impact

- **代码**：`openlatch-spring-boot-starter`、`openlatch-examples` 从占位 pom 变为完整模块；新增包 `io.github.lamspace.openlatch.spring`。
- **依赖**：父 pom `dependencyManagement` 引入 `spring-boot-dependencies:4.0.3` BOM（声明序在 netty-bom 之后，保住 netty 版本主导权）；starter 依赖 `openlatch-client` + `spring-boot-autoconfigure` + `spring-boot-starter-aop`（`spring-boot-starter-aop` 本地仓库无货，经 Maven Central 拉取，网络已确认可达）；examples 依赖全模块（不发布到 Maven Central）。
- **构建**：starter 与 examples 模块编译开启 `-parameters`（SpEL 按参数名求值的前置条件，README 明示对用户工程的同样要求）；新模块过 javadoc 校验（`show=private` + `failOnWarnings`）。
- **文档**：详设 §8.4 回写定案结论；§11-4 口径修订；README ×2 从零撰写；`docs/` 新增基准基线与 Phase 1 验收报告。
- **测试**：starter 新增条件装配单测与集成测试套件（内嵌真实服务器，复用 `ClientTestServers` 式夹具）。
