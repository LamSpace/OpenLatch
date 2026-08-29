## Purpose

让 Spring Boot 应用仅添加 starter 依赖与 `@OpenLatch` 注解即可使用 OpenLatch 分布式锁：自动装配提供带默认值的连接配置与客户端生命周期管理，注解提供声明式获取/释放、SpEL 动态 key、读写锁映射与"锁在事务外层"的顺序保证。

## ADDED Requirements

### Requirement: 配置属性绑定与默认值

starter MUST 绑定 `openlatch.*` 前缀的配置属性：`enabled`（默认 `true`）、`server-host`（默认 `127.0.0.1`）、`server-port`（默认 `9410`）、`request-timeout`（默认 `5s`）、`default-wait-timeout`（默认 `30s`）、`reconnect-initial-backoff`（默认 `200ms`）、`reconnect-max-backoff`（默认 `10s`）。时长类属性 MUST 支持标准 Duration 写法（如 `5s`、`100ms`）。未提供任何属性时 MUST 全部按默认值装配。

#### Scenario: 零配置默认值生效

- **WHEN** 应用仅引入 starter 依赖、不写任何 `openlatch.*` 属性并启动上下文
- **THEN** 客户端以 `127.0.0.1:9410`、请求超时 5s、等待兜底 30s、退避 200ms/10s 装配成功

#### Scenario: 属性覆盖生效

- **WHEN** 应用配置 `openlatch.server-port` 与 `openlatch.request-timeout=2s` 后启动上下文
- **THEN** 客户端 Bean 按覆盖值连接目标端口并以 2s 请求超时工作

### Requirement: 客户端 Bean 装配与用户覆盖

上下文 MUST 自动注册一个 `OpenLatchClient` Bean。应用自行定义同类型 Bean 时 MUST 以应用定义优先，starter 的自动装配退让且不报错。客户端 Bean 的创建 MUST NOT 因服务器暂不可达而阻塞或失败——连接异步发起并自动重连，应用上下文照常启动。

#### Scenario: 直接注入使用

- **WHEN** 应用上下文启动后注入 `OpenLatchClient`
- **THEN** 注入成功且客户端完成连接，可直接执行编程式获取

#### Scenario: 用户自定义 Bean 优先

- **WHEN** 应用声明了自己的 `OpenLatchClient` Bean（如自定义监听器）且 starter 在类路径上
- **THEN** 上下文仅装配用户定义的实例，自动装配让位

#### Scenario: 服务器未启动时上下文照常启动

- **WHEN** 目标端口无服务监听时启动应用上下文
- **THEN** 上下文启动成功不抛异常，客户端在后台按退避重连，服务器拉起后可正常获取锁

### Requirement: 上下文关闭时释放持锁

Spring 上下文关闭时 MUST 调用客户端优雅关停：尽力释放该客户端持有的全部锁（受至多一个请求超时约束），并停止重连。

#### Scenario: 关停后锁即时可竞争

- **WHEN** 客户端持有锁期间应用上下文关闭，另一客户端等待该锁
- **THEN** 等待方在关停完成后获得锁，无需等租约到期

### Requirement: enabled 开关

`openlatch.enabled=false` 时 MUST NOT 注册锁切面，带 `@OpenLatch` 注解的方法按无注解原样执行；`OpenLatchClient` Bean 本身 MUST 仍然装配（编程式使用不受影响）。

#### Scenario: 关闭开关后注解不生效

- **WHEN** 配置 `openlatch.enabled=false` 且锁被他人持有时调用带 `@OpenLatch` 的方法
- **THEN** 方法直接执行，不发生锁获取、不抛锁异常

### Requirement: 声明式获取与超时语义

`@OpenLatch` MUST 按 `waitTime` 决定获取方式：`waitTime < 0` 以等待总超时兜底排队获取；`waitTime = 0` 立即式尝试（锁被占直接判失败，不排队）；`waitTime > 0` 限时等待（单位由 `timeUnit` 决定）。获取失败 MUST 抛出 `LockAcquisitionTimeoutException` 且业务方法不执行。业务方法执行完毕或抛出异常 MUST 释放所获锁（每次获取配对一次释放，重入计数由服务端维护）。

#### Scenario: 无竞争直接执行

- **WHEN** 锁空闲时调用 `@OpenLatch` 方法
- **THEN** 获取成功、业务方法执行、方法返回后锁被释放（第三方可立即获取）

#### Scenario: 竞争时排队后执行

- **WHEN** 锁被其他客户端持有一小段时间后释放，等待方以 `waitTime < 0` 调用注解方法
- **THEN** 等待方在持锁方释放后获得锁并执行业务方法

#### Scenario: 立即式失败抛异常

- **WHEN** 锁被持有时调用 `waitTime = 0` 的注解方法
- **THEN** 抛出 `LockAcquisitionTimeoutException`，业务方法不执行，不产生排队

#### Scenario: 业务异常后锁仍被释放

- **WHEN** 注解方法体内部抛出业务异常
- **THEN** 业务异常原样向调用方传播，且锁已被释放

### Requirement: SpEL 动态 key

`@OpenLatch` 的 `key` MUST 按 SpEL 求值，上下文 MUST 注入方法形参供 `#参数名` 引用（依赖 `-parameters` 编译选项，starter 与文档 MUST 明示该前置要求）。求值结果 MUST 为非空字符串，否则抛出 `OpenLatchException` 且业务方法不执行。求值结果相同的调用 MUST 竞争同一把锁；结果不同的调用 MUST 互不阻塞。表达式解析结果 MUST 可缓存复用（同一方法同一表达式不重复解析）。

#### Scenario: 按参数隔离的并发

- **WHEN** 两个线程分别以不同参数值调用同一 `@OpenLatch(key = "#id")` 方法，且各自的参数被外部先行锁住对方的值
- **THEN** 两调用互不阻塞各自执行；相同参数值的并发调用则串行执行

#### Scenario: 空求值结果拒绝执行

- **WHEN** SpEL 表达式求值结果为 null 或空串
- **THEN** 抛出 `OpenLatchException`，业务方法不执行

### Requirement: 读写锁注解映射

`type = READ` MUST 映射为共享读锁、`type = WRITE` MUST 映射为互斥写锁（`REENTRANT`/`SIMPLE` 映射对应互斥类型）。并发行为遵循服务端语义：多读者可同时执行 READ 方法；WRITE 与任何读/写互斥；FIFO 公平排队。

#### Scenario: 多读者并发执行

- **WHEN** 两个线程同时调用 `type = READ` 的注解方法（同一 key）
- **THEN** 两方法并发执行，互不等待

#### Scenario: 写者排斥读者

- **WHEN** 一线程持有 WRITE 注解方法执行期间另一线程调用同 key 的 READ 注解方法
- **THEN** READ 调用排队等待，直至写方法完成并释放

### Requirement: 锁在事务外层

`@OpenLatch` 与 `@Transactional` 标注同一方法时，默认顺序 MUST 为锁在事务外层：获取锁先于事务开启，释放锁晚于事务提交。该顺序 MUST 有自动化测试锁定。

#### Scenario: 提交后才释放锁

- **WHEN** 同时带 `@Transactional` 与 `@OpenLatch` 的方法执行完成
- **THEN** 事务提交时刻锁仍被当前线程持有，解锁发生在提交之后（事件序可观察为 commit 先于 unlock）

### Requirement: 锁丢失时的释放守卫

业务方法执行期间锁若已丢失（租约失效、断连裁决），注解切面释放时 MUST NOT 因"本地已不持有"而抛出异常掩盖业务结果：业务异常仍原样传播，业务正常完成仍正常返回；锁丢失本身经客户端 `LockLostListener` 通道通知。

#### Scenario: 锁丢失不掩盖业务异常

- **WHEN** 注解方法执行期间服务端关闭导致锁丢失，且方法体随后抛出业务异常
- **THEN** 调用方收到的是业务异常而非锁相关异常，丢失事件经锁丢失监听器可见
