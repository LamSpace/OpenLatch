# Tasks: normalize-javadoc-and-license

## 1. Apache License 协议头

- [x] 1.1 为 `openlatch-core` 全部 26 个 Java 文件（main 20 + test 6，实施时核数）在 package 语句前添加 Apache 2.0 短式协议头（design D1）→ verify: `grep -L "Apache License" $(find openlatch-core/src -name '*.java')` 输出为空
- [x] 1.2 为 `openlatch-server` 全部 21 个 Java 文件添加协议头 → verify: 同上命令对 openlatch-server
- [x] 1.3 为 `openlatch-protocol` 测试文件 `ProtocolCodecTest.java` 添加协议头 → verify: 同上命令对 openlatch-protocol/src

## 2. core 主源码 Javadoc 补齐（design D2/D3）

- [x] 2.1 门面与时钟：`CoreEngine`（构造器、`acquire`/`release`/`renew` 补完整 Javadoc，其余方法补 `@param`/`@return`）、`Clock.nowMs`、`CoreEventListener.notifyHead` 补 `@param`、`CoreConfig` 补 record 组件与常量文档 → verify: 编译通过，每个 public 方法均有标签
- [x] 2.2 lock 包：`LockEntry`（构造器、`acquire`/`release`/`renew`、四个访问器含持锁调用契约）、`LockTable`（`computeIfAbsent`/`get` 补 Javadoc）、`Waiter`（方法与组件）、`Owner`（组件）→ verify: 同上
- [x] 2.3 command/result 包：`AcquireCommand`（补 6 个缺失组件）、`ReleaseCommand`/`RenewCommand`（全部组件）、`AcquireResult`/`ReleaseResult`/`RenewResult`（补缺失组件）、`Outcome`（7 个枚举常量）、`ReleaseStatus`（4 个枚举常量）→ verify: 同上
- [x] 2.4 session/lease 包：`SessionRegistry` 与 `LeaseManager`（`HeapEntry` 组件）的 `@param`/`@return` 补齐 → verify: `mvn -s /home/lam/repo/settings.xml -pl openlatch-core clean compile` 通过

## 3. server 主源码 Javadoc 补齐

- [x] 3.1 入口与配置：`OpenLatchServer`（构造器、`core`/`config`/`sessions`、`main`、`PROTOCOL_VERSION`）、`ServerConfig`（11 个 record 组件、常量、`load` 的 `@param`/`@return`/`@throws`）、`NotifyEventBridge` 构造器
- [x] 3.2 会话与分发：`ServerSession`（构造器与访问器）、`ServerSessionRegistry`（`register`/`get`/`size`）、`RequestDispatcher`（构造器、`dispatch`/`errorResponse` 与包级映射方法的标签）
- [x] 3.3 net 包：`ServerChannelInitializer` 构造器、`ServerSessionHandler` 构造器、`ServerBootstrapFactory.create` → verify: `mvn -s /home/lam/repo/settings.xml -pl openlatch-server clean compile` 通过

## 4. 测试代码注释（宽松标准）

- [x] 4.1 `OpenLatchServerTest`、`ServerConfigTest` 补类级 Javadoc；其余测试类类级注释已有，逐一核对无遗漏
- [x] 4.2 公共夹具补方法标签：`TestSupport`（`MutableClock`/`RecordingListener`/`QueueingListener`）、`TestServers`、`TestProtocolClient`（`hello`/`sendAndAwait`/`send`/`awaitPush`/`disconnectAbruptly` 的 `@param`/`@return`/`@throws`）→ verify: 测试编译通过

## 5. 构建期防回归

- [x] 5.1 摸底：对当前代码临时运行 `mvn -s /home/lam/repo/settings.xml javadoc:javadoc`，记录存量 doclint 告警并在第 2-4 组任务中一并消除
- [x] 5.2 根 `pom.xml` 的 `pluginManagement` 添加 `maven-javadoc-plugin` 配置（doclint=all、failOnWarnings、UTF-8、javadoc 目标绑定 verify），`build.plugins` 启用（design D4，版本实施为 3.12.0——环境已验证可用）
- [x] 5.3 `openlatch-protocol/pom.xml` 对 javadoc 插件设置 `skip=true`（生成代码）
- [x] 5.4 全量验证：`mvn -s /home/lam/repo/settings.xml clean verify` 全绿（测试通过 + javadoc 零告警）
- [x] 5.5 反向验证防回归：临时删除任一 public 方法的 `@param` 后 `verify` 应失败，随后恢复
