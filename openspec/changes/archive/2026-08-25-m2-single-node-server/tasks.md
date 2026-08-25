# Tasks: m2-single-node-server

对应设计说明书 §13.2 子任务 P1-11 ~ P1-17。所有 `mvn` 命令必须携带 `-s /home/lam/repo/settings.xml`。

## 1. 依赖锁定与启动骨架（P1-11）

- [x] 1.1 探测 aliyun 镜像/本地仓库可得的 Netty 4.1.x 稳定版、SLF4J、`maven-shade-plugin` 版本并定案；父 `pom.xml` 的 `dependencyManagement`/`pluginManagement` 显式锁定（design D1）
- [x] 1.2 `openlatch-server` pom 声明依赖：`openlatch-core`、`openlatch-protocol`、Netty、`slf4j-api`（运行时 `slf4j-simple`）；验证 `mvn -s /home/lam/repo/settings.xml -pl openlatch-server dependency:resolve` 通过且 Netty 在 Java 25 下无解析/运行告警
- [x] 1.3 `ServerConfig` record：§5.7 全部配置键与默认值；Properties 加载（`-Dopenlatch.config=<path>` 可选），非法值快速失败并给出明确错误信息
- [x] 1.4 `ServerConfig` 单元测试：默认值、指定文件覆盖、非法值拒绝、端口占用时启动失败退出（规格"服务启动与配置加载"三场景的配置侧）
- [x] 1.5 `OpenLatchServer` 骨架：加载配置 → 组装 `CoreEngine`（SystemClock、`NotifyEventBridge` 占位）→ 启动扫描调度与 Netty 监听 → 启动日志（端口、协议版本、关键限额）；`stop()` 关停序列（design D6）与 JVM 退出钩子
- [x] 1.6 验证：进程可启动监听（含默认端口 9410 与自定义端口）；关停顺序测试通过（扫描先停、连接后关、有限时间退出）

## 2. Netty pipeline（P1-12）

- [x] 2.1 `ServerBootstrapFactory` 与 `ServerChannelInitializer`：按设计说明书 §5.2 装配入站链（`LengthFieldBasedFrameDecoder(1MiB,0,4,0,4)` → `ProtobufDecoder(Envelope)` → `IdleStateHandler(read)` → `ServerSessionHandler`）与出站链（`ProtobufEncoder` → `LengthFieldPrepender(4)`）；boss 1 线程、worker 可配置（默认 2×CPU）
- [x] 2.2 EmbeddedChannel 分帧测试：半包、粘包（多帧合并发送）解码出正确数量的 `Envelope`；载荷超 1 MiB 触发断连（规格"帧长限制与自我保护限额"）
- [x] 2.3 验证：`mvn -s /home/lam/repo/settings.xml -pl openlatch-server test` 全绿

## 3. HELLO 握手（P1-13）

- [x] 3.1 `ServerSession`（sessionId、握手状态、inflight 计数）经 `AttributeKey` 绑定 Channel；`ServerSessionRegistry`（`ConcurrentHashMap<Long, ServerSession>`）在握手成功时登记（design D2）
- [x] 3.2 握手处理：合法 HELLO → `core.sessionOpened()`、登记注册表、回成功响应（含 sessionId、协议版本、默认租约）；握手前业务请求回 `INVALID_REQUEST` 不断连；版本 ≠ 1 与非空 `auth_token` 回 `INVALID_REQUEST` 并断连；重复 HELLO 回 `INVALID_REQUEST` 不断连（design D8，规格"会话握手"全部场景）
- [x] 3.3 握手规则单元测试（真实端口或 EmbeddedChannel 驱动）：覆盖 3.2 全部裁决分支与 HELLO 的 `request_id` 回显
- [x] 3.4 验证：§3.2.1 握手规则用例通过

## 4. 请求分发（P1-14）

- [x] 4.1 `RequestDispatcher`：纯函数映射 `Envelope → core command`（`wait_ms == 0 → queueIfBusy = false`，其余 → `true`）与 `core result → Envelope`（`Outcome`/`ReleaseStatus` → `StatusCode` 全表；授予响应补 `lease_expires_at_ms = now + grantedLeaseMs`；响应回显 `request_id`）（design D5）
- [x] 4.2 inflight 限额：分发入口自增检查（超限回 `OVERLOADED`）、响应完成递减；计数逻辑直接调用单测锁定（design D4）
- [x] 4.3 消息合法性校验：type 与 payload 不匹配/缺失 payload → `INVALID_REQUEST` 不断连；未知消息类型 → `INVALID_REQUEST`；`PING` 不回复；解码失败路径记日志并断连（规格"消息合法性校验"）
- [x] 4.4 分发表单元测试（纯单元，无 Netty）：ACQUIRE/RELEASE/RENEW 的每条映射、`PING`、不匹配类型逐一断言
- [x] 4.5 验证：§5.4 分发表逐消息用例通过

## 5. 通知桥与扫描调度（P1-15）

- [x] 5.1 `NotifyEventBridge` 实现 `CoreEventListener`：按 `sessionId` 查注册表 → 推送 `AWAIT_NOTIFY`（`request_id = 0`、`request_id_ref` 指向原请求）；连接不存在时静默丢弃；写出异常捕获并记日志（规格"队首通知推送"）
- [x] 5.2 单线程 `ScheduledExecutorService` 周期（默认 500ms）串行调用 `core.expireDue()` 与 `core.sweepNotifiedHeads()`（design D6）
- [x] 5.3 测试用最小协议客户端夹具（仅测试源码）：建连、握手、发任意 `Envelope`、按 `request_id` 等待响应、接收推送、主动断连；不含任何客户端锁语义（design D7）
- [x] 5.4 端到端测试（真实端口，bind 0 取临时端口）：两客户端竞争同 key——持有者释放后，排队者收到 `AWAIT_NOTIFY`，以同一 `request_id` 重发 ACQUIRE 获得授予；未续租短租约到期后排队者经通知获得锁（规格"租约到期扫描驱动"）
- [x] 5.5 验证：通知 → 队首重发 → 授予端到端用例通过

## 6. 断连清理与空闲检测（P1-16）

- [x] 6.1 `channelInactive` → 摘除注册表 → `core.sessionClosed(sessionId)`（先摘后清，幂等；design D3）；`IdleStateHandler` 读空闲事件 → 关闭连接走同一清理路径
- [x] 6.2 端到端测试：持锁客户端断连后其他客户端即刻获取（无需等租约）；排队客户端断连后授予顺序不含该等待者；短空闲时限下无读连接被断开且会话被清理（规格"断连会话清理与空闲检测"三场景）
- [x] 6.3 验证：断连清理事件测试、空闲超时测试通过

## 7. 可执行 jar 与冒烟（P1-17）

- [x] 7.1 `maven-shade-plugin` 配置（`mainClass = OpenLatchServer`），`package` 阶段产出可执行 jar；`slf4j-simple` 随 jar 打包
- [x] 7.2 冒烟测试：`java -jar` 独立启动（临时端口配置），完整执行 HELLO → ACQUIRE（授予）→ LEASE_RENEW → RELEASE（`fullyReleased = true`）序列，断言各响应状态（规格"可执行交付形态"）
- [x] 7.3 验证：`mvn -s /home/lam/repo/settings.xml clean verify` 全模块全绿；jar 独立启动成功；冒烟通过；**M2 退出**——规格 `lock-server` 全部 Scenario 均有对应测试证据
