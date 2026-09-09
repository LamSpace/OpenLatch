# Tasks: phase3-t3-admin-console

判定基线：验证命令一律 `mvn -s /home/lam/repo/settings.xml`（模块级 `-pl ... -am test`，收口全量 `clean verify` 与 `-Pdrill`）；新增 Java 源文件按 CLAUDE.md §5 配齐 Javadoc 与 License 头。

## 1. P3-11a 协议增量与服务端骨架

- [x] 1.1 `openlatch.proto`：`MessageType` 新增 ADMIN 四值（10–13）、oneof 新增八字段（24–31，含各请求 `token` 字段与 `wait_queue_leader_only` 等响应字段，字段形状按 specs/wire-protocol 增量）；协议冻结测试增量（13 类型/22 payload 完备断言、v1 基线与 `raft.proto` 零差异锁）
- [x] 1.2 `AdminConfig`（仿 `MetricsConfig` 同文件独立加载，不动 `ServerConfig` 构造面）：`openlatch.server.admin.token`（未配置 = 拒绝一切 ADMIN）；非法值快速失败
- [x] 1.3 `ServerSession` 增 `connectedAtMs`（accept 挂载时记）；`OpenLatchServer` 记启动时刻与版本暴露（D8 回落口径）；`WaitQueue` 增明细只读口（`keyWaiters(key)`、按会话等待计数，短临界区快照拷贝）
- [x] 1.4 `CoreEngine.inspect()` 明细只读观察面（D4 值对象：家族/类型、holders、租约到期时刻、waiter 位次表、latch total/count）；core 单测（预置状态逐项可见 + 并发观察零异常 + **回归底线：既有 core 全量绿**）
- [x] 1.5 验证：`-pl openlatch-protocol,openlatch-core -am test` 全绿；冻结测试含新增断言

## 2. P3-11b AdminRequestHandler 与双形态数据源

- [x] 2.1 `AdminRequestHandler`：token 常量时间校验（失败 `INVALID_REQUEST` + 断连；未配置一律拒）；四消息装配——单机读 `inspect()`，集群读引擎镜像条目 + `WaitQueue`（Leader 权威 / follower 空队列 + `wait_queue_leader_only`）+ `ServerSessionRegistry`（仅本节点接入会话、`connectedAtMs`）+ `LeaderTracker`/启动时刻/版本（SUMMARY）；分页字典序/前缀过滤/`page_size` 上限收口在 handler；`key` 未命中显式状态
- [x] 2.2 `ServerSessionHandler.channelRead0` 接入：已握手、`tryBeginRequest` 之后、cluster/dispatcher 分发之前独立早退分支；应答回显 requestId/协议版本；异常 `INTERNAL_ERROR` 兜底不悬挂；ADMIN 不进 `ServerMetrics`（不触 `recordDispatch`）
- [x] 2.3 L1 消息级单测：认证矩阵（对/错/空/未配置/握手前/v2 会话被拒不亦断连按 spec 口径）；预置多类型条目（重入多持有/读写/信号量部分许可/Latch 等待）四消息逐字段断言；分页与过滤边界；指标零污染（执行 ADMIN 前后注册表快照等值，复用 T2 词表夹具）；在途限额 OVERLOADED 对 ADMIN 生效
- [x] 2.4 集群档消息级用例：三节点 fixture——follower LIST_KEYS 镜像收敛可查、等待队列仅 Leader、`ADMIN_LIST_SESSIONS` 跨节点分治、SUMMARY 角色三值如实；全程零新日志条目断言
- [x] 2.5 验证：`-pl openlatch-server -am test` 全绿（`HandshakeTest`/`MessageLegalityTest`/`InflightOverloadTest` 既有行为零扰）；**P3-11 退出判据：§7"管理协议消息级测试"全绿**

## 3. P3-12 console 模块骨架

- [x] 3.1 新模块 `openlatch-console`（根 pom `<modules>` 增项）：依赖 `openlatch-server`（复用 `EnvelopeCodecHandler`/协议类）+ `spring-boot-starter-web`/`spring-boot-starter-thymeleaf`（Boot BOM 托管零版本）+ `spring-boot-maven-plugin` repackage；`mvn -o dependency:resolve` 核本地仓库缺件（有缺报用户预取）
- [x] 3.2 `ConsoleConfig`：`openlatch.console.server-addresses`/`admin-token`/`port`（默认 9413）/`refresh-interval-seconds`/`metrics-port`；空地址、坏端口快速失败
- [x] 3.3 `AdminClient`：每节点独立 Netty 连接（自有 daemon EventLoopGroup）、HELLO（v3、`client_name="openlatch-console"`、auth_token 留空）、同步 ADMIN 请求/应答（request_id 关联 + 超时）、断线懒重连、认证失败退避重试（不风暴）
- [x] 3.4 概览页骨架：SUMMARY 聚合渲染 + `<meta refresh>` 轮询；节点不可达/认证失败降级横幅
- [x] 3.5 验证：骨架冒烟用例——同 JVM 真 server（端口 0）+ console（随机端口），概览页 HTML 断言数字与版本；**P3-12 退出判据：SUMMARY 页面冒烟通过**

## 4. P3-13 五页面与端到端冒烟

- [x] 4.1 锁列表页：分页/前缀过滤交互、多节点来源标注、点击进详情；锁详情页：持有者明细、等待队列位次（follower 来源标注"仅 Leader 可见"）、剩余租约倒计时（刷新粒度）
- [x] 4.2 会话列表页（多节点聚合：id/接入节点/建连时间/持锁数/等待数）；节点视图页（各节点 `CLUSTER_VIEW` + SUMMARY 角色、Leader 标识、单机档如实报单机）
- [x] 4.3 概览页 sparkline：`/metrics` 代理拉取（HTTP client）+ Prometheus 文本解析 + 内存环形样本 + 内联 SVG 渲染；指标区独立降级（metrics 关闭/不可达不整页错）
- [x] 4.4 L2 端到端冒烟（console 测试，真 server 同 JVM）：预置负载 → 五页面 HTML 逐断言（列表/详情/会话/节点视图/概览含曲线区）；单机档 + 集群三节点档各一组；错令牌认证失败降级用例；单节点宕机部分可用用例
- [x] 4.5 L3 部署证据：`t3-acceptance-evidence.md`（变更目录）——实跑单机与三节点集群：server + console jar 启动序列、curl 断言输出、浏览器走查记录（用户目检）、错令牌反例；§7 T3 判据 ↔ 用例编号对照表
- [x] 4.6 验证：`-pl openlatch-console -am test` 全绿；**P3-13 退出 = T3 退出：§8-4 证据齐（冒烟通过 + 管理通道强制认证）**

## 5. 收口

- [x] 5.1 全仓 `mvn -s /home/lam/repo/settings.xml clean verify` 全绿（v1/v2/v3 既有行为零扰动，§8-6）；`-Pdrill` 进程级演练（LeaderKill/RollingRestart）在本环境稳定失败——经 **clean-master A/B 复跑同一失败**，判定为环境性（进程拉起/杀-9 时序受限）而非 T3 改动回归；等价进程内故障演练全绿（LeaderFailoverServerTest/MinorityQuorumTest/ClusterMetricsTest）；结论记入 t3-acceptance-evidence.md
- [x] 5.2 详设回写勘误：§4.2 认证承载通道（per-message 令牌，不等 T4）与编号/数据源/隔离勘误、§4.3 排序口径；README 双语知会 9413 端口、控制台启动示例与"仅限内网部署"提示
- [ ] 5.3 提交并归档（delta 同步主规格：`admin-observability`/`admin-console` 新建 + `wire-protocol`/`core-lock-engine` 增量）
