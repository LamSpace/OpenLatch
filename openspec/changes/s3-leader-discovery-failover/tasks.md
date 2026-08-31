# Tasks: s3-leader-discovery-failover

对应详设 §13.3 子任务：组 1=P2-11，组 2=P2-12，组 3=P2-13，组 4=P2-14（S3 退出门），组 5=文档收口。

## 1. 协议 v2 扩展（P2-11）

- [x] 1.1 `openlatch.proto`：`HelloResponse` 新增 `leader_address=6`（复用 `leader_hint=5` 语义定稿）；`AcquireResponse`/`ReleaseResponse`/`LeaseRenewResponse` 各新增 `leader_node_id`/`leader_address`；`MessageType.CLUSTER_VIEW=7` + `ClusterView`/`NodeInfo` 消息 + Envelope oneof 新增 `cluster_view=19`；注释逐字段标注 v1/v2 引入版本
- [x] 1.2 新增 `OpenlatchProtoContractFreezeTest`：Phase 1 基线字段号/枚举值逐项钉死 + v2 新增项编号冻结（对齐 `RaftProtoContractFreezeTest` 先例）
- [x] 1.3 服务端版本门：`OpenLatchServer.PROTOCOL_VERSION=2`，握手接受 `client_protocol_version ∈ {1,2}`、其余 `INVALID_REQUEST`+断连；应答 `Envelope.protocol_version` 回显请求版本、`server_protocol_version=2`（`ServerSessionHandler.helloResponse` 集群分支填 leader 字段，单机分支留空）
- [x] 1.4 `ProtocolCodecTest` 扩展：v2 全消息 round-trip；`HandshakeTest` 三分支（v1 OK/v2 OK/v3 拒绝）；v1 回归——现有 client 全套件不改一行全绿（单机 + 直连 Leader 两形态）

## 2. LeaderTracker 与 Follower 角色语义（P2-12）

- [x] 2.1 `ClusterConfig` 新增可选键 `openlatch.cluster.client-addresses`（`id@host:port` 列表）：解析、逐项校验（格式/nodeId 唯一）、缺省空表降级；`ClusterConfigTest` 补配置体系 spec 的 4 个新场景
- [x] 2.2 `LeaderTracker`（`server.raft`，Javadoc 按契约级撰写）：`LockStateMachine.notifyLeaderChanged` 保留 `newLeaderId` 并经扩展后的 observer 通道产出 `{leaderNodeId, leaderAddress}` volatile 快照；地址查 2.1 映射、缺则空串；`ClusterRuntime` 装配并暴露
- [x] 2.3 `ClusterRequestHandler` 分车道：`validateEnvelope` 角色门仅作用于 ACQUIRE 并补随附 hint；RELEASE/RENEW 摘角色门走 `gateway.submit()` 转发、跳过本地 `shadow().hasSession` 预检（权威判定在 Leader 应用点）；`retryableError` 拆分——leadership 丢失→`NOT_LEADER`+`leader_node_id=-1`，内部失败→`INTERNAL_ERROR`
- [x] 2.4 `CLUSTER_VIEW` 处理：`ServerSessionHandler` 路由，任意节点以 2.2 单源 + 本地配置作答（含单机模式 `INVALID_REQUEST` 拒绝）
- [x] 2.5 服务端集成用例（`ClusterHarness` 增"按角色取连接"入口 + `TestProtocolClient` 直发 v2）：Follower ACQUIRE 拒绝且 hint==真实 Leader（spec 场景 1/4）、选举空窗 hint=-1 且无悬挂（场景 3）、home=F 会话跨 failover 续租/释放成功（场景 2）、未登记会话转发被 `SESSION_EXPIRED` 拒（场景 5）、HELLO/PING/CLUSTER_VIEW 在 Follower 可用且不涨日志（cluster-node-lifecycle 新 ADDED）、三消费方 leader 身份一致（提示权威来源 spec）

## 3. 客户端 Leader 发现与故障转移（P2-13）

- [x] 3.1 spike（时限半天）：Ratis 3.3.0 leader handoff/transfer API 存在性与语义——产出结论回写 design D8（不可用即切换近似方案，不阻塞后续任务）
- [x] 3.2 `ClientConfig`/`Builder` 增 seeds 列表（单地址入口保持兼容）；`ConnectionManager` 连接目标参数化 + `redirect(host,port)` 入口 + 断连重连"先原地址后轮询种子"；`RequestMultiplexer`/`ConnectionManager`/`OpenLatchClient` 的 `PROTOCOL_VERSION` 升 2
- [x] 3.3 业务层 `NOT_LEADER` 拦截与改道（`OpenLatchClient`）：ACQUIRE 携效 hint→leader 连接重放（新会话新 requestId 空间）、hint=-1→原地退避重发；存量锁 RENEW/RELEASE 按 `HeldLockRegistry.HeldEntry.sessionId` 路由 home 连接；连续 3 次 `NOT_LEADER`→种子扇出 `CLUSTER_VIEW` 强制发现；全程受请求超时约束、到时快速失败；home 存量清零→连接收口
- [x] 3.4 客户端单元测试（脚本化 `LeaderStub`，不起真集群）：§6.3 逐分支——启动直连/hint=-1 退避/重定向重放/3 次强制发现/种子轮询/全灭超时抛错/双车道路由与收口，每条边一用例
- [x] 3.5 客户端集群 IT（client 测试域 in-JVM 起真 3 节点 `OpenLatchServer` 集群、真 TCP 接入，`ClientClusterIT`）：spec "Leader 发现与故障转移" 核心场景复跑——种子 Follower 重定向+杀主恢复、home=被杀 Leader 恢复（含重连收敛）、存活让位持锁不丢（§8 行 2 端到端）、隔离 Leader 续租连败判丢、等待者跨 failover 重排（恢复时限放宽防 CI 抖动，语义断言为主）；看门狗 `NOT_LEADER` 计入连续失败由 server 侧用例覆盖
- [x] 3.6 client-sdk 回归：断连重连、等待闭环、优雅关停既有套件全绿（单机形态零 diff）

## 4. 杀 Leader 演练自动化（P2-14 · S3 退出门）

- [x] 4.1 进程级演练骨架：`-Pdrill` profile + `@Tag("drill")`；3 进程起停（复用 `ClientProcessKillIT` 模式）、节点角色探测、单键混合负载驱动、计时器（t_kill→首次成功业务应答）
- [ ] 4.2 断言与证据：恢复 < 10s；双场景——home=死主（失锁回调触发，§8 行 1）/ home=存活 Follower（续租不断、failover 后释放成功，§8 行 2）；等待者重排队最终获授；不变式检查器（同 key 至多一写持有；停载+一租约期后锁表空）输出结构化演练报告入库 `docs/`
- [ ] 4.3 详设 §11 验收 1/2 证据收集：演练报告 + 组 2/3 IT 结果汇总为 S3 退出检查单

## 5. 文档与契约收口

- [x] 5.1 详设 v1.2 修订回写：§6.2 hint 字段编号定稿（复用 5 + 新增 6）与写响应 hint 载体补全；§9 配置表加 `client-addresses`；§6.1 v1 客户端口径收紧（存量锁可用、新获取建议直连主）；§13.3 P2-12 措辞对齐分车道裁决；§3.2 标注 ForwardingProxy 由 `ReplicationGateway` 既有车道承载、不另立类
- [x] 5.2 用户文档：部署升级顺序（先服务端后客户端）、NTP/配置说明沿用 §9、§8 一致性声明原文（含"改连即新会话、存量锁经 home 转发车道保全"表述）
- [ ] 5.3 `mvn -s /home/lam/repo/settings.xml clean verify` 全绿（默认构建不含 drill）+ `-Pdrill` 演练通过 → S3 退出

## 开放项（未闭环，随 S3 进度提交标记）

- **P2-14 进程级演练 <10s 计时门（4.2 / 4.3 / 5.3）**：`LeaderKillDrillIT`（`@Tag("drill")`，默认构建经 failsafe excludedGroups 排除）在本机 8 核共享环境 `kill -9` 崩溃选举间歇超过 10s 判定线（连跑 6 次约 2-3 通过；通过时实测恢复 0.8–1.8s，远优于阈值）。客户端在有主后即时恢复已验证，瓶颈为 SIGKILL 崩溃选举（无优雅让位）叠加多 JVM 资源争抢，属环境敏感项。语义正确性由确定性套件兜底：`ClientClusterIT`（in-JVM 真 3 节点：home-kill 恢复、存活让位锁不丢、隔离判丢、等待重排，满载 `clean verify` 下全绿）+ `LeaderFailoverServerTest`（选举空窗/提示跟随/转发车道/三源一致）。**处置**：`<10s` 数值门在独占硬件上复核通过后关闭 4.2/4.3/5.3，并执行 S3 退出检查单。
- **实现期修复记录**：crash failover 重连收敛缺陷——多种子客户端重连退避在恢复窗口内指数倍增、种子轮询被拖慢致恢复超时；修复为"扫完一圈种子才抬升退避"（单种子 Phase 1 语义逐字节不变）+ `SeedDiscovery` 串行改并发扇出 + retarget 建连 1.5s 封顶超时降级强制发现。修复后 home-kill IT 由间歇失败转 5/5 稳定。
- **用户文档已如实标注**：《Phase2 集群部署与故障转移》§5"已知限制"记录 drill 环境敏感性。
