# Design: s3-leader-discovery-failover

## Context

S2 已交付复制状态机全链路（`server.raft` 包：`RaftSubsystem`/`LockStateMachine`/`ReplicationGateway`/`SessionCoordinator`/`LeaseExpiryDriver`/`ClusterRequestHandler`），但 S3 的起点由五个代码事实塑形（均已核对于 master）：

1. `ClusterRequestHandler.validateEnvelope()` 对三类写请求一律做 `gateway.isLeaderAuthoritative()` 角色门，非 Leader 回无提示的 `NOT_LEADER`——注释自证为"S2 兜底：S3 未上线的重定向空窗"；`retryableError()` 亦把全部提交失败映射为 `NOT_LEADER`，语义混叠。
2. 转发通道已存在：`SessionCoordinator` 的 `SESSION_OPEN/CLOSE` 从 Follower 经 `gateway.submit()` 由 Ratis 内部客户端路由至 Leader 提交、本副本回放回执——即详设 §3.2 `ForwardingProxy` 的机制本体。
3. `CoreEngine` 归属校验不对称：`LockEntry.release` 要求 `Owner=(sessionId,threadId)` **且** token 匹配；`LockEntry.renew` 仅需调用会话存活 + token 匹配。跨会话可续不可解。
4. 客户端 `HeldLockRegistry.HeldEntry` 已记录每把锁获取时的 `sessionId`——"锁 → home 连接"路由的现成簿记。
5. Ratis `notifyLeaderChanged(memberId, newLeaderId)` 抵达 `LockStateMachine.java:156` 后被折算为 `boolean`，新 Leader 身份即被丢弃；而 `SessionCoordinator.handleLostPeer` 已有 `"n<id>" → nodeId` 的解析先例。

约束：`openlatch-core` 零改动；单机模式（`enabled=false`）行为与 Phase 1 逐测试一致；v1 wire 字段号冻结（`replicated-state-machine` spec "客户端 wire format 零扰动"）；详设 §5.2 规则 2 已预告"Leader 校验 sessionId **登记于复制状态**"。

动机见 proposal.md - Why；行为契约见本变更 specs/ 四份 delta。

## Goals / Non-Goals

**Goals:**
- 客户端零人工干预跟随 Leader（启动发现、运行中重定向、断连种子轮询），杀 Leader 端到端恢复 < 10s（详设 §11 验收 2 的计时部分）。
- 存活会话的存量锁跨 Leadership 变更可续可解（验收 2 的锁保留部分），机制对 v1 客户端同样成立。
- `NOT_LEADER` 从占位语义精确化为"拒绝 + 随附提示 + 空窗可辨（-1）"。
- 进程级演练自动化并纳入可重复的构建 profile。

**Non-Goals:**
- 会话跨节点移交（`resume_session_id`/`SESSION_ADOPT` 一类协议）——被 D1 论证为不必要。
- spring-boot-starter 的 seeds/集群配置透传；等待队列复制（§4.4 既定不做）；快照与追赶（S4）；分区/滚动重启演练（P2-18）；`ForwardingProxy` 新类——转发复用既有提交通道，不另立组件。

## Decisions

### D1. Follower 写请求分车道：ACQUIRE 拒绝+hint，RELEASE/RENEW 转发

**选择**：ACQUIRE 在 Follower 一律 `NOT_LEADER`+hint（现状保留并补 hint）；RELEASE/RENEW 摘除角色门，走 `gateway.submit()` 转发车道（与 `SESSION_OPEN` 同路），Leader 以复制状态做会话与归属权威校验。

**理由**：验收 2 要求"Leader 切换后存活会话的锁不丢"。纯拒绝模型下，home 被降级的客户端续租收 `NOT_LEADER` → 看门狗两次计数误判丢锁，或改连新会话——事实 3 注定了新会话"可续不可解"，存量锁必然烂到租约过期。而 ACQUIRE 恰不能转发：QUEUED 登记与 `AWAIT_NOTIFY` 推送是 Leader 本地态（`ReplicationGateway.pushAwaitNotify` 从 Leader 的连接注册表反查 channel），Follower 家会话的等待项在 Leader 上推不出去；§4.4 的"向新 Leader 重新排队、位次重置"本来就以换新会话为语义。两车道分界正落在"是否依赖当值节点本地态"上。

**备选（否决）**：(a) 全拒绝 + v2 会话恢复字段——需新增 wire 字段、`SESSION_ADOPT` 日志条目、旧连接关闭时抑制 `SESSION_CLOSE` 的移交竞态，改动面与竞态面都大一个量级；(b) 全转发——ACQUIRE 转发的排队通知断链（上述），且掩盖客户端直连 Leader 的延迟收益；(c) release 改为纯 token 校验以放行跨会话释放——违反 `CoreEngine` 零改动约束，且破坏误释放防护。

**推论（实现期修正）**：转发车道摘除的仅是角色门，Follower 本地载荷/键/`shadow().hasSession()` 预检全部保留——D12 保证握手完成时连接自身 sid 必已在本副本应用，本地预检不存在误拒；会话被批量清理后本地即 `SESSION_EXPIRED`（零转发、不产生条目），Leader 应用点的 `REJECT_SESSION` 为权威兜底（spec 场景"未登记会话的转发被拒"）。

### D2. hint 字段：复用 `leader_hint=5` 承载 nodeId，新增 `leader_address=6`

**选择**：`HelloResponse` 用既有 int64 `leader_hint`（field 5）= Leader nodeId（-1=未知），新增 string `leader_address`（field 6）。三个写响应各新增 `leader_node_id`/`leader_address` 两字段（编号按各自 message 现有最大字段号顺延，实现期定稿并冻结）。

**理由**：field 5 在 Phase 1 即按"Leader 提示"预留、类型（int64）与 nodeId 语义严丝合缝；另起 `leader_node_id=6`（详设 §6.2 字面）会永久空置 5 且同一响应出现两个 leader id 字段。详设 §6.2 的编号写错由本变更以 v1.2 修订回写收口（先例：v1.1 回写 S1）。OK 应答不填 leader 字段（proto3 缺省），仅拒绝路径携带，避免"每响应多两字段"的噪声。

**备选（否决）**：照抄 §6.2 的 6/7 编号——字段 5 成为死号，且违背"预留即所用"的原始意图。

### D3. `NOT_LEADER` 语义三分 + 单一提示源

`NOT_LEADER` 仅在两种场景出现并随附 `leader_node_id`：本节点非 Leader 且 Leader 已知（真实 nodeId，客户端改连）；选举空窗（-1，客户端原地退避重试，不改连）。`retryableError()` 的"一切提交失败 → NOT_LEADER"混叠拆开：leadership 丢失导致的在途失败按空窗处理（-1），网关关停等内部失败回 `INTERNAL_ERROR`。新增 `LeaderTracker`（`server.raft`）：事实 5 的通道保留 `newLeaderId`，折算 `{leaderNodeId, leaderAddress}`（地址查 D4 配置，缺映射则地址为空串），volatile 快照 + 订阅接口；HELLO 填充、`NOT_LEADER` 填充、`CLUSTER_VIEW` 作答三消费方共用此单源。**权威受理判定不变**（仍 `isLeaderAuthoritative()`）——提示视图滞后至多让客户端改连撞空一次，由 D6 的强制发现兜底；杜绝"陈旧提示致误受理"不可能发生，因受理从不读提示。实现上在 `ApplyObserver` 通道扩展携带 leader 身份的事件（或等价的注册监听），不建第二套订阅机制。

### D4. Leader 接入地址：新配置键 `openlatch.cluster.client-addresses`，可选、可降级

**选择**：`id@host:port` 列表，可选；未配置时 `leader_address`/`NodeInfo.address` 以空串表达，客户端以种子扇出发现兜底（逐一 `CLUSTER_VIEW`，每个节点自报本机接入地址——节点自身地址在绑定后本地即知，无需配置）。

**理由**：peer 表只携 raft 地址（`id@host:raftPort`），客户端接入地址无复制通道可考。备选 (a) 扩 peers 语法——破坏既有格式契约与 `ClusterConfigTest`；(b) Leader 当选时追加"自报地址"日志条目——详设 §4.2 条目类型冻结且"先组网后开端口"意味着 Raft 启动时本机接入端口尚未定（可配 0 由 OS 分配），自报无源；(c) 约定全节点同接入端口——同机多节点测试直接失效。独立可选键最外科：不改既有键语义，缺省即降级，降级路径（客户端扇出）本就是 D6 强制发现的既有能力。

### D5. 服务端版本门：接受 {1,2}，响应回显请求版本

`OpenLatchServer.PROTOCOL_VERSION` 升 2；握手门校验 `client_protocol_version ∈ {1,2}`；应答信封 `protocol_version` 回显**请求版本**，`server_protocol_version` 报 2。v1 客户端因此看到与 Phase 1 逐字段同形的响应（新增字段为未知字段，既有"未知字段容忍"需求已覆盖解码路径），v1 回归基线（现有 client 全套件）不改一行可过。v2 客户端 ↔ v1 服务端**不兼容**（v1 服务端按既有规则拒绝版本 2）——本项目无已部署实例，接受此前向不兼容，部署顺序"先服务端后客户端"写入用户文档。

### D6. 客户端连接拓扑：稳态单连接，故障期 home/leader 双车道

`ClientConfig` 增 seeds 列表（单地址入口 = 一元种子，builder 兼容）。`ConnectionManager` 目标参数化（连接目标从配置读取改为可变字段 + `redirect(host,port)` 入口），复用既有状态机与退避。`OpenLatchClient` 业务层拦截 `NOT_LEADER`（`RequestMultiplexer` 保持笨，不掺集群语义）：

```
稳态:   [home = leader]  单连接承载全部
failover 后 home ≠ leader 且 home 存活:
  [home 连接]   保留——该会话存量锁的 RENEW/RELEASE 按 HeldEntry.sessionId 路由至此(转发车道)
  [leader 连接] 新建——新 ACQUIRE/排队/通知闭环(同 requestId 幂等规则在新会话内成立)
home 会话存量锁清空(释放/裁决丢弃) → home 连接关闭,收敛回单连接
```

发现规则（§6.3 全分支）：启动 HELLO hint 有效→直连（连接可留作 `CLUSTER_VIEW` 查询）；hint=-1→退避重 HELLO；改连失败先试原地址再轮询种子；连续 3 次 `NOT_LEADER`（常量起步，不配置化）→ 强制种子扇出发现；一切等待受请求超时（默认 5s）约束，到时抛 `OpenLatchTimeoutException`/`ServerUnavailableException` 族错误。改连即新 `SessionContext`（新 requestId 空间）；同会话内的重放（空窗退避重试、通知重发）沿用原 `requestId`。看门狗对 `NOT_LEADER` 计入连续失败（与 OVERLOADED 同格）——转发车道正常工作时不会遇到，出现即异常信号，宁可保守计数。

**理由/备选**：把重定向做进 Multiplexer 或连接层单点——否，ACQUIRE 与 RENEW 的处置相反（一改连一守 home），必须有锁→连接的路由知识，只有业务层持有 `HeldLockRegistry`。

### D7. `CLUSTER_VIEW` 作答

任意节点本地作答：成员表来自 `peers`+`client-addresses`（或未配置时以本机自报、他员地址留空），`is_leader` 来自 `LeaderTracker`（自己=tracker 判定本节点当选）。不查 Ratis `DivisionInfo` 实时视图——提示允许一事件窗内陈旧（D3 论据），而 `CLUSTER_VIEW` 是诊断/发现辅助，客户端拿到候选后仍走 HELLO/请求验证。

### D8. 演练分层：进程级 drill（计时）+ in-JVM IT（语义）

- **in-JVM**（默认 verify）：`ClusterHarness.stopNode(leader)` 驱动，断言**语义**——hint 跟随换主、failover 后 home=F 的续租/释放成功、等待者重排队、选举空窗 `-1`；恢复时限放宽（如 30s）防 CI 抖动。
- **进程级**（`-Pdrill` + `@Tag("drill")`）：复用 `ClientProcessKillIT` 的 shaded-jar 起停模式起 3 进程，`kill -9` Leader 计时"首个成功业务应答"< 10s（S1 实测选主 548ms + 客户端改道，余量约 6 倍），双场景拆分：home=死主（失锁回调，§8 行 1）/ home=存活 Follower（锁全程不丢，§8 行 2 的验收形态）；全程不变式检查器（同 key 至多一写持有、停载+一租约期后锁表清空）——检查器 S3 建好，P2-19 复用。
- §8 行 2 的"节点存活但降级"精确态需真 stepdown：spike Ratis 3.3 的 leader handoff/transfer API（S1 未触及）；不可用则进程级以"重启原 Leader 后其以 Follower 复活"近似 + in-JVM hint 跟随用例覆盖语义，行 2 的精确分区形态移交 S4 命名空间演练。**先在 tasks 里立 spike 项。**

## Risks / Trade-offs

- [实现期发现并修复：home=被杀 Leader 的 crash failover 收敛慢] → 根因是重连退避在恢复窗口内指数倍增（200ms→…→10s），种子轮询被拖慢、叠加选举窗口致恢复超 10s 乃至不恢复。修复：`ConnectionManager.nextReconnectDelayLocked` 对多种子客户端"扫完一圈种子才抬升退避"（单种子 Phase 1 语义逐字节不变）；`SeedDiscovery` 由串行改并发扇出（无主 follower 的 HELLO 挂满超时不再队头阻塞）；`retargetAcquireLane` 建连预算封顶 1.5s 超时即降级发现。修复后 in-JVM home-kill IT 由间歇失败转 5/5 稳定。
- [进程级 kill -9 drill 在本机 8 核/共享环境仍间歇 >10s] → 崩溃选举（非优雅让位）叠加 3 个重 JVM 的 CPU 争抢，选举偶发超 10s；客户端一旦有主即 <2s 恢复（快通过即证）。缓解：语义正确性以 in-JVM `ClientClusterIT`（5/5，含 home-kill/存活让位/隔离/等待重排）+ `LeaderFailoverServerTest` 为确定性回归；drill 保持 `@Tag("drill")` 门控于默认 CI 之外，<10s 数值门应在独占硬件验证（用户文档 §5 如实标注）。
- [陈旧 hint 把客户端引向死节点] → 改连失败即落入种子轮询；3 次 `NOT_LEADER` 强制发现为二道保险；提示滞后窗口 ≤ 一个 Ratis 事件传播。
- [转发车道多一跳内部提交延迟（存量 RENEW/RELEASE）] → 仅发生在"home≠leader"的过渡态；稳态客户端直连 Leader，无额外跳。
- [选举窗跨越两个续租周期 → 看门狗真判丢] → 默认租约 30s ≫ 选举 ~3s；演练以默认参数跑，短租约用例单独断言"计数保守"行为而非误杀。
- [双车道连接生命周期泄漏（home 连接迟迟不收口）] → 收口条件绑定 `HeldLockRegistry` 会话存量清零事件，单元测试覆盖"清零即关"。
- [Ratis handoff API 不存在/行为不符] → D8 已备近似方案，无阻塞分支；spike 时限半天，超时即切换。
- [v1 客户端在 Follower 上无法新获取] → 与详设 §6.1 "可正常工作"表述的收紧（限存量锁 + 建议直连主）以 v1.2 文档回写收口，不掩盖。
- [v2 客户端连旧服务端失败] → 无既有部署；用户文档明示升级顺序。
- [10s 计时断言在重载 CI 抖动] → drill profile 与默认构建隔离；语义正确性不依赖计时阈值。

## Migration Plan

常规合入主干（与 S2 同名单一发布物）。部署顺序：先升级全部服务端（v2 同时兼容 v1/v2 客户端），再升级客户端。回滚：客户端回退 v1 即可继续工作（存量锁经转发车道无损）；服务端回退至 S2 则 S3 客户端的 ACQUIRE 改道逻辑收到 `NOT_LEADER` 无 hint（v1 码）→ 落入种子轮询，功能降级不破坏正确性。

## Open Questions

- ~~Ratis 3.3.0 leader handoff API~~ **已解决（3.1 spike）**：`AdminApi.transferLeadership(newLeader, timeoutMs)` 与 `LeaderElectionManagementApi.pause()/resume()` 均存在，§8 行 2"存活 Leader 主动让位"用例可在进程级演练中以 API 驱动（备选兜底仍在）。
- 写响应 hint 两字段的精确编号（顺延分配后由契约冻结测试钉死，归档前回写详设 v1.2 表格）。
