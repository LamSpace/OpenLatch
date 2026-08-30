# Tasks：Phase 2 S2 复制状态机

对应详设 §13.2 P2-05～P2-10；验证口径引详设 §10 与本变更 specs。全部 mvn 命令必须带 `-s /home/lam/repo/settings.xml`。

## 1. P2-05：raft.proto 定义

- [x] 1.1 `openlatch-protocol` 新增 `raft.proto`：`RaftEntryType`（§4.2 编号逐字一致）、`RaftLogEntry`、各条目 `command_payload` 包装消息（复用 Phase 1 请求消息 + session_id）、`SnapshotState` 骨架（§7.1 字段，锁条目 + 会话注册表，无等待队列）
- [x] 1.2 编号冻结测试：以 golden 文件比对全部枚举值与字段号，改动即构建失败；确认 `Envelope` oneof 与客户端 wire format 零 diff
- [x] 1.3 验证：`mvn -pl openlatch-protocol -am package` 生成代码编译通过，Phase 1 协议回归用例全绿

## 2. P2-06：LockStateMachine 适配器（PoC 内核转正）

- [x] 2.1 根 pom `dependencyManagement` 引入 `ratis.version=3.3.0`（ratis-server/grpc/client/metrics-default + server test-jar），`openlatch-server` 挂依赖（D7 首批 CI 暴露点；如 test-jar 引发 CI 时长问题按 D8 挂 integration tag）
- [x] 2.2 转正 `LockStateMachineCore` / `EntryClock` / `ShadowTable` 至 `io.github.lamspace.openlatch.server.raft`：payload 编解码切换至主干 `raft.proto`；apply 结果（`ApplyResult` 等价物）完整回传；Javadoc 按 CLAUDE.md §5 全量补齐（含 private；EntryClock 线程契约入类级 Javadoc，D2）
- [x] 2.3 Ratis `StateMachineBase` 适配器 `LockStateMachine`：initialize/apply（线程内 setApplyNow→applyEntry→clear）/notify 事件边界；NOOP 条目空操作
- [x] 2.4 确定性回放测试转正：`DeterminismTest` 迁入主干并新增随机序列属性测试（≥100 组混排序列两次回放 digest 一致）与"apply 线程外物理时钟回落"用例、多线程不串扰反例用例（D2）
- [x] 2.5 验证：`mvn -pl openlatch-server -am test` 全绿；`openlatch-core` git diff 为空

## 3. P2-07：ReplicationGateway + 节点装配（含 D6）

- [x] 3.1 配置体系：`ServerConfig` 并入 §9 `openlatch.cluster.*` 七键解析与校验（enabled=true 必填项缺失即启动失败，不静默降级）
- [x] 3.2 `RaftSubsystem`：Ratis 节点装配/启停、与 `OpenLatchServer` 生命周期绑定（先组网后开端口）、`enabled=false` 零触碰（懒加载不触 Ratis 类）
- [x] 3.3 `ReplicationGateway`：预检查快速失败 → `RaftLogEntry` 提交 → `CompletableFuture` 挂起 → apply 线程完成 → eventLoop 弹回写应答（D4）；接缝落在 `ServerSessionHandler` 的集群路由 + 公共 `RequestDispatcher.errorResponse`（实现期定夺：不改 `RequestDispatcher` 内部映射可见性，`ApplyResult→Envelope` 映射在 `ClusterRequestHandler` 以同构表实现，Phase 1 文件零触碰）
- [x] 3.4 排队路径（§4.5/D3）：需排队 ACQUIRE 本地登记即时回 QUEUED 不写日志；并发同键预演失效时应用点 Leader 补登记、Follower 忽略队列副作用
- [x] 3.5 Leadership 丧失事件：在途 future 全量异常完成（可重试错误）；`NotifyEventBridge` 事件经 session eventLoop 回写适配
- [x] 3.6 MiniRaftCluster 集成用例：授予应答=应用结果；提交后立即杀 1 follower 重启后 digest 一致（多数派证明）；QUEUED 与预检查拒绝前后日志条目数不变；在途失去 Leadership 快速失败
- [x] 3.7 验证：详设 §10"复制集成"行授予/排队两路径用例通过

## 4. P2-08：会话集群化

- [x] 4.1 spike（半天，D5）：验证 Leader 侧 peer 失联判定的 API 可得性与滞后口径，结论回写 design D5（含防抖阈值定夺）
- [x] 4.2 sessionId 生成改造：接入层 HELLO 路径按 `(nodeId<<32)|localSeq` 分配（仅集群模式；单机路径保持 Phase 1 随机 sid），`SessionCoordinator` 提交 `SESSION_OPEN`；sidMap（逻辑 sid→本地 engine sid）转正
- [x] 4.3 断连传播：接入节点连接断开 → `SESSION_CLOSE` 条目；副本回放执行与 `CoreEngine.sessionClosed` 等价清理；未登记会话写请求拒入（REJECT_SESSION 不写日志）
- [x] 4.4 失联批量清理：Leader 判定接入节点失联后按注册表批量补发 `SESSION_CLOSE`（每会话一条，D5 spike 结论参数化）
- [x] 4.5 集成用例：登记/断连清理/节点失联清理三路 digest 一致；宕机节点会话的锁被释放且可重新授予；failover 后新 Leader 认账存活会话
- [x] 4.6 验证：§13.2 P2-08 验证列三用例全绿

## 5. P2-09：租约到期复制

- [x] 5.1 Leader 到期驱动：集群模式下租约扫描仅在 Leadership 侧运行，到期 → `LEASE_EXPIRE_ENTRY(key, leaseToken)` 提交；`EntryClock` 回放语义保证到期条目按携带时刻生效；Leader 切换后新 Leader 扫描续驱（切换窗口漏扫由首扫补偿）
- [x] 5.2 回放幂等：`CoreEngine` 按 token 校验过期条目，不匹配空操作（ABA）；Follower 侧不启动到期扫描
- [x] 5.3 用例：到期全副本生效且队首唤醒仅 Leader 本地；ABA 交错用例（过期条目在途 + key 重新授予新 token → 条目空操作）；failover 后到期继续生效（不提前、不漏扫，误差 ≤ 一个扫描周期）
- [x] 5.4 验证：§13.2 P2-09 验证列用例通过

## 6. P2-10：3 节点复制集成（S2 退出门）

- [x] 6.1 全量比对工具：digest 比对从 ShadowTable 抽为 `openlatch-server` testFixtures 公开工具（S4 快照比对共用，D8）
- [x] 6.2 集成矩阵：正常 3 节点授予/释放/续租/到期/会话全链路；停 1 Follower 仍服务 + 恢复后 digest 追平；仅剩单节点不可授予（写全部可重试失败、无双授）
- [x] 6.3 退出核验：`mvn clean verify`（含 `-s`）全 reactor 绿；Phase 1 单机回归全绿（enabled=false 回退保证）；`openlatch-core` 零改动 `git diff` 证据留存；详设 §10 状态机单元 + 复制集成两行逐项勾稽
- [x] 6.4 观察项登记：Ratis 3.3.0 集成期新发现（日志段回收、install pipeline 相关）记入变更目录备 S4 评审
