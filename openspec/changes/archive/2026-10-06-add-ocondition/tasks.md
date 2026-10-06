# Tasks

## 1. 协议 v9 面

- [x] 1.1 `openlatch.proto` 新增 `CONDITION_OP = 21`、`ConditionOp` 枚举（0–2）、`ConditionOpRequest`/`ConditionOpResponse`、`Envelope.payload` 槽 45/46、`AcquireRequest.optional string condition = 8` 与管理面 `condition_waiters`/`AdminConditionWaiterInfo` 续接字段；`StatusCode`/`LockType` 零新增、`raft.proto` 零改动。验证：protocol 模块编译，生成代码字段与 design D9 冻结表逐项一致
- [x] 1.2 契约冻结测试扩 v9 面（21 消息类型/14 锁类型/14 状态码/37 payload 消息/编号表、v1 基线零变更、`RaftEntryType` 止于 13 且 `raft.proto`/`SnapshotLock` 字节零差异即半入日志编号证据、`AWAIT_NOTIFY` 定义零变更）。验证：冻结测试绿
- [x] 1.3 `ProtocolCodecTest` 扩 presence 往返：`condition` 缺省/空串/短名/恰限 `maxKeyLength` 四形态可判别、`ConditionOpRequest` 三操作全形状（含矩阵违例形）与应答字节级保真。验证：往返测试绿

## 2. core：条件等待集与折叠裁决

- [x] 2.1 `LockEntry` 增条件等待集（`condition_name → 到达序 Waiter 队列`，条目锁内读写）：折叠应用语义——同关键区内 [持有归属匹配则重入一步清零+清租约+既有队首通知评估，否则释放零操作] + [(会话,request_id) 幂等登记]；合并深度护栏（队列+集合合计达 `max-queue-depth-per-key` → 等待满拒绝）；SIGNAL 取一人/SIGNAL_ALL 按到达序全员搬运入 `waiters` 队列尾，搬运后锁空闲且队首即搬运项时同关键区即推；LEAVE 幂等摘除；授予应用点顺带摘除同 (会话,request_id) 集合残留。验证：`LockEntryConditionTest` 判定矩阵（一步清零/非持有 ghost/重挂幂等/护栏超限/SIGNAL FIFO/SIGNAL_ALL 序/空集无操作/LEAVE 幂等/授予摘除）绿
- [x] 2.2 `AcquireCommand` 增 `condition` 分量、`CoreEngine.acquire` 折叠分派与 `CoreEngine.conditionOp` 门面（判定顺序：会话→key→家族/形态匹配（READ/WRITE/非 LOCK 家族携带 condition 拒绝）→护栏→登记/搬运/摘除）；`waiterCount` 与明细观察面计入条件等待者（对照 topic 不计入口径注释互引）。验证：`CoreEngineConditionTest`（两路门面 + 观察口径 + Follower 语义下集合空）绿
- [x] 2.3 `SESSION_CLOSE`/断连清理扩条件集摘除（复用既有 closeSession 家族钩子，"死亡不吞锁"——持有/租约/队列零触碰）；恢复路径确认零改动（等待集不入快照与 `CoreStateRestore`）。验证：会话清理测试（摘除后登记归零、锁账不变）与恢复回归绿
- [x] 2.4 上述全部 core 侧 Javadoc（线程模型=条目锁单关键区、Leader-only 副作用声明、ghost 收敛三路、与 awaiters 判例同生命周期）。验证：`maven-javadoc-plugin`（show=private）构建通过

## 3. 服务端：接入层、复制边界与可观测

- [x] 3.1 `OpenLatchServer.PROTOCOL_VERSION` 8→9；`RequestDispatcher`/`ClusterRequestHandler` v9 门（v≤8 会话 CONDITION_OP 与带 condition 的 ACQUIRE 同型 `INVALID_REQUEST` 不断连）、折叠形状矩阵入口裁决（wait_ms=0 违例/形态白名单 REENTRANT·FAIR·SIMPLE/condition 非空≤maxKeyLength，判定唯一在接入层）、CONDITION_OP Leader 直裁决零日志 + Follower 同型 `NOT_LEADER` 零副作用、带 condition 的 ACQUIRE 走既有 ACQUIRE 提交通道（`NOT_LEADER` 含 v2 提示）。验证：`ConditionGatingTest` + 形状/门控矩阵绿
- [x] 3.2 唤醒链路：搬运即推与释放接力的通知经既有队首通知桥投递 `AWAIT_NOTIFY`（ref=折叠 ACQUIRE request_id，零新推送类型）；`SESSION_CLOSE` 双路（本节点断连/失联探针）经既有钩子位触达 core 摘除；换主事件后新 Leader 集合空、接纳重挂（幂等登记、非持有者照常）。验证：装配/角色测试绿；模拟 kill 等待者进程后 `condition_waiters` 归零、换主脚本下重挂无重复登记
- [x] 3.3 `ServerMetrics` 新命名点：`condition.total{op,status}`（含 `NOT_HELD` 权限线；无 await/QUEUED/DENIED/OVERLOADED 线——await 归 `acquire.total` 折叠口径钉死）、`condition.waiters.max`（不含搬运项）；`waiters` Gauge 计入条件等待者；四口径（等待队深/元素/订阅/条件等待）命名点注释互引。验证：指标夹具按线归数断言（含折叠 await 计数落 acquire 的对照场景）绿
- [x] 3.4 `AdminRequestHandler` 条件读数：LIST_KEYS `condition_waiters`（Leader 非零如实、Follower 恒 0）、KEY_DETAIL 条件明细五字段（与等待队列区段并列、搬运项不重复计数）、`wait_queue_leader_only` 同口径标注、SUMMARY 等待者合计含条件；观察零扰动（不推进搬运/清扫/摘除时序）。验证：admin 夹具扩条件维（Leader/Follower 双视角 + 换主过渡 case）绿
- [x] 3.5 控制台条件呈现：锁详情条件等待区段、LOCK 行计数、概览等待口径注记；"持有已随 await 释放"形态注记不空壳；Follower 来源如实零读；无写操作入口。验证：console HTML 测试扩维绿
- [x] 3.6 server 侧全部新类/Javadoc（Leader 本地态、零日志边界、死亡不吞锁对照句）。验证：javadoc 构建通过

## 4. 客户端 SDK

- [x] 4.1 `OCondition` 公开契约（`await()`/`await(timeout,unit)`/`signal()`/`signalAll()`）与 `RemoteCondition` 实现、`OLock.newCondition(String name)`（读/写锁实现抛 `UnsupportedOperationException`）；接口级 Javadoc 全量保真/差异/降级清单（虚假唤醒 guard loop 义务与来源清单、signal 双层 `IllegalMonitorStateException`、await 权限服务端降级及理由、超时/中断返回时持锁与重入 1 级算术、命名寻址跨进程等价、换主分层"等待是承诺/signal 是事件"、RTT 成本、无 `awaitNanos`/`awaitUntil`/`awaitUninterruptibly`/异步对偶、v9 门与升级序）。验证：契约 Javadoc 审读清单齐备、javadoc 构建通过
- [x] 4.2 折叠 await 实现：经 `RemoteLock` 构造点生成带 `condition` 的 ACQUIRE 信封入既有 `AwaitTracker`（同 R 重发、终止竞争、补偿释放纪律原样）；本地超时/中断 → LEAVE fire-and-forget → 常规阻塞获取重新入锁 → 返回 `false`/抛 `InterruptedException`；本地持有检查先行（未持有即抛，不发请求）；看门狗随解锁簿记自动停摆、重获取自动重启。验证：Scripted 测试（唤醒环、超时返回持锁、中断、双层权限、重挂幂等信封复用）绿
- [x] 4.3 握手常量 8→9（`ConnectionManager`/`RequestMultiplexer`）；`ClientMetrics.statusCodeOf` 扩 CONDITION_OP 归线；`OpenLatchClient` 工厂与文档链接。验证：握手回归（v8 既有行为逐项不变）与指标归线测试绿

## 5. 集群 E2E 与守卫回归

- [x] 5.1 `ClusterConditionTest`（共享 harness，守 W2 端口纪律）：生产-消费谓词闭环（guard loop 形态）、SIGNAL_ALL 全员醒来互斥串行、跨条件隔离（"x" signal 不扰 "y"）、FAIR 位次=搬运时刻（后来者不越位断言）、换主重挂续醒（有界总超时 eventually 形态，禁裸时序断言）+ 窗内 signal 不补偿如实、kill 持有者进程等待者不受扰（不代醒契约）、kill 等待者进程 `condition_waiters` 归零。验证：七测试绿
- [x] 5.2 `StateMachineConditionTest`（复制边界守卫常驻）：纯条件流量三副本 digest 逐字节一致；await 数=既有条目增量恰一对一、signal 家族贡献恒零；重启/快照追赶无陈旧唤醒、恢复后集合空重挂续链；误塞日志即红的对照断言。验证：确定性测试绿
- [x] 5.3 快照零增量夹具：N 键 × M 条件等待者（含 await/signal/LEAVE/重挂交替）负载快照字节与同持有基线逐字节相等。验证：`SnapshotFamilyRoundTripTest` 同族扩维绿
- [x] 5.4 `RejectCodecTableTest` 扩 CONDITION_OP 与带 `condition` ACQUIRE 同型拒绝行（NOT_LEADER/门控/形状三码形不得呈"成功空应答"，W10 判例常驻）；词表/config/指标 vocabulary 夹具扩 condition 维（零新配置断言随行）。验证：三夹具绿
- [x] 5.5 `ClientConditionIT` + 客户端 Scripted 扩维（双进程同 name 寻址闭环、超时自救、锁丢失窗 signal 权限拒绝映射）；W10 @Disabled 锚与既有公平性/混沌套件零新增红确认（condition 复用 ACQUIRE 车道迁移机制的暴露面登记说明）。验证：IT 与套件绿、锚状态不变
- [x] 5.6 `BenchmarkMain` 扩 condition 相（SIGNAL 受理 ops/s、await 全闭环 P50/P99、零等待基线），报告落 `target/benchmark/` 供 W13 引用（exec 注意 `-pl` 不带 `-am`）。验证：condition 相跑通并产出基线数据

## 6. 文档与治理

- [x] 6.1 ROADMAP：决策记录补"2026-10-05 三档立项裁决"行（OCondition 信号未至提前立项、OPhaser 留档、档位规则以本行为准）；三档 OCondition 行→进行中附提案链接（归档时流转已落地）。验证：表格与决策行落盘
- [x] 6.2 WATCHLIST 登记 W13（换主窗 signal 丢失率与条件等待集水位：触发条件=生产报 await 永睡且比对到换主时点/水位逼近护栏 → 另立 change 评估 signal 状态化（入日志）或换主窗补偿通道的判例翻案流程），与 W11 交叉引用（signal 家族直发车道）。验证：表行与交叉引用落盘
- [x] 6.3 指南 zh/en 对称扩章：01 概念（条件等待集、命名寻址、等待承诺/signal 事件分层、guard loop 义务）；03 SDK condition 章（惯用法示例、超时返回持锁、双层权限、计数算术、成本模型）；05 部署（零新配置注记、合并护栏口径、回滚窗三分轨句——队列清 key/topic 天然干净/condition 会话收敛唯一约束）；07 呈现（条件区段+释放态注记）；08（`condition.total` 与 NOT_HELD 线、await 归 acquire 折叠口径、四口径之辨、W13 告警口径）；09 排查（await 不醒三分法、NOT_HELD 双义判别）；10（v1–v9 矩阵、升级序服务端先行）；术语表新词八项。验证：双语对账无单边缺章、user-documentation delta 场景逐项过
- [x] 6.4 提交前 `bash scripts/check-source-citations.sh` 全源零命中（含 `--selftest`）+ Javadoc 无内部文档引用。验证：脚本退出码 0

## 7. 全量验证（集成）

- [x] 7.1 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆 8/8 模块 BUILD SUCCESS（0 失败 0 错误；跳过项逐一注记原因，基线 = 既有 1916 测试 + condition 全族新增）。验证：构建输出与模块计数
- [x] 7.2 `-Pdrill` 全量复跑（用户在非沙箱单轮执行，负载段扩 condition 相：滚动重启/杀主下重挂续醒+signal 丢失有界断言；红了先保 `target/drill-logs/` 再清理）。验证：演练全绿并留存报告
- [x] 7.3 `openspec validate add-ocondition --strict` 通过；实现与规格偏差回写 delta 后再归档（sync --strict + grep 双查段头残留，记忆档纪律）。验证：validate 输出与归档时主规格状态
