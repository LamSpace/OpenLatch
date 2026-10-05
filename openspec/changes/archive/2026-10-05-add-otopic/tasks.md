# Tasks

## 1. 协议 v8 面

- [x] 1.1 `openlatch.proto` 新增 `TOPIC_OP = 19`/`TOPIC_MESSAGE = 20`、`TopicOp` 枚举（0–2）、`TopicOpRequest`/`TopicOpResponse`/`TopicMessage`/`AdminTopicSubscriberInfo`、`Envelope.payload` 槽 42/43/44、`StatusCode.REJECT_SUBSCRIBERS = 13` 与管理面续接字段；`raft.proto` 零改动。验证：protocol 模块编译，生成代码字段与 design D9 冻结表逐项一致
- [x] 1.2 契约冻结测试扩 v8 面（20 消息类型/14 状态码/35 payload 消息/编号表、v1 基线零变更、`RaftEntryType` 止于 13 即零日志编号证据、`raft.proto` 字节零差异）。验证：冻结测试绿
- [x] 1.3 `ProtocolCodecTest` 扩 topic 往返：`payload_bytes` 缺省/零长度/非空/恰限 4KB 四形态 presence 可判别、`TopicMessage` 全字段字节级保真。验证：往返测试绿

## 2. 服务端·登记表与配置

- [x] 2.1 `ServerConfig` 增 `openlatch.server.limit.max-subscribers-per-key`（默认 64、校验 [1,1024]）与 `max-subscription-buffer`（默认 256、校验 [1,65536]），启动日志打印扩展。验证：配置夹具测试（默认值生效、越界快速失败两 case）绿
- [x] 2.2 新建 `TopicRegistry`（Leader 本地态）：(session,key) 唯一登记与幂等覆盖、subscription_id 与 term 内 topic_seq 分配、每会话 PUBLISH 去重槽、每订阅有界缓冲 drop-newest + 丢弃计数、退订/会话摘除 API。验证：`TopicRegistryTest` 判定矩阵（幂等/退订回收/上限拒绝/drop 计数/seq 单调/去重重放/并发冒烟）绿
- [x] 2.3 registry 装配接线：单机常驻权威；集群 Leader-only（角色事件换主清零、无快照来源）；`SESSION_CLOSE` 双路（本节点断连传播、失联探针补发）经既有应用钩子摘除该会话全部登记、槽与缓冲。验证：装配/角色测试绿，模拟会话关闭后 registry 订阅总数归零
- [x] 2.4 registry 与全部 topic 类 Javadoc（线程模型、Leader 本地态、死亡即退订与队列"死亡不吞元素"对照声明、term 语义）。验证：`maven-javadoc-plugin`（show=private）构建通过

## 3. 服务端·接入层与可观测

- [x] 3.1 `RequestDispatcher` TOPIC_OP 分发：v8 门（v≤7 同型 `INVALID_REQUEST` 不断连）、形状互斥矩阵入口拒绝、PUBLISH 逐条 `maxValueBytes` 入口钳制（判定唯一在接入层）、撞 key 只读探测（LockTable 命中 → 内部裁决 `REJECT_TYPE_MISMATCH`、线路送达 `INVALID_REQUEST`）。验证：`TopicGatingTest` + 形状/钳制/撞 key 矩阵测试绿（超限与违例路径零 fan-out、登记零扰动）
- [x] 3.2 `ClusterRequestHandler` topic 路径：Leader 直受理（不入提交通道、不进状态机）；Follower 任意 TOPIC_OP 同型 `NOT_LEADER` 零副作用。验证：集群接入层测试含 Follower 拒绝 case 绿
- [x] 3.3 fan-out 投递：PUBLISH 受理后按 key 全部登记订阅逐一入队（每订阅独立缓冲与交付队列），出队侧 `writeAndFlush` TOPIC_MESSAGE（request_id=0、无 ref），死通道静默丢弃、单订阅慢/满不阻塞他订阅与 Publisher 应答。验证：慢订阅 drop 不扩散 + 断订阅零干扰测试绿
- [x] 3.4 `ServerMetrics` 新命名点：`topic.total{op,status}`（含 `REJECT_SUBSCRIBERS` 线、重放计 OK）、`topic.dropped.total`、`topic.subscribers.max`；三口径（等待/元素/订阅）命名点注释互引；`waiters` Gauge 不含订阅者。验证：指标夹具按线归数断言（含丢弃不入拒绝面场景）绿
- [x] 3.5 `AdminRequestHandler` 订阅读数：SUMMARY `topic_entries`、LIST_KEYS Leader 合并 `family=topic` 行与 `topic_subscribers`、KEY_DETAIL 订阅者明细列表（会话/订阅 id/时刻）、Follower 如实缺席/未命中；消息内容零外发、观察零扰动。验证：admin 夹具扩 topic 维断言（含 Follower 视角 case）绿
- [x] 3.6 控制台 topic 呈现：概览计数、列表行、详情订阅者列表；Follower 视角未命中如实渲染；无写操作入口。验证：console HTML 测试扩维（消息内容零出现）绿

## 4. 客户端 SDK

- [x] 4.1 `OTopic`/`OTopicSubscription` 公开契约与 `RemoteTopic` 实现、`newTopic(key)` 工厂；接口级 Javadoc 全量降级/增强清单（至多一次、drop-newest 两级、term 序与 gap 推断、去重仅同 Leader、跨换主可双投、死亡即退订对照句、监听器线程模型、常驻成本、v8 门）。验证：契约 Javadoc 审读清单齐备、javadoc 构建通过
- [x] 4.2 握手常量 7→8（`ConnectionManager`/`RequestMultiplexer`）；`TOPIC_MESSAGE` 入站按 `subscription_id` 路由至句柄、SDK dispatcher 线程单订阅串行回调、异常吞并记录不断链。验证：Scripted 测试（串行性、线程非 EventLoop、异常吞并）绿
- [x] 4.3 PUBLISH 写车道：继承 `RemoteAtomicBase` 同 key 在途互斥 + 同 op_seq 重发（命中服务端去重槽不双投）；SUBSCRIBE/UNSUBSCRIBE 直发车道、`close()`/`unsubscribe()` 幂等。验证：Scripted 重发与幂等测试绿
- [x] 4.4 活跃订阅登记表挂 home 迁移钩子：重连成功自动重发 SUBSCRIBE（新 subscription_id 内部重映射、应用句柄无感）、`droppedCount()` 跨 term 基线重置；改道期拒绝按 W10 同型码形处理。验证：`ClientTopicIT` 换主场景（脚本 Leader 切换：续收、无陈重投、计数不跨 term）绿

## 5. 集群 E2E 与守卫回归

- [x] 5.1 `ClusterTopicTest`：N 订阅 fan-out 完整性与每订阅 seq 升序、并发 Publisher 互不扰动、中途 UNSUBSCRIBE registry 归零（admin 读数断言，防泄漏验收点）、kill 订阅者进程存续无泄漏、慢消费者 drop 计数端到端、LeaderKill 后自动重订阅续收且丢量有界。验证：六测试绿
- [x] 5.2 `StateMachineTopicTest`（零日志守卫常驻）：纯 topic 流量下三副本 digest 逐字节一致、日志条目数等于对照基线、重启/快照追赶无陈消息投递。验证：确定性测试绿
- [x] 5.3 快照零增量夹具：N topic × M 订阅 + 持续 publish/退订负载，快照字节与摘要逐字节等于无 topic 基线。验证：`SnapshotFamilyRoundTripTest` 同族扩展绿
- [x] 5.4 `RejectCodecTableTest` 扩 `TOPIC_OP` 同型拒绝行（NOT_LEADER/门控/钳制三码形不得呈"成功空应答"）；词表冻结夹具扩 `TopicOp`/`REJECT_SUBSCRIBERS`/family 词表。验证：两夹具绿
- [x] 5.5 `BenchmarkMain` 扩 topic 相（publish 吞吐、fan-out N=1/8/64 边际成本、drop 率零负载基线），报告落 `target/benchmark/` 供 WATCHLIST 观察行引用。验证：topic 相跑通并产出基线数据（`-pl` 不带 `-am`）

## 6. 文档与治理

- [x] 6.1 指南 zh/en 对称扩章：01 概念（广播/弱背压/term 序/死亡即退订与队列对照）、03 SDK topic 章（API、监听器线程模型、两级丢弃、droppedCount 口径）、05 部署（两限额行、告警口径、零持久态回滚干净注记与队列回滚注记对照、fan-out 容量评估）、07 呈现、08 三口径之辨与告警、09 排查三分法、10 v1–v8 矩阵（服务端先行升级序）、术语表新词。验证：双语对账无单边缺章、user-documentation delta 场景逐项过
- [x] 6.2 WATCHLIST 登记新观察行（高频 publish × 多订阅者 fan-out 放大：drop 率与 Leader 写出水位，触发 → 另立 change 评估批量帧/请求门控），与 W11 行交叉引用（topic 车道纳入收敛观察）。验证：表行与交叉引用落盘
- [x] 6.3 ROADMAP 状态流转：二档 OTopic 行"进行中"→"已落地"附归档链接；"一 key 一形态"契约声明在同 key 同类型约定登记处随行注记。验证：表格与归档时点一致

## 7. 全量验证（集成）

- [x] 7.1 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆 8/8 模块 BUILD SUCCESS（0 失败 0 错误；跳过项逐一注记原因）。验证：构建输出与模块计数
- [x] 7.2 `bash scripts/check-source-citations.sh` 全源零命中（含 `--selftest`）。验证：脚本退出码 0
- [x] 7.3 `-Pdrill` 全量复跑（用户在非沙箱单轮执行，含 topic 相滚动重启与换主场景；红了先保 `target/drill-logs`）。验证：演练全绿并留存报告
- [x] 7.4 `openspec validate add-otopic` 通过并走归档同步（规格 sync 后主规格无 delta 段头残留，grep 双查）。验证：validate 输出与主规格状态
