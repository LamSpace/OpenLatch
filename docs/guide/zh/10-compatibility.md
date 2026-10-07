# 10 · 兼容性与协议版本

## 支持矩阵

| 维度 | 支持 | 备注 |
|---|---|---|
| Java 运行时/编译 | **25**（唯一档位） | 全部构件以 `release=25` 编译；17/21 无法加载 |
| 调用方线程形态 | **平台/虚拟线程均受支持** | 同步阻塞接口虚拟线程亲和（无钉扎、归属与中断语义一致），详见 [03](03-client-sdk.md) |
| Spring Boot starter | **4.x** | 依赖 Boot 4 独有构件，Boot 3.x 不兼容；Boot 3 工程手动装配 SDK（[03](03-client-sdk.md)） |
| Spring Framework | 随 Boot 4（Framework 7） | starter 仅面向 Boot 4 应用上下文 |
| 线路协议 | 服务端接受 **v1 / v2 / v3 / v4 / v5 / v6 / v7 / v8 / v9 / v10 / v11**（HELLO 协商，支持区间 [1,11]） | 区间外拒绝，不做隐式兼容 |
| Micrometer | 宿主自备（不传递） | 注入 `MeterRegistry` 即启用客户端指标 |
| 构件分发 | 客户端 SDK 链路（`openlatch-protocol` / `openlatch-client` / `openlatch-spring-boot-starter`）发布 Maven Central（1.0.0+）；服务端与控制台可执行 jar 发布于 GitHub Releases | 客户端按坐标直接引入；服务端/控制台从 Releases 下载或本地构建 |

## 协议能力随版本累积

| 协议版本 | 能力面 |
|---|---|
| v1 | 单机锁语义：获取/释放/续租、等待-通知-重发、租约 |
| v2 | 集群：Leader 提示与改道、`CLUSTER_VIEW`、转发车道 |
| v3 | 扩展原语（公平锁/信号量/屏障）、`ADMIN_*` 只读观察、字段形态收紧 |
| v4 | 原子变量（`OAtomicLong`/`OAtomicInteger`/`OAtomicBoolean`）：`ATOMIC_OP` 消息对、每 key 版本戳与去重槽 |
| v5 | 循环屏障（`OBarrier`）：`BARRIER_AWAIT`/`BARRIER_LEAVE`/`BARRIER_ACTION_DONE` 消息对、世代账簿复制、离场即破障 |
| v6 | 有值引用（`OAtomicReference`）：ATOMIC 消息对 `optional bytes` 载荷字段（无新 `MessageType`）、`maxValueBytes` 入口钳制、载荷快照/预览 |
| v7 | 有界队列（`OBlockingQueue`）与延时队列（`ODelayQueue`）：`QUEUE_OP` 消息对、`LOCK_TYPE_QUEUE`/`LOCK_TYPE_DELAY_QUEUE` 形态、每会话去重槽、双"满"分轨、容量/drain 入口钳制、延时应用点折算、队列快照字段 |
| v8 | 广播发布/订阅（`OTopic`）：`TOPIC_OP` 消息对与 `TOPIC_MESSAGE` 推送、`REJECT_SUBSCRIBERS` 在带裁决、弱背压 drop-newest 两级缓冲、每会话去重槽（受理点）、**零复制日志/零快照贡献**（首个零持久态原语）、`max-subscribers-per-key`/`max-subscription-buffer` 入口钳制 |
| v9 | 条件变量（`OCondition`）：`CONDITION_OP` 消息对（SIGNAL/SIGNAL_ALL/LEAVE 三操作，**AWAIT 不在词表内**——await 以 `AcquireRequest` 的 `optional condition` presence 折叠进 ACQUIRE 闭环）、signal 家族零日志/等待集零快照足迹、`StatusCode`/`LockType`/`RaftEntryType` 三词表零新增、等待人数并入 `max-queue-depth-per-key` 合并护栏（零新配置） |
| v10 | 相位器（`OPhaser`）：`PHASER_OP` 单消息对（REGISTER/ARRIVE/ARRIVE_AND_AWAIT/ARRIVE_AND_DEREGISTER/AWAIT_ADVANCE/CANCEL/QUERY 七操作，到场与等待解耦）、**变异全入日志**（`PHASER_OP_ENTRY=14` 逐条经提交，等待/取消/查询零日志）、换代窗口与每会话注册幂等槽、死亡隐式摘除配额且不撤销已到场事实（不空转）、等待谓词跨换主自愈（对照条件 signal 丢失窗）、三 Non-Goal（onAdvance 钩子/终止态/父子分层派生均不做）、`max-parties-per-phaser`（默认 1024）新增护栏、`StatusCode` 零新增（两超限骑 `OVERLOADED` 以 op 分轨）、相位号 long |
| v11 | 延时触发（`OTimer`）：`TIMER_OP` 单消息对（SCHEDULE/DISARM/AWAIT/CANCEL/QUERY 五操作）、**半入日志裁决**——装载/撤销逐条 `TIMER_OP_ENTRY=15` 经提交、**到期零条目**（`marked` 为账簿与判定时刻的派生谓词，"到期不是迁移，是时间的兑现"，与租约到期入日志构成判例两端）、等待/取消/查询零日志 Leader 本地、粘滞共见与换代清钟（round 语义无需代次入参，对照 phaser `awaitAdvance` 已见相位入参）、DISARM 代终结以既有 `DENIED` 首次可达面了结等待（零新状态码延续）、**装载者死亡钟照响**（死亡三形态第三型：账簿零扰动，与屏障破障/phaser 摘除并列）、三 Non-Goal（周期重挂/任务回调/绝对时刻均不做）、`max-timer-horizon-ms`/`timer-ready-tick-ms` 两配置（tick 与队列分轨）、管理呈现面不折算 marked（时钟无关原始读数） |

混布规则：**服务端版本 ≥ 客户端版本**。旧客户端（v1/v2）连新服务端完全可用；
新客户端连旧服务端在握手即拒（显式失败优先于行为降级）。v4 原子能力要求服务端 ≥4；v5 屏障能力要求服务端 ≥5（v≤4 会话发 `BARRIER_*` 得 `INVALID_REQUEST` 消息级拒绝、不断连）；v6 有值引用要求服务端 ≥6（v≤5 会话发引用形态 `ATOMIC_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连，标量原子不受波及）；v7 队列要求服务端 ≥7（v≤6 会话发 `QUEUE_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连，其余原语不受波及）；v8 topic 要求服务端 ≥8（v≤7 会话发 `TOPIC_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连，其余原语不受波及）；v9 条件要求服务端 ≥9（v≤8 会话发 `CONDITION_OP` 或携带 `condition` 字段的 ACQUIRE 得 `INVALID_REQUEST` 消息级拒绝、不断连，其余原语不受波及）；v10 相位器要求服务端 ≥10（v≤9 会话发 `PHASER_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连，其余原语不受波及）；v11 延时触发要求服务端 ≥11（v≤10 会话发 `TIMER_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连，其余原语不受波及）；服务端升级后旧客户端（v≤10）行为逐项不变。

## 升级与回滚顺序

滚动升级（服务端先、客户端后）与回滚约束（含快照位点对旧二进制的不可回退性）
统一收录在 [05 升级与回滚](05-cluster-deployment.md)。

## 配置兼容

- 所有能力默认关闭即行为与更早形态一致（TLS/认证/集群/指标各有独立开关）；
- 配置键只增不改语义；废弃键会在发布说明中先行标注（当前无废弃项）。

## 数据兼容

Raft 日志与快照格式在 1.x 系列内向后兼容（新二进制可读旧目录）；
**反向不成立**——一旦产出新快照/截断，旧二进制不可再挂载该 `data-dir`（见 [05 回滚](05-cluster-deployment.md)）；v4 快照含原子条目、v5 快照含循环屏障条目（世代/账簿）、v6 快照含引用载荷字段（`atomic_ref_*`）后，回滚到旧二进制同样被此规则覆盖（回滚前先确认业务未写入对应 key，或接受屏障世代重置/引用载荷驻留）。**v8 topic 不受此规则约束**：topic 不产出任何日志条目与快照字段（零持久态，有快照零增量与日志零条目双向守卫回归钉死），回滚窗口与 topic 流量无关（见 [05 v8 回滚窗口](05-cluster-deployment.md)）。**v9 条件同样不产生新的持久格式**：等待集为进程易失、快照无条件专属字段（`SnapshotLock` v9 零扩展）、signal 家族零条目，await 仅骑既有 `LOCK_ACQUIRE_ENTRY`——回滚窗口**无 key 可清**，唯一约束是活跃 v9 会话收敛（区间外握手即拒，属既有纪律的显形；与队列"回滚前清 key"、topic"天然干净"三行并列、口径互不混读，见 [05 v9 回滚窗口](05-cluster-deployment.md)）。**v10 相位器受反向规则约束（屏障/队列同型，第四口径）**：账簿逐条入日志并写快照字段（`phaser_*`、条目类型 14 号），降级前确认无在途 phaser 流量或接受 phaser key 在旧二进制不可用；无可"排空"的驻留数据，前置动作仅在途清零（与 topic/condition"天然干净"并列、互不混读，见 [05 v10 回滚窗口](05-cluster-deployment.md)）。**v11 延时触发同受反向规则约束（屏障/队列/phaser 同型并列）**：账簿三元组与去重槽逐条入日志并写快照字段（`timer_*`、条目类型 15 号），降级前确认无在途 timer 流量或接受 timer key 在旧二进制不可用——**"到期零条目"不等于"零持久态"**，与 topic/condition 天然干净严格分轨（见 [05 v11 回滚窗口](05-cluster-deployment.md)）。
