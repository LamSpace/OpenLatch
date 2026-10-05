# 10 · 兼容性与协议版本

## 支持矩阵

| 维度 | 支持 | 备注 |
|---|---|---|
| Java 运行时/编译 | **25**（唯一档位） | 全部构件以 `release=25` 编译；17/21 无法加载 |
| Spring Boot starter | **4.x** | 依赖 Boot 4 独有构件，Boot 3.x 不兼容；Boot 3 工程手动装配 SDK（[03](03-client-sdk.md)） |
| Spring Framework | 随 Boot 4（Framework 7） | starter 仅面向 Boot 4 应用上下文 |
| 线路协议 | 服务端接受 **v1 / v2 / v3 / v4 / v5 / v6**（HELLO 协商，支持区间 [1,6]） | 区间外拒绝，不做隐式兼容 |
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

混布规则：**服务端版本 ≥ 客户端版本**。旧客户端（v1/v2）连新服务端完全可用；
新客户端连旧服务端在握手即拒（显式失败优先于行为降级）。v4 原子能力要求服务端 ≥4；v5 屏障能力要求服务端 ≥5（v≤4 会话发 `BARRIER_*` 得 `INVALID_REQUEST` 消息级拒绝、不断连）；v6 有值引用要求服务端 ≥6（v≤5 会话发引用形态 `ATOMIC_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连，标量原子不受波及）；v7 队列要求服务端 ≥7（v≤6 会话发 `QUEUE_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连，其余原语不受波及）；v8 topic 要求服务端 ≥8（v≤7 会话发 `TOPIC_OP` 得 `INVALID_REQUEST` 消息级拒绝、不断连，其余原语不受波及）；服务端升级后旧客户端（v≤7）行为逐项不变。

## 升级与回滚顺序

滚动升级（服务端先、客户端后）与回滚约束（含快照位点对旧二进制的不可回退性）
统一收录在 [05 升级与回滚](05-cluster-deployment.md)。

## 配置兼容

- 所有能力默认关闭即行为与更早形态一致（TLS/认证/集群/指标各有独立开关）；
- 配置键只增不改语义；废弃键会在发布说明中先行标注（当前无废弃项）。

## 数据兼容

Raft 日志与快照格式在 1.x 系列内向后兼容（新二进制可读旧目录）；
**反向不成立**——一旦产出新快照/截断，旧二进制不可再挂载该 `data-dir`（见 [05 回滚](05-cluster-deployment.md)）；v4 快照含原子条目、v5 快照含循环屏障条目（世代/账簿）、v6 快照含引用载荷字段（`atomic_ref_*`）后，回滚到旧二进制同样被此规则覆盖（回滚前先确认业务未写入对应 key，或接受屏障世代重置/引用载荷驻留）。**v8 topic 不受此规则约束**：topic 不产出任何日志条目与快照字段（零持久态，有快照零增量与日志零条目双向守卫回归钉死），回滚窗口与 topic 流量无关（见 [05 v8 回滚窗口](05-cluster-deployment.md)）。
