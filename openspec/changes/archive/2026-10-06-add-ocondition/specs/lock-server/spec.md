# Spec Delta

## ADDED Requirements

### Requirement: v9 条件门控与入口裁决

服务端 SHALL 在分发层对 v9 两形新语义执行 v9 门：协商版本 <9 的会话发送 `CONDITION_OP`、或发送携带 `condition` 字段的 `ACQUIRE`，MUST 以同型 `INVALID_REQUEST` 消息级拒绝、不断连（判例 v3–v8 门）。门后入口裁决 MUST 依序完成：key 非空与长度校验（既有纪律）、`condition` 非空与 ≤ `maxKeyLength` 校验、折叠形态形状矩阵（`wait_ms = 0` 携带 condition 违例；`lock_type` 限 {REENTRANT, SIMPLE, FAIR}，READ/WRITE/SEMAPHORE/LATCH/BARRIER/ATOMIC_* 携带 condition 违例——v1 支持面三互斥形态，违例同型 `INVALID_REQUEST`、状态零扰动）、CONDITION_OP 的 op×字段携带矩阵（SIGNAL/SIGNAL_ALL 必携 `thread_id` 且 `await_request_id` 为 0；LEAVE 必携 `await_request_id` 且 `thread_id` 为 0）。形状裁决唯一在接入层（判例 v6 D5 防回放分歧）。

分发车道分轨：携带 `condition` 的 `ACQUIRE` **走既有 ACQUIRE 车道**——集群形态经既有提交通道（复用 `LOCK_ACQUIRE_ENTRY`，无新条目类型）、Follower 按既有 v2 纪律回 `NOT_LEADER` 随附 leader 提示字段、应答为 `AcquireResponse` 既有词表（`QUEUED`/`OVERLOADED`/错误码）；`CONDITION_OP` 为 Leader 本地直裁决操作（signal 家族零日志）——集群形态仅 Leader 受理，Follower 对任意 CONDITION_OP MUST 以同型 `NOT_LEADER` 拒绝且零副作用（无 leader 提示字段，判例 QUEUE/TOPIC 直发车道）；单机形态两路均由本节点等效受理。结果→状态码映射延伸：等待满 → `OVERLOADED`、signal 权限拒绝 → `NOT_HELD`、家族不匹配 → `INVALID_REQUEST`、会话失效 → `SESSION_EXPIRED`（既有词表，零新增值）。

#### Scenario: v8 会话两形新语义均被门控

- **WHEN** 协商版本为 8 的会话分别发送 `CONDITION_OP.SIGNAL` 与携带 `condition` 字段的 `ACQUIRE`
- **THEN** 两形均收到同型 `INVALID_REQUEST` 消息级拒绝（op/形态回显），连接保持，该会话锁/topic 等既有请求照常服务

#### Scenario: 形状违例零扰动

- **WHEN** 折叠 ACQUIRE 出现 `wait_ms = 0` + condition、READ 形态 + condition、空串/超长 `condition`，或 SIGNAL 携非零 `await_request_id`、LEAVE 携非零 `thread_id`
- **THEN** 一律同型 `INVALID_REQUEST` 拒绝，等待集/等待队列/锁持有零扰动，不产生日志条目

#### Scenario: Follower 对 CONDITION_OP 零副作用拒绝

- **WHEN** 客户端向非当值 Leader 的集群节点发送 SIGNAL/SIGNAL_ALL/LEAVE
- **THEN** 同型 `NOT_LEADER` 拒绝（无提示字段），Follower 上不存在任何搬运、摘除或推送副作用

#### Scenario: 折叠 await 走 ACQUIRE 车道含提示改道

- **WHEN** 客户端向非 Leader 节点发送携带 `condition` 的 `ACQUIRE`
- **THEN** 收到 `acquire_response` 且 `status = NOT_LEADER`、随附 `leader_node_id`/`leader_address` 既有提示字段（v2 纪律原样适用，区别于其余非 ACQUIRE 车道的无提示判例），客户端经既有改道重投后照常受理

#### Scenario: 等待项超限在带拒绝

- **WHEN** 某 key 等待项合计（等待队列+条件集）达 `max-queue-depth-per-key` 后仍有新折叠 await 到达 Leader
- **THEN** 同型 `OVERLOADED` 拒绝（`queue_position` 零值），该 key 既有等待项与持有状态零扰动

### Requirement: 条件等待服务端闭环

集群形态下条件等待集为 Leader 本地结构（Leader-only 装配，判例 `TopicRegistry`/`WaitQueue` 装配位与生命周期钩子）：折叠 await 的登记发生在 **Leader 受理预检点**（先于提交登记，应用点仅经重放执行释放半程——"登记先于释放可见"不变式使第三方的 signal 在释放生效前必因权限检查被拒，丢唤醒窗为零）；合并深度护栏同在预检点裁决（等待队列深度 + 登记集合计达 `max-queue-depth-per-key` 即 `OVERLOADED` 且登记零发生）；受理回执由 Leader 在应用点结果上改写为 `QUEUED`（位次=本 key 等待项合计读数），提交失败/换主则登记随本地态清零。被唤醒后的重发信封（`condition` 已清除，协议纪律）经普通获取通道授予，授予成功时顺带按 (会话,线程) 归属摘除该等待者可能残留的陈旧登记；换主（当选）时登记结构清零，等待项经客户端 ACQUIRE 车道重挂以重登记（新会话重登记属常态、幂等接纳）。单机形态无此结构——折叠命令在 core 条目关键区内一体完成（core-lock-engine 能力条款）。

Leader SHALL 以既有队首通知推送通道投递条件等待的唤醒通知：搬运后的可通知等待项经 `AWAIT_NOTIFY`（`request_id = 0`、`request_id_ref` = 原折叠 `ACQUIRE` 的 request_id）送达等待项所属连接（判例队首通知推送与 `ServerSessionRegistry` 反查）；服务端 MUST NOT 为条件等待新增推送类型、定时器或扫描周期（唤醒全为事件驱动：搬运使等待项入队首纪律管辖，通知由释放/到期/会话关闭应用点接力——signal 权限论证下搬运恒发生于锁被持时点，不存在"搬运即空闲可推"的分支）。

等待集的服务端生命周期收口 MUST 经既有钩子：本节点断连传播的 `SESSION_CLOSE` 与失联探针补发的 `SESSION_CLOSE` 双路摘除该会话全部条件登记（复用既有会话关闭应用钩子位，判例 topic registry purge）；换主（当选/角色翻转）后新 Leader 的条件集为空（进程本地态，无快照/日志来源），等待项由客户端 ACQUIRE 车道迁移机制（`migrateWaitsTo`/`replayOnHome`）以重发折叠 ACQUIRE 的形式自动重挂——受理节点 MUST 以幂等登记接纳（同 request_id 重挂不双登记；重挂会话非持有者属常态，释放半程零操作照常登记，await 权限降级面在服务端的具体形态）。ShadowTable 与深度观察读数 MUST 与折叠 await 的释放半程一致（经复制的释放应用簿记原样镜像，条件集本身不入镜像——无复制来源可镜像，判例 topic registry 不进 ShadowTable）。

#### Scenario: 唤醒通知复用队首推送线路

- **WHEN** 持有者 SIGNAL 后释放锁，被搬运等待项成为可通知队首
- **THEN** 等待项连接收到 `AWAIT_NOTIFY`（request_id=0、ref 指向其折叠 ACQUIRE 的 request_id），与原 ACQUIRE 车道通知形态逐字段同型（客户端零新分发逻辑）

#### Scenario: 换主后等待项自动重挂续醒

- **WHEN** Leader 更替，await 中的客户端经车道迁移在新 Leader 重发折叠 ACQUIRE，随后持有者 SIGNAL
- **THEN** 新 Leader 上仅存在重挂后的登记（旧 term 集合随进程灭、无残留 ghost），重挂会话非持有者亦被幂等接纳；SIGNAL 后该等待项照常收到通知并重发获取，全链无应用可见中断

#### Scenario: 失联探针补发摘除条件登记

- **WHEN** 等待者进程被 kill，失联探针路径补发 `SESSION_CLOSE`
- **THEN** 该会话条件登记全部摘除、`condition_waiters` 读数回落，该 key 持有/等待队列逐项不变，后续 SIGNAL 不再搬运死登记

#### Scenario: 观察读数与释放半程一致

- **WHEN** 折叠 await 受理后读取该 key 的 ShadowTable/管理面深度
- **THEN** 持有归属显示已释放（无持有者），等待项计数含该条件等待者；三处读数（镜像/计数/明细）互洽
