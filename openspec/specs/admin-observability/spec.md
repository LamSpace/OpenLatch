# admin-observability Specification

## Purpose

定义 OpenLatch 服务端的管理观察协议：四类只读管理消息（摘要/锁列表/锁详情/会话列表）的应答契约，管理令牌的认证与未认证拒绝语义，单机与集群双形态下的数据源口径（含等待队列的 Leader 专属语义），以及管理流量与业务路径、指标词表、复制日志的隔离保证。

## Requirements

### Requirement: 管理查询应答契约

服务端 SHALL 对已握手且连接协商版本 ≥3 的会话提供四类只读管理查询，应答内容以本节点视角如实呈现：

- `ADMIN_SUMMARY`：按家族持有条目数（锁/Semaphore，Latch 无持有语义单列条目数）、等待者总数、本节点活跃会话数、节点角色（单机/Leader/Follower）、自启动运行时长、服务端版本；
- `ADMIN_LIST_KEYS`：分页返回 key、家族、持有者（会话/线程/计数）、剩余租约毫秒（未持有为 0）、等待者数，支持前缀过滤，`total_matched` 为过滤后总条数；
- `ADMIN_KEY_DETAIL`：单 key 的持有明细（会话、线程、重入计数或持有许可数、读/写角色）、等待队列（位次、会话、请求 id、已等待时长）、剩余租约到期时刻、Latch 的总量/剩余；key 不存在回明确的未命中状态；
- `ADMIN_LIST_SESSIONS`：本节点接入的会话列表——id、接入节点（由会话 id 高位导出）、建连时刻、持锁 key 数、在队等待数。

管理查询 MUST NOT 变更任何锁、租约、等待队列或会话状态（纯观察）；应答 MUST NOT 依赖任何写路径成功（负载处理阻塞时查询仍可作答，读数允许弱一致）。

#### Scenario: 预置状态摘要与明细一致

- **WHEN** 预置多类型条目（重入/读写/信号量/Latch、含等待者）后依次请求 SUMMARY、LIST_KEYS、KEY_DETAIL、LIST_SESSIONS
- **THEN** 各应答逐字段与预置状态吻合，SUMMARY 聚合与 LIST_KEYS 明细、KEY_DETAIL 与 LIST_SESSIONS 的持锁/等待计数互相自洽

#### Scenario: 管理查询零扰动

- **WHEN** 反复执行全量管理查询后，业务客户端对同批 key 执行获取/释放/续租
- **THEN** 全部业务语义与不执行管理查询时逐项一致（含租约时刻不被观察行为刷新）

#### Scenario: 未知 key 详情明确未命中

- **WHEN** 对不存在的 key 请求 ADMIN_KEY_DETAIL
- **THEN** 应答携带明确的未命中状态码，MUST NOT 回空壳成功应答

### Requirement: 管理令牌认证

`ADMIN_*` 请求 MUST 携带管理令牌字段；服务端以常量时间比较校验其等于配置的 `admin-token`。校验失败（含令牌缺失、为空、不符）MUST 以 `INVALID_REQUEST` 应答并立即断开该连接，MUST NOT 泄露失败原因（不区分"未配置/不匹配"的线路可见差异，防探测枚举）。服务端未配置 `admin-token` 时 MUST 拒绝一切 `ADMIN_*` 请求（安全默认：宁拒绝不裸奔）。认证判定发生在管理 handler 入口、任何状态读取之前；被拒请求 MUST NOT 产生任何可观察的状态副作用。管理令牌与业务 HELLO 认证（T4）MUST 相互独立：现行握手对 `auth_token` 非空即断连的 Phase 1 规则不因本能力改变。

#### Scenario: 错令牌断连且不泄露

- **WHEN** 已握手 v3 会话携带错误令牌发送 ADMIN_SUMMARY
- **THEN** 收到 `INVALID_REQUEST` 后连接被断开，应答体不含原因性字段

#### Scenario: 未配置令牌一律拒绝

- **WHEN** 服务端未配置 admin-token，任意令牌的 ADMIN 请求到达
- **THEN** 全部被 `INVALID_REQUEST` 拒绝并断连；业务请求不受影响照常服务

#### Scenario: 握手前 ADMIN 被门闩拒

- **WHEN** 未握手连接首条消息即 ADMIN_SUMMARY
- **THEN** 按既有握手门闩规则 `INVALID_REQUEST` 拒绝且不断连（连接仍可补发合法 HELLO）

### Requirement: 双形态数据源与集群视角口径

管理查询在单机与集群装配下 MUST 均由受理节点本地作答、不产生复制日志条目、不向其他节点转发：单机读核心引擎权威状态；集群读该节点已应用的复制状态镜像（key/持有者/租约与会话登记集合为全副本可见的弱一致镜像）。等待队列非复制状态、仅 Leader 内存持有：Leader 应答呈现权威等待队列，非 Leader 应答的等待队列区 MUST 为空并以 `wait_queue_leader_only` 标记明示（控制台据此标注，MUST NOT 静默显示"无人等待"误导）；`ADMIN_LIST_SESSIONS` 仅覆盖受理节点自身接入的会话（接入节点由会话 id 高位导出），跨节点全景由控制台多节点聚合呈现。`ADMIN_SUMMARY` 的 `node_role`：单机装配报"单机"，集群按 LeaderTracker 当前视图报 Leader/Follower（未知过渡窗如实报未知，MUST NOT 虚报）。

#### Scenario: 集群副本可查且镜像口径如实

- **WHEN** 集群档在 Leader 授予锁后，向 follower 请求 LIST_KEYS
- **THEN** follower 应答含该 key（经镜像收敛后），剩余租约按 follower 本地条目时刻计算；全程无新日志条目产生

#### Scenario: follower 等待队列标注

- **WHEN** Leader 上某 key 有等待者，向 follower 请求该 key 的 KEY_DETAIL
- **THEN** 应答等待队列为空且 `wait_queue_leader_only = true`；向 Leader 请求同 key 则返回位次完整的等待队列

#### Scenario: 会话列表按接入节点分治

- **WHEN** 两个客户端分别连接节点 A 与 B 并持锁，向 A 请求 LIST_SESSIONS
- **THEN** 应答仅含 A 接入的会话及其持锁计数（B 会话的持锁信息经 key 明细仍可见，但不出现在 A 的会话列表）

### Requirement: 管理流量与业务面隔离

`ADMIN_*` 处理 MUST NOT 进入服务端指标词表的任何计数/直方图（acquire/release/renew 的 total 与 duration、到期计数、gauge 采样源均不受管理请求影响）；MUST NOT 登记租约、触发通知、改变 inflight 之外的任何业务簿记。管理请求 SHALL 与业务请求同受单连接在途限额保护（超限照常 `OVERLOADED`，此为自我保护而非业务语义）。应答的回显规则（requestId、协议版本）与既有业务应答一致。管理 handler 异常 MUST 以 `INTERNAL_ERROR` 应答兜底，MUST NOT 使连接静默悬挂或影响后续业务请求处理。

#### Scenario: 管理请求零指标污染

- **WHEN** 执行固定脚本（授予/释放各 N 次）后高频执行任意 ADMIN 查询若干次，再抓取 `/metrics`
- **THEN** 全部业务指标值与不执行 ADMIN 时逐项一致（含 duration 样本数不变）

#### Scenario: 在途限额对 ADMIN 生效

- **WHEN** 管理连接在途请求数达到服务端单连接限额后继续发送 ADMIN 请求
- **THEN** 超限请求回 `OVERLOADED`，既有在途请求正常完成

### Requirement: 分页、过滤与排序语义

`ADMIN_LIST_KEYS` MUST 以确定性顺序（key 字典序）排序后按 `page`（0 起）/`page_size` 切片返回；`prefix` 为空串时不过滤，非空时按前缀匹配过滤。弱一致语义明示：切片基于查询时刻的快照，并发增删 MAY 导致相邻页之间条目漂移，但同一应答内 MUST 自洽（items 数 ≤ page_size、位次连续、`total_matched` 与快照一致）。`page_size` 越界（≤0 或超服务端上限）MUST 以 `INVALID_REQUEST` 拒绝（上限防观察面自身成为放大攻击源）。

#### Scenario: 分页稳定可遍历全量

- **WHEN** 状态静置下按 page 0..N 逐页拉取
- **THEN** 各页无重叠无遗漏地覆盖全部 key，字典序全局一致，末页 items 数 < page_size 或恰为末页

#### Scenario: 前缀过滤

- **WHEN** 预置 `order:*` 与 `job:*` 两组 key，以 `prefix="order:"` 请求
- **THEN** items 仅含 `order:` 前缀 key，`total_matched` 为该组条数
