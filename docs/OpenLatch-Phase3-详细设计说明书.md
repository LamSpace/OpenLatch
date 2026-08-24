# OpenLatch Phase 3（功能完整平台）详细设计说明书

| 项目     | 内容                                                                                                      |
|----------|-----------------------------------------------------------------------------------------------------------|
| 项目名称 | **OpenLatch**                                                                                             |
| 文档类型 | 详细设计说明书（Phase 3 / 平台功能）                                                                      |
| 依据文档 | 《OpenLatch 概要设计说明书》v1.0、《OpenLatch-总体实施计划与验证方案》v1.0、Phase 1/2 详细设计说明书 v1.0 |
| 版本     | v1.0                                                                                                      |
| 日期     | 2026-08-23                                                                                                |
| 作者     | Lam Tong                                                                                                  |
| 状态     | 待评审                                                                                                    |

---

## 1. 概述

### 1.1 范围

Phase 3 交付概要设计 §4.1 的 P2 功能，对应实施计划工作项 T1–T4：

| 工作项 | 内容                                                   |
|--------|--------------------------------------------------------|
| T1     | 扩展锁类型：FairLock 显式化、Semaphore、CountDownLatch |
| T2     | Micrometer 监控指标与 Prometheus 端点                  |
| T3     | 管理控制台（`openlatch-console`）                      |
| T4     | TLS 传输加密与 Token 认证                              |

### 1.2 与概要设计的追溯矩阵

| 概要设计章节                                | 本文对应章节 |
|---------------------------------------------|--------------|
| §4.1 P2：FairLock/Semaphore/CountDownLatch  | §2           |
| §4.1 P2：监控指标（Micrometer）与管理控制台 | §3、§4       |
| §4.1 P2：TLS/Token 认证                     | §5           |
| §6.5 Phase 3 扩展说明（MVP 队列已天然公平） | §2.2         |
| §5.2 `openlatch-console` 模块               | §4           |

### 1.3 前置条件

- T1 在语义上只依赖 Phase 1 的 `openlatch-core`，可提前开发，但按实施计划约束在 Phase 2 之后合入发布（集群行为需要一并验证）；
- T2/T3/T4 依赖 Phase 1 server 骨架；T3 依赖 T2 的指标采集。

---

## 2. 扩展锁类型（T1）

### 2.1 协议扩展

```proto
// LockType 枚举新增（不改动既有取值）：
//   LOCK_TYPE_FAIR      = 4;
//   LOCK_TYPE_SEMAPHORE = 5;
//   LOCK_TYPE_LATCH     = 6;   // CountDownLatch

// AcquireRequest 新增：
//   int32 permits = 6;   // 仅 SEMAPHORE 有效，默认 1；其他类型必须为 0/1

// ReleaseRequest 新增：
//   int32 permits = 4;   // SEMAPHORE 释放的许可数

// 新增消息（Latch 专用，走独立通道而非 ACQUIRE）：
message LatchCountDownRequest { string key = 1; int64 count = 2; }
message LatchCountDownResponse { StatusCode status = 1; int64 remaining = 2; }
message LatchAwaitRequest { string key = 1; }        // 复用 QUEUED/通知机制
message LatchAwaitResponse { StatusCode status = 1; } // OK=已归零；QUEUED=挂起等待
```

对应新增 `MessageType`：`LATCH_COUNT_DOWN = 8`、`LATCH_AWAIT = 9`。`AWAIT_NOTIFY` 复用（按 `request_id_ref` 关联）。

### 2.2 FairLock

**现状**：Phase 1 的等待队列已是严格 FIFO（获取判定规则要求"队列非空即排队"，无插队快路径），公平性事实上已成立（概要设计 §6.5："MVP 的 FIFO 队列已天然公平"）。

**Phase 3 交付**：

1. 显式 `LOCK_TYPE_FAIR` 类型：语义等价于 `REENTRANT`，作为 **显式公平承诺**的 API 标识；
2. 回归锁定：将"并发竞争下授予顺序 == 排队顺序"提升为独立测试套件，任何未来优化（如 §2.4 遗留的读者批量授予）破坏公平性时立即报警；
3. 客户端 `newFairLock(key)` 与 Starter 的 `type = FAIR`。

### 2.3 Semaphore

**语义**：N 个许可的共享资源门闸；许可获取同样受 **租约**保护（客户端崩溃不泄漏许可）。

- **数据结构**（`openlatch-core` 新增 `SemaphoreEntry`）：`permitsTotal`、`permitsAvailable`、`waiters: ArrayDeque<Waiter(permits)>`；
- **获取判定**：严格 FIFO——仅当请求者是队首（或队列为空）且 `permitsAvailable >= 请求数` 时授予；不允许后到的小请求越过队首（防大请求饥饿）。获取成功扣减许可并登记租约（租约挂在 `(session, thread, key)` 维度，与锁一致）；
- **释放**：显式 `release(permits)` 归还；租约到期自动归还该持有者的全部许可并通知队首；
- **重入**：同一 Owner 重复获取按次累加持有许可数，释放对称扣减（计数存于持有者条目）；
- **客户端**：`OSemaphore` 接口（`acquire()/acquire(n)/tryAcquire(...)/release()/release(n)`），看门狗与锁完全复用；
- **集群（承接 Phase 2）**：授予/释放/到期走复制日志；等待队列仍为 Leader 内存（与锁一致）。

### 2.4 CountDownLatch

**语义**：一次性倒计数屏障。

- **数据结构**：`LatchEntry`：`count`、`awaiters`；
- `countDown(n)`：计数减至 0 下限；归零瞬间对 **全部** awaiter 广播通知（CDL 的唤醒语义本就是全体放行，不构成惊群问题）；
- `await()`：计数已为 0 → 立即成功；否则入队挂起（复用 `QUEUED` + `AWAIT_NOTIFY` 通道）；
- **一次性**：归零后永久放行；不支持重置——新的屏障使用新的 key（文档明示）；
- **等待者无租约**：await 不持有任何资源，不参与租约/看门狗；断连时随会话清理摘除；
- **集群**：`count` 与 countDown 走复制日志；awaiter 队列为 Leader 内存（切换后 await 失败重试，与锁等待者一致）。

### 2.5 core 扩展点改造

Phase 1 的 `LockEntry` 面向互斥锁。T1 将条目抽象上移：

```
lock/
├── KeyEntry（接口）        key 级状态条目：类型判别、会话清理钩子、租约摘除钩子
├── LockEntry               既有互斥/读写实现（不改行为）
├── SemaphoreEntry          新增
└── LatchEntry              新增
```

`LockTable`、`LeaseManager`、`SessionRegistry`、`CoreEngine` 的骨架不变（它们操作的是 `KeyEntry` 抽象），互斥锁既有测试全部保持通过——这是 T1 的回归底线。

## 3. 监控指标（T2）

### 3.1 技术选型

Micrometer（`micrometer-core`）+ `micrometer-registry-prometheus`；服务端在独立管理端口（默认 **9412**）以 Netty HTTP 暴露 `/metrics`（Prometheus 文本格式）。不引入完整 Web 框架——只实现 `/metrics` 与 `/healthz` 两个端点。

### 3.2 服务端指标清单

| 指标名                                      | 类型      | 标签                            | 含义                                     |
|---------------------------------------------|-----------|---------------------------------|------------------------------------------|
| `openlatch.server.locks.held`               | Gauge     | `type`                          | 当前持有中的锁/许可条目数                |
| `openlatch.server.waiters`                  | Gauge     | —                               | 全部等待队列条目总数                     |
| `openlatch.server.sessions`                 | Gauge     | —                               | 活跃会话数                               |
| `openlatch.server.acquire.total`            | Counter   | `status`                        | 获取请求计数（按结果码）                 |
| `openlatch.server.acquire.duration`         | Histogram | `result`(granted/queued/denied) | 获取处理耗时                             |
| `openlatch.server.release.total`            | Counter   | `status`                        | 释放计数                                 |
| `openlatch.server.renew.total`              | Counter   | `status`                        | 续租计数（失败续租是锁丢失前兆，需告警） |
| `openlatch.server.lease.expired.total`      | Counter   | —                               | 租约到期强制释放次数                     |
| `openlatch.server.queue.depth.max`          | Gauge     | —                               | 单 key 队列深度最大值（采样）            |
| `openlatch.cluster.is_leader`（Phase 2 后） | Gauge     | `node_id`                       | 是否 Leader                              |

### 3.3 客户端指标（可选开启）

`openlatch.client.requests.total{type,status}`、`openlatch.client.request.duration`、`openlatch.client.reconnect.total`、`openlatch.client.locks.lost.total`。客户端暴露方式：注册到应用自身的 `MeterRegistry`（starter 自动注入，若存在）。

### 3.4 埋点位置

全部在 `RequestDispatcher`（服务端）与 `RequestMultiplexer`（客户端）的既有路径上插桩，不侵入 `openlatch-core`（core 保持零依赖）。

## 4. 管理控制台（T3）

### 4.1 模块与架构

概要设计 §5.2 已规划 `openlatch-console` 模块（依赖 server）。架构：

```
浏览器 ──HTTP──▶ openlatch-console（Spring Boot Web 应用）
                     │
                     └── 管理协议 ──▶ OpenLatch 节点（ADMIN 消息）
```

- 控制台是 **独立部署**的 Web 应用，通过管理协议查询服务器，不与锁流量共享连接；
- 页面采用服务端渲染（Thymeleaf）+ 轻量轮询刷新。不引入前端构建链（YAGNI）。

### 4.2 管理协议

新增 `MessageType`：`ADMIN_LIST_KEYS = 10`、`ADMIN_KEY_DETAIL = 11`、`ADMIN_LIST_SESSIONS = 12`、`ADMIN_SUMMARY = 13`。请求走独立连接， **必须通过 Token 认证**（§5），否则拒绝。

| 消息                  | 响应内容                                                                   |
|-----------------------|----------------------------------------------------------------------------|
| `ADMIN_SUMMARY`       | 锁/等待者/会话总数、节点角色、运行时长、版本                               |
| `ADMIN_LIST_KEYS`     | 分页：key、类型、持有者、剩余租约、等待者数；支持前缀过滤                  |
| `ADMIN_KEY_DETAIL`    | 单 key：持有明细（会话、线程、重入计数）、等待队列（位次、会话、等待时长） |
| `ADMIN_LIST_SESSIONS` | 会话列表：id、接入节点、建连时间、持锁数、等待数                           |

Phase 3 控制台 **只读**：不提供强制解锁/踢会话等写操作（规避误操作风险；确有需要时另立阶段设计并配二次确认与审计）。

### 4.3 页面清单

| 页面     | 内容                                                      |
|----------|-----------------------------------------------------------|
| 概览     | 汇总数字 + 核心指标图表（拉取 `/metrics` 渲染 sparkline） |
| 锁列表   | 分页/过滤/排序；点击进详情                                |
| 锁详情   | 持有者、等待队列、剩余租约倒计时                          |
| 会话列表 | 会话与其持锁/等待关联                                     |
| 节点视图 | 集群节点、Leader 标识（Phase 2 部署下）                   |

### 4.4 部署形态

可执行 jar（`openlatch-console-<ver>.jar`），配置：目标服务器地址列表、管理 Token、监听端口（默认 9413）。

## 5. 安全（T4）

### 5.1 TLS

- 服务端：`openlatch.server.tls.enabled` 开启后，pipeline 头部插入 `SslHandler`；配置项：`tls.cert` / `tls.key` / `tls.trust-store`（可选 mTLS：`tls.require-client-cert=true`）；
- 客户端：`tls.enabled` + `tls.trust-store`；mTLS 时附 `tls.client-cert` / `tls.client-key`；
- 证书更新：修改配置后滚动重启生效（不做热加载，避免半新半旧的连接状态混淆）；
- 开启 TLS 后 **拒绝明文连接**：握手超时（默认 5s）内未完成 TLS 即断开。

### 5.2 Token 认证

- 启用 Phase 1 预留的 `HelloRequest.auth_token` 字段：
    - `openlatch.server.auth.enabled=true` 时，服务端持配置的服务端令牌（支持多令牌列表，便于轮换）；
    - HELLO 令牌校验失败 → 不泄露具体原因：统一响应 `INVALID_REQUEST` 并断开连接（避免被探测枚举）；
    - 校验用常量时间比较（防时序侧信道）；
- 管理通道令牌独立：`ADMIN_*` 消息要求 `admin-token`（控制台配置），与业务令牌分离；
- 令牌轮换流程文档化：服务端先加新令牌（列表双活）→ 客户端切换 → 移除旧令牌。

### 5.3 与既有机制的关系

认证在会话建立时一次完成；会话生命周期内不做逐请求鉴权（连接即身份，与 `sessionId` 语义一致）。限流限额（Phase 1 §5.7）在认证之前生效，未认证连接同样受保护。

## 6. 兼容性与升级路径

| 项          | 策略                                                                |
|-------------|---------------------------------------------------------------------|
| 协议版本    | 升至 **3**；服务端继续接受 v1/v2 客户端；新锁类型仅对 v3 客户端开放 |
| 单机 → 集群 | Phase 2 已保证同一二进制开关切换，Phase 3 不改变                    |
| 指标端点    | 默认开启，管理端口独立，不影响锁端口                                |
| TLS/认证    | 默认关闭；开启为配置变更 + 滚动重启，无数据迁移                     |
| 控制台      | 独立部署，版本需 ≥ 服务端主版本                                     |

## 7. 测试设计

| 工作项 | 测试内容                                                                                                                                                                                                      |
|--------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| T1     | FairLock 顺序回归套件；Semaphore：许可计数、超额等待、FIFO（大请求不饿死）、租约到期归还许可、重入；CDL：countDown/await、归零广播、一次性、断连摘除；三者在集群部署下的切换行为（承接 Phase 2 故障演练框架） |
| T2     | 指标断言测试：执行固定脚本（N 次获取/释放/续租失败/到期）后逐项核对指标值与标签；`/metrics` 端点被 Prometheus 抓取联调                                                                                        |
| T3     | 管理协议消息级测试；控制台端到端冒烟：预置锁状态 → 页面断言（列表/详情/会话）；未认证管理连接被拒                                                                                                             |
| T4     | TLS：明文客户端被拒、mTLS 无证书被拒、正确证书通过；认证：空/错误令牌断连、多令牌轮换期双活、常量时间比较代码评审                                                                                             |

## 8. Phase 3 验收标准

即实施计划 §5.3 验收清单，判定口径：

1. ✅ FairLock 顺序回归套件在 CI 常开；
2. ✅ Semaphore/CDL 语义测试全绿（含集群切换场景）；
3. ✅ 指标清单逐项有断言用例，Prometheus 抓取联调记录在案；
4. ✅ 控制台冒烟通过，且管理通道强制认证；
5. ✅ 安全拒绝用例全绿（明文连接、错误令牌、未认证管理请求）；
6. ✅ 全部新功能默认值关闭或不改变既有行为（兼容性策略 §6 验证：v1 客户端在 Phase 3 服务端上行为不变）。

## 9. 遗留与后续展望（不在 Phase 3 范围）

| # | 项                                   | 说明                                     |
|---|--------------------------------------|------------------------------------------|
| 1 | 控制台写操作（强制解锁、踢会话）     | 需审计与权限体系支撑，另立阶段           |
| 2 | 读者批量授予优化（Phase 1 §12 遗留） | 与 FairLock 公平性回归套件互斥评估后决定 |
| 3 | 多租户/命名空间                      | 平台化方向，需求明确后立项               |
| 4 | 客户端指标细化（按 key 维度）        | 高基数风险，按用户反馈评估               |

## 10. 实施子任务拆分

**粒度定义**：同 Phase 1/2——每个子任务可独立交付、独立验证，编号（P3-xx）稳定。T1 的子任务允许在 Phase 2 期间提前开发（实施计划约束：合入与发布在 Phase 2 之后）。

### 10.1 T1：扩展锁类型

| ID    | 子任务                   | 内容与交付物                                                                                              | 前置                       | 验证                                               |
|-------|--------------------------|-----------------------------------------------------------------------------------------------------------|----------------------------|----------------------------------------------------|
| P3-01 | KeyEntry 抽象上移        | `KeyEntry` 接口；`LockEntry` 改为实现；`LockTable`/`LeaseManager`/`SessionRegistry`/`CoreEngine` 面向抽象 | Phase 2 完成（或提前开发） | **回归底线**：既有 core 测试全绿，互斥锁行为零变化 |
| P3-02 | FairLock                 | `LOCK_TYPE_FAIR` 别名语义、`newFairLock` API、公平性回归套件升级为独立常开套件                            | P3-01                      | 公平性套件全绿并进 CI                              |
| P3-03 | Semaphore（core）        | `SemaphoreEntry`、协议 `permits` 字段、严格 FIFO 判定（防大请求饥饿）、租约到期归还、重入计数             | P3-01                      | §7 T1 语义用例全绿（计数/超额等待/到期归还/重入）  |
| P3-04 | Semaphore（客户端）      | `OSemaphore` API、看门狗复用、集成测试                                                                    | P3-03                      | 端到端用例全绿                                     |
| P3-05 | CountDownLatch（core）   | `LatchEntry`、`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 消息、归零广播、一次性、等待者无租约                       | P3-01                      | CDL 语义用例全绿                                   |
| P3-06 | CountDownLatch（客户端） | `OCountDownLatch` API、集成测试                                                                           | P3-05                      | 端到端用例全绿                                     |
| P3-07 | 扩展锁集群行为           | 授予/释放/到期走复制日志；等待队列 Leader 内存；Leader 切换下行为与锁一致                                 | P3-04、P3-06 + Phase 2     | 集群用例矩阵全绿；**T1 退出**                      |

### 10.2 T2：监控指标

| ID    | 子任务           | 内容与交付物                                                                              | 前置           | 验证                                      |
|-------|------------------|-------------------------------------------------------------------------------------------|----------------|-------------------------------------------|
| P3-08 | 服务端指标与端点 | 按 §3.4 在 `RequestDispatcher` 插桩；gauge 采集；管理端口 9412 的 `/metrics` + `/healthz` | Phase 1 server | 端点可抓取；指标名与 §3.2 清单逐项一致    |
| P3-09 | 指标断言测试     | 固定操作脚本 → 指标值与标签逐项断言                                                       | P3-08          | §7 T2 用例全绿                            |
| P3-10 | 客户端可选指标   | 客户端插桩（默认关）、starter 自动注入应用 `MeterRegistry`                                | P3-08          | starter 环境指标可见用例通过；**T2 退出** |

### 10.3 T3：管理控制台

| ID    | 子任务           | 内容与交付物                                                                                   | 前置           | 验证                        |
|-------|------------------|------------------------------------------------------------------------------------------------|----------------|-----------------------------|
| P3-11 | 管理协议         | `ADMIN_SUMMARY`/`LIST_KEYS`/`KEY_DETAIL`/`LIST_SESSIONS` 四消息 + 服务端 handler（未认证拒绝） | Phase 1 server | 消息级测试全绿              |
| P3-12 | console 骨架     | `openlatch-console` 模块、节点连接管理、配置（地址列表/端口/令牌占位）                         | P3-11          | SUMMARY 页面冒烟通过        |
| P3-13 | 页面与端到端冒烟 | 概览/锁列表/锁详情/会话列表/节点视图五页面 + 轮询刷新                                          | P3-12、P3-09   | §7 T3 冒烟全绿；**T3 退出** |

### 10.4 T4：安全

| ID    | 子任务             | 内容与交付物                                                       | 前置           | 验证                                    |
|-------|--------------------|--------------------------------------------------------------------|----------------|-----------------------------------------|
| P3-14 | 服务端 TLS         | `SslHandler`、`tls.*` 配置、明文连接拒绝（握手超时 5s）、可选 mTLS | Phase 1 server | 服务端侧 TLS 用例全绿                   |
| P3-15 | 客户端 TLS         | `tls.enabled`/trust-store、mTLS 客户端证书                         | P3-14          | 客户端侧用例全绿（含 mTLS 无证书被拒）  |
| P3-16 | Token 认证         | HELLO 令牌校验、多令牌列表与轮换流程、常量时间比较、管理令牌分离   | P3-14          | 认证用例全绿（空/错令牌断连、双活轮换） |
| P3-17 | 安全套件与验收闭环 | 拒绝用例聚合（明文/错误凭证/未认证管理请求）；§8 六项验收证据收集  | P3-15、P3-16   | §8 验收清单逐项闭环；**Phase 3 发布**   |
