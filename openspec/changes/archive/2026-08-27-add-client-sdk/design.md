## Context

M1/M2 已交付：`wire-protocol`、`core-lock-engine`、`lock-server` 三个规格对应的实现与测试均为绿色；`openlatch-client` 目前是占位 pom（无任何依赖与代码）。客户端行为契约的权威来源是《OpenLatch-Phase1-详细设计说明书》§6，规格化版本见本变更 `specs/client-sdk/spec.md`。约束：客户端不依赖 `openlatch-core`（语义裁决全在服务端，防双份语义漂移）；依赖 netty + `openlatch-protocol`；Java 25。

## Goals / Non-Goals

**Goals:**

- 按 §6.1 的类总览落地 `openlatch-client`，公开 API 签名以 §6.3 冻结（M4 starter 依赖它）。
- 全部异步路径有超时兜底（概要设计 §4.3 标准 3），断连/故障路径有明确异常或回调（标准 2）。
- §10.3 集成测试与 §10.4 故障注入全绿，作为 M3 退出判据。

**Non-Goals:**

- 不实现显式"取消排队"消息（§6.3 已决议：以队首响应超时回收，必要时后续补 `ACQUIRE_CANCEL`）。
- 不引入多连接/连接池（单连接多路复用是既定设计）。
- 不做客户端侧锁语义判定（重入、授予与否全部信服务端）。

## Decisions

### D1：重发的 ACQUIRE 超时后保持挂起（决策点 1 定案）

重发请求本身仍挂每请求超时（5s，摘除 inflight 条目），但**超时不失败整个等待 future**，保持挂起等待下一次通知，由用户总超时（`waitTime` / `defaultWaitTimeout`）兜底整体失败。

- 理由：等待项仍在服务端队列（或已被 `sweepNotifiedHeads` 回收——最坏情况也只是等总超时），单次重发无响应不代表失去资格；直接失败会在瞬时丢包时产生误报的等待失败。
- 备选（否决）：重发超时即失败整个等待——更简单，但对网络抖动过于敏感，且调用方拿到失败后重新竞争反而增加服务端负担。

### D2：future 在 EventLoop 上 complete，文档化"回调勿阻塞"（决策点 2 定案）

`acquireAsync`/`releaseAsync` 的 future 在网络线程（EventLoop）上完成；锁丢失回调例外，走专用单线程执行器（§6.6 已规定）。公开 Javadoc 明示：链接在异步结果上的回调不得执行阻塞操作，需要阻塞处理时应切换到自己的执行器。

- 理由：与同类客户端（Redisson 等）一致；避免为回调引入额外执行器与线程切换开销，符合项目"简单优先"约束。
- 备选（否决）：为用户回调提供小执行器——多一组生命周期资源，收益仅是保护误用者，文档契约足够。

### D3：孤儿授予的补偿归还路由

`RequestMultiplexer` 收到无 inflight 条目对应的 `AcquireResponse(OK)` 时，路由给 `AwaitTracker`；`AwaitTracker` 在等待完结（成功/超时/失败）后短暂保留 `requestId → (key, threadId)` 映射，据此发送补偿 `RELEASE` 归还锁。覆盖三种孤儿时序：重复通知双授予、超时后在途重发被授予、取消后在途重发被授予。

- 理由：§6.5 明确要求"重复 OK 时释放归还"；不补偿会泄漏锁直到租约到期。
- 备选（否决）：不保留映射、孤儿 OK 直接丢弃——锁泄漏至租约到期，违背"不静默泄漏"契约。

### D4：HeldLockRegistry 只记归属、不记计数

登记结构 `key → (leaseToken, grantedLeaseMs, holderThreadId, listeners, lastRenewAtMs)`。同线程重入获取返回同 token 时本地不做任何变更；每次 `unlock()` 都发 RELEASE，以响应 `fullyReleased` 决定何时注销看门狗与移除登记。`isHeldByCurrentThread()` 与非持锁线程 `unlock()` 的 `IllegalMonitorStateException` 用本地归属（`holderThreadId`）判定，不发网络请求。

- 理由：重入计数的唯一事实源在服务端，本地记账必然漂移（§6.3）；归属信息服务端已在授予/续租中回传，本地只存结论。

### D5：断连期间看门狗不计数，失锁裁决正交化

看门狗触发时检查连接状态：非 ACTIVE 则跳过本次续租发送且**不增加连续失败计数**。断连场景的失锁裁决完全交给 §6.2 的 `lostAt` 定时任务（重连先于 `lostAt` → 重连成功即回调；`lostAt` 先到 → 到时回调）。看门狗只裁决"连接正常但服务端不认账"（明确错误码即时失锁 / 连续 2 次超时）。

- 理由：避免 `lostAt` 与看门狗"连续 2 次超时"双重裁决竞争同一事件、回调可能重复触发。
- 备选（否决）：断连期间照常计数——两条路径竞态，回调时机与次数不确定，测试无法稳定断言。

### D6：线程模型与定时器统一

三类线程：单（可配）EventLoop 承担全部网络 IO 与重连调度；共享 `HashedWheelTimer` 承担全部定时（每请求超时、等待总超时、看门狗周期、`lostAt`）；专用单线程执行器承担锁丢失回调。所有出站写收口于 `RequestMultiplexer`，所有入站分发收口于同一入站 handler——避免多处写 Channel 带来的竞态。

### D7：测试基础设施策略

- §10.3 常规集成：复用/参考 server 模块 `TestServers` 的进程内真实服务器方式。
- §10.4 杀进程：`ProcessBuilder` 启动 M2 的 shaded jar（非进程内），`destroyForcibly()` 后断言请求超时失败 + 重启恢复。
- §10.4 半开连接：在 P1-24 即为 `ConnectionManager` 预留测试可见的注入口（可暂停出站写的测试钩子），不在 P1-26 现凑。

## Risks / Trade-offs

- [D3 的补偿路径涉及三重时序竞态，易写错] → 三种孤儿时序各设独立测试用例（§6.5"测试覆盖"要求），先写测试再实现。
- [D5 正交化的前提是"断连事件必然先于看门狗感知"，半开连接时客户端感知不到断连] → 半开连接下看门狗连续超时路径仍独立成立（连接"看似"ACTIVE），两条路径覆盖面互补；以故障注入用例验证。
- [EventLoop 上 complete future，用户回调阻塞会拖慢网络线程] → Javadoc 契约 + 锁丢失回调已隔离；可接受的权衡（D2）。
- [半开连接测试难以真实构造] → 测试注入口模拟暂停出站写，而非真实网络故障；接受模拟与真实场景的差距，进程级场景由杀进程用例覆盖。
- [requestId 重连后从 1 重来，理论上可能与旧响应的 id 重合] → 断连瞬间已清空全部挂起 future 与映射（旧连接已关闭，旧响应物理不可达），重合无实际影响。

## Migration Plan

新增模块，无迁移。构建验证使用 `mvn -s /home/lam/repo/settings.xml`（项目约束）。回滚即还原 `openlatch-client` 为占位 pom。

## Open Questions

- 无阻塞性未决项。若集成测试暴露"重发保持挂起"（D1）在极端丢包下等待体验差，可在不改变规格的前提下为重发增加有限次主动重试。
