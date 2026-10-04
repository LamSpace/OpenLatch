# Design

## Context

动机与范围见 proposal.md（Why / What Changes）。本档记录实现层机制定案与技术决策。

**根因机制链**（6 轮取证 + 静态阅读定案，待任务 1 判红实证收口）：

1. `ClusterRequestHandler.handleQueueOp` 过 `validateEnvelope(msg, session, requireLeader=true)` 角色门；`handleLatchAwait` 同门；`handleLatchCountDown` 不过门但提交失败同样落 `commitFailure`。非权威角色与 `RetryableCommitException`（含选举窗 `LeaderNotReadyException`/`NotLeaderException`，run-1 现场亲见）两路都汇入 `notLeaderEnvelope`。
2. `notLeaderEnvelope` 的 switch 仅含 `LOCK_RELEASE`/`LEASE_RENEW`/`ATOMIC_OP`/`BARRIER_*` 六个 case，default 落 `AcquireResponse{NOT_LEADER}`——`QUEUE_OP`、`LATCH_COUNT_DOWN`、`LATCH_AWAIT` 三型以 `type` 回显请求类型、payload 却携异型 `acquire_response`。
3. `RequestMultiplexer.onResponse` 仅按 `request_id` 关联完成 future，不校验载荷形态；`RemoteBlockingQueue` 的 `exchange`/`parkLoop` 盲取 `.getQueueOpResponse()`——oneof 缺省返回 protobuf 默认实例，其 `status` 为枚举零值 `StatusCode.OK`。
4. 成型后果：`parkLoop` 对 OK-TAKE 缺元素交付 `byte[0]`（`takeAsString()==""`，形态 A）；`exchange` 对 OK-SIZE 缺省读数返回 0（形态 B）。队列从未提交进日志，故 W10-DBG 见两存活节点 `shadowDepth=2` 一致——复制面无辜。
5. 姊妹面已对：v7 轮同步了单机 `RequestDispatcher.errorResponse` 的 `QUEUE_OP` case，集群 `notLeaderEnvelope` 漏更；`ATOMIC_OP` 因有 case 而原子锚同窗稳定——同环境两车道一绿一红的差异被完整解释。

**修正既有定性**：WATCHLIST W10 行所记"错在 `mapQueueOp` OK-空成型"不准确——`mapQueueOp`（Leader 权威应用路径）本身正确；病灶是其邻居 `commitFailure → notLeaderEnvelope`。

**约束**：既有字段编号与语义 MUST NOT 变更（wire-protocol 冻结纪律）；`StatusCode` 词表零新增；队列车道未进入任何已发布版本（1.0.0 为 v6），无历史客户端兼容负担；LATCH 车道已发布（v3 起）。

## Goals / Non-Goals

**Goals**

- 三个消息类型的集群拒绝应答码形同型化，恢复"拒绝状态码线路可见"不变式。
- 队列车道对缺码形/违例应答的裁决护栏落地（spec delta 的两条 MUST）。
- 表驱动门禁使"新增消息类型漏配拒绝 case"机械性不可复发。
- `ClientClusterIT` 队列锚摘 `@Disabled` 转常驻；W10 销账。

**Non-Goals**

- 不收敛客户端其余车道（atomic/barrier/latch/lock）的缺码形盲读——其服务端码形经本次修复后已正确，护栏扩展另议。
- 不动 proto 定义、不升协议版本、不扩 `StatusCode`。
- 不处理选举窗本身的收敛速度（W8/形态 B 选举风暴残余另行承载）。
- 不回填历史 `queue.total{status}` 指标数据（修复后自然正确，历史失真不追改）。

## Decisions

### D1 服务端沿 `ATOMIC_OP` 无提示判例，不扩 leader 提示字段

`notLeaderEnvelope` 为三型各补 case：`QueueOpResponse{status=NOT_LEADER, op=回显}`、`LatchCountDownResponse{status=NOT_LEADER}`、`LatchAwaitResponse{status=NOT_LEADER}`。载荷字段形态照 `ATOMIC_OP` case 判例——**不**填 `leader_node_id`/`leader_address`。备选：仿 v2 三类应答附提示字段——否决：提示字段被 wire-protocol 成文为 Acquire/Release/LeaseRenew 三消息专属扩展（"提示字段仅在相关应答中出现"），扩到队列/latch 需动 proto 字段且违反"字段编号与语义 MUST NOT 变更"下的最小增量纪律；改道能力不因字段缺失而降级——客户端"连续 3 次 NOT_LEADER 触发强制发现"（Leader 发现与故障转移 §4）与 mux 层重发现本就兜底无提示场景，v4/v5 车道同型拒绝上线至今未见需求。

### D2 客户端护栏分级：缺码形=瞬态，OK-缺元素=协议违例显式抛

- `exchange`/`parkLoop` 取得应答后判 `hasQueueOpResponse()`：缺载荷按瞬态失败裁决（退避、重取 `latchRoute`、同信封重发），至时限耗尽落既有超时口径。理由：缺码形应答意味着请求**未被受理**（拒绝或异型残留），重发无双重生效风险（写有 `op_seq` 去重槽、读零迁移）；对"新服务端+旧客户端"升级序无影响（新服务端不再产出缺码形）。
- `TAKE` 真 `OK` 且缺 `element_bytes` presence：显式 `OpenLatchException` 协议违例，**不**入瞬态重发。理由：该形态仅在状态机交付契约被破坏时出现（OK-TAKE 必携交付，`optional bytes` 显式 presence 使空串元素与缺省可辨），静默重发会把状态机级缺陷伪装成可用性抖动；抛出保留现场可诊断性。`PEEK`/`SIZE` 的 OK-缺省为合法空读数，不受此护栏约束——护栏严格限定 `op==TAKE`。
- 实现载体：判定收口在队列车道私有方法（信封→响应解包一处），不引入跨车道公共抽象（单点护栏，避免为尚未收敛的其余车道预留接口）。

### D3 LATCH 两 case 并入本 change（用户定夺）

同病灶（同一 switch 的 default 兜底）同药方，每 case 一行；且 latch 的默认实例误读语义比队列假空更恶性（`LatchAwaitResponse` 默认 OK=0 剩余/已破、`LatchCountDownResponse` 默认 remaining=0——非 Leader 拒绝可被成型为"屏障已破"假绿）。latch 虽无现场显形证据，但**单测级码形复现是确定性的**（向 latch 请求喂非权威节点即得异型应答），不依赖时序竞态，满足红先纪律。备选：只修队列、LATCH 挂 W 行——否决：留一行已知可判红的缺口跨 change 不合算。

**已发布 v3 客户端兼容注记**：服务端修复后，latch 拒绝从"异型 acquire_response（老客户端盲读默认 OK→假绿）"变为"同型 NOT_LEADER（老客户端读到 NOT_LEADER）"。老客户端对 latch 应答 NOT_LEADER 的处理以 apply 阶段用例验证归入既有瞬态/异常口径——两种形态下行为都不如新客户端正确，但修复使老客户端从静默假绿进入显式失败，属严格改善，不构成契约破坏（SDK 文档从未承诺 latch 收到拒绝时的成型行为）。

### D4 表驱动门禁置于 server 侧夹具，枚举两类拒绝码形

门禁测试枚举客户端接入车道全部请求 `MessageType`（HELLO 除外），对两类拒绝码形路径（单机 `errorResponse` 门控拒绝、集群非权威拒绝）逐一构造请求并断言应答 `has{ExpectedResponse}()` 且状态码正确——新消息类型未配 case 即门禁红。判例：既有 vocabulary 夹具的"词表逐项对照"型防线。备选：编译期穷尽 switch（去 default 改 Java exhaustive switch + 抛错兜底）——部分采纳为实现细节（default 分支加 WARN 日志以暴露未来漏配），但编译期穷尽对 proto 生成枚举不可靠（`UNRECOGNIZED` 强制 default），运行时门禁才是单一事实源。

### D5 确定性复现以"钉 follower"替代"杀 Leader 竞速"

W10 动作列要求的复现是时序竞态（3/6 概率）；本 change 的复现夹具全部构造化：服务端码形单测直接向非权威 handler 喂请求（无时序）；客户端 Scripted 单测注入异型/违例应答信封（无服务端）；`ClusterHarness` 三节点 E2E 令客户端**直连钉死在已知 follower** 发 `size`/`take`（拒绝窗必然命中）。`ClientClusterIT` 原锚（真杀主+漂移）摘 `@Disabled` 后作为集成回归常驻，不再充当根因探针。

### D6 换主窗改道收敛：扩面试验判负，收窄为安全性质 + W11 另立（用户定夺）

**判读**：信封修复后锚点浸泡仍 2/6 红，但失败形态已从"伪成功"（OK-空/0——W10 根因，已根治）变为"take 于换主窗 churn 满预算显式超时"。机制：非 ACQUIRE 车道（队列/原子/barrier/latch）经 `latchRoute()` 取路由，改道仅靠 home 连接**断裂重连时**的 HELLO 提示建道——若 home 活着停在非权威节点（Leader 已迁他处），无断连即无新 HELLO、无提示、车道永不重建，同型 NOT_LEADER（无提示字段，D1）只被 `continue` 消化至超时。

**试验与判负**：接线共享强制发现（`noteLaneNotLeader`：NOT_LEADER/缺码形计入既有 `notLeaderStreak`，达阈值 3 触发 `SeedDiscovery` 扇出并 `retargetAcquireLane` 重建车道）后，锚点由 4/6 绿**回归至 1/6 绿**——异步发现完成恰逢重发环 mid-flight：发现前的尝试若落"不确定窗"（连接失败/超时，不属零生效、`noEffectReject` 不置位），车道换建触发严格换代守卫显式抛错，失败率反升。要修正确需重做非 ACQUIRE 车道的路由语义（可判别"未发出"的传输失败形态、或驻留窗定期 re-HELLO 提示刷新），显著超出本 change 的信封码形范围。**全部回退**，零残留（既有 Scripted/LeaderDiscovery 复跑全绿）。

**收口取径（用户批准：方案 1 收窄+另立）**：本 change 兑现的 spec 承诺收窄为**安全性质**——换主窗队列请求 MUST NOT 伪成功（空串/0 错值交付已被杜绝），改道收敛或显式失败皆为合法终态；`ClientClusterIT` 队列锚改为双分支（收敛轮全链路断言 a→b→null→c→1，未收敛轮以 `OpenLatchException` 收场即绿）。改道收敛缺口的正确修复登记为 WATCHLIST **W11**（带本判负数据与候选设计），按流程另立 change。

**全量复验追加判读（用户独立复跑第 4 步命中）**：同一缺口在**启动提示滞后窗**亦可显形——套件负载下 seed 节点 HELLO 提示尚为 -1、车道不建且 home 不断连，换主**前**的 put 即 churn 满 30s 预算显式超时。据此锚的显式失败豁免扩至**整个操作序列**（伪成功回归仍必红：错值走 `AssertionError`，不属被容忍的 `OpenLatchException` 异常族）；W11 复现口径与候选设计同步补入该窗（候选"驻留期定期 re-HELLO 提示刷新"正对应）。

**护栏保留项**：`noEffectReject`（零生效拒绝后的跨会话重建续发）**保留**——它是信封修复的必然客户端对偶（此前伪 OK 掩盖了该窗口），6 轮浸泡证实其对 put 首窗稳定化且无双插回归（锚名 `WithoutDoubleApply` 的 size/序列断言即守卫）；不确定窗（超时/应答丢失）后的会话切换维持既有严格放弃。

## Risks / Trade-offs

- [T1 单测判红若不如预期（例如 latch 路径实际不经过 `notLeaderEnvelope`）] → 红先任务先行：任务 1 判红是修复的前置证据，形态与预判不符则回到探索修正 spec delta，不带病修复。
- [已发布 v3 latch 客户端对同型 NOT_LEADER 的反应未经现场验证] → apply 阶段以老版本形态用例锁行为（读码确认 `doAwait`/`countDown` 对 NOT_LEADER 的分支归属）；D3 注记已声明两害相权——假绿严格劣于显式失败。
- [缺码形按瞬态重发在"服务端永久异型"（未升级旧节点长期混布）下退化为超时失败] → 可接受：这正是把伪成功换成显式失败的目标行为；升级序由发布纪律（同批产物）保证，混布窗内队列请求本就不可用。
- [门禁测试与被测码形路径的耦合]（gate 需触达包私有拒绝组装）→ 经既有测试夹具通道（`ClusterHarness`/`ClusterReplicationTest` 模式）构造，不为门禁新增公开 API。
- [`parkLoop` 重发循环对瞬态缺码形的退避节奏未单独定义] → 沿用既有瞬态车道口径（`NOT_LEADER continue` 同环重取路由），不另发明退避参数。

## Migration Plan

无配置变更、无数据迁移、无 proto 变更。部署即随服务端产物生效；回滚=还原 switch/护栏代码，无状态残留（拒绝路径零日志条目、零状态变更）。升级序宽容：新服务端+旧 v3 客户端（D3 注记）、旧服务端+新客户端（缺码形护栏瞬态兜底）均可用，无窗口约束。

## Open Questions

（无——范围、形态、门禁落点均已定案；D3 的老客户端分支归属属 apply 阶段验证事项而非设计未决。）
