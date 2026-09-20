## Context

动机与范围见 proposal.md。设计语境：库内已有两条"非锁家族 + 等待-通知-重发闭环"判例——Latch（v3：`countDown` 进日志、`await` 零日志纯 Leader 内存队列、归零后永久放行）与 ATOMIC（v4：全操作进日志、`(session, op_seq)` 条目内去重槽、超时重发同槽不双加）。协议门判例链 v3→v4 均为"新消息对仅高版本会话开放、低版本消息级拒绝不断连"。Barrier 与 Latch 的关键差异在于**可复用世代**：Latch 归零态不可逆，重发天然命中"已归零"免疫竞态；Barrier trip 后状态回卷进入新世代，旧世代的"已通知未重发/已重发未落账"窗口若不加世代感知，旧请求会被误认作新世代到场。本设计的主轴就是把这个竞态钉死，并让"离场即破障"成为覆盖超时/中断/死亡/显式 break 四条路径的统一契约。

## Goals / Non-Goals

**Goals:**

- JDK `CyclicBarrier` 的核心承诺按分布式可裁决形态承载：多方到场合拢、可复用、动作由最后到场者先行执行且完成前其余方不放行、超时/中断/显式 break 使全体感知。
- 死亡即破障：在队参与者与动作执行者的会话消亡是世代的一等裁决事件，不是静默挂起。
- 到场计次跨 Leader 切换不双计、世代号不回退；了结记录覆盖"通知丢失/应答丢失/换主"三类重放路径。
- 全部新契约进规格与双语指南的增强/降级声明清单，契约 Javadoc 按 `Lock` 接口级撰写。

**Non-Goals:**

- 不做 `getNumberWaiting()`/`reset()` 的公开 API（前者需新增查询消息面且价值边际，后者被世代自愈语义取代；均记"不做"理由于 D6/D4）。
- 不做屏障等待的跨会话自动重放（到场是有副作用请求，纪律同 `countDown` 至多一次）。
- 不做动作执行的服务端超时强制（JDK 无此语义，挂起的动作由执行者死亡破障兜底；若运维压力成真按 WATCHLIST 流程另立观察项）。
- 不做 barrier 载荷/键值绑定（纯协调面，一档定位）。
- 不改 Latch 的既有契约与"新屏障=新 key"约定（D1 分叉的边界即在此）。

## Decisions

### D1 新建 `BarrierEntry` + `KeyFamily.BARRIER`，不扩展 `LatchEntry`

ROADMAP 原文"LatchEntry 状态机扩展"读作**机制复用**（`Waiter` 队列、`AWAIT_NOTIFY` 推送桥、`headReplyTimeoutMs` 已通知清扫、`REJECT_QUEUE_FULL` 护栏、`LatchAwaitResult` 形态的应答结构），而非类级复用。替代方案"在 `LatchEntry` 上加开关"被否：Latch 的三条定型契约——一次性不可逆、断连静默摘除、归零后 await 无条件放行——与 barrier 逐条相反，原位扩展迫使 v3 LATCH 家族重谈契约并背全量回归风险；家族合一还会制造"latch 定型 key 被 barrier await 加入"的不可裁决状态。分叉成本（一个平行条目类 + 第 5 家族判定）远小于契约污染。

### D2 协议门取 v5

v4 已被原子变量提案消耗（`OpenLatchServer.PROTOCOL_VERSION = 4`、握手区间 {1..4}、`ATOMIC_OP=14`/payload 32–33/raft 枚举至 9）。ROADMAP 落地纪律"下一个门为 v4"系过期行文，本提案随带勘正为 v5。编号落位：`MessageType` 15/16/17、`LockType.LOCK_TYPE_BARRIER=10`、`StatusCode.BARRIER_BROKEN=12`、`Envelope.payload` 34–39、`RaftEntryType` 10/11/12、admin 字段 11（SUMMARY）/8–10（KeyInfo）/16–20（KeyDetail）。

### D3 barrierAction 两阶段执行（`ACTION_DONE` 消息对）

服务端只能判定"谁是最后到场者"（apply 序），动作只能在客户端执行。方案 A（最后者应答后即广播全体、动作与其并行）保不住"动作完成前全体关闭"，barrierAction 退化为摆设，否。方案 B：trip 时刻若最后到场者 `carries_action=true`，世代进入**动作待决**——仅挂账不广播；执行者在本 `await()` 调用栈内跑完 `Runnable` 后提交 `BARRIER_ACTION_DONE`，服务端 apply 后才定型 TRIPPED、回卷、广播。代价每带动作世代多一跳（无动作世代零额外开销，请求标志位区分）。动作抛异常时 SDK 改发 `BARRIER_LEAVE`（离场即破障），放大为全体破障——恰好等价 JDK"动作异常 → BrokenBarrierException"路径，无需新消息。

### D4 破障为世代局部，不提供 `reset()`，提供 `breakBarrier()`

JDK 的 broken 粘滞至 reset 是单进程语义（同一批线程要么全部存活要么已散）。分布式下粘滞 broken 会让"一批参与者被杀后，下一批无辜参与者被历史破障卡死"，世代局部破障 + 自然回卷是正确泛化：破障只裁决当世代已到场者，新到场进新世代。因此 `reset()` 无剩余用途（不做，Non-Goal 记档）；`breakBarrier()` 保留（JDK 有此面且语义清晰）：`BARRIER_LEAVE` 携 `await_request_id=0`，破当前世代，无条目幂等。

### D5 离场即破障：超时/中断/死亡/显式 break 统一为一条契约

```
        ┌───────────────────── 当前世代 G ─────────────────────┐
  触发面 │ await(timeout) 超时 │ 本地中断 │ 会话死亡/失联清理 │ breakBarrier() │ 动作异常(D3)
        └──────────────┬──────────────────────────────────────────────────────┘
                       ▼
        BARRIER_LEAVE(G 在队项摘除) 或 SESSION_CLOSE apply
                       ▼
        世代 G := BROKEN，generation+1，动作挂账清除
                       ▼
        全体在队者 AWAIT_NOTIFY → 同 request_id 重发 → 了结记录 BROKEN → BARRIER_BROKEN
```

统一规则消灭了"幽灵到场者"问题：JDK 用超时即破障防止缺席者饿死其余方，我们同样让**任何已到场身份的离场连带破障**，到场账簿永远与潜在放行能力一致（不会以 N-1 方合拢"成功"）。替代方案"超时仅本地离场、世代续等"被否：会静默少人合拢或永久挂起，两头都更差。会话死亡破障顺带覆盖"进程被杀"——ROADMAP 头号验收点的机制本体就是 `removeSession → 当前世代 BROKEN` 这一条接线。

### D6 公平套件豁免 + API 面裁剪

`FairOrderingSuite` 对 BARRIER 不适用：无 FIFO 授予序语义（合拢是广播事件，到场顺序仅决定"谁是最后"，由 Raft apply 序承载并由确定性测试钉住），豁免正文沿 ATOMIC 判例写入 tasks。`getNumberWaiting()` 不做查询消息：其在队位次读数已由 `queue_position` 与 admin 面承载，SDK 面裁剪进 proposal 已声明的 API 清单。`isBroken()` 取句柄本地最近所见了结（零网络；声明非实时一致）。

### D7 状态切分：复制侧账簿 vs Leader 内存队列

```
 复制状态（进日志+快照，跨副本一致）          Leader 内存（不复制，failover 即失）
 ───────────────────────────────           ──────────────────────────
 parties / generation                       在队位次、AWAIT_NOTIFY 时序
 当前世代到场账簿 {(session,requestId)}      已通知等待者的响应超时窗口
 动作挂账 (session,requestId,generation)
 了结记录 {完结世代G, 结果, G 全部已了结
          (session,requestId) 集合}（有界：
          仅保留最近一个完结世代）
```

三条 BARRIER 命令全进日志（到场改变上表左列，与 Latch"await 零日志"刻意分道——barrier 到场**是**状态迁移）。重放幂等的根：去重与了结都在左列。换主后旧请求重发命中回放账簿 → 一致裁决；了结记录窗口外（完结两代以上）的极端迟到按新到场计入当前世代——声明竞态，与锁/Latch 等待队列不迁移同口径，窗口下界由已通知清扫超时刻意压住（正常客户端不可能迟到出窗，出窗者本就已放弃该等待）。

### D8 `BARRIER_BROKEN` 为在带裁决状态码，拒绝类复用 `INVALID_REQUEST`

破障不是请求错误而是所等待世代的裁决结果，混入 `INVALID_REQUEST` 会让 SDK 无法区分"请求形状错"与"世代已破"（重试策略完全不同），故新增状态码一枚（纯增量）。parties 断言、类型不匹配、非执行者回报等拒绝类一律复用 `INVALID_REQUEST`（`REJECT_SEMAPHORE_TOTAL`/`REJECT_LATCH_TOTAL`/`REJECT_ATOMIC_INIT` 三判例），core 侧新增 `Outcome.REJECT_BARRIER_PARTIES` 承载细分。

### D9 BARRIER 条目定型后存续不回收

与 Latch"条目存续 + 新屏障=新 key"约定相反的一面：barrier 的 key 即屏障身份，世代回卷使同 key 复用是常态语义（循环），回收再重建会丢 parties 定型与世代单调性。无租约家族不入到期堆；常驻内存量 = O(key 数)，maxKeys 护栏兜底。快照/恢复与 ATOMIC/Latch 同型（`CoreStateRestore` 加 BARRIER 分支 + `BarrierEntry.restored` 工厂）。

## Risks / Trade-offs

- [了结记录只保留最近一个完结世代，极端迟到重发被计入新世代] → 窗口下界 = `headReplyTimeoutMs` 且正常 SDK 在窗内必重发或离场；E2E 加"通知丢失→自发重发仍正确了结"锚；声明写进契约 Javadoc。
- [动作待决世代被执行者网络分区卡住其余方] → 执行者会话失联判定时 `SESSION_CLOSE` 经复制破障（失联批量清理既有机制）；破障延迟上界 = 失联判定窗口，指南运维节写明。
- [三命令全进日志，热 key 高频换世代的日志增长] → 与 ATOMIC 同读 W6 纪律；barrier 到场频率天然受 parties 整批约束，基准套 barrier 相测合拢延迟而非单到场吞吐，量级异常按 WATCHLIST 流程评估读路径。
- [超时即破障放大单客户端抖动的爆炸半径] → 这是 JDK 原生语义（超时破障）的忠实分布化，非新增风险；指南"编程建议"节提示：对抖动网络用 `await()` + 外部监督或加大超时预算。
- [世代号跨重启经快照恢复若有序列缺陷可能回退] → 快照恢复测试断言 generation 单调不回退；了结记录与账簿逐字段往返保真。
- [v5 客户端 × v4 服务端不可在单仓实例化] → 沿 v3/v4 判例：以指南兼容矩阵注记承担，新客户端向 v4 服务端握手成功但发 BARRIER 消息得 `INVALID_REQUEST` 属服务端侧可测面（`BarrierGatingTest` 反向夹具）。

## Migration Plan

纯增量发布：协议门 [1,5]、proto 编号纯新增、既有家族零改动。滚动升级序沿用现行集群纪律（先升服务端后升客户端）；回滚窗口 = 仅含 BARRIER 流量的日志条目对 v4 服务端是未知类型（回放按未知条目 error 路径现行口径处置）——运维指南注记"降级前确认无在途 barrier 使用"，与 ATOMIC 回滚注记同款。

## Open Questions

（无——D1–D9 已覆盖规格与任务拆分所需全部裁决。）
