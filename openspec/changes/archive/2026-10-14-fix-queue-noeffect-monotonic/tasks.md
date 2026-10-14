# Tasks

## 1. 复现（门槛：造不出即停止实现，回 design D3）

- [x] 1.1 策略点单测（红）：以"不确定窗 → 明确零生效拒绝 → 换会话"序列驱动判定，断言**当前**判定放行跨会话续发。验证=该用例红，且失败信息指明放行发生在"最近一次明确拒绝"这一步。**执行顺序修正（如实记录）**：修复前的判定内联在两条写循环里、**没有可测的策略点**，故"红"是在**端到端**层面取得的（见 1.2）；策略点抽出于 2.1 之后，单测（`SessionContinuationTest`，6 例）钉住的是**修复后**语义，作为该纪律的判据层与回归网。
- [x] 1.2 端到端可达性核验（脚本化服务面）：构造"首发无应答（读界超时）→ 同会话重发收 `NOT_LEADER`/缺码形 → 路由指向新会话"的三段时序，核验当前实现是否在新会话上真的续发。验证=**已验**：夹具 `RemoteBlockingQueueScriptedTest#uncertainWindowIsNotClearedByLaterZeroEffectReject` 修复前**红**（新会话侧 `countType(QUEUE_OP) == 1`，且 `put` 正常返回——即该写被重新执行），**连续 3 轮稳定红**；时序要点＝**先吃拒绝（标志被置）再翻路由**，车道建早了会被守卫拦下（不命中）。

## 2. 收紧

- [x] 2.1 抽取判定为单一策略点（单调不确定闩），`parkLoop` 与 `exchange` 两处共用；判定为纯函数、不搬移循环控制流。验证=**已验**：新增 `SessionContinuation`（包私有），两处写循环的 13 个判定点全部改经它（`grep noEffectReject` 残留 0）。
- [x] 2.2 1.1 / 1.2 的复现夹具转绿：断言跨会话续发被拒、以显式失败收场（`SESSION_EXPIRED`/超时），且新会话零 `QUEUE_OP`。验证=**已验**：夹具转绿（新会话零 `QUEUE_OP`，且 `put` 以显式 `OpenLatchException` 收场）。
- [x] 2.3 兼容性锚逐项不变（design D4）：`notLeaderRetriesSameEnvelopeSameOpSeq`、`malformedRejectEnvelopeRetriedNeverReadAsSuccess`、`queuedThenPushLostSelfResendClosesLoop` 全绿。验证=**已验**：整族脚本用例 10/10 绿（含上述三条）。

## 3. 规格与注释

- [x] 3.1 实现与 delta 规格逐条对齐（措辞以实现为准回校）：MUST 条款的判据顺序、显式失败形态、同会话重发不受影响三条均与代码一致。验证=**已验**：措辞回校一处——单调位的来源由"应答丢失/超时、传输失败"**扩为**并含"**已被服务端受理/入队**"（`QUEUED`/`DENIED` 同样意味着已进账簿，此后换会话同样不安全，实现按此落地）；`openspec validate --strict` 通过。
- [x] 3.2 `RemoteBlockingQueue` 类级与方法级 Javadoc 同步：单调不确定闩的契约、判据顺序、判例来源（去重槽按 `(会话, op_seq)` 记账）。验证=**已验**：类级"车道"段落与两处写循环的注释按新语义改写；新增类 `SessionContinuation` 全成员 Javadoc（契约/线程模型/调用者义务）；全量构建含 `javadoc` 目标。
- [x] 3.3 双语文档对账：`docs/guide/{zh,en}` 队列章涉及"重发/超时不确定窗"的段落同步该纪律一句。验证=**已验**：`03-client-sdk.md` 队列章降级清单第 3 条中英双侧同步（"会话中途切换仅在本次操作从未有过可能已生效的尝试时续发"）。

## 4. 收口

- [x] 4.1 全量构建：`mvn -s /home/lam/repo/settings.xml clean verify` 通过（0 失败 0 错误）。验证=**已验**：全反应堆 8/8 模块 SUCCESS（客户端 135 单测 0 失败 + 89 IT 0 失败 1 跳过）。过程中另有一次全量运行在**与本 change 代码路径无关**的用例上红过（见 4.2 附注）。
- [x] 4.2 门禁：`bash scripts/check-source-citations.sh` 与 `bash scripts/check-links.sh` 零命中/零死链。验证=**已验**：citation 零命中（退出码 0）、死链 0（退出码 0）。**附注（如实记录）**：一次全量运行中 `ClientSemaphoreClusterIT#countingReentrancyAndTotalAssertAcrossCluster` 以 `release of 'csem-count' failed: INVALID_REQUEST` 报错一次；隔离复跑 3 轮 + 客户端模块全量负载 2 轮均绿，随后全量复跑绿——定性为**低频间歇**，代码路径与本次改动不相交（信号量族走 ACQUIRE 车道），归因未证，已向评审人提报（拟登记在册观察项，待其定夺）。
- [x] 4.3 `WATCHLIST.md` W11 行注记：本 change（安全前置）已落地/在途，且为路由收敛 change 的前置。验证=**已验**：行内新增引用指向本 change（名与内容），且未写长文；表完整性复查 14 行号集合不变、每行 6 列、零粘连。
