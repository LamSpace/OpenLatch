# Tasks

## 1. 复现（门槛：造不出即停止实现，回 design D3）

- [ ] 1.1 策略点单测（红）：以"不确定窗 → 明确零生效拒绝 → 换会话"序列驱动判定，断言**当前**判定放行跨会话续发。验证=该用例红，且失败信息指明放行发生在"最近一次明确拒绝"这一步。
- [ ] 1.2 端到端可达性核验（脚本化服务面）：构造"首发无应答（读界超时）→ 同会话重发收 `NOT_LEADER`/缺码形 → 路由指向新会话"的三段时序，核验当前实现是否在新会话上真的续发。验证=计数证据（新会话侧 `countType(QUEUE_OP) > 0` 且元素被二次投递路径走到）。**1.1 与 1.2 均造不出 → 停止，转证据记录。**

## 2. 收紧

- [ ] 2.1 抽取判定为单一策略点（单调不确定闩），`parkLoop` 与 `exchange` 两处共用；判定为纯函数、不搬移循环控制流。验证=策略点单测覆盖两条判据分支（"曾不确定 ⇒ 拒"、"从未不确定 ⇒ 允"）。
- [ ] 2.2 1.1 / 1.2 的复现夹具转绿：断言跨会话续发被拒、以显式失败收场（`SESSION_EXPIRED`/超时），且新会话零 `QUEUE_OP`。验证=夹具由红转绿。
- [ ] 2.3 兼容性锚逐项不变（design D4）：`notLeaderRetriesSameEnvelopeSameOpSeq`、`malformedRejectEnvelopeRetriedNeverReadAsSuccess`、`queuedThenPushLostSelfResendClosesLoop` 全绿。验证=指定用例跑绿。

## 3. 规格与注释

- [ ] 3.1 实现与 delta 规格逐条对齐（措辞以实现为准回校）：MUST 条款的判据顺序、显式失败形态、同会话重发不受影响三条均与代码一致。验证=`openspec validate fix-queue-noeffect-monotonic --strict` 通过。
- [ ] 3.2 `RemoteBlockingQueue` 类级与方法级 Javadoc 同步：单调不确定闩的契约、判据顺序、判例如来源（去重槽按 `(会话, op_seq)` 记账）。验证=`bash scripts/check-source-citations.sh` 零命中，且注释只读自洽。
- [ ] 3.3 双语文档对账：`docs/guide/{zh,en}` 队列章涉及"重发/超时不确定窗"的段落同步该纪律一句。验证=中英双侧对应段落均更新、语义一致，无单侧更新。

## 4. 收口

- [ ] 4.1 全量构建：`mvn -s /home/lam/repo/settings.xml clean verify` 通过（0 失败 0 错误）。验证=构建输出。
- [ ] 4.2 门禁：`bash scripts/check-source-citations.sh` 与 `bash scripts/check-links.sh` 零命中/零死链。验证=命令退出码。
- [ ] 4.3 `WATCHLIST.md` W11 行注记：本 change（安全前置）已落地/在途，且为路由收敛 change 的前置。验证=行内新增引用指向本 change 目录，且不写长文。
