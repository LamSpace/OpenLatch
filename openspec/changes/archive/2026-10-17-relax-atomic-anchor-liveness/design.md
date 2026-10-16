# Design

## Context

动机见 `proposal.md`。塑造方案的现状与约束：

- **两枚锚的现状断言**：`addAndGet(5)==5`、`incrementAndGet()==6`、杀主、`get()==6` 且 `getVersion()==2`、`incrementAndGet()==7`（引用族同构，CAS 形态 + 载荷字节）。任一环节的显式终态都会让用例 `ERROR`。
- **契约面**：原子族可重试失败（`NOT_LEADER`、断连快速失败、超时）以同一 `op_seq` 重发至请求总超时界限；界限内不可判定抛 `OpenLatchTimeoutException`，**效果不确定**由 `getStamped()` 核对。异常族层次：`OpenLatchTimeoutException`／`ServerUnavailableException` 皆 extends `OpenLatchException` ⇒ **一个 catch 覆盖全部显式终态**（`InterruptedException` 单独，不吞）。
- **同文件既有口径**：队列锚 `committedQueueElementsSurviveLeaderKillWithoutDoubleApply` 在显式失败分支 `catch (OpenLatchException) { return; }`，Javadoc 写明"显式失败即安全形态，伪成功不可能从该分支成型"。W11 收敛锚亦声明"显式超时是合法终态……只有静默错值才是缺陷"。**本 change 沿用同一判据哲学**。
- **幂等性来源**：读（`get`/`getStamped`/`getVersion`）无副作用，可安全重发；写带 `op_seq` 去重槽——**同一逻辑增量的重发**是幂等的，但**重新发起一次同增量的逻辑写**不是（新 `op_seq`⇒新槽⇒重复生效）。这条区分决定了"复核先行"纪律。
- **测试支撑位置**：判据须能被负向控制直接喂非法值 ⇒ 抽成纯判定（不依赖集群）；`ClientClusterIT` 与负向控制同包（`io.github.lamspace.openlatch.client`）。

## Goals / Non-Goals

**Goals:**

- 两枚锚在**契约面**上仍然红得住：静默重复生效（双加）、版本漂移、已提交值丢失、载荷被写成第三方值 —— 一律 `AssertionError`。
- 显式终态**不再**判失败，但**不放弃安全面断言**：先复核、再据实况推进、把断言跑完。
- 判据放宽**不得**变成"永远绿"：真 wedge（有界推进推不动）仍红，并有负向控制**证明**判据非空网。

**Non-Goals:**

- 零 `src/main` 改动、零规格面改动。
- 不加"重试直到绿"的循环（那会把"必须收敛"也删掉）。
- 不动 W11 收敛锚与其余锚（收敛面的严格活性由它们承载）。

## Decisions

### D1 判据改"复核后的账本合法性"，不跳断言

显式失败到达后：**读（幂等）复核 → 按读到的事实推进/收场**。标量增量步读到的值 MUST ∈ `{pre, pre+δ}`；引用 CAS 步读到的载荷 MUST ∈ `{expected, update}`。第三态即"静默重复生效／被写成第三方值"，直接 `AssertionError`。

> 备选（照抄队列锚的 `catch → return`）被否：那会**跳过**换主窗内的安全面断言，而"换主窗内不得静默错值"恰是 W19 调查的核心关注面。本 change 取更强的形态：容忍显式失败，但要求失败后账本仍合法。

### D2 "复核先行"，不盲目重发

写遇到显式终态后，**不**直接重发同一逻辑增量：先读复核——若复核显示增量已生效，即视为成功收场（重发会重复生效）；只有复核显示未生效（或读本身也失败、下一轮再复核）才继续尝试。这条纪律把"不确定"在**每一次**失败后都收干净。

### D3 有界（尝试上限 + 时限）⇒ 真 wedge 仍红

推进循环受 `MAX_ATTEMPTS`（尝试上限）与 `ADVANCE_DEADLINE_MS`（时限）双重约束，超出即 `AssertionError`（"有界推进未达目标"）。放宽的是"预算内一次成功"，**不是**"可以不收敛"——活性方向仍有一条（更弱的）网。

### D4 判据纯函数化 + 负向控制（证明非空网）

`LeaderKillLedger` 暴露两个**纯判定**：`assertAdvanceResolved(what, pre, δ, observed)` 与 `assertDurable(what, observedValue, observedVersion, expectedValue, expectedVersion)`；负向控制直接喂非法值断言其抛错，并加一枚**端到端**负向控制：脚本化桩对 `ATOMIC_OP` 回一个"双加过的值"（`value=pre+2δ`），断言 `advanceScalar` 必拒。**没有这一层，"放宽"就无法与"删掉判据"区分开**。

### D5 读的幂等重发也带界

复核读同样受尝试上限约束（读可安全重发，但不能无限重发）；读全失败即 `AssertionError`。这样"读面整体不可用"仍被判失败（而不是静默略过断言）。

## Risks / Trade-offs

- [放宽后掩盖真实活性回归] → D1 保安全面、D3 保"可收敛"面、D5 保"读数可用"面；且**显式声明收窄**（proposal 的 Impact 与台账 W19 行写明：两枚锚不再对"换主窗内成功率"敏感，该面由 W11 收敛锚等承载）。
- [复核先行带来的额外往返] → 仅发生在显式失败路径（本就是用例的坏窗）；稳态零额外开销。
- [负向控制把判据写死成"实现细节"] → 负向控制只喂**账本语义**上的非法态（双加/漂移/丢失/第三方值），不绑定实现细节。

## Migration Plan

纯测试口径变更：无迁移、无回滚残留（还原三个文件即回到严格活性口径）。

## Open Questions

（无。若后续希望重新收紧某一步为严格活性，只需把那一步从 `advanceScalar`/`advanceRef` 换回直接断言——本 change 的 helpers 正是为这种单点收紧留的接缝。）
