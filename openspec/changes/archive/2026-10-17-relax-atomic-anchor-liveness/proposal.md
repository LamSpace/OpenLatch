# Proposal

## Why

台账 W19 的按序处置走到第 (3) 步：**(2) 步的同负载复测已证两枚严格活性锚在持续重负载下仍红**（pin 侧 2/4、master 侧 4/4；签名一律是契约**已声明**的显式终态——`SESSION_EXPIRED`／`OpenLatchTimeoutException`），而同口径还能打破**契约面**锚（W11 收敛锚 master 侧 2/4）⇒ 该口径不能充当活性神谕。

同时，(1) 已完（取口同代 + 跨代快照 pin 均已落地并各有确定性夹具）⇒ 台账那句"**MUST NOT 跳过 (1) 直接放宽**——那会把 W22 缺陷与真实活性回归一并从判据里删掉"的**前提已解除**：现在放宽不会再掩盖 W22。

两枚锚（`ClientClusterIT#committedAtomicValueSurvivesLeaderKillWithoutDoubleApply`、`#committedReferencePayloadSurvivesLeaderKillWithoutDrift`）当前断言的是**比契约更严的活性**：每个写/读必须在预算内一次成功。原子族的契约恰好有意让渡了这条活性（exactly-once 优先：可重试失败以同一 `op_seq` 重发至总超时界限，界限内仍不可判定即抛 `OpenLatchTimeoutException` 并**声明效果不确定**、以 `getStamped()` 核对）。同文件的**队列锚**早已按此口径写就（"未收敛时以显式 `OpenLatchException` 收场即为合法终态；回归任何静默错值必红"）。

## What Changes

把两枚原子锚的判据**改为契约面（不静默错值）**，与同文件队列锚同款，但**比队列锚更进一步**：

- **不跳过安全面**：队列锚在显式失败分支 `return`（放弃断言）；本 change 改为**先以幂等读复核解除不确定（`getStamped()`），再据实况推进并继续把安全面断言跑完**——恰好把"换主窗内不得静默错值"这条网的覆盖留在最需要的窗口里。
- **不盲目重发**：显式失败后**先复核**；只有复核证明"本次增量未生效"才重试（重发已生效的增量会重复生效），复核证明"已生效"即以成功收场。
- **有界**：推进受尝试上限与时限约束；"永远推不动"（真 wedge／无法收敛）**仍红**——放宽的是"必须在预算内一次成功"，不是"可以不收敛"。
- **判据纯函数化 + 负向控制**：账本合法性判据抽成纯判定（`LeaderKillLedger`），并配**确定性负向控制**证明判据不是空网：喂双加值（`pre+2δ`）、版本漂移、已提交值丢失 → 必须 `AssertionError`；再加一枚**端到端**负向控制（脚本化桩回一个"双加过的值"，断言 `advanceScalar` 必拒）。

**非目标**：不动 `src/main`（本 change 零产品面改动）；不动 W11 收敛锚与其余锚；不把"活性"从判据里整体删掉（真正的 liveness 面由 W11 收敛锚与各功能 IT 继续承载）。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

（无——零规格面：`openspec/specs/` 中无任何规格记载这两枚锚的判据；本 change 为测试口径变更，`skip_specs: true`。）

## Impact

- **测试**：`openlatch-client/src/test` 三处——新增 `LeaderKillLedger`（判据 + 有界推进纪律）与 `LeaderKillLedgerTest`（负向控制，含一枚脚本化桩端到端控制）；`ClientClusterIT` 两枚锚改写为契约面判据。
- **产品**：零改动；失败形态语义不变（显式终态仍是显式终态）。
- **判据面收窄（显式声明）**：两枚锚**不再**断言"预算内一次成功"的严格活性；换以"显式失败 + 复核后账本合法 + 有界可推进"。若将来出现"换主窗内成功率显著劣化"这类**产品活性回归**，本判据**不再敏感**——该面改由 W11 收敛锚与 `ClientFaultInjectionIT`／`LeaderKillDrillIT` 等承载（本 change 在台账 W19 行写明这一收窄）。
