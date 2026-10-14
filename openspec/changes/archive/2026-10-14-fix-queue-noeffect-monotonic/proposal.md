# Proposal

## Why

队列写路径的"零生效逃逸"判定是**逐次覆写**的单布尔（`RemoteBlockingQueue.noEffectReject`）：超时/传输失败置 `false`（不确定窗）、`NOT_LEADER`/缺码形置 `true`（明确零生效），而跨会话续发的放行只看**最近一次**尝试的取值。

于是同一逻辑操作内"先前进入过不确定窗"这一事实，会被后一次明确拒绝**洗白**：

```
  同一逻辑写 (session=S, opSeq=N)  —— 队列现状
  #1 发往当值 Leader，服务端已 apply，回执丢失  => 超时(不确定)  => 标志=false
  #2 同会话重发，节点已非权威               => NOT_LEADER/缺码形 => 标志=true
  #3 判据只看最近一次 => 放行跨会话续发 => 去重槽键含会话
     （(session, opSeq)，见 core 的 AtomicEntry 规则 4），新会话槽不中 => 重新执行
                     结果：静默重复生效（PUT 双插）
```

这是**静默错值**族——与 W11 回退轮"假绿严格劣于显式失败"的判据同轴。今天的复现面窄（需"回执丢失 → 换主/拒码形 → 换会话"三段时序），但 W11 的收敛修复（`fix-lane-route-refresh`）会显著提高换会话频率，会把这条洞一起带进更高频的路径。

> **证据状态**：该洞由代码阅读推得，**尚未实证**。本 change 的第一道门就是造出确定性复现——造不出则本 change 转为证据记录，不落地收紧（见 design D3）。

## What Changes

- 队列写路径的零生效逃逸判定，由"逐次覆写"改为**单调不确定闩**：同一逻辑操作内一旦有任一尝试进入不确定窗（超时/传输失败），后续判据 MUST NOT 因后一次明确拒绝而放行**跨会话续发**；此时跨会话 MUST 以显式失败收场（`SESSION_EXPIRED`），**同会话重发不受影响**（服务端去重槽照旧兜底）。
- 判定收敛为**单一可测策略点**（小类，判例 `AwaitTracker` 的可测切分），使"是否放行续发"成为可单测的纯函数，而非散落在两个写循环里的布尔读写。
- 语义落规格：把该纪律以 MUST 条款与可执行场景写进 client-sdk 的队列需求。
- **非目标**：不改触发器、不做路由收敛（W11 本体另立 change）；不改缺码形与 `NOT_LEADER` 的既有处置语义；**只收紧不放宽**——不得把任何现存显式失败变成静默成功；不改服务端、不改协议、不动其余族。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

- `client-sdk`：修改「OBlockingQueue API」需求——追加"单调不确定闩"条款及其场景。既有九条场景逐字保留；既有"换主窗口写重发不双插不偷吃"场景在首发即 `NOT_LEADER`（无不确定窗）时语义**不变**。

## Impact

- **代码**：`openlatch-client` 的 `RemoteBlockingQueue`（`parkLoop` 与 `exchange` 两处写循环的判定收敛为共享策略点）。服务端、协议、其余族零触碰。
- **行为**：一个窄窗（先超时后明确拒绝再换会话）由"静默重复生效"改为"显式失败"。可用性净影响为零或正——该窗内原行为本就不安全。
- **测试**：新增确定性夹具（策略点单测必做；端到端可达时补，见 design D3/D2）；既有 `notLeaderRetriesSameEnvelopeSameOpSeq`、`malformedRejectEnvelopeRetriedNeverReadAsSuccess` 等脚本用例必须保持全绿。
