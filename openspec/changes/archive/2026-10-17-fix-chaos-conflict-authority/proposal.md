# Proposal

## Why

台账 **W21** 再次命中（第二次）：CI run `39012354774`（PR #47 头 `d511514`）的 verify 内层第一轮红在 `ClientChaosIT.randomKillRestartNeverBreaksMutualExclusionOrLeaks`——`[CHAOS] kills=16 restarts=14 grants=118 conflicts=1`，`expected: 0L but was: 1L`（`ClientChaosIT.java:267`）；内层重试第二轮绿（`kills=15 restarts=13 grants=106 conflicts=0`）。首次命中为 2026-10-15（`kills=17 restarts=15 grants=124 conflicts=1`）。两次形态一致：单次命中、与杀/授予计数成比例、非连续多轮。

W21 行已写明假阳性机理：**判定面是客户端注册表**。`holders.put(key, tid)` 返回的 `prev` 只证明"另一线程曾登记持有"，而混沌杀节点会让锁通过**租约到期/会话清理合法易手**；原持有者的 `finally` 摘除要等其 release 尝试收场（最长 3s），其间新持有者登记即打亮计数器。**合法路径即可打亮安全面断言** ⇒ 它测的是"客户端簿记与混沌时序的竞态"，不是互斥性。

按 W21 行动列：再现 → 另立 change 把判据换成**服务端权威**核对，并同时证伪上述假阳性机理；**MUST NOT 删除或放宽为 `≤1`**（安全面只能加强）。

## What Changes

- **冲突判定改由服务端权威读数裁决**：客户端注册表命中只作**触发**。命中时核对本次授予在 Leader 影子表上的读数——`heldRef(key)` 的 `leaseToken` 与本次 `LockGrant` 的租约凭证一致，且 holders 集合出现 **≥2 个不同 `(session, thread)`** 才计 `conflicts`；否则计入 `handoffs`（合法易手，仅观测）。
- **周期全节点采样**：混沌每个扰动步对全部存活节点的 `heldEntries()` 取样，任何测试 key 出现 ≥2 holders 即计一次 `conflicts`（覆盖"双授在两次授予核对之间"的形态）。
- **授予可核验性**：每次成功授予须在 3s 内于至少一个存活节点上找到匹配 `leaseToken` 的读数；找不到则断言失败（`unverified` 必须为 0）。
- **最终断言不变**：`conflicts == 0`（现在是服务端权威面）；`handoffs`、`unverified` 仅进 `[CHAOS]` 汇总行。
- 新增**确定性反向夹具**：停掉持有者所在节点 → 对端再授予，证明旧口径必记 conflict、新口径记 handoff。

## Capabilities

**无规格面**：纯测试判据改造，产品互斥语义与契约不变（`skip_specs: true`）。

### New Capabilities

- （无）

## Impact

- 仅 `openlatch-client/src/test/java/io/github/lamspace/openlatch/client/ClientChaosIT.java`（必要时加一个测试内辅助类型）。
- 无 `src/main`、无协议/规格面；常规档窗口预算不变。
