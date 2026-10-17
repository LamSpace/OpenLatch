# Design

## Context

见 `proposal.md`（动机与两次命中证据）。与本设计相关的现状与可用面：

- `ClientChaosIT.runChaos` 现有判定：两驱动线程立即式 `acquire` 共享 key；授予成功即 `holders.put(key, tid)`，`prev != null && prev != tid` 即 `conflicts++`；收尾断言 `conflicts == 0`。
- 服务端权威读数已在同一测试内可用：`n.server.cluster().core().shadow()`（`ShadowTable`）暴露
  - `heldRef(String key)` → `HeldRef(leaseToken, expiresAtMs, Set<Holder> holders, lockType)`；
  - `heldEntries()` → `Map<String, HeldRef>`；
  - `isHeldBy(sessionId, threadId, key)`；
  - `Holder(sessionId, threadId)`。
- `LockGrant` 携带本次授予的 `leaseToken`（现用于释放）。
- 服务端授予语义：key 忙时一律 QUEUED/DENIED，**授予成功即该 key 在应用点只有一个持有者**；应用到 grant 条目意味着此前同 key 的释放/清理条目都已应用（日志序）。
- 集群为 in-JVM 三节点；`leader()` 与摘要/位点读数已有先例；复制异步。

## Goals / Non-Goals

**Goals:**
- 把"互斥性"判定从客户端簿记迁到服务端权威面，消除混沌时序导致的假阳性。
- 保持并**加强**安全面：真双授在新的两路核对下仍必红（授予时点 + 周期采样）。
- 用确定性夹具**证伪**假阳性机理（合法易手被旧口径误记）。

**Non-Goals:**
- 不改产品代码、不改混沌驱动与杀/重启策略、不改常规档窗口预算与 soak 入口。
- 不把断言放宽为阈值或删除（`conflicts == 0` 不变）。

## Decisions

**D1：授予时点核对以 `leaseToken` 为真值锚。**
- 每次成功授予后，在全部存活节点上找 `heldRef(key)`，要求其 `leaseToken` 与本次 `LockGrant.leaseToken()` 一致；一致的节点即"已应用本次授予"的权威读数。
- 判定：该读数 holders 集合出现 **≥2 个不同 `Holder`** ⇒ `conflicts++`；恰为 `{本次持有者}` ⇒ 正常；找不到任何匹配 stamp 的读数 ⇒ `unverified`（见 D3）。
- **为什么 stamp 是必要的**：没有它就无法区分"该节点尚未应用本次授予（读数陈旧）"与"真实双授"。应用日志序保证"应用了 grant ⇒ 此前的释放/清理都已应用"，故匹配 stamp 的读数若仍见旧持有者，即为真双授。
- 备选（否决）：直接 `isHeldBy(oldSession, oldThread, key)`——需要把注册表升级为 `(session, thread)`，且同样有陈旧读问题，stamp 方案更直接。

**D2：客户端注册表命中只作触发，另加周期全节点采样。**
- 触发：`holders.put` 返回 `prev != null && prev != tid` 时才进入 D1 核对；核对判定为"仅新持有者"时记 `handoffs`（合法易手，仅观测）。
- 采样：混沌每个扰动步遍历全部存活节点的 `heldEntries()`，对测试 key 的任何 `holders().size() >= 2` 记 `conflicts++`。
- **为什么采样不会假阳性**：状态机确定性 + 应用序 ⇒ 任何节点（哪怕落后）都不会展示"双持有"这个从未出现过的状态；出现即真。
- **为什么两路都要**：授予时点核对是精确点；采样兜住"核对之间"的形态与被核对遗漏的授予。

**D3：授予可核验性（`unverified`）硬判为 0。**
- 每次成功授予须在 5s 内于至少一个存活节点找到匹配 stamp 的读数；找不到即断言失败。
- 依据：授予响应在 commit+apply 之后才返回；即使 Leader 随即死亡，新 Leader 上任后会推进提交/应用，5s 余量足够。
- 风险与对策：若实测出现慢节点重放导致的偶发不可核验，再评估放宽窗口（不得直接取消核验）。

**D4：确定性反向夹具（证伪假阳性机理）。**
- 场景：A 授予 key K（记入注册表 `tidA`）→ 杀掉 A 所连 Leader → 等服务端该 key 释放（`heldRef(K) == null`）→ B 授予 K。
- 断言：①服务端读数只有 B（`holders() == {B}` 且 stamp 匹配）；②**旧口径**（`prev = tidA != tidB`）必记 conflict（证明假阳性机理成立）；③**新口径**不给 K 记 conflict，记 handoff。
- 该夹具不依赖随机混沌，确定性成立。

## Risks / Trade-offs

- [采样成本] → 每步遍历存活节点的小型 `heldEntries()`，18s 窗口约百余步，可忽略。
- [`unverified == 0` 在极端时序下可能偏严] → 5s 窗口 + 先实测 20 轮；若出现，按 W21/新行登记并在"核验窗口"上收紧，而不是取消核验。
- [新判据更严格 ⇒ 真双授可能比旧口径更早红] → 这是加强；红即产品缺陷，按安全面缺陷处置，不得回退到客户端计数。

## Migration Plan

无迁移面：纯测试文件改动，随 PR 合并生效，回滚即还原 `ClientChaosIT`。
