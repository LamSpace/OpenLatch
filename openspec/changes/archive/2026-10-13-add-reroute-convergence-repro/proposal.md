# Proposal

## Why

`WATCHLIST` W11（换主窗非 ACQUIRE 车道改道收敛缺口）是在册的**已知存活缺陷**：非 ACQUIRE 车道（队列/原子/屏障/闩/订阅发布/相位器/定时器）经同一路由入口 `latchRoute()` 取 `(会话, 出站)` 对，而改道决策**只挂在握手回调**上——home 会话连通期间没有任何时刻重评估 Leader 身份。于是两个窗内车道永不重建：

- **home 驻留非权威节点**：Leader 已迁他处、home 仍连通（无断连即无新提示）；
- **启动窗提示滞后**：seed 节点的 `LeaderTracker` 尚未收到当选事件，HELLO 提示为 `-1`。

两窗内 `NOT_LEADER`（无提示字段）被重发环消化至预算耗尽——**换主前的 `put` 也会 churn 满等待预算显式超时**。

2026-10-11 曾以「home 驻留期周期核对」修复过并**整体回退**：该核对于启动窗内改道，与在途写换会话相撞，实测令 `ClientClusterIT#committedAtomicValueSurvivesLeaderKillWithoutDoubleApply` 出现 `addAndGet` **返回 0**（proto3 默认实例读作 OK——W10「伪成功」同族，**静默错值**）与服务端 `SESSION_EXPIRED`。对照数据：改动前 0/8、引入后 2/6（CI 两次尝试同签名）、停用核对 0/10。回退裁决据此立下**前置硬门槛：必须先造出确定性复现再红先**。

本 change **只兑现该门槛**——把缺口变成可红先、可复跑的确定性判据，**不含任何修复**。

## What Changes

- 新增**两枚确定性红先夹具**，opt-in 门控（`@Tag("w11-repro")`，默认构建排除，`-Pw11-repro` 纳入）：
  1. **启动窗提示滞后**（脚本化服务端，无真集群）：握手返回无效 Leader 提示，随后对该会话的 `CLUSTER_VIEW` 查询报出当值 Leader——无修复时非获取请求以**显式超时**收场，有修复时在预算内收敛。
  2. **home 驻留非权威节点**（真集群）：客户端 home 直连当值 Leader 并正常受理，随后令该节点**存活让位**（连接与会话保留、已非权威），把 Leader 迁至第三节点——窗内队列请求在无修复时耗尽预算显式超时。
- 两枚夹具同时钉住**安全性质守卫**：换主窗 **MUST NOT 伪成功**——`addAndGet` 不得返回 0、任何请求不得被读作 OK-空；**显式失败是合法终态，静默错值不是**。这是回退轮留下最大教训的固化。
- 新增测试门控设施：`w11-repro` 标签 + failsafe 默认排除 + profile 纳入，使"红着"的夹具常驻而不使默认 `verify` 变红。
- **非目标（明确不做）**：不含修复本体（不新增周期核对、不改路由/改道/等待环语义、不改 wire 协议、**`src/main` 零触碰**）；不把概率锚（真杀主 + Leader 漂移）升为判据（它不可控，仍只作安全网）；不改安全性质与既有等待预算语义。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

（无——本次是**纯测试与构建门控**变更，行为零变更：既有的「换主窗 MUST NOT 伪成功」安全性质与等待预算语义均不改写，不新增也不收窄任何规格条款。故按 schema 规约在 `.openspec.yaml` 置 `skip_specs: true`，不制造规格 delta。）

## Impact

- **代码**：`openlatch-client/src/test`（新增夹具测试类）与 `openlatch-client/pom.xml`（新增 profile、failsafe `excludedGroups` 追加 `w11-repro`）。`src/main` 与其 javadoc 零触碰，双语指南与主规格零触碰。
- **构建/CI**：默认 `clean verify` 行为不变（新夹具默认排除，Skipped 计数不变）；门禁（citation gate、防假绿断言）沿用既有。
- **运行环境**：夹具 B 需进程内三节点集群（复用客户端模块既有 IT 基础设施，如 `ClientClusterIT` 的集群夹具）；夹具 A 仅需本地 TCP 桩。两者均无需外部依赖或算力前提。
- **后续**：修复 change 的验收判据即本夹具**由红转绿**，且必须携带本条守卫断言——若修复再次引入静默错值，守卫立即红。
