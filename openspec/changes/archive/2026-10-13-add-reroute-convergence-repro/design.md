# Design

## Context

动机见 `proposal.md`。以下只记录塑造方案的现状与约束。

- **夹具蓝本仍在历史里**：回退轮（`c65af3d`）删除的两枚确定性夹具可逐字取回——实现提交为 `46bd28b`，其中 `LeaderDiscoveryTest.java`（+156 行，脚本化提示滞后形态）与 `ClientClusterIT.java` 的 `residentNonAuthoritativeHomeConvergesQueueRequest`（+24 行，真集群 home 驻留形态）在归档 change `2026-10-10-fix-lane-reroute-convergence`（tasks 1.1/1.2、design D7）中有完整记载。故本 change 不是从零设计复现，而是**复活 + 补守卫 + 加门控**。
- **两枚蓝本的形态**（读原文所得）：
  - **A 启动窗提示滞后**：`ScriptedServer` 真 TCP 桩驱动完整 `OpenLatchClient`（含 `ConnectionManager` 与获取车道编排），不起真集群；握手返回无效 Leader 提示，随后 `CLUSTER_VIEW` 应答报出当值 Leader。修前该请求以显式超时收场。
  - **B home 驻留非权威节点**：`startCluster(3)` → home 直连当值 Leader 并正常受理（无车道）→ 该节点**存活让位**（连接与会话保留、已非权威）→ 窗内非 ACQUIRE 请求。窗口由"让位"**单向构造**，与概率锚（真杀主 + Leader 漂移）刻意不同：可重复。
- **门控先例**：本仓 `@Tag("drill")` + failsafe `<excludedGroups>drill</excludedGroups>` + `-Pdrill` 时以 `combine.self="override"` 置 `<groups>drill</groups>`；既有 drill 用例**全部**落在 `*IT` 类（failsafe），客户端模块 `pom.xml` 已有该 profile 与测试依赖（server test-jar、`ScriptedServer`）。
- **约束**：`src/main` 零触碰；默认 `clean verify` 必须保持全绿——红先夹具不得使任何门禁变红。

## Goals / Non-Goals

**Goals:**

- 把 W11 的两个根因窗（home 驻留非权威节点 / 启动窗提示滞后）固化为**默认可复跑、无修复必红**的确定性判据。
- 把回退轮的两枚失败形态之一（**静默错值**）写成常驻**守卫断言**。
- 让夹具的红/绿只由「收敛是否发生」决定，不依赖竞速、负载、真杀主。

**Non-Goals:**

- 不实现收敛（不改路由/改道/等待环/协议/`src/main`）。
- 不取代概率锚——`ClientClusterIT` 既有安全锚继续作安全网。
- 不要求夹具在单机形态可跑（夹具 B 需三节点集群，属可接受前提）。

## Decisions

### D1 范围：本 change 到「红」为止，修复另立

行使的是 W11 行写死的前置硬门槛。理由（回退轮的教训）：没有确定性判据的收敛改动，实测把安全性质改坏（`addAndGet` 返回 0 的静默错值）。先有可复跑的红，再谈修——否则"假绿严格劣于显式失败"这条回退判据无从落地。修复 change 的验收＝**本夹具翻绿**，且必须携带本条守卫。

### D2 红态落地：`@Tag("w11-repro")` + profile 门控

**备选与否决理由**：

| 备选 | 否决理由 |
|---|---|
| `@Disabled` 留证（W10 先例） | 红态靠改注解才跑得起来，极易腐化成静默死代码；与"判据"身份不符。 |
| characterization：断言现值＝超时 | 默认绿但把缺陷写成**期望值**，修复时须回头改断言；语义上把"应当收敛"降级为"就是会超时"，与安全网混读。 |
| 标签 + profile | **采纳**：红态永不腐坏（每次跑都是真红），取证随时可复跑；默认构建排除，门禁不受影响。 |

### D3 门控实现：复用 drill 判例，不新开机制

`@Tag` 全部落在 `*IT` 类，由 **failsafe** 管：默认 `excludedGroups` 追加 `w11-repro`，新增 profile `w11-repro` 以 `combine.self="override"` 置 `<groups>w11-repro</groups>`。不引入 surefire 侧的第二套门控（同一机制两处配置会漂移）。**本 change 对既有构建配置的唯一改动**＝把 `excludedGroups=drill` 改为 `drill,w11-repro`。

### D4 夹具取材：以历史蓝本复活，剥离内部依赖

两枚夹具从 `46bd28b` 取回为骨架，但**只断言外部可观测行为**（请求是否在预算内完成、是否出现静默错值），**不**断言"探测发生了 / 查询了几次 `CLUSTER_VIEW`"——后者把判据绑死在某一具体修复取径上，而修复路径本就允许另择（D1）。夹具 B 的"存活让位"窗口构造逐字保留（它正是可重复性的来源）。若蓝本断言的 API 已随回退变化，以**主干为准**重写断言，不以历史为准。

### D5 守卫断言：MUST NOT 伪成功

两枚夹具各追加一条断言：

- **静默错值判红**：任何被读作成功的应答，其值/状态不得来自 proto3 默认实例（原子 `addAndGet` 返回 0 即红）；
- **非静默终态不判红**：服务端 `SESSION_EXPIRED` 或客户端显式超时**是合法终态**。

理由：回退轮两枚失败形态中，显式失败本就合法，静默错值才是必须永久拦住的。夹具若把两者混判，会把合法终态误报为缺陷。

### D6 判据口径：不 flaky 才算复现成功

夹具 A 为确定构造（脚本化应答），预期恒红；夹具 B 的窗口单向构造，蓝本已声明"可连续复跑不 flaky"。验收要求两者在无修复下**连续多轮恒红**（达到本仓"真回归两侧都红"的判据强度）；达不到即视为复现未达成，回到本设计。

## Risks / Trade-offs

- [红先夹具被误纳入默认套件 → 门禁变红] → 门控三层：类级 `@Tag` + 默认 `excludedGroups` + 收口时复跑默认 `clean verify` 断言零失败。任务内含"默认构建仍绿"验证项。
- [夹具 B 吃端口/进程，与并行套件互撞] → 复用客户端既有 IT 集群夹具与共享 fork 端口纪律（W2 判例），不新增裸 `freePort()` 习作。
- [夹具断言过强，把合法终态判红] → D5 明确区分静默错值与非静默终态。
- [蓝本与现主干不兼容] → 复活时以主干 API 为准重写断言（D4）。
- [判据与修复取径耦合，限制后续方案空间] → D4 只断言外部行为，不断言内部机制。

## Migration Plan

纯测试与构建门控变更：无生产代码、无协议、无配置项、无数据迁移。默认构建行为不变；`-Pw11-repro` 为取证入口。回滚＝删除夹具类与 profile，无残留。

## Open Questions

- 修复 change 是否进一步要求"收敛轮次占比达标"（而非"单窗收敛即绿"）——属判据收紧，可延后，不改本 change 的夹具与任务分解。
