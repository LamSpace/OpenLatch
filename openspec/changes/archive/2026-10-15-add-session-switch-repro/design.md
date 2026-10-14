# Design

## Context

动机与证据状态见 `proposal.md`。塑造方案的现状与约束：

- **已核事实**：`RemoteCondition` 与 `RemoteTimer` 全族 `SESSION_EXPIRED` 命中 0 次——即**没有**"会话换代即放弃"的守卫；两者的重发环共享形态：`if (env == null || envSid != route.session().sessionId()) { 重建信封 }`，会话一变即**在新会话上重建并续发**。
- **对照**：队列/原子/相位器/闩/屏障/订阅均有显式换代守卫（`throw SESSION_EXPIRED`），且 `fix-queue-noeffect-monotonic` 已把队列的续发许可收紧为**单调不确定闩**（`SessionContinuation`）。
- **契约措辞**：在册 W11 行 v11 注记写"变异超时跨会话放弃（契约，`isArmed`/`isFired` 读数可核对是否已换代）"。
- **可用的构造手段**：`ScriptedServer`（`setHandler`/`push`/`countType`）+ 经 ACQUIRE 路径建车道（`NOT_LEADER` + 地址提示）＝`RemoteBlockingQueueScriptedTest#uncertainWindowIsNotClearedByLaterZeroEffectReject` 已验证过的"确定性换会话"构造；本 change 复用它。
- **约束**：`src/main` 零触碰；红态不得使默认构建变红。

## Goals / Non-Goals

**Goals:**

- 用一条确定性夹具回答唯一的问题：**条件/定时器变异请求在会话换代时是否会静默续发（并因此重复生效）**。
- 让该结论成为可复跑的证据（而非一次性观测），并据此决定"修"还是"回写措辞"。

**Non-Goals:**

- 不修（修复另立 change；本 change 只给判据）。
- 不裁决"应该怎样"（那是后续 change 的设计问题）。
- 不改 `fix-lane-route-refresh` 已落地的路由取用纪律。

## Decisions

### D1 先问"会不会"，不问"该不该"

夹具只断言**可观测事实**：a) 会话换代后在途变异请求是否在新会话上出现（`countType` 证据）；b) 服务端按 `(会话, op_seq)` 记账时，该次续发是否构成**第二次执行**（脚本化服务端模拟去重槽：同会话同序号→重放存储应答，新会话→执行）。不断言任何"应当"。

### D2 两个族各一条夹具，共用同一构造

条件（`signal`）与定时器（`SCHEDULE`/`DISARM`）各造一例——它们的**语义后果不同**（signal 是事件且无去重槽、timer 变异有 op_seq），若只测一族就下结论，等于用一个族的形态替另一个族说话。

### D3 门控复用 `w11-repro`

复用既有 `@Tag("w11-repro")` + `-Pw11-repro`（同为"无修复时应当红"的夹具）；不新开门控机制（判例：`fix-lane-route-refresh` 的 D3，同一机制不两处配置）。

### D4 结论的两条出路都要落盘

- **红**（续发成立）→ 另立修复 change，并把"W11 行契约措辞与实现不符"回写 W11 行；
- **绿**（续发不成立）→ 把"实测不可达 + 实现事实"记为证据，回写 W11 行措辞（说明该契约句在实现上由何保证）。

两条出路都**不**在本 change 内动 `src/main`。

## Risks / Trade-offs

- [构造不出换会话时序] → 复用已验证的构造（`-Pw11-repro` 夹具 B 的同类时序）；仍造不出则按 D4 的第二条出路记录，不硬凑。
- [把"无守卫"误读为"必然重复生效"] → D1 只断言计数事实；"重复生效"以脚本化服务端的记账语义**显式化**，不靠推理。
- [结论被误当契约裁决] → D1/D4 明确：判据与措辞回写分离。

## Migration Plan

纯测试变更：无协议/配置/状态迁移；回滚＝删除夹具类。默认构建行为不变（`w11-repro` 默认排除）。
