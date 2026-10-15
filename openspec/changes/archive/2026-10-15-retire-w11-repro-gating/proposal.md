# Proposal

## Why

W11 的判据集（9 枚确定性夹具）此前以 `@Tag("w11-repro")` + `-Pw11-repro` profile 门控，**默认构建排除**。该门控在当时是必要的：夹具**无修复时恒红**，让它进默认构建等于让 CI 常红。

如今缺口的四个面都已收口（收敛本体 / 队列零生效逃逸 / 等待面 `NOT_LEADER` / 变异车道放弃核查），**9 枚夹具全绿**——门控的成立前提消失。留在排除集里的代价变得实在：**这些契约再没有任何回归网兜底**，日后谁把 `latchRoute` 的取用纪律改回去，默认 CI 一声不响。

夹具自己的契约声明早就写明了收口方式（"缺口修复后本类应由红转绿，**届时该标签与 profile 一并退役**"），本 change 就是兑现它。

## What Changes

- 摘除三处 `@Tag("w11-repro")`：`RerouteConvergenceReproIT`（类级，5 枚）、`SessionSwitchReproIT`（类级，3 枚）、`ClientClusterIT#residentNonAuthoritativeHomeConvergesNonAcquireRequests`（方法级，1 枚）。
- 退役 `openlatch-client/pom.xml` 的 `w11-repro` profile，并把 failsafe 的 `<excludedGroups>drill,w11-repro</excludedGroups>` 收窄为 `drill`。
- 同步三处注释：夹具不再是"红是预期／默认排除"，而是**默认构建内的契约回归网**；`RerouteConvergenceReproIT` 的类名与过时表述一并整理（名字保留——它是"改道收敛"的判据集，语义未变）。
- `WATCHLIST.md` W11 行：把"`@Tag("w11-repro")` + `-Pw11-repro` 门控（默认构建排除）"的表述改为**已转正**，并记明判据集规模。

## Capabilities

### New Capabilities

（无。）

### Modified Capabilities

（无——纯测试门控与构建配置变更，产品行为零改动，故 `.openspec.yaml` 标 `skip_specs: true`。）

## Impact

- **代码**：零 `src/main` 改动。
- **测试**：默认构建新增 9 枚用例（其中 8 枚为脚本化进程内夹具、1 枚为真集群让位夹具）；`-Pw11-repro` 取证命令退役。
- **构建**：`openlatch-client/pom.xml` 的 failsafe 分组排除与 profile 变更；`drill` 门控不受影响。
- **风险**：转正后这些夹具的运行时长与稳定性进入默认 CI 的关键路径——尤其真集群那枚（`transferLeadership`）。若它在默认负载下不稳，须回退该枚并另立观察项，**不得**为了让 CI 变绿而放宽其断言。
