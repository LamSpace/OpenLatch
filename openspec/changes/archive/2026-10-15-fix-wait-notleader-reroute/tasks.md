# Tasks

## 1. 红先：确定性夹具

- [x] 1.1 闩夹具：构造"路由陈旧但真主存在"（脚本化服务面：home 以 `NOT_LEADER` 应答 await、获取路径指向真主），断言**当前**实现下 `await` 以 `NOT_LEADER` 失败（红）。验证=用例红且耗时短（立即失败，非超时）。→ 实测红：`await of latch 'repro-wait-latch' rejected: NOT_LEADER`，0.113s
- [x] 1.2 屏障夹具：同口径（`RemoteBarrier.doAwait` 的 `NOT_LEADER` 状态分支）。验证=同 1.1。→ 实测红：`await of barrier 'repro-wait-barrier' rejected: NOT_LEADER`，0.413s
- [x] 1.3 两侧锁定：注入"永久 `NOT_LEADER`"（无真主可达）形态，断言失败**有界**（≤ 短预算 + 容差）且异常携带 `NOT_LEADER`。验证=该用例在修复后为绿、修复前亦为绿（防"过修复"成悬挂）。→ 闩/屏障各一（`permanentNotLeader*FailsBounded`），修复前亦绿（首轮 5 用例仅 2 红＝两条收敛夹具）

## 2. 实现：零生效重道

- [x] 2.1 `RemoteCountDownLatch.doAwait`：`NOT_LEADER` 状态分支改为"摘登记 → 触发共享刷新 → 短预算内轮询路由；会话确已变则以新会话重发续等；否则到点显式抛出"。验证=1.1 由红转绿、1.3 保持绿。→ 实测由红转绿（5/5 绿）
- [x] 2.2 `RemoteBarrier.doAwait`：同款（并保持"到场不跨会话重放"只对不确定窗生效）。验证=1.2 由红转绿。→ 已由红转绿；`uncertain` 位覆盖"传输失败/读界超时/应答缺码形"三类，曾入不确定窗者不重道
- [x] 2.3 预算常量取 `OpenLatchClient` 既有刷新周期派生（不新造魔法数）。验证=常量引用可追溯（单一事实源）。→ `OpenLatchClient#NOT_LEADER_REROUTE_BUDGET_MS = 2 × ROUTE_REFRESH_MIN_INTERVAL_MS`，两处等待环引用同一常量

## 3. 契约与文档

- [x] 3.1 等待环 Javadoc 同步：`NOT_LEADER` 应答状态的处置（零生效重道）、与不确定窗纪律的边界、短预算与异常的形态。验证=`bash scripts/check-source-citations.sh` 零命中且注释只读自洽。→ 两个等待环类级/方法级同步；公有契约面 `OCountDownLatch`/`OBarrier` 一并同步（契约变更，只改注释零改签名）
- [x] 3.2 既有 `sameShapeNotLeaderRejectOnAwaitThrowsNotFalseGreen` 仅同步口径注释（**断言不变**）。验证=该用例仍绿且断言未改。→ 仅加"口径同步"段（单机形态下刷新静默降级 ⇒ 仍在短预算内显式抛出），断言与上界未动
- [x] 3.3 双语文档对账：`docs/guide/{zh,en}` 涉及"await 不醒/`NOT_LEADER`"的排查段落同步该增强（含短预算与降级）。验证=中英双侧一致，无单侧更新。→ 两语 `09-troubleshooting.md` §3 第 6 项同款补段（有界重道 2s / 只在会话确已变化时重发 / 相位器与定时器不在内）

## 4. 收口

- [x] 4.1 主判据：`-Pw11-repro` 下 1.1/1.2 由红转绿，连续 ≥3 轮不 flaky。验证=各轮报告。→ 修前 2 红（0.113s/0.413s 立即抛）；修后 3 轮 × 5 用例全绿，耗时 5.206 / 5.169 / 5.222 s（无 flaky）
- [x] 4.2 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆通过（0 失败 0 错误）；既有等待/闩/屏障用例全绿；默认构建零执行 `w11-repro` 夹具。验证=构建输出。→ BUILD SUCCESS，反应堆 8/8；客户端 143 单测 + 89 IT（0 失败 1 跳过）；日志中 `RerouteConvergenceReproIT` 命中 0 次
- [x] 4.3 门禁：citation 零命中、links 零死链。验证=命令退出码。→ `check-source-citations.sh` 退出 0（零命中）、`check-links.sh` 退出 0（死链 0）
- [x] 4.4 `WATCHLIST.md` W11 行更新：把"等待面 `NOT_LEADER` 终态残余"一句按本 change 的落地结果收窄/销账。验证=行内容与实测判据一致。→ 该句改为"等待面残余已修（2026-10-15）"，并写明仍未覆盖的相位器/定时器等待面；表完整性复检（17 行、行 id 集合不变、每行 6 竖线）
