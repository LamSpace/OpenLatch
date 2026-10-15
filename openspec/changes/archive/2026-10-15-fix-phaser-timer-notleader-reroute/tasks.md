# Tasks

## 1. 红先：确定性夹具

- [x] 1.1 相位器纯等待夹具：陈旧 home 以 `NOT_LEADER` 应答 `PHASER_OP_AWAIT_ADVANCE`、`CLUSTER_VIEW` 指向真主；断言**当前**实现下 `awaitAdvanceInterruptibly` 以 `NOT_LEADER` 立即失败（红）。验证=用例红且耗时短（立即失败，非超时）。→ 实测红：`awaitAdvance of phaser 'repro-wait-phaser' rejected: NOT_LEADER`，0.148s
- [x] 1.2 定时器纯等待夹具：同口径（`TIMER_OP_AWAIT` / `await(timeout)`）。验证=同 1.1。→ 实测红：`await of timer 'repro-wait-timer' rejected: NOT_LEADER`，0.129s
- [x] 1.3 两侧锁定：注入"永久 `NOT_LEADER`"（选举空窗，视图无 `is_leader`）形态，断言失败**有界**（远小于等待总预算）且异常携带 `NOT_LEADER`；相位器与定时器各一枚。验证=该两枚修复后为绿、修复前亦为绿（防"过修复"成悬挂）。→ 两枚（`permanentNotLeaderPhaserAwaitFailsBounded` / `...TimerAwaitFailsBounded`），红轮即绿（当轮 9 用例仅 2 红＝两条收敛夹具），断言 `<4s` 对 10s 等待预算

## 2. 实现：零生效重道（纯等待）

- [x] 2.1 `RemotePhaser.awaitAdvanceInterruptibly`：`NOT_LEADER` 状态分支改为"摘登记 → `client.awaitReroutedRoute(envSession, deadline)` → 会话确已变化则以新会话重发续等；否则到点显式抛出"。验证=1.1 由红转绿、1.3 保持绿。→ 由红转绿（`awaitAdvance(long)` 经委托同获覆盖）
- [x] 2.2 `RemoteTimer.awaitInterruptibly`：同款。验证=1.2 由红转绿。→ 已由红转绿（`await()`/`await(timeout)` 同获覆盖）
- [x] 2.3 不确定窗护栏：两族对"传输失败/读界超时/应答缺码形"置不确定位，置位后不再重道（沿闩/屏障同款边界）。验证=既有自愈用例（相位器"换主窗等待自愈"、定时器"重连重挂同 request_id"）仍全绿。→ 两族各三处置位；既有自愈用例随全量 verify 复跑（见 4.2）
- [x] 2.4 到场半程与变异/读数**零触碰**：`arriveAndAwaitAdvance`、`register`/`arrive`/`arriveAndDeregister`、`schedule`/`disarm`、`isFired`/`isArmed`/`getRemainingMillis` 的行为与注释均不变。验证=`git diff` 面只含两个 `await*` 方法 + 注释。→ `git diff` 复核：两个 `await*` 方法体内新增分支 + 三处置位 + 注释，其余面无改动

## 3. 契约与文档

- [x] 3.1 契约注释同步：两个等待环（类级 + 方法级）与公有 `OPhaser`/`OTimer` 接口声明该条款**及其边界**（到场半程/变异不在内、不确定窗护栏、短预算与异常形态）。验证=`bash scripts/check-source-citations.sh` 零命中且注释只读自洽。→ 两族类级各补一段、公有接口"增强面"各补一条（含边界）；citation 门禁零命中（含新注释）
- [x] 3.2 既有拒绝用例口径注释同步（**断言不变**）：相位器/定时器脚本用例中涉及 `NOT_LEADER` 的分支说明补一句"单机形态下刷新静默降级 ⇒ 仍在短预算内显式抛出"。验证=断言与耗时上界均未改且仍绿。→ **复核后无需改动**：两个脚本用例类中没有任何 `NOT_LEADER` 断言（相位器那枚显式拒绝断言是 `OVERLOADED` 的 `register` 变异面，与本条款不相交）；该形态的既有保障由新增的两枚"永久 NOT_LEADER 有界失败"夹具承担，断言未改

## 4. 收口

- [x] 4.1 主判据：四枚夹具全绿，收敛侧两枚**由红转绿**，连续 ≥3 轮不 flaky。验证=各轮报告。→ 修前 2 红（0.148s / 0.129s 立即抛 NOT_LEADER）；修后 3 轮 × 9 用例全绿，耗时 9.875 / 9.884 s（无 flaky）
- [x] 4.2 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆通过（0 失败 0 错误），既有相位器/定时器用例全绿。验证=构建输出。→ **BUILD SUCCESS**，8/8；客户端 143 单测 + 102 IT（0 失败 1 跳过）；`RemoteTimerScriptedTest` 5/5、`RemotePhaserScriptedTest` 7/7、`RerouteConvergenceReproIT` 9/9 全绿（本轮跑在"门控已退役"的树上，故判据集默认执行）
- [x] 4.3 门禁：citation 零命中、links 零死链。验证=命令退出码。→ 两脚本退出码均 0
- [x] 4.4 `WATCHLIST.md` W11 行：把"相位器/定时器的等待面仍未覆盖"收窄为**纯等待已覆盖**，并记明仍未覆盖的是到场半程与变异/读数面及其理由（须先立"未生效/已记账"判据）。验证=行内容与实测一致；表完整性复检。→ 行内改写为"纯等待已覆盖（2026-10-15）"+ 仍未覆盖面与理由；表复检 17 行、行 id 集合不变、每行 6 竖线
