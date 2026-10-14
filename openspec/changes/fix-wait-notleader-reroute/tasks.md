# Tasks

## 1. 红先：确定性夹具

- [ ] 1.1 闩夹具：构造"路由陈旧但真主存在"（脚本化服务面：home 以 `NOT_LEADER` 应答 await、获取路径指向真主），断言**当前**实现下 `await` 以 `NOT_LEADER` 失败（红）。验证=用例红且耗时短（立即失败，非超时）。
- [ ] 1.2 屏障夹具：同口径（`RemoteBarrier.doAwait` 的 `NOT_LEADER` 状态分支）。验证=同 1.1。
- [ ] 1.3 两侧锁定：注入"永久 `NOT_LEADER`"（无真主可达）形态，断言失败**有界**（≤ 短预算 + 容差）且异常携带 `NOT_LEADER`。验证=该用例在修复后为绿、修复前亦为绿（防"过修复"成悬挂）。

## 2. 实现：零生效重道

- [ ] 2.1 `RemoteCountDownLatch.doAwait`：`NOT_LEADER` 状态分支改为"摘登记 → 触发共享刷新 → 短预算内轮询路由；会话确已变则以新会话重发续等；否则到点显式抛出"。验证=1.1 由红转绿、1.3 保持绿。
- [ ] 2.2 `RemoteBarrier.doAwait`：同款（并保持"到场不跨会话重放"只对不确定窗生效）。验证=1.2 由红转绿。
- [ ] 2.3 预算常量取 `OpenLatchClient` 既有刷新周期派生（不新造魔法数）。验证=常量引用可追溯（单一事实源）。

## 3. 契约与文档

- [ ] 3.1 等待环 Javadoc 同步：`NOT_LEADER` 应答状态的处置（零生效重道）、与不确定窗纪律的边界、短预算与异常的形态。验证=`bash scripts/check-source-citations.sh` 零命中且注释只读自洽。
- [ ] 3.2 既有 `sameShapeNotLeaderRejectOnAwaitThrowsNotFalseGreen` 仅同步口径注释（**断言不变**）。验证=该用例仍绿且断言未改。
- [ ] 3.3 双语文档对账：`docs/guide/{zh,en}` 涉及"await 不醒/`NOT_LEADER`"的排查段落同步该增强（含短预算与降级）。验证=中英双侧一致，无单侧更新。

## 4. 收口

- [ ] 4.1 主判据：`-Pw11-repro` 下 1.1/1.2 由红转绿，连续 ≥3 轮不 flaky。验证=各轮报告。
- [ ] 4.2 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆通过（0 失败 0 错误）；既有等待/闩/屏障用例全绿；默认构建零执行 `w11-repro` 夹具。验证=构建输出。
- [ ] 4.3 门禁：citation 零命中、links 零死链。验证=命令退出码。
- [ ] 4.4 `WATCHLIST.md` W11 行更新：把"等待面 `NOT_LEADER` 终态残余"一句按本 change 的落地结果收窄/销账。验证=行内容与实测判据一致。
