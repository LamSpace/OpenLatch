# Tasks

## 1. 红先：确定性复现夹具

- [x] 1.1 相位器侧：夹具——home 对 `PHASER_OP(AWAIT_ADVANCE)` 回 `NOT_LEADER`，另一节点为当值 Leader 并回 `OK{相位}`，集群视图在核对周期内报出该 Leader；断言 `awaitAdvanceInterruptibly` 在预算内以该相位了结。验证=未改实现前**红**（以 `NOT_LEADER` 异常收场）。
- [x] 1.2 定时器侧：同型夹具（`TIMER_OP(AWAIT)` 回 `NOT_LEADER`，真主回 `OK`）；断言 `await(timeout)` 返回 `true`。验证=未改实现前**红**。
- [x] 1.3 有界失败侧：夹具——home 持续回 `NOT_LEADER` 且集群视图始终无当值 Leader；断言两族等待均以 `NOT_LEADER` 显式抛出、不伪造读数、耗时落在 `[重道预算, 5s)` 区间。验证=未改实现前**红**（当前立即抛出，耗时下界断言失败）。

## 2. 实现：复用 #22 的共享机械

- [x] 2.1 `RemotePhaser.awaitAdvanceInterruptibly` 接入重道：状态分派的终态分支前增 `NOT_LEADER` 分支（摘登记 → `client.awaitReroutedRoute(envSession, deadline)` → 未改道则抛 `NOT_LEADER`，改道则 `route = rerouted` 续环）；同步方法级 Javadoc。验证=1.1、1.3 由红转绿；既有 `RemotePhaserScriptedTest` 与 `ClientPhaserIT` 全绿。
- [x] 2.2 `RemoteTimer.awaitInterruptibly` 同款接入；方法级 Javadoc 同步，并显式写明"跨会话重道必然换 `request_id`，与本环同会话保活护栏不冲突"。验证=1.2 由红转绿；既有 `RemoteTimerScriptedTest` 与 `ClientTimerIT` 全绿。
- [x] 2.3 保活护栏与跨会话重建块**零改动**复核：确认 `QUEUED` 分支仍以原 `request_id` 重发、重建块仍按新会话分配新 `request_id`。验证=`ClientTimerIT`/`ClientPhaserIT` 与 v10 判例用例全绿、无"重挂即重装载/假象双换代"回归。

## 3. 契约与文档

- [x] 3.1 接口级 Javadoc 同步：`OPhaser` 与 `OTimer` 的等待面对 `NOT_LEADER` 的处置显式声明（零生效重道、预算上界、不伪造读数、与保活护栏的边界）。验证=`mvn -s /home/lam/repo/settings.xml -pl openlatch-client javadoc:javadoc` 通过。
- [x] 3.2 双语用户指南同步：`docs/guide/{zh,en}/09-troubleshooting.md` 的等待类条目由"相位器/定时器仍为终态"改为"等待族一致（闩/屏障/相位器/定时器均为零生效有界重道）"。验证=中英双侧对应段落均已更新、语义一致。

## 4. 集成回归

- [x] 4.1 客户端全量套件（单测 + IT）复跑。验证=零失败、无 flaky；既有换主窗安全性质锚不回退。
- [x] 4.2 跨模块收口：全仓 `clean verify` 与引用门禁自查。验证=`mvn -s /home/lam/repo/settings.xml clean verify` 通过，`bash scripts/check-source-citations.sh` 零命中。
