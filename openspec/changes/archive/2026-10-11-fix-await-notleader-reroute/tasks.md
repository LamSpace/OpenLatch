# Tasks

## 1. 红先：确定性复现夹具

- [x] 1.1 闩侧：新增脚本化夹具——home 对 `LATCH_AWAIT` 回 `NOT_LEADER`，另一节点为当值 Leader 并回 `OK`，集群视图在核对周期内报出该 Leader；断言 `await(10, SECONDS)` 返回 `true`。验证=未改实现前该用例**红**（以 `NOT_LEADER` 异常收场、且未返回 `true`）。
- [x] 1.2 屏障侧：同型夹具（`BARRIER_AWAIT` 回 `NOT_LEADER`，真主回 `OK`）；断言 `await(10, SECONDS)` 正常返回。验证=未改实现前**红**。
- [x] 1.3 有界失败侧：夹具——home 持续回 `NOT_LEADER` 且集群视图始终无当值 Leader；断言 `await` 以 `NOT_LEADER` 显式抛出、**永不返回 `true`**，且耗时落在 `[重道预算, 5s)` 区间。验证=未改实现前**红**（当前立即抛出，耗时下界断言失败）。

## 2. 实现：零生效重道

- [x] 2.1 在 `OpenLatchClient` 暴露重道预算常量（= 2 × 核对周期，包私有）并补齐 Javadoc（为何是 2 个周期、与周期耦合的意图）。验证=`javadoc:javadoc`（`show=private`）通过，且两原语引用同一常量。
- [x] 2.2 `RemoteCountDownLatch.doAwait` 接入重道：`NOT_LEADER` 时摘登记 → 重取路由 → 仅当会话变化才以新会话重建信封重发 → 预算耗尽或路由始终未变则抛 `NOT_LEADER`；同步更新方法级 Javadoc。验证=1.1、1.3 由红转绿；`RemoteCountDownLatchScriptedTest` 全绿（含既有拒绝用例，其断言不变）。
- [x] 2.3 `RemoteBarrier.doAwait` 同款接入（含执行者形态下不发生重道的边界：重道只作用于状态分派的终态分支）；同步更新方法级与类级 Javadoc，并显式写明与「到场不跨会话重放」纪律的边界。验证=1.2 由红转绿；`ClientBarrierIT` 全绿。
- [x] 2.4 既有用例口径同步：`sameShapeNotLeaderRejectOnAwaitThrowsNotFalseGreen` 的断言**不动**，仅把"不悬挂至预算耗尽"的表述补上"有界重道窗（2s）"这一新事实；核对两条断言在新行为下仍成立（显式异常 + 耗时 < 5s）。验证=该用例仍绿且注释与现实一致。

## 3. 契约与文档

- [x] 3.1 客户端契约 Javadoc 同步：`OCountDownLatch` 与 `OBarrier` 接口面对 `NOT_LEADER` 的处置显式声明（零生效重道、预算上界、不假绿）。验证=`mvn -s /home/lam/repo/settings.xml -pl openlatch-client javadoc:javadoc` 通过。
- [x] 3.2 双语用户指南同步：`docs/guide/{zh,en}/09-troubleshooting.md` 故障转移基线节声明等待类 `NOT_LEADER` 改为有界重道及其上界（与上一轮"改道收敛"条目并列）。验证=中英双侧对应段落均已更新、语义一致。

## 4. 集成回归

- [x] 4.1 客户端全量套件（单测 + IT）复跑。验证=零失败、无 flaky；既有换主窗安全性质锚不回退（MUST NOT 伪成功）。
- [x] 4.2 跨模块收口：全仓 `clean verify` 与引用门禁自查。验证=`mvn -s /home/lam/repo/settings.xml clean verify` 通过，`bash scripts/check-source-citations.sh` 零命中。
