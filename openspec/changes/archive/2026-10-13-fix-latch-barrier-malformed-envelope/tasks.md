# Tasks

## 1. 红先：确定性复现

- [x] 1.1 扩展 `RemoteCountDownLatchScriptedTest`：`LATCH_AWAIT` 应答**缺码形**（`type` 回显而载荷 oneof 缺 `latch_await_response`）；断言 `await(timeout)` **不返回 `true`（不假绿）**。验证=未改实现前该用例**红**，失败原因是"返回了 `true`"而非其他。
- [x] 1.2 同夹具覆盖 `countDown`：`LATCH_COUNT_DOWN` 应答缺码形；断言**不静默返回 `remaining=0`**、以显式失败收场，且 `server.countType(LATCH_COUNT_DOWN)` **不因本次失败增长**（不重发）。验证=未改实现前**红**。
- [x] 1.3 新增 `RemoteBarrierScriptedTest`：`BARRIER_AWAIT` 应答缺码形；断言 `await(timeout)` 不假绿返回 `true`、`generation`/`parties` 不读默认值。验证=未改实现前**红**。
- [x] 1.4 同上覆盖执行者收尾路径：`BARRIER_ACTION_DONE` 缺码形断言按世代幂等重发（`countType > 1`）、不静默了结。验证=未改实现前**红**。
- [x] 1.5 同上覆盖 `BARRIER_LEAVE` 两路径：`breakBarrier()` 缺码形断言抛显式异常（非静默空成功）；超时/中断离场确认缺码形断言按"不确定"（`false`）收场、`isBroken()` 不虚报。验证=未改实现前**红**。

## 2. 实现：presence 判定（按 design D1 分流）

- [x] 2.1 `RemoteCountDownLatch.doAwait`：读 `status` 前增 `!resp.hasLatchAwaitResponse()` 判定 → 同会话同 `request_id` 重发（并入既有等待闭环）。验证=1.1 由红转绿；`ClientLatchIT` 全绿。
- [x] 2.2 `RemoteCountDownLatch.countDownOp`：增判定 → 抛 `OpenLatchTimeoutException`（"结果不确定、未重发"，D2），MUST NOT 重发。验证=1.2 由红转绿。
- [x] 2.3 `RemoteBarrier.doAwait`：增判定 → **同会话**同 `request_id` 重发，会话已换走既有"到场不跨会话重放"裁决（D3 守卫）。验证=1.3 由红转绿；`ClientBarrierIT`/`ClientBarrierLoopIT` 与既有场景"换会话不自动重放到场"全绿。
- [x] 2.4 `RemoteBarrier.barrierActionDone`：增判定 → 沿用既有 `(会话,世代)` 幂等重发循环（**补预算闸**：重发路径不得依赖发送超时兜底，服务端秒回异型应答时须按预算显式收场）。验证=1.4 由红转绿。
- [x] 2.5 `RemoteBarrier.breakBarrier`（抛显式异常）与 `leaveAndAwait`（返回 `false`）：增判定，MUST NOT 读默认实例作确认（D4）。验证=1.5 由红转绿。
- [x] 2.6 方法级 Javadoc 同步（D1 分流口径与"不得读默认实例"），保持只读注释即可懂契约。

## 3. 契约与文档

- [x] 3.1 接口级 Javadoc：`OCountDownLatch` 与 `OBarrier` 声明缺码形处置（等待路径重发、至多一次路径不重发显式失败）。验证=`mvn -s /home/lam/repo/settings.xml -pl openlatch-client javadoc:javadoc` 通过（`show=private`）；等价门禁 `verify-javadoc`（`javadoc:jar`）在全仓 `clean verify` 内逐模块通过。
- [x] 3.2 双语排障指南：`docs/guide/{zh,en}/09-troubleshooting.md` 增一条排查项——"latch/barrier `await` 疑似假绿返回（服务端未放行却返回 `true`）"→ 该族现按缺码形重发、不再假绿；并给出复核手段（对照服务端读数/会话日志）。验证=中英双侧对应段落均已更新、语义一致。

## 4. 集成回归

- [x] 4.1 客户端全量套件（单测 + IT）复跑。验证=零失败、无 flaky。
- [x] 4.2 跨模块收口：全仓 `clean verify` 与引用门禁自查。验证=`mvn -s /home/lam/repo/settings.xml clean verify` 通过，`bash scripts/check-source-citations.sh` 零命中。
