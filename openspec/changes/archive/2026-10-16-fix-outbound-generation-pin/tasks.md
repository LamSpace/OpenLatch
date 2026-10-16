# Tasks

## 1. 红先：端到端夹具（经陈旧路由出站）

- [x] 1.1 夹具：脚本化桩（`HELLO` 回 `OK` + 逐次递增的 `sessionId`、其余请求不应答）+ 真实 TCP + 真实重连——客户端连上取路由（快照会话 A）→ 关闭通道强制换代（等 `session().sessionId() != A`）→ 经**该陈旧路由**出站；断言**服务端侧零出现该请求**且本端以瞬态 `ServerUnavailableException` 收场。验证=修前**红**。→ 实测红 2/2 轮：`[陈旧路由出站不得落在新会话的连接上] expected: 0 but was: 1` ⇒ **该窗口由"潜在"升为"已证存在"**（请求确实落在新会话的连接上）
- [x] 1.2 反向锁：同代路由出站**必须**照常写出（陈旧/同代两侧都锁）。验证=该用例修复前后皆绿。→ `businessRequestIsWrittenWhenSessionIsStable`（内存通道）与 `businessRequestIsNotWrittenBeforeHandshakeCompletes` 的握手豁免断言分别在修前/修后皆绿
- [x] 1.3 红先留档：首轮红日志落盘（本会话 job 临时目录），含失败断言原文。→ `$CLAUDE_JOB_DIR/tmp/w22b-red-round{1,2}.log`（`Tests run: 4, Failures: 1`，同签名两轮）
- [x] 1.4 **"红先"的诚实说明**：断言（服务端零出现 + 瞬态失败）自始未变；夹具的**调用形态**随本 change 的 seam 迁移（修前 `route.mux().sendWithId(env, ms)`、修后 `route.send(env, ms)`），因为修法本身就是"取消裸出口"。记录于此以免读者误读为"改了断言"。

## 2. 实现：出站口绑定同代基准

- [x] 2.1 `RequestMultiplexer`：新增公开 `sendPinned(envelope, pinned, timeoutMs)`（pin 为空/通道不可用/基准已非当前会话 → 瞬态失败且不写出）；`sendWithId(env, ms)` 委派为"以当前会话为基准"的便捷形态；`send()` 改用 `sendPinned`。验证=1.1 由红转绿（2.2/2.3 完成后）。→ 三条路径全部收口，Javadoc 明写各形态语义与边界
- [x] 2.2 `LatchRoute`：由 `record` 改为**不暴露裸 mux** 的形态（`session()` + `send(envelope, timeoutMs)` = `mux.sendPinned(envelope, session, timeoutMs)`）。验证=迁移后编译通过且 `grep` 无路由裸出口。→ 两处构造点未受影响（同签构参）；`route.mux()` 在路由侧已不存在
- [x] 2.3 迁移 24 处 `route.mux().sendWithId(...)` → `route.send(...)`（8 个 `Remote*` 原语）。验证=仅余非路由用法。→ `RemoteAtomicBase` 2、`RemoteBarrier` 4、`RemoteBlockingQueue` 2、`RemoteCondition` 2、`RemoteCountDownLatch` 2、`RemotePhaser` 4、`RemoteTimer` 3、`RemoteTopic` 5 ＝ 24；`grep "\.mux()"` 仅余 `muxForSession` 的 `LaneRef.mux()`
- [x] 2.4 保留形态的边界 Javadoc：`sendWithId` 的"现取现发"语义与"持有快照 MUST 用带基准形式"；`AwaitTracker`/种子探针为何无须 pin（等待在会话变化时失败或迁移，不跨会话重发）。验证=只读注释即可懂边界。→ `sendWithId`/`sendPinned`/`LatchRoute` 三处 Javadoc 落定

## 3. 契约与文档

- [x] 3.1 契约 delta：`client-sdk`「断连快速失败与重连」加"信封与其落点 MUST 同代"一条与 1 枚场景（既有条款与 6 枚场景逐字保留）。验证=`openspec validate --strict` 通过、场景计数 7。→ 主规格同步后场景计数 7、`openspec validate --specs --strict` 通过
- [x] 3.2 门禁：`bash scripts/check-source-citations.sh` 与 `--selftest` 零命中。验证=退出码。→ 两者皆 0
- [x] 3.3 用户文档对账：`docs/guide/{zh,en}` 是否有与新失败形态相悖的表述。验证=结论落本行。→ **无需改**：失败形态仍落在既有"`ServerUnavailableException` ｜连接不可用 ｜重试"与"会话换代以显式失败收场"表述内（`09-troubleshooting.md`、`05-cluster-deployment.md`）

## 4. W19 第 (2) 步：同负载复测两枚严格活性锚

- [x] 4.1 造载口径：2×nproc 燃烧进程（nproc=8 ⇒ 16 个）+ 客户端模块 **failsafe-only** 跑 `ClientClusterIT`，与台账既载口径一致；单轮约 75–88 s。→ 脚本 `$CLAUDE_JOB_DIR/tmp/w19_measure.sh`（含收尾杀燃烧进程）
- [x] 4.2 ≥4 轮结果（成对）：**pin 侧 4 轮 2 红**——`committedReferencePayloadSurvivesLeaderKillWithoutDrift:272`，签名 `OpenLatchTimeout`（预算耗尽）；**master 侧 4 轮 4 红**——`…WithoutDrift:279`（`SESSION_EXPIRED`）与 `:272`（超时）各一，另 **W11 收敛契约锚** `residentNonAuthoritativeHomeConvergesNonAcquireRequests:371` 两例超时。→ 逐轮日志 `$CLAUDE_JOB_DIR/tmp/w19-{pin,master}-round{1..4}.log`
- [x] 4.3 结论落台账 W19 行：两锚在该口径下**仍红** ⇒ (3) 的取舍激活；pin **未使该面更差**（红数 2 vs 4、且不落在 W11 契约锚上；n=4+4 按信号记）；口径的**预算容忍度上限**＝仍能打破契约面锚（master 侧 2/4）⇒ 该口径不得单独作为产品活性证据。**(3) 待用户裁决，本 change 不预判**。→ 已写入 W19 行动作列
- [x] 4.4 收尾：燃烧进程全杀、机器空闲确认。→ 脚本 `trap` 收尾输出 `burners killed`；轮间与收尾后 `pgrep -c` 已核无残留

## 5. 收口

- [x] 5.1 夹具 3 轮全绿（不 flaky）。→ 全反应堆轮内 4/4 绿 + 单跑 2 轮 4/4 绿（`w22b-green-round{1,2,3}.log`）
- [x] 5.2 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆通过（含 `verify-javadoc`）。→ `BUILD SUCCESS`（8/8；客户端夹具 4/4、`ClientClusterIT` 9/9 绿）
- [x] 5.3 归档 change、按 fix/docs 拆分提交、推送开 PR；CI 两检查绿（PR 侧回读）。→ PR #43（run 38914002504：`verify` 14m17s 与 citation gate 均 pass，`attempt=1`、`mergeStateStatus=CLEAN`）
