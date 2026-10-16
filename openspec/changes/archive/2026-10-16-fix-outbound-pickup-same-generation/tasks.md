# Tasks

## 1. 红先：确定性夹具

- [x] 1.1 脚本化连接夹具：`ScriptedServer`（TCP 接受、`HELLO` 不应答）下启动客户端，等 `HELLO` 到达桩（证明 TCP 已连通、会话未建立），从 `HELLO` 之后清账发出业务请求，断言**服务端入站零出现该业务请求**且本端以 `ServerUnavailableException` 收场。验证=用例在修复前为**红**。→ 实测红：`[业务请求不得写出到线路] expected: 0 but was: 1`（请求确已写到桩侧）
- [x] 1.2 同夹具内锁定**豁免**：断言同一窗口内 `HELLO` 已正常发出（`countType(HELLO) ≥ 1`）。验证=允许"掐死后连握手一起掐死"的过修复被此断言挡住。→ 夹具内以"窗口前提断言"落地（`HELLO ≥ 1` 且 `state == HELLO_SENT`、`session() == null`）；修复前后皆为绿
- [x] 1.3 取口期间换代夹具：内存通道 + **脚本化会话供应者**（首次取到 S1、回读时给出 S2），断言业务请求零写出且以 `ServerUnavailableException` 收场。验证=修复前为**红**。→ 实测红：`[不得写出到换代后的通道] expected: null but was: type: LOCK_ACQUIRE … request_id: 1`
- [x] 1.4 红先留档：三条用例各自的首轮红日志落盘（本会话 job 临时目录）。验证=日志文件存在且含失败断言原文。→ 2 轮同签名：`$CLAUDE_JOB_DIR/tmp/w22-red-round{1,2}.log`（`Tests run: 3, Failures: 2`）
- [x] 1.5 反向锁（过修复防线）：会话稳定且通道可用时业务请求**必须**照常写出。验证=该用例修复前后皆绿。→ `businessRequestIsWrittenWhenSessionIsStable`，首轮即绿

## 2. 实现：取口同代与握手豁免口

- [x] 2.1 `RequestMultiplexer.sendWithId`：准入改为"取会话 → 取通道 → 回读会话比身份"，三者任一不成立即以瞬态 `ServerUnavailableException` 失败且**不写出**。验证=1.1/1.3 由红转绿。→ 由红转绿；`send()` 改为显式把分配 requestId 的会话作为同代基准传入（消除"两次各自读"的空档）
- [x] 2.2 `RequestMultiplexer.sendHandshake(envelope, channel, timeout)`：以给定通道为出口、不查会话；登记/超时/观测口径与 `sendWithId` 同表（共用私有分发方法 `dispatch`）。验证=1.2 保持绿。→ 两条发送路径收口到 `dispatch`，`sendHandshake` 为唯一握手入口（路径级豁免，非旗标）
- [x] 2.3 `ConnectionManager.sendHello` 与 `SeedDiscovery` 探针 `HELLO` 改走 `sendHandshake`。验证=握手与发现类既有用例不因豁免改口变红。→ `ClientHandshakeTest`、`LeaderDiscoveryTest` 绿；`ClientClusterIT`/`ClientTlsAuthIT` 等含握手与重连的 IT 在客户端模块 verify 中全绿
- [x] 2.4 `ConnectionManager.activeChannel()` 按 Javadoc 只在 `ACTIVE` 交出通道。验证=1.1 保持绿且既有用例无回归。→ 依约实现（`stateLock` 内联读状态+通道）；类级线程模型 Javadoc 与字段注释同步
- [x] 2.5 既有单测口径同步：`ClientMetricsTest` 中四枚"活通道 + 空会话"夹具补上会话（这是本 change 判负的形态，不再是可写出形态），用例语义（状态/超时/交叠的指标标签）不变。验证=该四枚恢复绿且断言未改。→ 首轮全反应堆红 3 枚（`responseStatusNameBecomesLabel`/`timeoutCountsTimeoutLabel`/`supersededOldFutureCountsFailed`），补会话后全绿（断言未动）

## 3. 契约与文档

- [x] 3.1 `RequestMultiplexer`/`ConnectionManager` 的相关 Javadoc 同步（取口判据、判负语义、握手豁免路径级的理由）。验证=`bash scripts/check-source-citations.sh` 零命中且注释只读自洽。→ 门禁退出 0（含 `--selftest`）；`RequestMultiplexer` 类级不变量补一条、`sendWithId`/`sendHandshake`/`dispatch`/`activeChannel` 逐条同步
- [x] 3.2 用户文档对账：`docs/guide/{zh,en}` 涉及"连接不可用/快速失败"的表述是否需改。验证=结论落本行。→ **无需改**：现有表述即"`ServerUnavailableException` ｜连接不可用（含切换窗口快速失败）｜重试"（`09-troubleshooting.md`、`05-cluster-deployment.md`），本 change 使该表述更成立
- [x] 3.3 `WATCHLIST.md` W22 行更新：现状收窄为"取口自身已同代 / 无会话零写出"，残留（跨代 `LatchRoute` 快照）显式登记，动作列与记录位置同步；W19 行动作列第 (1) 步销账。验证=行内容与实测判据一致、表完整性复检。→ 19 行、每行 6 竖线、id 单调无重（W19/W22 两行联动改动后复检通过）

## 4. 收口

- [x] 4.1 主判据：夹具三枚全绿，且**连续 ≥3 轮**不 flaky。验证=各轮报告。→ 修后 3 轮全绿（0.422 / 0.431 / 0.420 s，`Tests run: 3, Failures: 0`）
- [x] 4.2 `mvn -s /home/lam/repo/settings.xml clean verify` 通过（全反应堆 8/8，含 `verify-javadoc`）。验证=构建输出 0 失败 0 错误。→ `BUILD SUCCESS`：客户端单测 148 枚 + IT 全绿（含 `ClientClusterIT` 9/9 与本次夹具 3/3）；另单跑 `-pl openlatch-client clean verify` 亦 `BUILD SUCCESS`（单测 148 / IT 102，1 跳过）
- [x] 4.3 门禁：`bash scripts/check-source-citations.sh` 与 `--selftest` 退出 0。验证=命令退出码。→ 两者皆 0
- [x] 4.4 归档 change（目录移入 `openspec/changes/archive/2026-10-16-fix-outbound-pickup-same-generation/`）并按 fix/docs 拆分提交。验证=`git log`/`git status`（另：CI 两检查绿与合并由 PR 侧回读，结论不入本文件）。

## 5. 全反应堆验证中的观察（不计入判据，如实登记）

- [x] 5.1 全反应堆 `clean verify` 首轮（`w22-verify-full2.log`）：客户端模块 1 枚 `ClientClusterIT#committedAtomicValueSurvivesLeaderKillWithoutDoubleApply:247 » SESSION_EXPIRED; effect indeterminate`（`Errors: 1`）——即台账 **W19 家族签名**，且失败点在换主**之前**的首个 `addAndGet`（启动窗）；客户端模块单独 verify 时该用例 9/9 绿。**复跑（`w22-verify-full3.log`）BUILD SUCCESS、该用例绿** ⇒ 三次相关运行 1 红 2 绿，与台账 W19 行记的"对负载敏感、隔离跑常复现不出"同型。**判定归属待 W19 处置第 (2) 步**（同负载对照）；本 change 无意/无机制新增该签名（本 change 只减少"无会话仍写出"，不产生服务端 `SESSION_EXPIRED` 应答）。
