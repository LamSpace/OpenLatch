# Proposal

## Why

台账 W20：`RemotePhaserScriptedTest#awaitTimeoutCancelsThenThrowsTimeoutException` 在造载/整模块 surefire 口径下间歇红在 `cancelFrame.get()` 的 `Expecting actual not to be null`（历史记录造载 6/6 红；本机整模块复现一次，`elapsed 0.928s`）。判定＝**迟到（跨线程可见性竞态），不是未发出**：

- **契约面**：超时路径 `cancelQuietly` 是 fire-and-forget——`route.send(...)` 的 future 从不等待，`TimeoutException` 紧接着抛出。契约**不承诺**"服务端已观察到 CANCEL 帧"与"异常返回"的先后。
- **实测**：空载 / 16 / 32 燃烧进程，单测、类级、整模块共 20+ 轮，CANCEL 帧**每次都到达**，延迟 0–15ms；整模块唯一一次复现失败（0.928s）的同轮，同类打点变体测得 **7ms** 到达。
- **同文件先例**：`interruptDuringAwaitCancelsAndPreservesFlag` 对同一族断言早已用 **3s / 20ms 有界轮询**——同一契约面、同一夹具里"立即断言"是孤例。
- **静默不打路径不成立**：本用例会话语稳定、路由恒非空，`cancelQuietly` 的两条提前返回路径（`route == null`、`sendPinned` 判负）都不会命中。
- **保留**：失败轮本身未打点。若修复后仍出现"3s 窗内不到达"，则升级为"未发出"方向另案处理。

## What Changes

- `awaitTimeoutCancelsThenThrowsTimeoutException`：把"抛出后立即断言帧非空"换成**有界轮询等待最终必达**（3s 上限 / 20ms 间隔，复用同文件既有先例），随后原有三条断言（非空、`op == CANCEL`、`await_request_id > 0`）**原样保留**。
- 零产品代码改动、零规格面。

## Capabilities

**无规格面**：纯测试口径修正，产品行为契约不变（`skip_specs: true`）。

### New Capabilities

- （无）

### Modified Capabilities

- （无）

## Impact

- 仅 `openlatch-client/src/test/java/io/github/lamspace/openlatch/client/RemotePhaserScriptedTest.java`。
- 无 `src/main`、无协议/规格面；稳态时长不变（帧到达即返回，仅未到达时最多等 3s——这正是要保留的判负窗）。
