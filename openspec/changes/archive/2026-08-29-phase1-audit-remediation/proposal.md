# Proposal: phase1-audit-remediation

## Why

Phase 1（M1–M4）已按《OpenLatch-Phase1-详细设计说明书》v1.1 全量交付，但对详设逐条核对发现三类残留：5 个实现缺陷（正确性/资源泄漏级）、约 10 处测试缺口、约 14 处详设正文滞后于 M3/M4 openspec 定案。缺陷中两项（未知 MessageType 挂连接、multiplexer 同 id 覆盖竞态）会在真实故障/前向兼容场景下造成客户端挂等，必须在进入 Phase 2 前闭环；文档漂移会误导 Phase 2/3 设计者。

## What Changes

- **server：未知 MessageType 数值兜底**（A1）——`Envelope.type` 为协议未定义数值时，回 `INVALID_REQUEST`（无法回显未知 type 时以 `MESSAGE_TYPE_UNKNOWN` 回包）且不断连；修复当前 `errorResponse` 回显 `UNRECOGNIZED` 抛 `IllegalArgumentException` 后不回包、不断连、客户端挂至超时的行为。
- **server：port=0 配置口径闭环**（A5）——`ServerConfig.validate()` 放行 `port=0`（OS 分配临时端口），与 record javadoc 及 `OpenLatchServer` 既有处理对齐。
- **core：重入/加入租约刷新口径统一**（A3）——写侧重入与读侧加入既有读者群统一为"按本次请求 `requestedLeaseMs`（0 用默认、钳制 `[min,max]`）刷新整段租约"；消除当前两路径语义不一致。
- **client：multiplexer 条目身份校验**（A2）——`sendWithId` 同 requestId 重复登记时旧 pending 不得被静默覆盖丢失；`onTimeout` 摘除采用 `remove(id, pending)` 身份校验，杜绝误杀新条目；保证任何请求 future 均有界完成。
- **client：按 key 锁丢失监听器生命周期**（A4）——锁完全释放（fullyReleased 且无人重持）后清理该 key 的监听器登记，消除高基数 key 场景的无界累积。
- **测试补强**（B 类）——core：WRITE-WRITE 互斥、`sessionClosed` 新队首补通知断言、规则 7 防御分支、队首异 requestId 重发；server：pipeline 层 mismatch 存活断言、inflight→OVERLOADED 端到端；client：RELEASE INVALID_TOKEN 已丢锁分支、OVERLOADED 错误码路径；A1/A2/A3/A4/A5 回归用例；确认 `ClientProcessKillIT` 在 CI 中真实执行（非静默 skip）。
- **详设回写 v1.2**（C 类，纯文档）——修正文档内部矛盾 2 处（§4.4 释放规则 vs §4.6 堆陈旧校验、§3.2.2 lease 越界口径）、§4/§5/§6/§8/§9 类清单与行为描述共约 14 处滞后（含 `IdleEventHandler` 不存在、`LockMode`→`LockType`、切面直通 `acquireAsync` 定案、D1/D3/D5 定案回写等）。

无 **BREAKING** 变更：协议 wire 格式、公开 API 签名、错误码语义均不变；A3 是文档未定义自由度的收敛。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `lock-server`：「消息合法性校验」新增未知 MessageType 数值的行为要求（回 INVALID_REQUEST 不断连）；「服务启动与配置加载」配置取值域新增 port=0 放行。
- `core-lock-engine`：「可重入计数与租约刷新」重入/加入路径的租约刷新值口径修改（按请求值钳制刷整段，读写两侧一致）。
- `client-sdk`：「全程超时保证」扩展——同 requestId 并发登记/超时竞争下所有挂起 future 仍有界完成；「看门狗续租与锁丢失通知」扩展——按 key 监听器登记的清理时机。

## Impact

- 代码：`openlatch-server`（RequestDispatcher/ServerSessionHandler/ServerConfig）、`openlatch-core`（LockEntry）、`openlatch-client`（RequestMultiplexer/AwaitTracker/OpenLatchClient）。
- 测试：core/server/client 三模块各补用例（对应详设 §10.1/§10.2/§10.3 未覆盖行）。
- 文档：`docs/OpenLatch-Phase1-详细设计说明书.md` 升 v1.2；代码侧 Javadoc 按 CLAUDE.md §5 同步。
- 不受影响：协议 `.proto`、wire 兼容性、公开 API 签名、Spring starter、examples、基准。
