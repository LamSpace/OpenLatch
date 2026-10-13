# Design

## Context

缺码形裁决在库内已有两处判例：原子族（`RemoteAtomicBase`，change
`2026-10-11-fix-atomic-malformed-envelope`）与队列/条件/相位器/定时器族，均为"读应答前判
`has(<成员>)`，缺码形并入该族既有失败车道"。等待族两族（`RemoteCountDownLatch`、`RemoteBarrier`）
是仅存的裸露点。动机与危害见 proposal.md·Why。

实现约束（读代码所得，非推测）：

- **读点分布**：latch 两处——`doAwait` 的 `getLatchAwaitResponse()`（走等待-通知-重发闭环，可同
  `request_id` 重发）与 `countDownOp` 的 `getLatchCountDownResponse()`（**至多一次**，无 `op_seq`
  去重槽、无重试循环）。barrier 四处——`doAwait` 的 `getBarrierAwaitResponse()`（等待闭环）、
  `barrierActionDone` 的 `getBarrierActionDoneResponse()`（既有 `(会话,世代)` 幂等重发循环）、
  `breakBarrier` 的 `getBarrierLeaveResponse()`（直发无循环）与内部 `leaveAndAwait` 的
  `getBarrierLeaveResponse()`（以 `== OK` 判定"确认成立"）。
- **既有契约红线**：`OBarrier API` 明写"在途 `BARRIER_AWAIT` 遇传输失败/断连/**换会话**
  MUST NOT 自动重发"。`doAwait` 的异常分支已经实现该守卫（`sameSession` 判定，跨会话即抛
  "abandoned on session change"）。缺码形重发**必须**受同一守卫约束。
- **至多一次纪律**：`countDown` 是该纪律在库内的判例（barrier 契约原文引用之）。其重发会双扣。

## Goals / Non-Goals

**Goals:**

- 两族六处读点在缺码形应答下 MUST NOT 读 protobuf 默认实例（`status=OK`）而假绿/静默错值。
- 复用各族**既有**失败车道：等待路径重发、至多一次路径显式失败、幂等路径按 `(会话,世代)` 重发。
- 缺口可由脚本化夹具**确定性复现**（红先），不依赖负载敏感的竞态。

**Non-Goals:**

- 不动 `RemoteTopic`（同型缺口，按 W17 另立 change；其 `publishAsync` 无重试循环属独立设计选择）。
- 不触碰 v9 对 latch await `NOT_LEADER` 应答状态的终态裁决（presence 判定在读 `status` **之前**，不改变 `status` 语义）。
- 不放宽任何既有"至多一次/跨会话不重放"守卫，不新增机制。

## Decisions

### D1 按读点**分流**处置，不照抄原子族"一律重发"

原子族的车道是"缺码形 → 可重试失败 → 同 `op_seq` 重发"，因其写路径**有**去重槽。等待族没有统一
车道，必须按读点归类：

| 读点 | 归类 | 缺码形处置 |
|---|---|---|
| `latch.doAwait` / `barrier.doAwait` | 等待-通知-重发闭环（同 id 幂等） | 同会话同 `request_id` 重发；预算耗尽显式失败 |
| `barrier.barrierActionDone` | `(会话,世代)` 幂等 | 同 id 重发（沿用既有循环） |
| `latch.countDownOp` | **至多一次**（无去重槽） | **不重发**，显式失败 |
| `barrier.breakBarrier` / `leaveAndAwait` | `BARRIER_LEAVE` 幂等但直发无循环 | **不重发**，显式失败 / 确认返回"不确定" |

*备选*：一律按可重试失败重发。**否决**——`countDown` 无去重槽，重发会双扣计数，比静默错值更难排查。

### D2 `countDown` 缺码形的失败口径 = `OpenLatchTimeoutException`（结果不确定、未重发）

与 `countDownOp` 既有时限耗尽分支同口径（现文案 "result unknown, not retried"）：应答到达但码形违例
⇒ 受理与否不可判定，且不能安全重发。**不采用** `OpenLatchException(status)`（无有效 status 可报）
与协议违例专用异常（该形态用于 `status==OK` 却缺载荷的场合，此处连 `status` 都不可信）。

### D3 barrier await 的缺码形重发 MUST 携带"同会话"守卫

读界判定后，比较 `client.latchRoute()` 当前会话与 `envSession`：同会话按同 `request_id` 重发；已换会话
走既有"到场不跨会话重放"裁决（抛 `OpenLatchException`）。*备选*：照 `RemotePhaser` 直接 `continue`
——**否决**：phaser 的 loop 顶重建语义与 barrier 不同，bare `continue` 会在会话已换时借道重建 `env`
把到场静默重放到新会话，直接违反既有契约场景"换会话不自动重放到场"。

### D4 `breakBarrier` / `leaveAndAwait` 缺码形 → 显式失败，不重发

`breakBarrier` 抛显式异常；`leaveAndAwait` 返回 `false`（"不确定"，与既有网络失败降级同出口）。堵的是
"离场未生效却被读作已确认"——后者破坏 `OBarrier API` 的"返回即已破"次序契约。*备选*：给 `breakBarrier`
加重发循环——**否决**：`leave` 虽幂等，但为最小改动不新增循环，显式失败已是所需的正确下界。

### D5 复用既有机制，零新增通道

语义上只是把"缺码形"并入各族既有失败清单；等待族重发通道（同 `request_id` + `LatchNotifyRegistry` 重注册）
早已存在。改动仅是在每处 `get<X>Response()` **之前**插入 `has<X>Response()` 判定与分支。

## Risks / Trade-offs

- **误伤正常路径** → 正常服务端恒填对应 oneof，presence 恒真、分支不进入；正常应答回归单测覆盖（既有
  `ClientLatchIT`/`ClientBarrierIT`/`ClientBarrierLoopIT`）。
- **barrier 缺码形重发破坏"换会话不重放"** → 由 D3 同会话守卫约束；以既有场景"换会话不自动重放到场"
  回归验证。
- **`countDown` 误重发双扣** → D1 明确不重发；夹具断言 `countType(LATCH_COUNT_DOWN)` 不因缺码形增长。
- **一次改两族多点不利归因** → 逐读点独立脚本化用例（红先）+ 分族提交，失败可定位到具体读点。

## Migration Plan

纯客户端行为修正，无数据/协议迁移。正常服务端从不产生缺码形应答 ⇒ 对既有部署零影响；变化仅是异常输入下
"假绿/静默错值"→"重发，必要时显式失败"。回滚 = revert 本 change 的客户端改动。
