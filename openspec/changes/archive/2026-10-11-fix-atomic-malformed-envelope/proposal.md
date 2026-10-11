# Proposal

## Why

`RemoteAtomicBase` 读取应答时**不判 presence**（两处：标量族与有值引用族），直接 `resp = answer.getAtomicOpResponse();`。若到达的应答不是本请求的载荷（异型/串号），`getAtomicOpResponse()` 返回 **proto3 默认实例**——`status` 是枚举零值 **`OK`**、`value` 是 **0**——于是 `addAndGet(5)` **静默返回 0**。

这正是 2026-10-11 回退轮中**始终未获解释**的失败形态 A（见 `DECISIONS.md` 同日「周期核对整体回退裁决」）。该护栏原则在本仓**已有判例**，但只覆盖队列：`OBlockingQueue API` 明写「MUST 校验 `queue_op_response` 载荷……MUST NOT 读取 protobuf 默认实例并据此裁决」。本次把同一原则扩到原子族。

## What Changes

- `RemoteAtomicBase` **两处**应答读取前增 `hasAtomicOpResponse()` 判定；缺码形按**可重试失败**处置——与既有 `NOT_LEADER`/断连/超时**同车道**：以**同一 `op_seq`** 同信封重发至请求总超时界限。
- **不新增机制**：语义上只是把"缺码形"并入该族既有的可重试失败清单，其通道（同 `op_seq` 重发、服务端去重槽保证不重复生效）早已存在。
- **非目标**：不动 `RemoteCountDownLatch` / `RemoteBarrier` / `RemoteTopic` 的同型缺口——它们同样缺少该判定，但**无观测证据**，本次登记在册不动（一次改多处写路径不利归因）。

## Capabilities

### New Capabilities

（无——既有原语的应答裁决修正。）

### Modified Capabilities

- `client-sdk`：`OAtomicLong 族 API` 新增「应答缺码形 MUST NOT 读作成功」条款及其场景；既有条款与 4 个既有场景逐字保留。

## Impact

- **代码**：仅 `openlatch-client` 的 `RemoteAtomicBase` 两处（标量族、有值引用族）。
- **测试**：新增脚本化夹具——异型应答信封（`Envelope.type` 回显 `ATOMIC_OP` 而载荷缺 `atomic_op_response`），断言**不返回 0、不静默成功**，而是按可重试失败收场。
- **兼容**：纯客户端行为修正。正常服务端从不产生缺码形应答，故对既有部署**零影响**；变化的只是"异常输入下静默错值"→"重发，必要时显式失败"。
- **文档**：接口契约 Javadoc 声明该处置；双语排障指南增一条排查项（"原子操作疑似静默错值"）。
