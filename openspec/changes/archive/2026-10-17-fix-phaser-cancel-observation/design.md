# Design

## Context

见 `proposal.md`（动机与证据）。与本设计相关的现状约束：

- `RemotePhaser#cancelQuietly` 明确是 fire-and-forget：`route.send(...)` 的 future 不作等待，异常静默吞掉；契约声明"超时到期 MUST 发 CANCEL fire-and-forget 后抛异常"。
- 同文件 `interruptDuringAwaitCancelsAndPreservesFlag` 已有先例：`while (cancelFrame.get() == null && now < deadline) Thread.sleep(20)`，3s 上限，之后才断言非空。
- 失败模式是**跨线程可见性竞态**：主线程抛出异常的时刻，Netty/脚本服务端的处理线程可能尚未运行回调。

## Goals / Non-Goals

**Goals:**
- 断言表达契约真正承诺的东西：**CANCEL 最终必达**（有界窗内），而不是"抛出瞬间对端已可见"。
- 与同文件既有先例同款实现，不引入新写法。
- 若帧真的不到达，仍在有界窗后红（判负保留）。

**Non-Goals:**
- 不改 `cancelQuietly` 的 fire-and-forget 语义（那会把超时路径变成同步等待，改变延迟与契约）。
- 不放宽三条实体断言（非空、`op == CANCEL`、`await_request_id` 为正）。

## Decisions

**D1：有界轮询"最终必达"，而不是其它四选。**
- 候选 A（本设计）：3s / 20ms 轮询后再断言。
- 候选 B：维持立即断言。**否决**——把跨线程可见性压进毫秒窗口，是本次间歇红的直接成因。
- 候选 C：固定 `sleep`。**否决**——既慢又不构成判据（睡醒与否与帧到达无关）。
- 候选 D：删除该断言。**否决**——契约要求 "MUST 发 CANCEL"，删断言即删判据。
- 候选 E：把 `cancelQuietly` 改成同步等待应答。**否决**——改变契约语义（fire-and-forget 的目的是不阻塞超时收场），且会把服务端抖动引入超时路径的时延。

**D2：参数取 3s / 20ms，与 `interruptDuringAwaitCancelsAndPreservesFlag` 完全一致。**
- 依据：同一契约面、同一夹具，已有先例；差异只在"超时"与"中断"两条收场路径。
- 备选：5s。**否决**——无额外证据支持更长窗口；3s 已是毫秒级正常延迟的 200 倍余量。

## Risks / Trade-offs

- [真出现"未发出"时，红从"立即"推迟 3s] → 可接受：失败信息仍是同一断言，且 3s 后必红，不会假绿。
- [掩盖真正的顺序缺陷] → 不覆盖：契约只承诺"发"（fire-and-forget），不承诺对端观察时刻；若未来契约收紧为"必达确认"，本断言需随之重议（届时轮询也不够，需要应答关联）。

## Migration Plan

无迁移面：纯测试文件改动，随 PR 合并生效，回滚即还原该测试文件。
