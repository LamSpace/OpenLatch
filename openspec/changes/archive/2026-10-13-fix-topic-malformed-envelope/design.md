# Design

## Context

见 proposal.md·Why。实现约束（读代码所得）：

- **四条独立读点、四种路径性质**：`executePublish`（同步发布，同 `op_seq` 去重槽 + 重发循环）、
  `executeSubscribe`（幂等 SUBSCRIBE 重投循环，源码注释已声明 "registration is idempotent — retry is safe"）、
  `publishAsync`（**单发 `sendWithId(...).whenComplete`，无重试循环**）、`rebindAsync`
  （fire-and-forget 重挂，失败即 `return` 留待下周期）。
- **既有红线**：PUBLISH 为**至多一次**（proposal·Why 引 `OTopic API`），跨换主重试可能双投；`executePublish`
  的换会话守卫（`SESSION_EXPIRED`）已在，重发**仅限同 `op_seq` 同会话**。

## Goals / Non-Goals

**Goals:**

- 四读点在缺码形应答下 MUST NOT 读默认实例（`status=OK`/`subscription_id=0`/`topic_seq=0`）。
- 复用各路**既有**出口：同步发布重发、订阅重投、异步发布显式失败、重挂放弃。

**Non-Goals:**

- 不动 topic 的至多一次/去重/弱背压交付契约。
- 不为 `publishAsync` 新增重试车道（属独立设计选择，非本缺口修复）。

## Decisions

### D1 按路径性质分流（不搞"一刀切重发"）

| 读点 | 路径性质 | 缺码形处置 |
|---|---|---|
| `executePublish` | 同 `op_seq` 去重槽 + 重发循环 | `continue`（同信封同 `op_seq` 重发） |
| `executeSubscribe` | 幂等登记重投循环 | `continue`（同 `request_id` 重投） |
| `publishAsync` | 单发无循环，至多一次 | `completeExceptionally`（**不重发**） |
| `rebindAsync` | fire-and-forget | `return`（放弃本次，留待下周期） |

### D2 `publishAsync` 缺码形 = 显式失败，**不得重发**

异步发布无重试车道；即便补一个，PUBLISH 的至多一次语义决定**重发会双扇出**（跨会话/去重槽失效）。
故缺码形 MUST 以 `future` 异常完成收场——正确的下界是"显式失败"，而非"以 `topicSeq=0` 假受理"。
本 change **不**为其引入重发车道（超出缺口修复范围）。

### D3 `executePublish` 可安全重发（沿既有车道）

逻辑同 `RemoteAtomicBase`：缺码形并入 `NOT_LEADER`/超时同车道，同信封同 `op_seq` 重发，服务端去重槽
保证同 term 不双扇出。判定插入点与既有 `catch (ExecutionException)` / `catch (TimeoutException)` 的
`continue` 平行。

### D4 `executeSubscribe` 可安全重投（幂等登记）

登记幂等（同会话同键服务端唯一登记），其循环已在超时分支 `continue`。缺码形同款并入。

### D5 `rebindAsync` 缺码形 = 放弃本次重挂

该路径本就是"失败留待下一周期/事件"的 fire-and-forget；缺码形视作失败即 `return`。**关键**：
默认实例的 `status=OK` + `subscription_id=0` 会误导其走"换绑"分支（`detachTopicSubscription` + `bind(sid, 0)`），
拆掉有效绑定、错绑 id 0——必须由 presence 判定拦在 `resp.getStatus()` 之前。

## Risks / Trade-offs

- **`publishAsync` 盲目重发双扇出** → D2 明确不重发；用例断言 `countType(TOPIC_OP)` 不因缺码形增长。
- **`rebindAsync` 错绑** → 判定拦在状态读取前；用例断言既有绑定不被拆除。
- **误伤正常路径** → 正常服务端恒填 oneof，presence 恒真、分支不进入；既有 `ClientTopicIT` 与
  `RemoteTopicScriptedTest` 回归覆盖。

## Migration Plan

纯客户端行为修正，无数据/协议迁移。正常服务端从不产生缺码形应答 ⇒ 对既有部署零影响。回滚 = revert 本 change。
