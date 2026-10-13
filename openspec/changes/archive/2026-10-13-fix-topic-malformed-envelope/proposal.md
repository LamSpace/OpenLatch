# Proposal

## Why

`RemoteTopic` 读取应答时**四处均不判 presence**，直接读 `answer.getTopicOpResponse()`。缺码形应答
（`Envelope.type` 回显 `TOPIC_OP` 而载荷 oneof 缺 `topic_op_response`）会被读作 protobuf 默认实例，
其 `status` 为枚举零值 `OK`、`subscription_id`/`topic_seq` 为 0。四种显形：

- `publishAsync`（单发无重试车道）：`future` 以"受理序号 0"**正常完成**（虚假受理）；
- `executeSubscribe`（幂等重投循环）：`subscriptionId=0` 被绑为订阅**（虚假登记）**；
- `executePublish`（同 `op_seq` 去重槽重发车道）：静默返回 `topicSeq=0`；
- `rebindAsync`（fire-and-forget 重挂）：默认 `OK` 致**拆旧绑定、错绑订阅 id 0**（丢交付）。

这与 `fix-atomic-malformed-envelope` / `fix-latch-barrier-malformed-envelope` 同型（W17 的三族缺口之一），
且同样可由脚本化夹具确定性复现。

## What Changes

四处读点在 `getTopicOpResponse()` **之前**插入 `answer.hasTopicOpResponse()` 判定，按**路径性质分流**
（详见 design D1）：

- `executePublish` → `continue`：同信封同 `op_seq` 重发（去重槽保证不双扇出，判例原子族）。
- `executeSubscribe` → `continue`：幂等 SUBSCRIBE 重投（其车道已声明 retry is safe）。
- `publishAsync` → `completeExceptionally`：**无重试车道且不可重发**（至多一次发布，重发会双扇出），
  缺码形 MUST NOT 以 `topicSeq=0` 假受理，以显式失败收场。
- `rebindAsync` → `return`：留待下一周期/事件再挂，与其既有失败出口同。

- **不新增机制**：只把"缺码形"并入各路既有失败车道。
- **非目标**：不改 topic 的至多一次/去重/弱背压契约；不为 `publishAsync` 引入重试车道（属独立设计选择）。

## Capabilities

### New Capabilities

（无——既有原语的应答裁决修正。）

### Modified Capabilities

- `client-sdk`: `OTopic API` 增「应答缺码形 MUST NOT 读作成功」条款及其场景；既有条款与 5 个既有场景逐字保留。

## Impact

- **代码**：仅 `openlatch-client` 的 `RemoteTopic` 四处读点。
- **测试**：扩展已有 `RemoteTopicScriptedTest`——`ScriptedServer` 直驱注入缺码形应答，逐路径断言
  （同步发布重发不返回 0、异步发布显式失败不假受理、订阅不绑 id 0、重挂不错绑）。
- **兼容**：纯客户端行为修正。正常服务端从不产生缺码形应答，对既有部署**零影响**。
- **文档**：接口契约 Javadoc 声明各路径缺码形处置；双语排障指南 topic 排查节增一条排查项。
