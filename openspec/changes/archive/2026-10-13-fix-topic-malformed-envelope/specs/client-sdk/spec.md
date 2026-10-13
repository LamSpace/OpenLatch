# Spec Delta: client-sdk

## MODIFIED Requirements

### Requirement: OTopic API

`OpenLatchClient` SHALL 提供 `newTopic(String key)` 工厂返回 `OTopic`，公开契约为：`publish(byte[])`/`publish(String)`（UTF-8 便利族）同步返回受理结果、`publishAsync` 异步对偶、`subscribe(OTopicMessageHandler)` 返回 `OTopicSubscription` 句柄、`unsubscribe()` 幂等退订该键本会话全部订阅。`OTopicSubscription` 暴露 `droppedCount()`（本地观察值）、`isActive()`、`close()`（幂等，等价 UNSUBSCRIBE）。消息体为不透明字节（服务端不解释、无反序列化）；PUBLISH 必携非 null 消息体（零长度合法），与队列元素纪律同判例、与引用形态 null 语义刻意不同。

交付语义与相对 JDK `Flow.Publisher`/`SubmissionPublisher` 的差异 MUST 以接口级 Javadoc 全量声明：

- **至多一次**：`publish` 返回 `OK` 仅表示服务端已受理并入 fan-out，不承诺任何订阅者收到；订阅者可能因缓冲满/断线/换主窗而丢消息，服务端不重投、不通知单条丢失。
- **弱背压 drop-newest 两级**：服务端每订阅缓冲满丢最新（既有缓冲照常交付），SDK 本地缓冲同策略；Publisher 不被慢消费者阻塞，慢消费者不被断开（显式不采纳 JDK `onOverflow → close` 判例——断连即会话死亡、连带全部持锁释放）。
- **顺序承诺仅限单订阅内**：同一订阅收到的 `topic_seq` 在**同 Leader term 内**严格升序；`droppedCount()` = 本地缓冲溢出计数 + 同 term 内 seq gap 推断，换 term（重连重订阅后）基线重置不跨 term 累计；跨 Publisher、跨订阅无全局序承诺。
- **去重仅同 Leader**：PUBLISH 沿用同 key 在途互斥 + 同 `op_seq` 重发（判例 `RemoteAtomicBase` 写车道），受理节点去重槽保证同 term 不双扇出；跨换主重试可能双投——消费侧幂等为应用义务，契约显式声明。
- **订阅绑定会话**：订阅者会话死亡即退订（服务端三路回收），与队列"死亡不吞元素"刻意相反；换主后订阅关系由 SDK 自动重订阅维持（应用句柄与语义不变，`subscription_id` 内部重映射），重挂前的窗口内消息不可追回。
- **监听器线程模型**：`OTopicMessageHandler` 单订阅内串行回调，执行于 SDK dispatcher 线程（MUST NOT 占用 Netty EventLoop）；回调异常被吞并记录，不中断后续交付。
- **服务端权威钳制**：`maxValueBytes`/`max-subscribers-per-key`/`max-subscription-buffer` 由服务端入口裁决，超限/上限拒绝以对应状态码同型送达（`INVALID_REQUEST`/`REJECT_SUBSCRIBERS`）；订阅存在有常驻登记表与缓冲成本。
- **版本门**：需 v8 握手（客户端三处常量升 8）；对 v7 服务端连接按既有区间外握手拒绝纪律失败——升级序先服务端后客户端（判例 v3–v7）。

`newTopic` 对同 key 可多实例；同会话同键服务端唯一登记，SDK 层后到 SUBSCRIBE 覆盖在前句柄的交付路由时 MUST 在句柄 `isActive()` 与回调行为上如实呈现覆盖语义（不静默双路由）。**应答缺码形 MUST NOT 读作成功**：SDK 收到应答信封后 MUST 校验其携带 `topic_op_response` 载荷——`Envelope.type` 回显 `TOPIC_OP` 而载荷 oneof 缺该成员（异型/空载荷码形违例）时 MUST NOT 读取 protobuf 默认实例（其 `status` 为枚举零值 `OK`、`subscription_id`/`topic_seq` 为 0）并据此裁决；处置按路径性质分流——同步 PUBLISH 与 SUBSCRIBE 两条**幂等重发车道**（`executePublish` 同 `op_seq` 去重槽、`executeSubscribe` 幂等登记）MUST 按可重试失败以同信封同 `request_id` 重发至请求预算界限，MUST NOT 静默返回 `topic_seq=0` 或绑定 `subscription_id=0`；异步 `publishAsync` **无重试车道且不可重发**（至多一次发布，重发会双扇出）MUST 以显式失败收场（`future` 异常完成），MUST NOT 以 `topicSeq=0` 假受理；重挂路径（`rebindAsync`）缺码形 MUST 视作失败放弃本次重挂（留待下一周期/事件），MUST NOT 拆除既有绑定或错绑默认 `subscription_id`。

#### Scenario: 发布-订阅闭环

- **WHEN** 两客户端分别 SUBSCRIBE 后，第三方对同 key PUBLISH 多条消息
- **THEN** 两订阅者各自收到全部消息的字节级副本（payload 无损、`topic_seq` 升序、`publisher_sid` 与 `publish_ts_ms` 可读），回执 `OK` 携带 `topic_seq`

#### Scenario: 退订停止交付

- **WHEN** 订阅者 `close()` 其后该 key 再 PUBLISH
- **THEN** 该订阅者不再收到任何推送；对已退订键重复 `close()`/`unsubscribe()` 不报错（幂等）

#### Scenario: 换主自动重订阅续收

- **WHEN** Leader 被 kill，订阅客户端重连至新 Leader 后第三方继续 PUBLISH
- **THEN** 应用句柄无感（未重调 subscribe）持续收到新 term 消息；换主窗内发布的消息不再出现（无重投），`droppedCount()` 不因 term 切换跨窗累计 gap

#### Scenario: 慢监听器本地丢弃不反压

- **WHEN** 监听器长时间阻塞致 SDK 本地缓冲溢出，同时该连接上其他原语（锁/队列）正常操作
- **THEN** 仅 topic 交付链丢弃并计数（`droppedCount()` 增长），其他原语请求不受影响，连接不断开，Publisher 侧无感知

#### Scenario: 发布重发同序号不双投

- **WHEN** PUBLISH 应答在途丢失，SDK 以同 `op_seq` 自动重发（未换主）
- **THEN** 订阅者各收到一份该消息（去重槽命中重放回执，无第二份）

#### Scenario: 缺码形同步发布应答不得读作受理

- **WHEN** 同步 `publish` 的 `TOPIC_OP` 应答 `type` 回显而载荷 oneof 缺 `topic_op_response`（脚本化服务面注入）
- **THEN** SDK 不读取默认实例、MUST NOT 返回 `topic_seq=0` 假受理；按可重试失败以同 `op_seq` 重发（`countType(TOPIC_OP) > 1`），预算内仍不可判定则以显式异常收场

#### Scenario: 缺码形异步发布应答不得假受理

- **WHEN** `publishAsync` 的应答缺 `topic_op_response` 载荷
- **THEN** `future` 以显式异常完成、MUST NOT 以 `topicSeq=0` 正常完成；且因至多一次发布不可重发，MUST NOT 自动重发（`countType(TOPIC_OP)` 不因本次失败增长）

#### Scenario: 缺码形订阅应答不得读作登记

- **WHEN** `SUBSCRIBE` 的应答缺 `topic_op_response` 载荷
- **THEN** SDK 不读取默认实例、MUST NOT 绑定 `subscription_id=0` 为订阅；按幂等重投以同 `request_id` 重发（`countType(TOPIC_OP) > 1`），预算内仍不可判定则以显式异常收场

#### Scenario: 缺码形重挂应答不得错绑

- **WHEN** `rebindAsync` 触发的 `SUBSCRIBE` 应答缺 `topic_op_response` 载荷
- **THEN** 本次重挂按失败放弃（留待下一周期/事件），MUST NOT 拆除既有绑定、MUST NOT 以默认 `subscription_id=0` 覆盖句柄绑定
