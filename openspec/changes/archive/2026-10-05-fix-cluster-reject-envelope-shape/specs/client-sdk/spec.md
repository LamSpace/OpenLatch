# Spec Delta: client-sdk

## MODIFIED Requirements

### Requirement: OBlockingQueue API

客户端 SHALL 提供 `OpenLatchClient.newBlockingQueue(String key, long capacity)` 与 `newDelayQueue(String key, long capacity)` 工厂（capacity ≤0 MUST 本地快速拒绝 `IllegalArgumentException`——协调面无无界队列；容量作为定型主张随每次写携带，首次 PUT 完成条目定型），返回公开接口 `OBlockingQueue`（延时工厂返回其子接口 `ODelayQueue`）。`byte[]` 为主元素形态、`String` 便利形态按 UTF-8 编解码且两形态语义逐项等价。`OBlockingQueue` 接口面：`void put(byte[] e)`/`put(String e)`（阻塞至有容量，可中断）、`boolean offer(byte[] e)`（立即式，满回 false）、`boolean offer(byte[] e, long timeout, TimeUnit unit)`（挂起式带客户端本地超时，判例 `wait_ms > 0` 客户端计时）、`byte[] take()`/`String takeAsString()`（阻塞至有可消费元素，可中断）、`byte[] poll()`（立即式，无回 null）、`byte[] poll(long timeout, TimeUnit unit)`（本地超时）、`int drainTo(Collection<? super byte[]> out)` / `int drainTo(Collection<? super byte[]> out, int maxElements)`（返回实际摘取数）、`byte[] peek()`（队首读数，空/未到期回 null）、`int size()`（驻留元素数，延时形态含未到期项）、`int remainingCapacity()`（`capacity − size`，一次 RTT）、`long capacity()`（定型回显）。`ODelayQueue` 追加 `boolean offerDelayed(byte[] e, long delay, TimeUnit unit)` / `offerDelayed(String e, …)`（延时注入——**不照抄 JDK `DelayQueue.offer(e, timeout, unit)` 签名**：该形态在 JDK 表"延迟后才可见"，与本地超时语义同形异义，显式改名以消除混读；契约 Javadoc MUST 声明该差异）。全部同步阻塞方法 SHALL 声明 `throws InterruptedException`，失败以非受检 `OpenLatchException`/`OpenLatchTimeoutException` 表达；服务端以 `INVALID_REQUEST` 拒绝（超限/形状/低版本/容量断言冲突）时 SDK MUST 抛显式异常并透传拒绝语义，MUST NOT 静默截断、本地改写参数或以同序号重发。阻塞 put/take 走等待-通知-重发闭环：`QUEUED` → 挂起 → `AWAIT_NOTIFY` → 同 `requestId` 重发 → `DENIED`（应用点竞态回弹）继续挂起 → `OK` 终态；重发超时纪律沿用既有等待车道裁决。写操作 MUST 以**同一 `op_seq`** 在可重试失败（`NOT_LEADER`、断连快速失败、超时）时自动重发至请求总超时界限（继承 `RemoteAtomicBase` 同 key 在途写互斥纪律，服务端每会话去重槽保证 PUT 不双插、TAKE 重放交付同一份字节）；界限内不可判定抛 `OpenLatchTimeoutException` 并声明效果不确定窗。队列操作 MUST NOT 依赖看门狗/租约/`LockLost` 通知。队列 MUST 以 v7 握手使用（SDK 握手版本声明升至 7）；对握版本上限 <7 的服务端，`QUEUE_OP` 收到 `INVALID_REQUEST` 时 SDK MUST 以显式异常表达协议不支持，MUST NOT 重发。元素为不透明字节：SDK 与服务端 MUST NOT 解释或反序列化。契约 Javadoc MUST 显式声明降级/增强/差异清单：每操作一次网络往返（`drainTo` 一次摘多）、超时不确定窗口（幂等由槽兜底但可重试窗内结果延迟）、元素绑定 key 不绑定会话——**投递者进程死亡不吞元素**（强于/异于 JDK 同进程堆消散语义）、`DELAY_QUEUE` 同到期时刻 FIFO（增强，JDK 不承诺）、未到期元素对 `PEEK/TAKE` 不可见但计入 `SIZE`、无 `iterator/contains/remove(Object)/spliterator` 面、元素不可为 null（与 `OAtomicReference` 的 null 语义刻意不同）、容量服务端上限权威钳制、条目常驻不回收（驻留成本与清理为消费/运维职责）、延时到点唤醒精度为服务端 tick 级。

应答形态裁决（MUST）：队列车道收到应答信封后 MUST 校验其携带 `queue_op_response` 载荷——`Envelope.type` 回显 `QUEUE_OP` 但载荷 oneof 缺该成员（异型/空载荷码形违例，如未含 `QUEUE_OP` 分支的拒绝组装路径产物）MUST 按瞬态失败裁决（与 `NOT_LEADER`/断连同车道：退避、重取路由、重发），MUST NOT 读取 protobuf 默认实例并据此裁决（默认实例 `status` 为枚举零值 `OK`，会被误判为"成功空应答"致 `take` 交付零长度元素、`size` 读回 0）。`TAKE` 终态收到 `OK` 而应答缺 `element_bytes` presence 属协议违例（交付契约要求 OK-TAKE 必携元素，显式 presence 使"零长度空串元素"与"缺省"可辨），MUST 以显式 `OpenLatchException` 抛出、MUST NOT 交付 `byte[0]`；`PEEK`/`SIZE` 的 OK-缺省载荷为合法空读数形态，不受本条约束。

#### Scenario: 生产消费 FIFO 闭环

- **WHEN** 会话 A 对容量 8 的队列依次 `put("a")、put("b")、put("c")`，会话 B 依次 `take()` 三次，随后 `offer("d")` 与 `poll()`
- **THEN** take 依序返回 "a"、"b"、"c"（UTF-8 往返字节级一致）；offer 回 true、poll 返回 "d"；`poll()` 于空队回 null、`offer` 于满队回 false

#### Scenario: 阻塞 put 腾位唤醒闭环

- **WHEN** 满容量队列上一个 `put` 挂起（服务端 QUEUED），另一会话消费一个元素，随后挂起者收到推送
- **THEN** 挂起的 `put` 经"通知→同 requestId 重发→OK"正常返回，总深度不超过容量，元素不双插（同 op_seq 槽保证）

#### Scenario: 中断与本地超时

- **WHEN** 挂起的 `take` 被线程中断；另一 `poll(50ms)` 于持续空队到期返回
- **THEN** 前者抛 `InterruptedException` 且服务端等待项经离场路径摘除（不吞元素）；后者回 null（超时为客户端本地计时，等待项由其超时臂自行了结）

#### Scenario: 换主窗口写重发不双插不偷吃

- **WHEN** `put(x)` 首发落入 Leader 切换窗口收到 `NOT_LEADER`，SDK 经重发向新 Leader 以同 `op_seq` 重发；另一场景 `take()` 交付后应答丢失重发
- **THEN** 队列中 x 恰出现一次；take 场景 SDK 收到与服务端首交付**字节相同**的元素，且未多摘一个

#### Scenario: 投递者进程死亡元素存续

- **WHEN** 会话 A `put` 若干元素后进程被杀，会话 B `size`/`take`
- **THEN** B 见 A 投递的全部元素并按序消费——无因 A 死亡产生的元素丢失（契约声明的可执行化）

#### Scenario: 延时元素按到期可见

- **WHEN** `offerDelayed("late", 300, MILLISECONDS)` 后立即 `peek`，等待到期后再 `take`
- **THEN** 到期前 `peek` 回 null 而 `size` 计 1；到期后 `take` 返回 "late"

#### Scenario: 超限与低版本显式失败零生效

- **WHEN** `put` 携带超过服务端 `maxValueBytes` 的元素；或 v7 SDK 对握版本上限 6 的服务端调用 `put`
- **THEN** 均抛 `OpenLatchException`（拒绝语义、非超时），队列零变化，且无同序号重发发生

#### Scenario: 换主窗非 Leader 拒绝不得伪成功（安全性质）

- **WHEN** 元素已提交后 Leader 被杀，客户端队列请求的路由落在非当值 Leader 节点
- **THEN** 该节点以同型 `queue_op_response` 的 `NOT_LEADER` 拒绝、SDK 按瞬态退避重取路由重发；全程 MUST NOT 出现"`take` 交付空串/错误元素"或"`size` 读 0"的伪成功形态——改道收敛时依序交付真实元素（透明），路由未能在请求预算内重建的驻留窗以显式异常收场（超时/会话换代口径，客户端改道机制的完整收敛兑现归"Leader 发现与故障转移"能力承载）

#### Scenario: 缺码形应答不得读作成功

- **WHEN** 队列车道收到 `Envelope.type = QUEUE_OP`、`request_id` 匹配、但载荷为异型（如仅 `acquire_response`）或空载荷的应答（脚本化服务面注入）
- **THEN** 车道按瞬态失败重取路由重发（不交付元素、不上报读数），至请求时限耗尽以 `OpenLatchTimeoutException`/会话族异常失败；`TAKE` 的真 `OK` 且缺 `element_bytes` 注入形态以显式协议违例异常失败、MUST NOT 交付 `byte[0]`
