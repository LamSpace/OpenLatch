# Spec Delta

## ADDED Requirements

### Requirement: OTimer API

客户端 SDK SHALL 提供 `OTimer` 公开契约与 `OClient.newTimer(key)` 工厂，对应"延时触发（定时单次标记）"原语（JDK `java.util.Timer` 的协调面可判定子集，协议 v11）。契约要求：

- **构造零网络**：句柄创建不发起任何请求（判例 `newPhaser`/`newTopic`）；无初始装载参数——装载恒为显式 `schedule` 操作（对照 `newPhaser(initialParties)`：timer 无"构造即预装"的语义来源）。
- **API 面**：`long schedule(long delay, TimeUnit unit)`（装载/重装载，返回本代代次回显；`delay < 0` 或超 horizon 由服务端拒绝映射异常，`delay == 0` 即立即可共见）；`void disarm()`（撤销装载，幂等；无条目 `INVALID_REQUEST` 映射异常）；`void await()`（等待当代标记；当代被撤销时以 `OpenLatchException`（`DENIED` 终态映射）异常收束——"等待不可满足"为异常终态而非布尔混读，契约成文）；`boolean await(long timeout, TimeUnit unit)`（`true`=见到当代标记；`false`=超时；当代被撤销 → 同 `await()` 的异常收束）；`boolean isFired()` / `boolean isArmed()` / `long getRemainingMillis()`（`QUERY` 读数映射，advisory——即刻过期是契约，判例 `getPhase()` 语言事实句；`getRemainingMillis()` 为客户端本地差值）。不暴露 `cancel(await)` 面（超时/中断自救内部发 `CANCEL` fire-and-forget，判例 v9/v10）。
- **周期与任务为显式不提供**：无 `scheduleAt(Instant)`（客户端墙钟偏差注入即污染谓词判据——仅收相对延迟，绝对到期恒由服务端应用点折算，v7 `offerDelayed` 同轴）；无周期重挂参数（服务端周期自触=常驻重发变异，恰与"到期零条目"裁决相反；客户端循环 `schedule` 即等价，差异声明入降级清单）；无任务载荷/回调（服务端不执行用户代码——v10 D4 onAdvance 同轴论证）。
- **单次与粘滞共见**：一次装载恰一发——到期后标记对一切到达者共见（含 fire 之后新到的 `await`/`isFired` 即刻通过），直至 `disarm`（代终结、此后等待者收 `DENIED` 终态/异常）或再 `schedule`（换代清钟、开新一轮）。相对 JDK `TimerTask` 的"跑完即弃"差异成文：本原语的"用完"判据是观察面的代次推进，非消费动作。
- **改期以最新代为准**：`schedule` 重装载换代清钟——已挂起的 `await` 以最新代到期时刻为准（推后多睡/提前先响皆合法形态，无"旧代承诺"补偿）；推荐 timed await 自救（判例条件"无人 signal 则永睡"运维提示同轴）。
- **死亡不撤钟（增强声明）**：装载方进程/会话死亡不影响触发——钟照响、标记照共见（账簿绑定 key；与 Barrier 破障、Phaser 摘除并列为死亡语义第三形态，接口级 Javadoc 对照呈现）；等待方自身死亡仅失其订阅位。
- **等待闭环与自动重挂**：`AWAIT` 收 `QUEUED` 后按 request_id 过滤 `AWAIT_NOTIFY` 挂起、以**原 request_id** 重发取终态（`OK`/`DENIED`）；活跃等待项经双通道自动重挂（车道激活事件+周期保活，判例 v8/v10）——谓词在复制账簿，**换主/断连重挂无损：已到期即刻了结、未到期续挂**（与 v9 条件 signal 丢失窗的对照防混读句随行）；重挂 MUST NOT 误换 request_id（v10 过程真缺陷教训的本原语复现钉死——装载去重槽面防假象双换代）。超时/中断 → `CANCEL` fire-and-forget → 抛对应异常（`TimeoutException`/`InterruptedException` 形态沿既有 SDK 纪律）。
- **直发车道与读数口径**：`TIMER_OP` 不走 ACQUIRE 车道（同 key 无在途互斥——装载无持有概念，判例 QUEUE/TOPIC/CONDITION/PHASER 直发族）；`NOT_LEADER` 既有退避改道（无提示字段判例）；`isFired` 等读数的"已响"判定随判定节点时钟，跨节点可见偏差 ≤ 节点间时钟偏移（显式降级声明——管理面原始 `fire_at_ms` 呈现无歧义读数，接口 Javadoc 同步）。
- 接口级 Javadoc 全量降级/增强/保真三清单（每次调用至少一次 RTT 的成本模型、`schedule` 回执代次即观察序、`await` 无持有语义、无 `awaitUninterruptibly`/`awaitUntil` 对偶为 Non-Goal）；握手常量升 11；`ClientMetrics.statusCodeOf` 扩 `TIMER_OP` 归线。

#### Scenario: 装载-到期-共见闭环

- **WHEN** 会话 A `schedule(50ms)` 后进程照常运行，会话 B 与 C 并发 `await(2s)`，D 在约 60ms 时新到 `isFired()`
- **THEN** B/C 于服务端到期后经 `QUEUED→AWAIT_NOTIFY→原 rid 重发` 链路先后返回 `true`（tick 级滞后为声明精度），D 即刻 `true`（粘滞共见不经等待）；A 再次 `schedule` 后新到的 `await` 落入新一轮阻塞

#### Scenario: 撤销等待者的异常收束

- **WHEN** 等待者挂起于未到期当代，另一会话 `disarm()`
- **THEN** 等待者经唤醒重发收 `DENIED`：`await()`/`await(timeout)` 均以 `OpenLatchException` 收束（非 `false`——超时与撤销两形态不得混读）；`isFired()` 转 `false`、`isArmed()` 转 `false` 如实呈现代终结

#### Scenario: 重连重挂同 request_id 不假象双换代

- **WHEN** 等待项挂起期间其连接重建（会话切换放弃旧挂起），SDK 对新句柄等待项以新 request_id 登记；对照同句柄保活重挂路径
- **THEN** 保活重挂恒以原 request_id 重发（命中服务端等待幂等登记、不二次入集）；装载幂等重发命中去重槽回放声（代次不前进）——构造任何"重挂即重装载"的形态被回归用例钉死（v10 真缺陷教训的 SDK 侧护栏）

#### Scenario: 低版本服务端与回滚口径

- **WHEN** v11 句柄对 v≤10 服务端使用
- **THEN** 区间外握手即拒（既有纪律）；不存在旧服务端误执行 timer 语义的窗口；升级序服务端先行的部署声明随指南
