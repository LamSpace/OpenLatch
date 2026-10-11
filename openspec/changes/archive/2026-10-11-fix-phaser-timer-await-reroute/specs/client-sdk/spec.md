# Spec Delta: client-sdk

## MODIFIED Requirements

### Requirement: OPhaser API

客户端 SDK SHALL 提供 `OPhaser` 公开契约与 `OClient.newPhaser(String key)` / `newPhaser(String key, int initialParties)` 工厂：句柄构造零网络；`initialParties > 0` 时首次业务操作前同步提交一次 `REGISTER(count = initialParties)` 归属本会话（JDK `new Phaser(n)` 的注册语义对偶——构造后 n 方已注册未到场；注册失败按抛出形态传播，同句柄不重复提交）。API 面：`int register()` / `void bulkRegister(int parties)`（返回/回显到场相位——注册者进入当前相位应到集合）、`int arrive()`、`int arriveAndDeregister()`、`int arriveAndAwaitAdvance()`、`int awaitAdvance(int phase)`（无限挂起形态）、`int awaitAdvanceInterruptibly(int phase, long timeout, TimeUnit unit)`、`long getPhase()` / `int getRegisteredParties()` / `int getArrivedParties()` / `int getUnarrivedParties()`（JDK 返回值形态对齐：相位 long、计数 int）。**不提供** `onAdvance` 覆写钩子、`isTerminated()`/`forceTerminated()` 与父子分层构造——三砍为显式 Non-Goal，与 JDK `Phaser` 的这组差异 MUST 在接口级 Javadoc 降级清单逐条声明并给出应用侧替代惯用法（钩子→以 `arriveAndAwaitAdvance()` 返回的到场相位号判别后本地执行；终止→键级生命周期治理；分层→独立 key）。

车道与闭环：`PHASER_OP` 为直发请求-应答车道（判例 QUEUE/TOPIC/CONDITION——无 ACQUIRE 在途互斥，phaser 无持有概念；同请求超时重发以同 request_id 幂等重演，到场类去重槽、等待登记幂等与了结记录承载重放安全）。变异操作（register/arrive/arriveAndDeregister/arriveAndAwaitAdvance 的到场半程）经提交路径，客户端对 `NOT_LEADER` 执行既有退避改道；等待操作（awaitAdvance/arriveAndAwaitAdvance 的等待半程）收 `QUEUED` 后进入挂起环——按 request_id 过滤 `AWAIT_NOTIFY`（判例队列读车道与 v3 推送桥）、原 request_id 重发取数了结（`OK{相位}`）；`awaitAdvanceInterruptibly` 超时到期 MUST 发 `CANCEL` fire-and-forget 后抛 `TimeoutException`（JDK 对偶签名；`awaitAdvance(int)` 无限形态由等待总超时兜底——超时纪律条款延伸，MUST NOT 提供事实上永挂而无界收口的路径）；被中断（interrupt）同超时路径收束并抛 `InterruptedException`。换主/会话重建后 SDK MUST 以双通道自动重挂全部活跃等待项（车道激活事件 + 周期保活，判例 v8 topic 重挂；谓词在复制态——重挂即刻了结或续挂、**无损耗**，该增强与 v9 条件"换主窗 signal 丢失"的对照差异 MUST 在 Javadoc 与指南声明防混读）。等待面收到 `NOT_LEADER` **应答状态**时按零生效处置：MUST 摘除本机等待登记、在独立短预算（= 2 个 Leader 核对周期）内等待路由改道，**仅当路由会话确已变化**时以新会话重发；预算耗尽或路由始终未变则以 `NOT_LEADER` 显式抛出（MUST NOT 伪造相位读数、MUST NOT 悬挂至等待总预算）。该形态与上文"换主/会话重建后自动重挂"通道并列且互补：重挂处理**结果不确定**的换主窗，本条款处理**已答复未受理**的陈旧路由。

契约三清单（接口级 Javadoc 全量承载）：**保真面**——动态注册/离场、到场与等待解耦、按已见相位等待、相位号单调、arrive 族返回到场相位、`awaitAdvance` 返回当前相位、`getUnarrivedParties` 口径、旁观者 awaitAdvance 无需配额；**降级面**——每次调用至少一次 RTT（JDK 本地计数器的 `getPhase`/`arrive` 为纳秒级——网络成本模型显式声明，观察类计数建议低频读取或应用侧缓存句柄本地最近所见）、查询为 Leader 本地读数不保证线性化（返回即刻过期是契约）、无 onAdvance 的"动作先于全体放行"排序（应用侧钩子与他人的醒转无先后承诺）、无终止态（JDK 终止语义不映射）、`arriveAndDeregister`/`bulkRegister` 的配额严格归属（JDK 匿名 party 面收敛为会话记账，跨会话代扣不存在）、等待超时的 CANCEL 为尽力撤销（ghost 有界且受护栏钳制——与 v9 AWAIT 权限降级同型的宽容面声明）；**增强面**——跨进程参与者同相合拢（JDK 句柄进程内私有）、等待谓词随复制态跨换主自愈、管理面全集群配额/到场/等待读数可见。句柄无状态：同 key 重复 `newPhaser` 等价绑定同一服务端账簿（判例 v9 命名寻址跨进程等价的同型声明）；SDK MUST NOT 在本地伪造计数（读数恒来自服务端应答回显）。

指标与诊断：`ClientMetrics.statusCodeOf` 扩 `PHASER_OP` 归线；握手常量升 10；`FairOrderingSuite` 类公平性套件对 PHASER 不适用（合拢是广播事件、到场先后由 Raft apply 序承载——判例 v5 D6 BARRIER/ATOMIC 豁免的正文同款，豁免登记随实现提交）。

#### Scenario: 多方会合与返回值保真

- **WHEN** 两个进程各 `newPhaser(key, 2)`、各自 `register()` 使 registered=4，随后四方（跨两会话）依次 `arriveAndAwaitAdvance()`
- **THEN** 前三次到场返回到场相位号且调用挂起/排队，最后一次到场触发合拢；全部四方以 `OK` 收束，`arriveAndAwaitAdvance` 均返回同一到场相位号（JDK 返回值语义）；`getPhase()` 推进 1、`getArrivedParties()` 归零

#### Scenario: 超时不泄漏等待位

- **WHEN** 单参与者 `awaitAdvanceInterruptibly(0, 50ms)` 而无人使其相位推进
- **THEN** 到期抛 `TimeoutException`，SDK 已发出 `CANCEL`；该 request_id 的后续迟到唤醒通知被挂起环按既有终止竞争纪律忽略；服务端等待集经三路回收不残留（ghost 至多存续至清扫/会话收口，且全程计入合并护栏）

#### Scenario: 换主窗等待自愈

- **WHEN** 等待方 `expected_phase=5` 挂起期间集群换主，新相位 6 的变异条目已提交，旧 Leader 的通知随进程灭失
- **THEN** SDK 经车道激活/保活通道重挂 `AWAIT_ADVANCE(5)`，新 Leader 即刻回 `OK{phase=6}`——等待无损耗醒转；该差异（对照条件 signal 丢失窗）在 Javadoc 声明且 E2E 钉住

#### Scenario: 配额严格归属与构造注册

- **WHEN** 会话 A `newPhaser(key, 3)` 后，未注册的会话 B 对其 key 调用 `arriveAndDeregister()`；随后会话 A 第三次 `arriveAndDeregister()` 后又追加一次
- **THEN** B 被 `INVALID_REQUEST`（零配额拒绝，账簿零扰动）；A 的第三次离场正常（registered 归 0、空转）、第四次的配额透支同样被拒——JDK 匿名 party 的未定义行为在本 SDK 显式化为拒绝

#### Scenario: 陈旧路由但存在真主时等待重道了结

- **WHEN** 等待方 `awaitAdvanceInterruptibly` 首发落在已非权威的旧路由上收到 `NOT_LEADER`，而集群在重道预算内已给出当值 Leader
- **THEN** 客户端重取路由、以新会话重发该等待，并在预算内按新 Leader 的即刻判定以 `OK{相位}` 了结（不因路由陈旧而失败）

#### Scenario: 无真主时等待有界失败

- **WHEN** 路由始终陈旧且集群在重道预算内未给出当值 Leader
- **THEN** 客户端在该预算耗尽后以 `NOT_LEADER` 显式抛出，MUST NOT 伪造相位读数、MUST NOT 悬挂至等待总预算

### Requirement: OTimer API

客户端 SDK SHALL 提供 `OTimer` 公开契约与 `OClient.newTimer(key)` 工厂，对应"延时触发（定时单次标记）"原语（JDK `java.util.Timer` 的协调面可判定子集，协议 v11）。契约要求：

- **构造零网络**：句柄创建不发起任何请求（判例 `newPhaser`/`newTopic`）；无初始装载参数——装载恒为显式 `schedule` 操作（对照 `newPhaser(initialParties)`：timer 无"构造即预装"的语义来源）。
- **API 面**：`long schedule(long delay, TimeUnit unit)`（装载/重装载，返回本代代次回显；`delay < 0` 或超 horizon 由服务端拒绝映射异常，`delay == 0` 即立即可共见）；`void disarm()`（撤销装载，幂等；无条目 `INVALID_REQUEST` 映射异常）；`void await()`（等待当代标记；当代被撤销时以 `OpenLatchException`（`DENIED` 终态映射）异常收束——"等待不可满足"为异常终态而非布尔混读，契约成文）；`boolean await(long timeout, TimeUnit unit)`（`true`=见到当代标记；`false`=超时；当代被撤销 → 同 `await()` 的异常收束）；`boolean isFired()` / `boolean isArmed()` / `long getRemainingMillis()`（`QUERY` 读数映射，advisory——即刻过期是契约，判例 `getPhase()` 语言事实句；`getRemainingMillis()` 为客户端本地差值）。不暴露 `cancel(await)` 面（超时/中断自救内部发 `CANCEL` fire-and-forget，判例 v9/v10）。
- **周期与任务为显式不提供**：无 `scheduleAt(Instant)`（客户端墙钟偏差注入即污染谓词判据——仅收相对延迟，绝对到期恒由服务端应用点折算，v7 `offerDelayed` 同轴）；无周期重挂参数（服务端周期自触=常驻重发变异，恰与"到期零条目"裁决相反；客户端循环 `schedule` 即等价，差异声明入降级清单）；无任务载荷/回调（服务端不执行用户代码——v10 D4 onAdvance 同轴论证）。
- **单次与粘滞共见**：一次装载恰一发——到期后标记对一切到达者共见（含 fire 之后新到的 `await`/`isFired` 即刻通过），直至 `disarm`（代终结、此后等待者收 `DENIED` 终态/异常）或再 `schedule`（换代清钟、开新一轮）。相对 JDK `TimerTask` 的"跑完即弃"差异成文：本原语的"用完"判据是观察面的代次推进，非消费动作。
- **改期以最新代为准**：`schedule` 重装载换代清钟——已挂起的 `await` 以最新代到期时刻为准（推后多睡/提前先响皆合法形态，无"旧代承诺"补偿）；推荐 timed await 自救（判例条件"无人 signal 则永睡"运维提示同轴）。
- **死亡不撤钟（增强声明）**：装载方进程/会话死亡不影响触发——钟照响、标记照共见（账簿绑定 key；与 Barrier 破障、Phaser 摘除并列为死亡语义第三形态，接口级 Javadoc 对照呈现）；等待方自身死亡仅失其订阅位。
- **等待闭环与自动重挂**：`AWAIT` 收 `QUEUED` 后按 request_id 过滤 `AWAIT_NOTIFY` 挂起、以**原 request_id** 重发取终态（`OK`/`DENIED`）；活跃等待项经双通道自动重挂（车道激活事件+周期保活，判例 v8/v10）——谓词在复制账簿，**换主/断连重挂无损：已到期即刻了结、未到期续挂**（与 v9 条件 signal 丢失窗的对照防混读句随行）；重挂 MUST NOT 误换 request_id（v10 过程真缺陷教训的本原语复现钉死——装载去重槽面防假象双换代）。超时/中断 → `CANCEL` fire-and-forget → 抛对应异常（`TimeoutException`/`InterruptedException` 形态沿既有 SDK 纪律）。等待面收到 `NOT_LEADER` **应答状态**时按零生效处置：MUST 摘除本机等待登记、在独立短预算（= 2 个 Leader 核对周期）内等待路由改道，**仅当路由会话确已变化**时以新会话重发（跨会话必然换 `request_id`——与本节"重挂 MUST NOT 误换 request_id"的护栏不冲突：那条约束的是**同一会话**的保活重挂）；预算耗尽或路由始终未变则以 `NOT_LEADER` 显式抛出（`await()` 形态 MUST NOT 伪作"已响"、MUST NOT 悬挂至等待总预算）。
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

#### Scenario: 陈旧路由但存在真主时等待重道了结

- **WHEN** 等待方 `await(timeout)` 首发落在已非权威的旧路由上收到 `NOT_LEADER`，而集群在重道预算内已给出当值 Leader
- **THEN** 客户端重取路由、以新会话重发该等待，并在预算内按到期与否如实收束（已到期 `true`、未到期续挂）

#### Scenario: 无真主时等待有界失败

- **WHEN** 路由始终陈旧且集群在重道预算内未给出当值 Leader
- **THEN** 客户端在该预算耗尽后以 `NOT_LEADER` 显式抛出，MUST NOT 伪作"已响"、MUST NOT 悬挂至等待总预算
