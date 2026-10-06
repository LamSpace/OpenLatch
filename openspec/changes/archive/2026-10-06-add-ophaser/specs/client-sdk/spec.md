# Spec Delta

## ADDED Requirements

### Requirement: OPhaser API

客户端 SDK SHALL 提供 `OPhaser` 公开契约与 `OClient.newPhaser(String key)` / `newPhaser(String key, int initialParties)` 工厂：句柄构造零网络；`initialParties > 0` 时首次业务操作前同步提交一次 `REGISTER(count = initialParties)` 归属本会话（JDK `new Phaser(n)` 的注册语义对偶——构造后 n 方已注册未到场；注册失败按抛出形态传播，同句柄不重复提交）。API 面：`int register()` / `void bulkRegister(int parties)`（返回/回显到场相位——注册者进入当前相位应到集合）、`int arrive()`、`int arriveAndDeregister()`、`int arriveAndAwaitAdvance()`、`int awaitAdvance(int phase)`（无限挂起形态）、`int awaitAdvanceInterruptibly(int phase, long timeout, TimeUnit unit)`、`long getPhase()` / `int getRegisteredParties()` / `int getArrivedParties()` / `int getUnarrivedParties()`（JDK 返回值形态对齐：相位 long、计数 int）。**不提供** `onAdvance` 覆写钩子、`isTerminated()`/`forceTerminated()` 与父子分层构造——三砍为显式 Non-Goal，与 JDK `Phaser` 的这组差异 MUST 在接口级 Javadoc 降级清单逐条声明并给出应用侧替代惯用法（钩子→以 `arriveAndAwaitAdvance()` 返回的到场相位号判别后本地执行；终止→键级生命周期治理；分层→独立 key）。

车道与闭环：`PHASER_OP` 为直发请求-应答车道（判例 QUEUE/TOPIC/CONDITION——无 ACQUIRE 在途互斥，phaser 无持有概念；同请求超时重发以同 request_id 幂等重演，到场类去重槽、等待登记幂等与了结记录承载重放安全）。变异操作（register/arrive/arriveAndDeregister/arriveAndAwaitAdvance 的到场半程）经提交路径，客户端对 `NOT_LEADER` 执行既有退避改道；等待操作（awaitAdvance/arriveAndAwaitAdvance 的等待半程）收 `QUEUED` 后进入挂起环——按 request_id 过滤 `AWAIT_NOTIFY`（判例队列读车道与 v3 推送桥）、原 request_id 重发取数了结（`OK{相位}`）；`awaitAdvanceInterruptibly` 超时到期 MUST 发 `CANCEL` fire-and-forget 后抛 `TimeoutException`（JDK 对偶签名；`awaitAdvance(int)` 无限形态由等待总超时兜底——超时纪律条款延伸，MUST NOT 提供事实上永挂而无界收口的路径）；被中断（interrupt）同超时路径收束并抛 `InterruptedException`。换主/会话重建后 SDK MUST 以双通道自动重挂全部活跃等待项（车道激活事件 + 周期保活，判例 v8 topic 重挂；谓词在复制态——重挂即刻了结或续挂、**无损耗**，该增强与 v9 条件"换主窗 signal 丢失"的对照差异 MUST 在 Javadoc 与指南声明防混读）。

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
