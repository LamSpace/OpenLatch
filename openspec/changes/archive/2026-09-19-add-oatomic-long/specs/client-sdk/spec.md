## ADDED Requirements

### Requirement: OAtomicLong 族 API

客户端 SHALL 提供 `OpenLatchClient.newAtomicLong(String key)`、`newAtomicInteger(String key)`、`newAtomicBoolean(String key)` 工厂与携带非零初值主张的对应重载（`newAtomicLong(key, initial)`——首建生效、与既有条目定型初值冲突时抛 `OpenLatchException`，语义映射服务端初值断言），分别返回公开接口 `OAtomicLong`、`OAtomicInteger`、`OAtomicBoolean` 实例。`OAtomicLong` 面：`long get()`、`void set(long)`、`long getAndSet(long)`、`long incrementAndGet()`、`long addAndGet(long delta)`、`boolean compareAndSet(long expected, long update)`（值 CAS，ABA 风险由契约声明）、`getStamped()`（返回接口内嵌 `Stamped` 读数记录，字段为值与版本戳）、`long getVersion()`、`boolean compareAndSetStamped(long expectedValue, long expectedVersion, long update)`（ABA-free 形态）、`long accumulateAndGet(long identity, LongBinaryOperator accumulatorFunction)`（客户端携带版本戳 CAS 循环，与 JDK 内部实现同构）。`OAtomicInteger`/`OAtomicBoolean` 为对应标量形态的同构面（int32 域 wrap / {true,false} 值域）。同步方法 SHALL 声明 `throws InterruptedException`（本地等待可中断），失败以非受检 `OpenLatchException`/`OpenLatchTimeoutException` 表达。全部操作 MUST NOT 依赖看门狗、租约与 `LockLost` 通知（无持有语义）；`get` 与写操作均为请求-应答即时路径，MUST NOT 进入等待-通知-重发闭环。可重试失败（`NOT_LEADER`、断连快速失败、超时）时写操作 SDK MUST 以**同一 `op_seq`** 自动重发至请求总超时界限——去重槽保证重发不重复生效；界限内仍不可判定则抛 `OpenLatchTimeoutException`，此时契约声明该操作效果不确定（值可能已变也可能未变，`getStamped` 可复核）。会话或进程死亡 MUST NOT 回滚本会话写入的值。

#### Scenario: 六操作与 stamped 读数往返

- **WHEN** 客户端对同一 key 依次执行 set、get、incrementAndGet、addAndGet、getAndSet、compareAndSet（命中与不命中各一次）、getStamped
- **THEN** 各返回值与串行语义一致；失败的 CAS 不推进版本戳；getStamped 读数等于服务端 (value, version)

#### Scenario: 换主窗口写操作自动重发不双加

- **WHEN** `incrementAndGet` 首发落入 Leader 切换窗口收到 `NOT_LEADER`，SDK 经重发现向新 Leader 以同 `op_seq` 重发
- **THEN** 方法正常返回、值恰 +1（新值 = 旧值 + 1，版本恰 +1）

#### Scenario: accumulateAndGet 争用有界重试

- **WHEN** 两客户端对同 key 并发执行 `accumulateAndGet(0, (a,b) -> a+b)` 各 100 次
- **THEN** 全部成功返回且终值为两端增量之和（CAS 循环在界限内收敛）；循环超出重试界限时抛 `OpenLatchException` 而非静默丢更新

#### Scenario: 会话死亡值存续

- **WHEN** 会话 A 写入值后关闭，会话 B 读取同 key
- **THEN** B 读到 A 写入后的值与推进后的版本戳，无任何回滚
