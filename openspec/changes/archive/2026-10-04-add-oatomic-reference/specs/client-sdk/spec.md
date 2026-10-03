## ADDED Requirements

### Requirement: OAtomicReference API

客户端 SHALL 提供 `OpenLatchClient.newAtomicReference(String key)` 工厂与携带初值主张的对应重载（`byte[]` 形态与 UTF-8 `String` 便利形态各一；主张值为 null 等价于不主张——条目初值即为 null，零长度数组为"空串初值主张"），返回公开接口 `OAtomicReference` 实例。`byte[]` 为主形态、`String` 便利形态按 UTF-8 编解码且两形态语义逐项等价。接口面：`byte[] get()`（可返回 null）、`String getAsString()`、`Stamped getStamped()`（内嵌读数记录：`byte[] value()` 可为 null、`long version()`，附 `String valueAsString()` 便利）、`long getVersion()`、`void set(byte[] value)`/`set(String value)`（null=清空）、`byte[] getAndSet(byte[] value)`（+String 形态）、`boolean compareAndSet(byte[] expected, byte[] update)`（字节内容相等判定，null 为合法期望态；ABA 风险由契约声明）、`boolean compareAndSetStamped(byte[] expectedValue, long expectedVersion, byte[] update)`（ABA-free 形态；另各配 String 便利重载）。接口 MUST NOT 提供 `ADD`/`accumulateAndGet` 类算术面（相对 JDK `AtomicReference` 无降级的缺省——JDK 本无此面）。载荷为不透明字节串：SDK 与服务端 MUST NOT 对载荷做解释或对象反序列化。同步方法 SHALL 声明 `throws InterruptedException`，失败以非受检 `OpenLatchException`/`OpenLatchTimeoutException` 表达；服务端以 `INVALID_REQUEST` 拒绝超限载荷时 SDK MUST 抛 `OpenLatchException` 并透传拒绝语义，MUST NOT 静默截断或本地重试。可重试失败（`NOT_LEADER`、断连快速失败、超时）时写操作 MUST 以**同一 `op_seq`** 自动重发至请求总超时界限（沿用 `OAtomicLong` 族裁决的现成车道，去重槽保证重发不重复生效）；界限内不可判定抛 `OpenLatchTimeoutException` 并声明效果不确定。会话或进程死亡 MUST NOT 回滚本会话写入的值。全部操作 MUST NOT 依赖看门狗/租约/`LockLost` 通知，MUST NOT 进入等待-通知-重发闭环。有值引用形态 MUST 以 v6 握手使用（SDK 握手版本声明升至 6）；对握版本 <6 的服务端，引用形态请求收到 `INVALID_REQUEST` 时 SDK MUST 以显式异常表达协议不支持，MUST NOT 以同序号重发。契约 Javadoc MUST 显式声明降级/增强清单：每操作一次网络往返、超时不确定窗口、ABA 仅 stamped 消除、null 与空串可区分、载荷上限由服务端权威钳制（默认 4KB、随部署配置）、条目常驻不回收（载荷持久驻留）。

#### Scenario: 三形态载荷往返保真

- **WHEN** 对同一 key 依次 `set(字节串)`、`set(String)`、`set(null)` 并各以 `get`/`getStamped` 读取，随后 `compareAndSet(null, "x")` 与 `compareAndSet(new byte[0], "y")`
- **THEN** 各读数与写入字节逐一一致（String 形态按 UTF-8 往返）；`set(null)` 后读数为 null；期望 null 的 CAS 命中、期望空串的 CAS 不命中（两态可区分）

#### Scenario: 超限载荷显式失败零生效

- **WHEN** 客户端 `set` 携带超过服务端 `maxValueBytes` 的载荷
- **THEN** 抛 `OpenLatchException`（拒绝语义、非超时），条目值与版本戳不变，且无同序号重发发生

#### Scenario: 换主窗口写操作自动重发不双写

- **WHEN** `getAndSet(4KB 字节串)` 首发落入 Leader 切换窗口收到 `NOT_LEADER`，SDK 经重发现向新 Leader 以同 `op_seq` 重发
- **THEN** 方法正常返回旧值，服务端该 key 版本戳恰 +1，无重复落值

#### Scenario: 低版本服务端显式拒绝不重试

- **WHEN** v6 SDK 对握版本上限 <6 的服务端调用引用形态 `get`
- **THEN** SDK 以显式异常表达协议不支持（源于 `INVALID_REQUEST` 消息级拒绝），MUST NOT 自动重发或静默降级

#### Scenario: 会话死亡值存续

- **WHEN** 会话 A 写入有值引用后关闭，会话 B 读取同 key
- **THEN** B 读到 A 写入后的值与推进后的版本戳，无任何回滚
