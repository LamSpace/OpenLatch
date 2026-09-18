## MODIFIED Requirements

### Requirement: 条目家族定型与类型不匹配拒绝

key 条目 SHALL 由首次创建它的请求定型所属家族：LOCK（`REENTRANT`/`SIMPLE`/`READ`/`WRITE`/`FAIR`）、SEMAPHORE、LATCH、ATOMIC（`ATOMIC_LONG`/`ATOMIC_INTEGER`/`ATOMIC_BOOLEAN` 三形态同族共享条目类型，形态间互斥——条目定型后形态不可变更）。`FAIR` 与 `REENTRANT` 同为锁家族且语义等价（互通互认，可相互重入）。跨家族请求 MUST 返回结果 `REJECT_TYPE_MISMATCH` 且 MUST NOT 改变条目任何状态；同跨形态的 ATOMIC 请求（如 `ATOMIC_LONG` 条目上请求 `ATOMIC_INTEGER`）MUST 同样返回 `REJECT_TYPE_MISMATCH`；server 层将该结果映射为协议 `INVALID_REQUEST`。锁家族既有条目的定型规则（可重入性由首次请求决定）MUST NOT 变化。

#### Scenario: 跨家族请求被拒

- **WHEN** 以 `REENTRANT` 建立并持有的 key 上发起 `SEMAPHORE`、`LATCH_AWAIT` 或 `ATOMIC_OP` 请求
- **THEN** 返回类型不匹配拒绝，原持有者、租约与等待队列逐项不变

#### Scenario: FAIR 与 REENTRANT 互通

- **WHEN** 归属已以 `REENTRANT` 持有 key，再以 `FAIR` 类型重复获取
- **THEN** 按可重入语义授予（计数 +1），视同同类型重入

#### Scenario: 同族跨形态被拒

- **WHEN** 已以 `ATOMIC_LONG` 建立并写入的 key 上发起 `lock_type = ATOMIC_INTEGER` 的 ATOMIC 操作
- **THEN** 返回 `REJECT_TYPE_MISMATCH`，值与版本戳逐项不变

## ADDED Requirements

### Requirement: AtomicEntry 原子操作与版本戳

引擎 SHALL 支持 ATOMIC 家族条目的操作集 `GET / SET / GET_AND_SET / ADD / CAS / CAS_STAMPED`，每个条目装形态（`kind`）、当前值（`value`，int64 域）、单调版本戳（`version`，自 0 起）与初值记录（`initial`）。版本戳契约：任一成功改变值的写操作 MUST 使 `version` 恰 +1；`GET` MUST NOT 改变值与版本戳；失败的 CAS/CAS_STAMPED/条件 ADD MUST NOT 改变值与版本戳。各操作语义：`SET(x)` 落 `x` 返回旧值；`GET_AND_SET(x)` 同 `SET` 并返回旧值；`ADD(d)` 落 `value+d`（int64 溢出按二进制补码 wrap，与 JDK 一致），可选携带 `expected_version`（>0 为断言：不符则以 `applied=false` 拒绝且不落值）；`CAS(e,x)` 当且仅当 `value==e` 时落 `x` 并回 `applied=true`，否则 `applied=false` 并回当前值；`CAS_STAMPED(e,ev,x)` 当且仅当 `value==e` 且 `version==ev` 时落 `x`（ABA-free 形态）。应答 MUST 携带 `(applied, old_value, value, version)` 四元组。`ATOMIC_INTEGER` 形态的全部落值 MUST 截断为 int32 域（溢出 wrap，与 JDK `AtomicInteger` 一致）；`ATOMIC_BOOLEAN` 形态值域 MUST 限于 {0,1}，SET/GET_AND_SET 携带越界 `operand`、CAS 携带越界 `expected` 或更新值 MUST 拒绝（`INVALID_REQUEST` 映射）且条目零扰动。建条目走"非零主张"判例：写操作对不存在的 key 携带非零 `initial_value` 时 MUST 以该值创建条目（`version=0` 后紧接应用本操作，`version` 终为 1）；`initial_value=0` 为不主张、以 0 创建；对既有条目携带的非零 `initial_value` 与定型初值不符 MUST 返回新结果 `REJECT_ATOMIC_INIT`（条目状态零扰动，server 层映射 `INVALID_REQUEST`）。`GET` 对不存在的 key MUST 返回 `(0, 0)` 且 MUST NOT 创建条目。去重：每一写操作携带会话内单调 `op_seq`，条目记最近已应用写操作的 `(session, op_seq, 应答四元组)` 单槽；同槽重放请求 MUST 直接返回原应答且 MUST NOT 重复推进版本戳；`GET` 的 `op_seq=0` 不参与去重。所有操作 MUST 校验会话存在（不符返回 `REJECT_SESSION`），但 MUST NOT 将 key 登记入会话触及集。

#### Scenario: CAS 成功与失败

- **WHEN** 值为 5、版本 3 的条目上先后执行 `CAS(5, 9)` 与 `CAS(5, 7)`
- **THEN** 前者回 `applied=true, old=5, value=9, version=4`；后者回 `applied=false, value=9, version=4`（值与版本均不变）

#### Scenario: 版本戳恰进与 GET 零迁移

- **WHEN** 对同一条目连续执行 SET、ADD(2)、GET、CAS_STAMPED 成功各一次
- **THEN** version 依次为 1、2、2、3；GET 读数与之前一致且未推进版本

#### Scenario: 条件 ADD 版本不符拒绝

- **WHEN** 版本 4 的条目上执行 `ADD(1, expected_version=3)`
- **THEN** 返回 `applied=false` 与当前 `(value, version=4)`，值不变、版本不推

#### Scenario: 同 op_seq 重发不双加

- **WHEN** 携带 `(session, op_seq=7)` 的 `ADD(1)` 应用成功后，同会话以完全相同请求重发
- **THEN** 返回与原应答一致的应答四元组，且值与版本戳不再推进

#### Scenario: 非零初值主张与冲突拒绝

- **WHEN** key 不存在时 `SET(5, initial_value=100)`；随后另一请求对既有条目携带 `initial_value=200`
- **THEN** 前者建条目并以 100 为初值与旧值基准、落值 5（version=1）；后者返回 `REJECT_ATOMIC_INIT`，条目零扰动

#### Scenario: 整数形态溢出 wrap

- **WHEN** `ATOMIC_INTEGER` 条目值为 `2147483647` 时执行 `ADD(1)`
- **THEN** 值落 `-2147483648`（int32 wrap），version +1

#### Scenario: 布尔形态越界拒绝

- **WHEN** `ATOMIC_BOOLEAN` 条目上执行 `SET(2)`
- **THEN** 请求以 `INVALID_REQUEST` 映射拒绝，值与版本零扰动

### Requirement: ATOMIC 条目生命周期与会话无关

ATOMIC 条目 SHALL 为无持有者、无租约、无等待队列的常驻状态单元：MUST NOT 进入租约到期堆，到期强制回收路径对其不可达；会话关闭清理 MUST NOT 改变其值、版本戳与去重槽（值不绑定 `(sessionId, threadId)` 归属——任何会话的创建、持有、死亡均不构成值的回滚理由）；`isEmpty()` 恒 false，条目一经创建 MUST NOT 随操作或会话收尾从条目表回收，存续至进程重启后按快照恢复。形态判别随条目创建定型，之后不可变更。

#### Scenario: 创建会话死亡值不变

- **WHEN** 会话 A 创建 ATOMIC 条目并写入值后，A 被关闭（主动或失联清理）
- **THEN** 条目值与版本戳逐项不变，其他会话可继续操作

#### Scenario: 到期扫描零触碰

- **WHEN** 到期扫描与队首清扫驱动经过含 ATOMIC 条目的时刻堆
- **THEN** 无到期堆登记、无清理动作发生于该条目
