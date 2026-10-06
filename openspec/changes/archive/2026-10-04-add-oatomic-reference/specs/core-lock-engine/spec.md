## MODIFIED Requirements

### Requirement: 条目家族定型与类型不匹配拒绝

key 条目 SHALL 由首次创建它的请求定型所属家族：LOCK（`REENTRANT`/`SIMPLE`/`READ`/`WRITE`/`FAIR`）、SEMAPHORE、LATCH、ATOMIC（`ATOMIC_LONG`/`ATOMIC_INTEGER`/`ATOMIC_BOOLEAN`/`ATOMIC_REFERENCE` 四形态同族共享条目类型，形态间互斥——条目定型后形态不可变更）、BARRIER。`FAIR` 与 `REENTRANT` 同为锁家族且语义等价（互通互认，可相互重入）。跨家族请求 MUST 返回结果 `REJECT_TYPE_MISMATCH` 且 MUST NOT 改变条目任何状态；同跨形态的 ATOMIC 请求（如 `ATOMIC_LONG` 条目上请求 `ATOMIC_INTEGER`，或标量形态条目上请求 `ATOMIC_REFERENCE` 及反向）MUST 同样返回 `REJECT_TYPE_MISMATCH`；server 层将该结果映射为协议 `INVALID_REQUEST`。锁家族既有条目的定型规则（可重入性由首次请求决定）MUST NOT 变化。

#### Scenario: 跨家族请求被拒

- **WHEN** 以 `REENTRANT` 建立并持有的 key 上发起 `SEMAPHORE`、`LATCH_AWAIT`、`ATOMIC_OP` 或 `BARRIER_AWAIT` 请求
- **THEN** 返回类型不匹配拒绝，原持有者、租约与等待队列逐项不变

#### Scenario: FAIR 与 REENTRANT 互通

- **WHEN** 归属已以 `REENTRANT` 持有 key，再以 `FAIR` 类型重复获取
- **THEN** 按可重入语义授予（计数 +1），视同同类型重入

#### Scenario: 同族跨形态被拒

- **WHEN** 已以 `ATOMIC_LONG` 建立并写入的 key 上发起 `lock_type = ATOMIC_INTEGER` 的 ATOMIC 操作，或已以标量形态定型的 key 上发起 `lock_type = ATOMIC_REFERENCE` 的操作（及反向）
- **THEN** 返回 `REJECT_TYPE_MISMATCH`，值与版本戳逐项不变

#### Scenario: LATCH 与 BARRIER 互斥

- **WHEN** 已以 `LATCH_AWAIT` 定型的 key 上发起 `BARRIER_AWAIT`，或反向
- **THEN** 返回 `REJECT_TYPE_MISMATCH`，既有条目状态零扰动

## ADDED Requirements

### Requirement: AtomicRefEntry 有值引用操作与版本戳

引擎 SHALL 支持 ATOMIC 家族的有值引用形态条目：装定型形态（`ATOMIC_REFERENCE`）、当前值（`byte[]`，null 与零长度空数组为两个可区分的合法值）、单调版本戳（`version`，自 0 起）与初值记录（`initial`，null 表示建条目时无主张）。操作集为 `GET / SET / GET_AND_SET / CAS / CAS_STAMPED`（`ADD` 对该形态为值域外操作，MUST 返回 `REJECT_ATOMIC_RANGE` 且条目零扰动，判例布尔形态 ADD 拒绝）。版本戳契约与标量形态逐项一致：任一成功改变值的写操作 MUST 使 `version` 恰 +1；`GET` MUST NOT 改变值与版本戳；失败的 CAS/CAS_STAMPED MUST NOT 改变值与版本戳。值比较 MUST 为字节内容相等判定。各操作语义：`SET(x)` 落 `x` 返回旧值（`x` 可为 null——清空语义）；`GET_AND_SET(x)` 同 `SET` 并返回旧值；`CAS(e,x)` 当且仅当当前值与 `e` 字节相等（含 null==null）时落 `x` 并回 `applied=true`，否则 `applied=false` 并回当前值；`CAS_STAMPED(e,ev,x)` 当且仅当值匹配且 `version==ev` 时落 `x`（ABA-free 形态）。应答 MUST 携带 `(applied, old_value, value, version)` 四元组，其中载荷读数为字节串（可为 null）。建条目走"非零主张"判例的 presence 形态：写操作对不存在的 key 携带初值主张（`initial` 非 null，含空数组主张）时 MUST 以该值创建条目并紧接应用本操作；对既有条目携带的主张与定型初值字节比对不符 MUST 返回 `REJECT_ATOMIC_INIT`（条目状态零扰动，server 层映射 `INVALID_REQUEST`）；不主张时以 null 创建。`GET` 对不存在的 key MUST 返回 `(null, 0)` 且 MUST NOT 创建条目。去重：写操作携带会话内单调 `op_seq`，条目记最近已应用写操作的 `(session, op_seq, 应答四元组)` 单槽（槽内载荷读数为字节串）；同槽重放请求 MUST 直接返回原应答且 MUST NOT 重复推进版本戳；`GET` 的 `op_seq=0` 不参与去重。全部操作 MUST 校验会话存在（不符返回 `REJECT_SESSION`），但 MUST NOT 将 key 登记入会话触及集。条目与引擎 MUST NOT 以本地配置复核载荷字节数——尺寸钳制是接入层的专属判定点，进入状态机的命令视为已钳制（节点本地配置参与 apply 判定会引入跨副本回放分歧）。生命周期条款（无租约、不入到期堆、会话关闭零触碰、`isEmpty()` 恒 false、常驻不回收）沿用 ATOMIC 家族既有条款，对两种形态一体适用。

#### Scenario: CAS 字节比较命中与不命中

- **WHEN** 值为字节串 `"ab"`、版本 3 的条目上先后执行 `CAS("ab", "cd")` 与 `CAS("ab", "ef")`（参数均为字节串）
- **THEN** 前者回 `applied=true, old="ab", value="cd", version=4`；后者因当前值已变回 `applied=false`、读数 `("cd", 4)`，值与版本不变

#### Scenario: null 与空串两形态可区分

- **WHEN** 依次执行 `SET(null)`、`GET`、`CAS(null, "x")`、`SET(空数组)`、`CAS(null, "y")`
- **THEN** 第一步后读数为 null 且版本推进；`CAS(null,…)` 命中 null 态；置空数组后 `CAS(null,…)` 不命中（空串≠null），回 `applied=false`

#### Scenario: 版本戳恰进与 GET 零迁移

- **WHEN** 对同一条目连续执行 SET、GET、CAS_STAMPED 成功各一次
- **THEN** version 依次为 1、1、2；GET 读数与之前一致且未推进版本；key 不存在时 GET 返回 `(null, 0)` 且不建条目

#### Scenario: 同 op_seq 重发载荷不双写

- **WHEN** 携带 `(session, op_seq=7)` 的 `SET(4KB 字节串)` 应用成功后，同会话以完全相同请求重发
- **THEN** 返回与原应答一致的载荷四元组（old 字节级一致），值与版本戳不再推进

#### Scenario: 初值主张冲突与 ADD 越域拒绝

- **WHEN** key 不存在时 `SET("a", initial 主张="b")` 成功；另一请求携带 `initial 主张="c"` 作用于既有条目；再有请求对该条目携带 `ADD`
- **THEN** 第一次以 "b" 为初值基准建条目并落 "a"（version=1）；主张冲突返回 `REJECT_ATOMIC_INIT` 零扰动；ADD 返回 `REJECT_ATOMIC_RANGE`，值与版本零扰动

#### Scenario: 条目侧不复核超限载荷

- **WHEN** 以 `maxValueBytes` 配置为较小值的节点，对日志中既有的更大载荷条目执行回放 apply
- **THEN** apply 正常落值不因本地配置拒绝（尺寸判定仅属接入层；回放结果与 Leader 一致）

#### Scenario: 会话关闭与到期扫描对载荷零触碰

- **WHEN** 会话 A 写入有值引用条目后关闭，或到期扫描经过含该条目的时刻
- **THEN** 值、版本戳与去重槽逐项不变；条目不入到期堆、不因收尾回收
