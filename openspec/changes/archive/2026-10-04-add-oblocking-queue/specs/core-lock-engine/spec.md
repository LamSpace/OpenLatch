## MODIFIED Requirements

### Requirement: 条目家族定型与类型不匹配拒绝

key 条目 SHALL 由首次创建它的请求定型所属家族：LOCK（`REENTRANT`/`SIMPLE`/`READ`/`WRITE`/`FAIR`）、SEMAPHORE、LATCH、ATOMIC（`ATOMIC_LONG`/`ATOMIC_INTEGER`/`ATOMIC_BOOLEAN`/`ATOMIC_REFERENCE` 四形态同族共享条目类型，形态间互斥——条目定型后形态不可变更）、BARRIER、QUEUE（`LOCK_TYPE_QUEUE`/`LOCK_TYPE_DELAY_QUEUE` 两形态同族共享条目类型，形态间互斥——出队规则由定型形态决定：到达序 vs 最早到期序）。`FAIR` 与 `REENTRANT` 同为锁家族且语义等价（互通互认，可相互重入）。跨家族请求 MUST 返回结果 `REJECT_TYPE_MISMATCH` 且 MUST NOT 改变条目任何状态；同跨形态的 ATOMIC 请求（如 `ATOMIC_LONG` 条目上请求 `ATOMIC_INTEGER`，或标量形态条目上请求 `ATOMIC_REFERENCE` 及反向）与同跨形态的 QUEUE 请求（`QUEUE` 条目上请求 `DELAY_QUEUE` 及反向）MUST 同样返回 `REJECT_TYPE_MISMATCH`；server 层将该结果映射为协议 `INVALID_REQUEST`。锁家族既有条目的定型规则（可重入性由首次请求决定）MUST NOT 变化。

#### Scenario: 跨家族请求被拒

- **WHEN** 以 `REENTRANT` 建立并持有的 key 上发起 `SEMAPHORE`、`LATCH_AWAIT`、`ATOMIC_OP`、`BARRIER_AWAIT` 或 `QUEUE_OP` 请求
- **THEN** 返回类型不匹配拒绝，原持有者、租约与等待队列逐项不变

#### Scenario: FAIR 与 REENTRANT 互通

- **WHEN** 归属已以 `REENTRANT` 持有 key，再以 `FAIR` 类型重复获取
- **THEN** 按可重入语义授予（计数 +1），视同同类型重入

#### Scenario: 同族跨形态被拒

- **WHEN** 已以 `ATOMIC_LONG` 建立并写入的 key 上发起 `lock_type = ATOMIC_INTEGER` 的 ATOMIC 操作，或已以标量形态定型的 key 上发起 `lock_type = ATOMIC_REFERENCE` 的操作（及反向），或已以 `QUEUE` 形态建立并写入的 key 上发起 `lock_type = DELAY_QUEUE` 的队列操作（及反向）
- **THEN** 返回 `REJECT_TYPE_MISMATCH`，值/元素队列与版本戳逐项不变

#### Scenario: LATCH 与 BARRIER 互斥

- **WHEN** 已以 `LATCH_AWAIT` 定型的 key 上发起 `BARRIER_AWAIT`，或反向
- **THEN** 返回 `REJECT_TYPE_MISMATCH`，既有条目状态零扰动

## ADDED Requirements

### Requirement: QueueEntry 有界队列操作与每会话去重槽

引擎 SHALL 支持 QUEUE 家族的队列条目：定型形态（`QUEUE`/`DELAY_QUEUE`）、定型容量（`capacity > 0`，由首次携带非零主张的 PUT 建条目时定型——对不存在 key 的 PUT 携带 `capacity ≤ 0` MUST 返回 `REJECT_QUEUE_CAPACITY`（server 层映射 `INVALID_REQUEST`；协调面无无界队列），对既有条目的主张与定型值不符 MUST 返回 `REJECT_QUEUE_CAPACITY` 且条目零扰动，0 为不主张）、元素双端队列（每项含不透明字节载荷与绝对到期时刻）与**每会话去重槽**（`session → (op_seq, op, 已交付回执)`）。操作判定顺序（首个命中者即为结果）：会话存在校验 → key 校验 → 家族/形态互拒 → 容量断言 → 写操作去重判定 → 执行。操作语义：

- `PUT(e, blocking, delay?)`：深度 < capacity → 入队（QUEUE 形态按到达序追加且到期时刻无意义；DELAY 形态按到期时刻升序插入、**同到期时刻内保持到达 FIFO——相对 JDK `DelayQueue` 的语义增强，契约注释 MUST 显式声明**）、写槽回执并返回 `GRANTED`，并使容量减一；满且 `blocking` → 挂入等待轨回 `QUEUED`（位次 1 起，等待深度超 `maxQueueDepthPerKey` 回 `REJECT_QUEUE_FULL`）；满且非阻塞 → `DENIED` 零变更。
- `TAKE(blocking)`：存在可消费头元素（QUEUE 形态恒真；DELAY 形态要求头元素到期时刻不晚于当前判定时刻——**未到期头 = 不可见，其后已到期元素不得越过它先出**）→ 摘出队首、写槽回执（含交付字节）、返回 `GRANTED` 并携带元素；无 → 阻塞挂起回 `QUEUED` / 非阻塞回 `DENIED`。
- `DRAIN(maxN)`：立即式（阻塞位 MUST NOT 为 true），摘出 `min(maxN, 自队首起连续满足谓词段)` 项按出队序返回列表（空队回 `GRANTED` + 空列表，不报错），写槽回执（含交付列表）。
- `PEEK`：与 TAKE 同谓词的头元素读数（未到期/空队回 null），零迁移、不建条目、不去重。
- `SIZE`：当前元素总数读数（DELAY 形态含未到期项——可见元素数由 `PEEK`/消费侧判定，SIZE 为驻留口径），零迁移、不建条目（key 不存在回 0）、不去重。

去重（写操作携会话内单调 `op_seq`）：条目记每会话最近已应用写操作的 `(op_seq, op, 回执)` 单槽——同会话同 `op_seq` 重放 MUST 直接返回原回执且 MUST NOT 重复入队/出队（`TAKE`/`DRAIN` 的重放回执 MUST 重发**同一份已交付字节**，MUST NOT 再次摘取元素）；同会话新 `op_seq` 覆盖本会话槽位（客户端"同 key 在途写互斥"纪律保证被覆盖槽必已了结）；他会话槽互不遮蔽；`PEEK/SIZE` 的 `op_seq=0` 不参与去重。唤醒收集：成功入队 MUST 收集该 key take 轨队首（DELAY 形态仅当队首已到期）至通知列表；成功出队（TAKE/DRAIN）使容量释放 MUST 收集 put 轨队首至通知列表——两轨判定与收集在条目锁内完成，通知触发在条目锁外（判例锁/Latch/Barrier）。全部操作 MUST 校验会话存在（不符返回 `REJECT_SESSION`），但队列 MUST NOT 将 key 登记入会话触及集的持有面（元素不绑定归属；挂起等待随 `removeSession` 摘除）。条目与引擎 MUST NOT 以本地配置复核载荷字节数/容量/批量上限——尺寸与钳制属接入层专属判定点（判例 v6，节点本地配置参与 apply 判定会引入回放分歧）。

#### Scenario: 满容量阻塞挂起与立即拒绝

- **WHEN** 容量 2 的空队列先后 `PUT(a)`、`PUT(b)` 成功，随后 `PUT(c, blocking=true)` 与 `PUT(d, blocking=false)`
- **THEN** c 挂起回 `QUEUED`（位次 1），d 回 `DENIED` 且队列不变；此后一次 `TAKE` 成功出队 MUST 唤醒 c（d 早已终结不受影响）

#### Scenario: 空队消费立即拒绝与阻塞挂起

- **WHEN** 空队列上 `poll` 式 `TAKE(blocking=false)` 与 `take` 式 `TAKE(blocking=true)` 先后到达
- **THEN** 前者回 `DENIED`；后者挂起回 `QUEUED`；随后 `PUT(x)` 提交 MUST 唤醒该等待者且其重发取回 x

#### Scenario: 同 op_seq 重放不双插不偷吃

- **WHEN** 会话 A 以 `op_seq=7` 的 `PUT(4KB 元素)` 应用成功后应答丢失，同会话以完全相同请求重发；随后会话 B 亦写入一元素，会话 A 再以 `op_seq=8` 的 `TAKE` 成功取走一元素后重发同请求
- **THEN** PUT 重放返回原回执（深度不因重发 +1）；TAKE 重放返回**同一份已交付字节**且深度不再减一（不偷吃下一个元素）

#### Scenario: 跨会话槽互不遮蔽

- **WHEN** 会话 A 与会话 B 先后各自完成 `PUT`（各占本会话槽），随后 A 以原 `op_seq` 重发
- **THEN** A 命中 A 槽重放回执（B 的写入不影响 A 的去重判定），元素不双插

#### Scenario: DELAY 未到期货不可见与同到期 FIFO

- **WHEN** 延时队列依次注入 +5s、+2s、+2s 三个元素（到期时刻后到者与之相等），在 t=+1s 执行 `PEEK` 与 `SIZE`，t=+2s 起连续 `TAKE` 两次
- **THEN** t=+1s：`PEEK` 回 null（头元素未到期，+5s 元素不得越过 -2s 未到期头先出）、`SIZE` 回 3（驻留口径含未到期）；t=+2s 两次 TAKE 按注入到达序取回两个 +2s 元素（同到期 FIFO 增强），第三元素仍不可见

#### Scenario: 容量断言与跨形态互拒

- **WHEN** 不存在 key 上 `PUT(capacity 主张=0)`；既有容量 4 条目上 `PUT(capacity 主张=8)`；既有 `QUEUE` 形态条目上 `DELAY_QUEUE` 形态请求
- **THEN** 三者分别回 `REJECT_QUEUE_CAPACITY`、`REJECT_QUEUE_CAPACITY`、`REJECT_TYPE_MISMATCH`，条目与元素逐项零扰动

#### Scenario: DRAIN 空列表与部分摘取

- **WHEN** 深度 3 的队列执行 `DRAIN(maxN=5)`，随后空队列执行 `DRAIN(maxN=5)`
- **THEN** 前者回 3 元素列表（出队序）且队清空；后者回 `GRANTED` + 空列表（不报错、不挂起）

#### Scenario: 条目侧不复核配置限额

- **WHEN** 以较小 `maxValueBytes`/`maxQueueCapacity` 配置的节点对日志中既有的超限尺寸元素/容量执行回放 apply
- **THEN** 照常入队落态不因本地配置拒绝（钳制仅属接入层；回放结果与 Leader 一致）

### Requirement: QUEUE 条目生命周期与会话无关

队列条目的元素 SHALL 绑定 key 而非会话：元素生命周期与投递者/任何消费者的会话存续无关——会话关闭 MUST NOT 摘除、改写或回收任何元素（`SESSION_CLOSE` 仅摘除该会话的挂起等待项与其去重槽）；进程死亡后元素照常对其余会话可见并可消费（相对 JDK 队列的分布式增强声明点：JDK 内同进程死亡随堆消散，协调面元素为复制状态、投递者消亡不吞元素）。队列条目 MUST NOT 具备租约语义（到期字段恒 0、不入租约到期堆、不被租约到期清扫回收）；元素的 `expires_at_ms` 为**消费可见性判据**而非条目回收判据——未消费元素 MUST NOT 因到期被服务端删除（与 JDK `DelayQueue` 本体一致，条目常驻不回收判例延伸）。`isEmpty()` 对队列条目 MUST 恒为 false（元素清零不触发条目回收，判例 Latch/ATOMIC/BARRIER 常驻条款）；key 清理仅经显式消费清空或运维处置（不做清单口径：服务端无自动回收路径）。

#### Scenario: 投递者死亡元素存续

- **WHEN** 会话 A 向队列 `PUT` 两个元素后进程被杀（会话关闭传播），会话 B 执行 `SIZE` 与 `TAKE`
- **THEN** `SIZE` 回 2、`TAKE` 按 FIFO 取回 A 投递的元素——元素逐项保真，无因 A 死亡产生的丢失或回滚

#### Scenario: 等待者死亡仅摘等待

- **WHEN** 会话 C 在满队列上 `PUT` 挂起（`QUEUED`）后被杀，其余挂起者与在队元素状态如何
- **THEN** C 的挂起等待项与其去重槽被摘除，队列元素与他会话等待逐项不变，队首推进正常唤醒下一等待者

#### Scenario: 到期清扫与租约机制对队列零触碰

- **WHEN** 租约到期扫描经过含队列条目（含 DELAY 形态元素全部到期）的时刻
- **THEN** 队列条目不被回收、元素不因到期消失；条目无租约凭证、不入到期堆
