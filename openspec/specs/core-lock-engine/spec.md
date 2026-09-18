# core-lock-engine Specification

## Purpose
提供与网络、协议、存储完全解耦的分布式锁语义引擎：锁的获取/释放/续租裁决、可重入与读写语义、严格 FIFO 的等待与通知、租约到期强制释放、会话生命周期清理、幂等去重与保护限额，使全部锁正确性可在纯单元环境中闭环验证。
## Requirements
### Requirement: 锁归属与会话校验

锁归属 MUST 由 `(sessionId, threadId)` 二元组唯一确定。引擎仅服务已登记的会话；未登记会话的任何请求 MUST 被拒绝（对应协议 `SESSION_EXPIRED`）。

#### Scenario: 未登记会话被拒

- **WHEN** 以一个从未登记（或已关闭）的会话发起获取/释放/续租
- **THEN** 引擎拒绝该请求并返回会话无效的结果

#### Scenario: 跨会话同线程不共享归属

- **WHEN** 会话 A 以 `threadId = t` 持有锁，会话 B 以相同 `threadId = t` 请求同一锁
- **THEN** 不视为重入，会话 B 按普通竞争者处理

### Requirement: key 校验与保护限额

空 key MUST 被拒绝；超过最大长度（默认 512 字节，按 UTF-8 计）的 key MUST 被拒绝。单 key 等待队列达到深度上限（默认 4096）后，新的排队请求 MUST 被拒绝（对应协议 `OVERLOADED`）。

#### Scenario: 非法 key 拒绝

- **WHEN** 以空字符串或超过 512 字节 UTF-8 长度的 key 请求获取
- **THEN** 引擎分别返回"key 为空"与"key 过长"的拒绝结果，不触碰锁表

#### Scenario: 队列深度超限

- **WHEN** 某 key 的等待队列已有 4096 个等待者，第 4097 个请求要求排队
- **THEN** 引擎拒绝入队并返回过载结果

### Requirement: 互斥授予

同一时刻一个锁键至多有一个写侧持有者。无持有者且无等待者时，获取请求 MUST 立即被授予，并返回租约凭证（`leaseToken`）与生效租约时长。

#### Scenario: 无竞争授予

- **WHEN** 会话 A 请求一个空闲锁键
- **THEN** 引擎立即授予，返回租约凭证与生效租约时长

#### Scenario: 竞争者排队

- **WHEN** 会话 A 持有锁键，会话 B 以可排队方式请求同一锁键
- **THEN** 会话 B 被登记到等待队列并返回排队结果（含队列位次）

### Requirement: 可重入计数与租约刷新

对可重入锁类型（REENTRANT、READ、WRITE 的写侧），同一归属再次获取 MUST 使持有计数加一并返回成功，且租约 MUST 刷新为整段新租期（沿用原凭证）。新租期 MUST 按本次请求携带的租约时长取值：请求值为 0 时取服务端默认租约，否则钳制到 `[minLeaseMs, maxLeaseMs]`——写侧重入与读侧加入既有读者群两条路径 MUST 采用同一取值口径。不可重入类型（SIMPLE）的同归属再次获取 MUST 按普通竞争处理（排队等待，直至让出或租约到期）。

#### Scenario: 重入递增并按计数释放

- **WHEN** 同一归属对可重入锁连续获取 2 次，随后释放 1 次
- **THEN** 锁仍被持有；再释放 1 次后锁才完全释放

#### Scenario: 重入刷新租约

- **WHEN** 持有者在租约中途重入获取
- **THEN** 租约到期时间被刷新为"当前时刻 + 完整租期"，凭证不变

#### Scenario: 重入按请求租约值刷新

- **WHEN** 持有者以默认租约 30s 获取后，携带 `requestedLeaseMs=10s` 重入获取
- **THEN** 租约到期时间刷新为"当前时刻 + 10s"（而非沿用条目现有 30s）

#### Scenario: 读侧加入按请求租约值刷新

- **WHEN** 读者群已有共享租约，新读者（或重入读者）携带请求租约加入
- **THEN** 全体读者共享租约按与写侧重入相同的口径刷新（请求值 0 用默认、非 0 钳制后刷整段）

#### Scenario: 不可重入锁自锁等待

- **WHEN** 持有 SIMPLE 锁的归属再次请求同一锁
- **THEN** 请求进入等待队列而非直接授予
### Requirement: 读锁语义

读锁（`LockType = READ`）在无写持有者且无等待者时 MUST 授予，允许多个读者并发持有并各自维护重入计数。存在写持有者或等待队列非空（即使队首是读者）时，读请求 MUST 排队。锁降级（持写取读）与升级（持读取写）MUST 不做特判，一律按通用规则处理。

#### Scenario: 多读者并发授予

- **WHEN** 两个不同归属先后请求同一锁键的读锁，且无写持有者与等待者
- **THEN** 两者均被授予，锁同时处于两个读者的持有之下

#### Scenario: 写者持有时读者排队

- **WHEN** 写侧持有者占用锁键，读者请求该锁键
- **THEN** 读者进入等待队列

### Requirement: 严格 FIFO 等待

授予顺序 MUST 等于入队顺序。等待队列非空时，即使锁当前无冲突持有者，新请求者也 MUST 排入队尾，不得越过在队者（包括队首正处于"已通知、待重发"窗口的场景）。先到达的读者 MUST 阻止其后到达的写者越过自己获得授予。

#### Scenario: 授予顺序等于入队顺序

- **WHEN** 多个竞争者依次排队，持有者释放锁
- **THEN** 后续授予按入队先后逐个发生，无越位

#### Scenario: 通知窗口内新到者不越位

- **WHEN** 锁无持有者，但队列非空且队首正处于已通知待重发窗口，此时新请求到达
- **THEN** 新请求排入队尾，不获得即时授予

### Requirement: 立即式获取

请求方声明不可排队时（对应协议 `wait_ms = 0`），若锁无法立即授予，引擎 MUST 返回拒绝结果且不登记任何等待项。

#### Scenario: 立即式失败不入队

- **WHEN** 锁被占用，一个不可排队的获取请求到达
- **THEN** 引擎返回拒绝结果，等待队列不新增任何条目

### Requirement: 队首通知与重发授予

锁让出（释放、到期、会话清理、等待者移除）后，若等待队列非空且锁无冲突持有者，引擎 MUST 仅对队首发出"可重试"通知事件，不做批量唤醒。队首随后以原请求标识重发获取时，若与当前持有状态兼容，MUST 被出队并授予。

#### Scenario: 释放后仅通知队首

- **WHEN** 持有者完全释放锁，队列中有多个等待者
- **THEN** 只有队首收到通知事件，其余等待者不受影响

#### Scenario: 队首重发被授予

- **WHEN** 已收到通知的队首以原 `(会话, 请求标识)` 重发获取，且锁无冲突持有者
- **THEN** 该等待者出队并被授予锁

### Requirement: 队首响应超时回收

队首收到通知后进入"已通知待重发"状态，并带有响应截止时刻（默认 5 秒）。截止前未收到其重发请求时，引擎 MUST 将该队首移出队列，并在锁无持有者时对新队首补发通知。

#### Scenario: 放弃者被回收且队列前进

- **WHEN** 队首收到通知后一直未重发，响应截止时刻已过
- **THEN** 引擎执行周期清扫后该等待者被移出队列，新队首收到通知事件

### Requirement: 租约生命周期

每次授予 MUST 附带租约。请求的租约时长为 0 时使用默认值（默认 30 秒），非 0 值 MUST 被钳制到 `[1 秒, 1 小时]` 区间。续租在凭证匹配时 MUST 将到期时间延长为"当前时刻 + 续租时长"。租约到期时引擎 MUST 强制释放该锁（与客户端状态无关），并在队列非空时对队首发出通知。到期后以旧凭证释放或续租 MUST 被拒绝。

#### Scenario: 到期自动释放

- **WHEN** 持有者未续租，时间推进越过租约到期点，引擎执行到期扫描
- **THEN** 锁被强制释放，队首（若有）收到通知事件

#### Scenario: 续租延长到期时间

- **WHEN** 持有者以有效凭证请求续租
- **THEN** 租约到期时间更新为当前时刻加续租时长

#### Scenario: 过期凭证被拒

- **WHEN** 锁因租约到期被强制释放后，原持有者以旧凭证请求释放或续租
- **THEN** 引擎返回凭证无效/未持有的拒绝结果

### Requirement: 释放校验

释放请求 MUST 校验租约凭证与归属：凭证不匹配返回凭证无效（`INVALID_TOKEN`）；归属不匹配返回未持有（`NOT_HELD`）。持有计数减至零时锁完全释放，结果中 MUST 携带"完全释放"标志。

#### Scenario: 错误凭证不能解锁

- **WHEN** 非持有者以错误凭证请求释放他人持有的锁
- **THEN** 引擎返回凭证无效，锁的持有状态与等待队列均不受影响

#### Scenario: 完全释放标志

- **WHEN** 持有者释放锁且计数归零
- **THEN** 释放结果携带"完全释放"标志，锁可被下一个等待者获取

### Requirement: 会话生命周期清理

会话登记 MUST 返回新的会话标识。会话关闭时，引擎 MUST 释放该会话持有的全部锁（写侧与读侧）、摘除其全部等待项，并对因此空出的锁按队首通知规则补发通知。会话关闭 MUST 幂等。

#### Scenario: 断连清理持锁与等待

- **WHEN** 某会话同时持有一个写锁、一个读锁并在另一锁上排队，该会话关闭
- **THEN** 写锁与读锁被释放（触发各自的队首通知），排队项被摘除，会话记录被移除

#### Scenario: 重复关闭幂等

- **WHEN** 对同一会话标识连续执行两次关闭
- **THEN** 第二次关闭不产生任何副作用

### Requirement: 获取请求幂等

同一 `(会话, 请求标识)` 的等待项已在队列中时，重复的获取请求 MUST 返回排队结果（携带当前位次）且不二次入队。

#### Scenario: 重复请求不二次排队

- **WHEN** 等待项已在队列中，同一 `(会话, 请求标识)` 的获取请求再次到达
- **THEN** 引擎返回排队结果与当前位次，队列长度不变

### Requirement: 无网络与零依赖

引擎 MUST 为纯 Java 实现，不依赖协议模块、网络库或任何第三方库；引擎 MUST 不持有任何连接相关对象，对外仅通过事件回调接口报告"应通知哪个会话的哪个请求"。

#### Scenario: 模块依赖隔离

- **WHEN** 检查核心模块的构建依赖
- **THEN** 不存在对协议模块、Netty 或任何第三方运行库的依赖

### Requirement: 快照状态重建入口
核心引擎 SHALL 提供一个仅供快照加载使用的状态重建入口：以 core 模块原生的数据形态（不依赖协议/序列化/网络类型）一次性注入复制状态全集（锁条目与持有计数、租约凭证与到期、会话登记），MUST NOT 依赖重放历史授予命令来复现状态（历史释放空洞下凭证序列不可复现）。该入口为纯新增：既有公开方法的行为 MUST NOT 因此改变。

#### Scenario: 继承状态操作正确
- **WHEN** 经重建入口注入含写持有（重入计数>1）、多读持有与租约的状态后执行操作
- **THEN** 按凭证的释放/续租正确生效（计数递减、token 校验一致）；凭证不符返回 INVALID_TOKEN；无按快照前会话登记的调用返回 REJECT_SESSION

#### Scenario: 发号不复用继承凭证
- **WHEN** 重建注入的状态中最大凭证为 T，随后授予新锁
- **THEN** 新凭证 > T，与继承条目无凭证碰撞

#### Scenario: 到期扫描覆盖继承租约
- **WHEN** 重建注入到期时刻为 E 的条目，时钟越过 E 后执行到期扫描
- **THEN** 该条目被释放，与未经重建、由授予命令原生到期的行为一致

#### Scenario: 单机路径零扰动
- **WHEN** 不调用重建入口时运行全部既有核心引擎测试
- **THEN** 行为与既有规格逐条一致（纯新增入口）
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

### Requirement: Semaphore 队首式授予

`SemaphoreEntry` SHALL 以许可为授予单位：`permits_total` 由首次请求定型，`permits_available` 随授予扣减、随归还回升。授予 MUST 同时满足：请求者为等待队列队首或队列为空，且 `permits_available ≥` 请求数；非队首请求（含所需许可更少的请求）MUST NOT 越位授予（防大请求饥饿）。同归属（session, thread）的重入获取 MUST 先于队首与空位检查、按次累加持有许可并刷新租约。`queueIfBusy = false` 且条件不满足时 MUST 返回拒绝（不排队、不入队）。授予成功 MUST 登记租约（挂在归属维度）。

#### Scenario: 队首许可不足时无人获授

- **WHEN** 总许可 3 已全部授予，等待队列为 [请求 2, 请求 1]，此时归还 1 个许可
- **THEN** 队首（请求 2）不满足、不获通知；队列第 2 位（请求 1）MUST NOT 越位获得该许可

#### Scenario: 队首满足时按序授予

- **WHEN** 总许可 3，A 持 2、B 持 1 后释放全部，队列为 [C:2, D:1]
- **THEN** C 获授（余 1），D 保持等待；再归还使 `permits_available ≥ 1` 且 C 已释放时 D 获授

#### Scenario: 重入不受队首约束

- **WHEN** 归属持有 2 个许可，等待队列非空，同归属再获取 3 个许可
- **THEN** 直接授予（累计 5），不因队列存在他者而排队，`permits_available` 相应扣减

#### Scenario: 立即式不足即拒

- **WHEN** `permits_available = 1` 时以 `queueIfBusy = false` 请求 2 个许可
- **THEN** 返回拒绝且不进入等待队列

### Requirement: Semaphore 租约到期与会话清理归还

持有者租约到期时，系统 SHALL 归还该持有者名下的**全部**许可并恢复其可分配状态，随后按队首规则尝试推进；会话关闭时同样摘除其持有（归还许可）与在队等待。释放请求的归还数超过该归属持有数时 MUST 拒绝且许可总量不变；正常释放按请求数对称扣减持有计数，全部持有归零后撤销该归属租约。

#### Scenario: 到期整体归还

- **WHEN** 某持有者名下有 3 个许可且租约到期被强制回收
- **THEN** 3 个许可全部回到可用池，其队列位次撤销通知按队首规则推进

#### Scenario: 超额释放拒绝

- **WHEN** 归属持有 2 个许可时释放 3 个
- **THEN** 拒绝该释放，`permits_available` 与持有计数均不变

#### Scenario: 会话关闭不泄漏许可

- **WHEN** 持有许可且排队等待的会话被关闭
- **THEN** 其持有许可全部归还、在队项全部摘除，其他等待者按队首规则获得推进机会

### Requirement: Latch 倒计数与一次性屏障

`LatchEntry` SHALL 承载倒计数：初始值由首个携带非零 `total` 断言的请求（`countDown` 或 `await` 任一通道）定型；`countDown(n)` 以 `count = max(0, count − n)` 更新，归零瞬间 MUST 对**全部**等待者发出通知（全体放行，等待者在各自重发命中"已归零"时离队）；归零后 `await` MUST 立即通过、`countDown` MUST 为无操作。Latch 的等待者 MUST NOT 登记租约（不参与到期堆与续租），会话关闭仅摘除其等待者身份且 MUST NOT 影响计数。条目一经定型即存续（不随参与者散尽或归零而回收）：归零屏障的放行判定与一次性护栏由条目存续承载，直至节点重启；key 的复用治理遵循"新屏障 = 新 key"约定（详设 §2.4）。从未定型的屏障上，任何不携带非零 `total` 断言的操作均被拒。

#### Scenario: 归零全体广播

- **WHEN** 屏障计数为 1 且有 3 个等待者，收到 `countDown(1)`
- **THEN** 3 个等待者全部收到通知，此后新到 `await` 立即通过

#### Scenario: 一次性不重置

- **WHEN** 屏障已归零后再收到 `countDown(5)`
- **THEN** 无操作、返回剩余计数 0，屏障保持放行态

#### Scenario: 到期扫描不触及 latch

- **WHEN** 存在持有 awaiter 的 latch 条目且到期扫描执行多轮
- **THEN** latch 计数与等待者均不变（无租约语义）

#### Scenario: 断连摘除不影响计数

- **WHEN** 一个 awaiter 会话断开
- **THEN** 其等待者身份摘除、计数不变，其余 awaiter 继续等待

### Requirement: 只读统计观察面

`CoreEngine` SHALL 提供只读统计访问，一次调用返回：按条目家族/类型聚合的"当前持有中"条目数（LATCH 家族恒不计入）、全部等待队列条目总数、单 key 等待队列深度的最大值、活跃会话数。该访问 MUST 为纯读操作：MUST NOT 改变引擎任何状态、MUST NOT 引入第三方依赖（core 零依赖底线保持）、MUST NOT 使既有方法的行为或线程模型发生变化。遍历允许弱一致（与授予/释放并发时观察抓取时刻附近的近似状态），但 MUST NOT 抛并发修改异常或长时间阻塞写路径。

#### Scenario: 统计与已知状态一致

- **WHEN** 以手工时钟脚本建立"N 个held 锁条目、M 个等待者、某 key 队深 K"的稳态后读取统计
- **THEN** held 按类型计数 == N、等待者总数 == M、最大队深 == K、会话数与登记数一致

#### Scenario: 与在途变更并发安全

- **WHEN** 授予/释放/到期进行中反复读取统计
- **THEN** 无异常抛出，观察值始终处于合法区间（弱一致可接受）

#### Scenario: 回归底线

- **WHEN** 既有 core 全量测试运行
- **THEN** 全绿，互斥锁/信号量/屏障行为零变化


### Requirement: 明细只读观察面

核心引擎 SHALL 在聚合统计观察面（`stats()`）之外提供**明细级只读观察面**，供上层管理协议装配每条 key 的可见状态快照：条目家族与定型类型、持有者列表（会话、线程、重入计数或持有许可数、读/写角色）、生效租约的到期时刻、等待队列的位次列表（会话、请求、已等待时长）、Latch 条目的定型总量与剩余计数与等待/参与者会话。契约要求：

- **纯读零扰动**：观察 MUST NOT 变更任何锁状态、租约、队列或簿记——与既有全部行为逐项一致（回归底线）；
- **弱一致快照**：逐条目取锁内快照、条目间无全局原子性；遍历序与快照间撕裂 MUST NOT 造成异常或非法值（如持有者数为负、位次跳号出现在同一条目内）；单条目快照内部自洽（位次连续、持有列表与计数一致）；
- **有界临界区**：快照构造仅做字段拷贝，MUST NOT 在条目锁内执行网络、时钟回退或集合深拷贝级别的昂贵操作，与授予/释放路径的锁竞争窗口同数量级；
- **零依赖底线保持**：core MUST NOT 因观察面引入任何第三方依赖，MUST NOT 感知管理令牌、分页或 HTTP。

#### Scenario: 预置状态逐项可见

- **WHEN** 构造引擎并预置多持有者重入锁、读写混合持有、Semaphore 部分许可与在队大请求、Latch 等待者，随后调用明细观察面
- **THEN** 每条目快照的家族/类型、持有者 (会话,线程,计数/许可)、等待位次与已等待时长、Latch 总量/剩余与聚合 `stats()` 口径互相一致

#### Scenario: 并发观察零异常

- **WHEN** 授予/释放/到期/会话清理持续进行中，另一线程循环调用明细观察面
- **THEN** 全部调用正常返回弱一致快照（无并发异常、无自相矛盾的条目内快照），且引擎最终状态与不加观察调用时逐字段一致

#### Scenario: 既有行为回归底线

- **WHEN** 引入观察面后运行 core 全部既有用例
- **THEN** 全量通过，互斥/读写/Semaphore/Latch 语义与线程模型零变化

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
