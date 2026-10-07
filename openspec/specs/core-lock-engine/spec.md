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

key 条目 SHALL 由首次创建它的请求定型所属家族：LOCK（`REENTRANT`/`SIMPLE`/`READ`/`WRITE`/`FAIR`）、SEMAPHORE、LATCH、ATOMIC（`ATOMIC_LONG`/`ATOMIC_INTEGER`/`ATOMIC_BOOLEAN`/`ATOMIC_REFERENCE` 四形态同族共享条目类型，形态间互斥——条目定型后形态不可变更）、BARRIER、QUEUE（`LOCK_TYPE_QUEUE`/`LOCK_TYPE_DELAY_QUEUE` 两形态同族共享条目类型，形态间互斥——出队规则由定型形态决定：到达序 vs 最早到期序）、PHASER（`LOCK_TYPE_PHASER` 单形态家族——phaser 无子形态判别，动态注册使"参与者构成"成为账簿内容而非定型属性，对照 BARRIER 的 parties 定型断言为刻意分轨）、TIMER（`LOCK_TYPE_TIMER` 单形态家族——timer 无子形态判别，装载代次与到期时刻是账簿内容；延时语义与 QUEUE 延时形态**同题异机**：队列到期是元素可见性开关、timer 到期是全体共见的标记位，两状态机互不相通）。`FAIR` 与 `REENTRANT` 同为锁家族且语义等价（互通互认，可相互重入）。跨家族请求 MUST 返回结果 `REJECT_TYPE_MISMATCH` 且 MUST NOT 改变条目任何状态；同跨形态的 ATOMIC 请求（如 `ATOMIC_LONG` 条目上请求 `ATOMIC_INTEGER`，或标量形态条目上请求 `ATOMIC_REFERENCE` 及反向）与同跨形态的 QUEUE 请求（`QUEUE` 条目上请求 `DELAY_QUEUE` 及反向）MUST 同样返回 `REJECT_TYPE_MISMATCH`；server 层将该结果映射为协议 `INVALID_REQUEST`。锁家族既有条目的定型规则（可重入性由首次请求决定）MUST NOT 变化。

#### Scenario: 跨家族请求被拒

- **WHEN** 以 `REENTRANT` 建立并持有的 key 上发起 `SEMAPHORE`、`LATCH_AWAIT`、`ATOMIC_OP`、`BARRIER_AWAIT`、`QUEUE_OP`、`PHASER_OP` 或 `TIMER_OP` 请求
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

#### Scenario: BARRIER 与 PHASER 互斥

- **WHEN** 已以 `BARRIER_AWAIT` 定型的 key 上发起 `PHASER_OP`（REGISTER 或任意操作），或已以 REGISTER 定型的 phaser key 上发起 `BARRIER_AWAIT`
- **THEN** 返回 `REJECT_TYPE_MISMATCH`，世代账簿与相位账簿逐项零扰动（两家族同为"会合"语义家族但状态机互不相通——泛化关系不构成条目复用，判例 v5 D1 Latch/Barrier 分立）

#### Scenario: TIMER 与 DELAY_QUEUE 互斥

- **WHEN** 已以 `LOCK_TYPE_DELAY_QUEUE` 定型的队列 key 上发起 `TIMER_OP`（任意操作词），或已以 SCHEDULE 定型的 timer key 上发起 `QUEUE_OP`
- **THEN** 返回 `REJECT_TYPE_MISMATCH`，元素队列与装载账簿逐项零扰动（两家族同具"到期"语义但到期对象不同——元素可见性 vs 全体共见标记位，状态机互不相通，判例 v5 D1 家族分立）
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

### Requirement: BarrierEntry 到场合拢与世代状态机

引擎 SHALL 支持 BARRIER 家族条目 `BarrierEntry`，装定型许可数（`parties`，`> 0`）、当前世代号（`generation`，自 1 起每 key 单调递增）、当前世代到场账簿（`(会话, 请求) → 在场` 集合及计数）、在队等待队列（位次与通知时序，等待项身份 = `(会话, 请求)`，无归属线程概念、threadId 位恒 0 占位）、动作挂账（被指定执行 barrierAction 的 `(会话, 请求)` 与其世代）与最近完结世代了结记录（`{世代, 结果 TRIPPED|BROKEN, (会话,请求) → 已了结}`）。`await` 的判定顺序（首个命中者即为结果）：(1) parties 非零主张断言——对不存在条目未携带 `> 0` 主张、或对既有条目非零主张与定型值不符，MUST 返回新结果 `REJECT_BARRIER_PARTIES`（条目状态零扰动，server 层映射 `INVALID_REQUEST`；判例 `REJECT_LATCH_TOTAL`）；(2) 幂等去重——同 `(会话, 请求)` 重发命中当前世代到场账簿/了结记录或在队等待项时 MUST NOT 重复计次，按其所处阶段返回（在队未合拢 → `QUEUED` 原位次；所属世代已 TRIPPED → `GRANTED`；已 BROKEN → 破障了结；等待动作了结的执行者本人 → 维持其执行者语义应答）；(3) 队列深度限额——在队等待者数达 `maxQueueDepthPerKey` 时 MUST 返回 `REJECT_QUEUE_FULL` 且 MUST NOT 计入到场；(4) 入队到场——计入当前世代到场账簿并入队，应答 `QUEUED` 携位次与该世代号。到场数达 parties 的瞬间 MUST 合拢（trip）：条目携带 `carries_action` 的最后到场者被指定为执行者（应答 `GRANTED` 携执行者标记），当前世代转入动作待决态，其余等待者 MUST NOT 收到放行通知；最后到场者不携带动作（`carries_action=false`）时世代即时 TRIPPED——了结记录定型、`generation` 回卷（新到场进新世代）、全体等待者进入放行通知。TRIPPED 世代的了结 MUST 对全部已到场 `(会话, 请求)` 可查（重发按账簿了结，MUST NOT 落入新世代）。执行者经 `barrierActionDone` 回报后：世代 MUST 定型 TRIPPED、回卷新世代、其余等待者进入放行通知；回报者非该世代指定执行者或世代号不认识时 MUST 拒绝（`INVALID_REQUEST` 映射）；对已了结世代的重复回报 MUST 幂等成功。动作待决超时的等待者可经 `barrierLeave` 离场（离场语义见"离场即破障"）。全部操作 MUST 校验会话存在（不符返回 `REJECT_SESSION`）并将会话与 key 登记触及集（与 Latch await 同口径）。

#### Scenario: 到场合拢与世代回卷

- **WHEN** parties=3 的空 barrier 上会话 A、B、C 先后 await（均 `carries_action=false`），随后 D、E、F 再先后 await
- **THEN** C 的 await 应答 GRANTED（当回合拢），A、B 的重发得了结 TRIPPED 放行，世代号 +1；D 的入队进入新世代，与上一批互不干扰

#### Scenario: 旧世代重发不落新世代

- **WHEN** 世代 G 的等待者收到放行通知后重发，此时条目已回卷且已有新到场者进入世代 G+1
- **THEN** 重发经 `(会话,请求)` 命中 G 的了结记录得 TRIPPED 放行，MUST NOT 计入 G+1 的到场数

#### Scenario: 到场幂等不双计

- **WHEN** 同 `(会话, 请求)` 的 await 在应答丢失后原样重发两次
- **THEN** 到场计数恰 +1（第二次命中去重），位次与世代号回显与首次一致

#### Scenario: parties 断言不成立拒绝

- **WHEN** 定型 parties=3 的 barrier 上收到携带 `parties=4` 的 await
- **THEN** 返回 `REJECT_BARRIER_PARTIES`，到场账簿、队列与世代逐项不变

#### Scenario: 队列满护栏

- **WHEN** 在队等待者数已达 `maxQueueDepthPerKey`，新 await 到达
- **THEN** 返回 `REJECT_QUEUE_FULL`，该请求 MUST NOT 计入到场账簿

#### Scenario: 执行者两阶段放行

- **WHEN** parties=2 的 barrier 携动作：A 先 await（QUEUED），B 后 await 且 `carries_action=true`
- **THEN** B 的应答 GRANTED 且标记执行者；A 此刻 MUST NOT 收到放行通知；B 回报 `barrierActionDone` 后世代 TRIPPED，A 收通知、重发得放行

#### Scenario: 非执行者回报动作被拒

- **WHEN** 世代 G 的动作挂账在会话 B，会话 C 以 G 的世代号回报 `barrierActionDone`
- **THEN** C 的请求被拒绝（`INVALID_REQUEST` 映射），世代 G 维持动作待决

### Requirement: BARRIER 条目生命周期与离场即破障

BARRIER 条目 SHALL 为无租约家族：等待者与到场账簿 MUST NOT 登记租约（不入到期堆、零续租流量，判例 Latch）；条目一经定型即存续，不随世代完结或参与者散尽回收（key 复用治理：同 key 恒同 parties，新阶段需求沿用同 key 的世代回卷而非"新 key"——与 Latch 的一次性"新屏障=新 key"约定相反，系循环语义使然；maxKeys 护栏兜底无上限 key）。**离场即破障**：当前世代内任何已到场等待项的离场——无论 `barrierLeave`（超时离场或显式 `breakBarrier()`）、会话死亡（关闭/失联清理经 `removeSession`）、还是执行者死亡（动作待决态下其会话摘除）——MUST 使当前世代即时 BROKEN：全体在队等待者与未了结到场项收到放行通知、重发得了结 BROKEN（协议 `BARRIER_BROKEN`），动作待决挂账清除，`generation` 推进。从未到场的会话不构成参与者，其死亡 MUST NOT 破障（parties 是期望到场数，非名册）。破障作用域 MUST 限于当前世代：破障后新到场的 await MUST 进入全新世代并正常合拢（无 JDK"粘滞 broken 直至 reset"效应，引擎 MUST NOT 提供条目级 reset）。`barrierLeave` 对已终结（TRIPPED/BROKEN）世代或无对应到场记录者 MUST 幂等无操作；`breakBarrier()` 路径（`await_request_id=0`）对不存在或已空当前世代的 key MUST 同样幂等。

#### Scenario: 在队者会话死亡即时破障

- **WHEN** parties=3 的 barrier 已有 2 个在队等待者，其中一会话被清理（断连/失联）
- **THEN** 当前世代 BROKEN：另一在队者收通知、重发得 `BARRIER_BROKEN`；随后新 await 进入新世代正常计次

#### Scenario: 执行者死亡破障

- **WHEN** 世代 G 处于动作待决（执行者 B 尚未回报），B 的会话被清理
- **THEN** 世代 G BROKEN，其余等待者以 `BARRIER_BROKEN` 了结，动作挂账清除

#### Scenario: 破障后世代自愈

- **WHEN** 世代 G 被破障后，新会话以相符 parties 主张 await
- **THEN** 进入世代 G+1 正常到场计次，MUST NOT 继承 G 的破障态、MUST NOT 要求任何 reset

#### Scenario: 超时离场连带破障

- **WHEN** 世代 G 的两名到场者之一超时发 `barrierLeave`（携带其 await 的 `request_id`）
- **THEN** 离场成功，且世代 G 即时 BROKEN，另一到场者以 `BARRIER_BROKEN` 了结

#### Scenario: 到期堆与清扫零触碰

- **WHEN** 租约到期扫描与队首清扫驱动经过含 BARRIER 条目的时刻堆
- **THEN** 无该条目的到期登记与清扫动作（其等待者的通知-重发窗口兜底与 Latch 同机制：已通知等待者的响应超时摘除等待项身份，MUST NOT 触发破障——已通知即已了结在途）

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

### Requirement: 条件等待集与 await 释放折叠

LOCK 家族条目（`LockEntry`）SHALL 承载进程内条件等待集：`condition_name → 到达序等待队列`，条目锁内读写、与既有 `waiters` 等待队列同生命周期（Leader 本地裁决态、MUST NOT 进入快照与日志——边界条款由 replicated-state-machine 与 snapshot-recovery 能力钉定）。**双拓扑落位**：单机形态下折叠命令在本条目关键区内原子执行"释放半程+登记半程"（下述判定顺序）；集群形态下应用点仅执行**释放半程**（引擎 release-only 应用方法：持有归属则重入一步清零+清租约+队首通知评估，非持有零操作，恒经复制确定重放），登记由受理节点的 Leader 本地结构在预检点承载——两拓扑对上层呈现同一判定语义与"登记先于释放可见"不变式。await 折叠的引擎裁决（单机 ACQUIRE 应用点，判定顺序）：会话有效 → key 合法 → 家族与形态匹配（非 LOCK 家族/READ·WRITE 形态携带 condition → 既有类型不匹配拒绝映射）→ **合并深度护栏**：本 key 等待项合计（`waiters` 队列 + 全部条件等待集）达 `max-queue-depth-per-key` 时返回等待满拒绝（线路 `OVERLOADED`），集合零扰动 → **同一关键区内**依序执行释放半程与登记半程：(会话,线程) 恰为当前持有归属时重入计数一步清零、清除租约并按既有队首通知纪律评估可推者；非持有归属时释放半程零操作（服务端宽容面，权限降级条款由 client-sdk 能力承载声明）；随后以 (会话, request_id) 登记入 `condition_name` 等待集，返回排队回执（位次为本 key 等待项合计口径）。登记 MUST 幂等：同 (会话, request_id) 重复到达（应答丢失重发/换主重挂）不二次入集、不重复执行释放半程、返回同位次；已在集等待项的重复经复制应用（条目重放）MUST NOT 产生第二登记。

清理与搬运的摘除 MUST 三路收口且互不重复计数：LEAVE 按 (会话, request_id) 显式摘除（幂等，未存在亦回无操作成功）；会话关闭摘除该会话在全部条件集与全部 key 的登记（与既有 `closeSession` 家族扫描同钩子；摘除仅触等待集，MUST NOT 改动锁持有/租约/等待队列——"死亡不吞锁"，等待者死亡时不持锁、无账可碰）；授予侧收口：任一归属 (会话,线程) 经正常或折叠获取被授予时，MUST 顺带摘除该归属在全部条件集的陈旧登记（LEAVE 在途丢失的 ghost 由此收敛——同一线程不可能既持锁又条件等待，授予即其上一次 await 的终结，按 (会话,线程) 收口覆盖同线程换 request_id 的重新获取形态）。等待集的 `registered_at_ms` 为观察值（条目时刻注入，MUST NOT 参与任何判定；换 term 后基线重置）。

`waiterCount` 与明细只读观察面口径 SHALL 计入条件等待者（它们是等待者——对照 topic 订阅者不计入既有分轨，口径差异随行注释）；Follower 侧条件集恒空（登记为 Leader-only 应用副作用），读数如实呈现。

#### Scenario: 持有者 await 单步全量释放并登记

- **WHEN** 重入计数为 3 的持有者 (s,t) 对条件 c 提交折叠 ACQUIRE（request_id=R），随后应用
- **THEN** 同一关键区内：writeCount 一步清零、租约清除、既有队首通知照常评估、(s,R) 入 c 到达序队尾，应答排队回执；后续对同 R 的重复应用（重发/重放）不双登记、不再次释放

#### Scenario: 非持有者登记为合法 ghost

- **WHEN** 未持有该锁的会话提交折叠 ACQUIRE（误用/重挂形态）
- **THEN** 释放半程零操作、登记照常入集（服务端不查持有权限），应答排队回执；该 ghost 的收敛路径为 LEAVE/会话死亡/被授予时顺带摘除三路，人数受合并护栏钳制

#### Scenario: 合并深度护栏超限拒绝

- **WHEN** 某 key 的 `waiters` 队列 + 条件等待集合计已达 `max-queue-depth-per-key`，新折叠 ACQUIRE 到达
- **THEN** 返回等待满拒绝（线路 `OVERLOADED`），等待队列与条件集零扰动、释放半程不执行

#### Scenario: 会话死亡只摘等待集不碰锁账

- **WHEN** 承载条件等待者的会话经 `SESSION_CLOSE` 摘除
- **THEN** 该会话全部条件集登记摘除；该 key 的持有者、重入计数、租约、等待队列逐项不变（死亡不吞锁）

#### Scenario: 授予顺带摘除 ghost 登记

- **WHEN** (s,R) 的 LEAVE 请求在途丢失，其后同归属 (s,t) 以新请求 R2 经正常获取被授予
- **THEN** 授予应用点同关键区摘除 (s,t) 在各条件集的陈旧登记（含 R 项），等待集不残留死登记

#### Scenario: Follower 条件集恒空

- **WHEN** Follower 副本回放含折叠 ACQUIRE 的日志并对外提供观察读数
- **THEN** 释放半程照常生效（复制态），条件等待集为空、观察读数如实零（登记为 Leader-only 副作用）

### Requirement: signal 权限与条件等待搬运

SIGNAL/SIGNAL_ALL 的引擎裁决 SHALL 以服务端为权威：(会话,线程) 恰为该 key 当前持有归属方可搬运，否则返回未持有拒绝（线路 `NOT_HELD`）——与 JDK"非持有者 signal 抛 IllegalMonitorStateException"同型；该权限检查与搬运 MUST 在条目同一关键区内完成（持有者变更与搬运互斥）。SIGNAL 摘取该条件到达序队首**一人**（搬运候选人 MUST 排除调用归属 (会话,线程) 自身——集群预检登记窗内 awaiter 短暂持有且在集，自 signal 防御性跳过；JDK 中持有者恒不在集，同款效果）搬入既有 `waiters` 队列尾（搬运时刻定序——FAIR 位次承诺按搬运入队序约束同队列后来者，MUST NOT 承诺优先于搬运前已入队者）；SIGNAL_ALL 按到达序全员搬运。权限先行的收束口径：条目不存在/无人持有亦属"归属不匹配"（恒 `NOT_HELD`——JDK 中不持锁即 IllegalMonitorStateException，无锁对象则更无从 signal）；调用者恰为持有归属而目标集为空/无此条件名 = 无操作成功（JDK 对齐：signal 不报错、不追溯）。搬运项的唤醒权自此移交既有等待队列纪律：signal 权限论证使搬运恒发生于"调用者持有（锁必忙）"的时点——不存在"搬运即空闲可推"的分支，通知由后续**释放/到期/会话关闭应用点的队首通知**接力送达（事件驱动——无就绪定时器、无 tick 精度语义，对照 v7 延时形态的缺失为有意设计）。被搬运等待项的了结走既有队首重发授予路径（规则 7 语义），授予时重入计数从 1 起、签发新租约凭证。

`headReplyTimeoutMs` 已通知队首清扫 MUST 覆盖搬运后未回重发的等待项（既有纪律原样适用，摘除仅触等待队列；其条件集登记已随搬运消失，无二次摘除）。

#### Scenario: 非持有者 signal 被权威拒绝

- **WHEN** 未持有该锁的会话（或持有但线程不匹配的会话）发送 SIGNAL
- **THEN** 返回未持有拒绝（线路 `NOT_HELD`），条件集与等待队列零扰动，连接与会话不受影响

#### Scenario: SIGNAL 取一人按到达序

- **WHEN** 条件 c 集内依次有 W1、W2、W3 到达，持有者连续 SIGNAL 两次
- **THEN** W1、W2 依次被搬入等待队列（W3 留集），搬运序=到达序；每次 SIGNAL 回无操作歧义不存在（命中即搬）

#### Scenario: SIGNAL_ALL 全员搬运且通知由释放接力

- **WHEN** 持有者发 SIGNAL_ALL（集内 3 人）随后释放锁
- **THEN** 3 人按到达序全部搬入等待队列尾（搬运时点锁恒被持——权限论证），释放应用点的队首通知逐个推进唤醒链（事件驱动，无 tick 等待）

#### Scenario: 空集 signal 无操作成功

- **WHEN** 对无任何等待者的条件名（或从未出现过的条件名）发送 SIGNAL/SIGNAL_ALL
- **THEN** 回无操作成功（`OK`），零扰动——signal 是事件不是状态，不追溯历史

#### Scenario: 搬运后重发授予重入一级

- **WHEN** 被搬运等待项收到队首通知并以原信封重发折叠 ACQUIRE
- **THEN** 队首重发命中被授予：新租约凭证、重入计数 1（与其 await 前的 N 级无关），同 (会话,request_id) 的条件集残留登记顺带摘除

### Requirement: PhaserEntry 相位账簿与合拢

PHASER 家族条目（`PhaserEntry`）SHALL 承载 JDK `Phaser` 的可判定分布式子集：复制侧账簿 = **相位号**（自 0 起、单调递增、不取模、跨重启/换主不回退）+ `registeredParties` 注册总数 + **每会话注册配额**（`session → parties`，死亡摘除与 `ARRIVE_AND_DEREGISTER` 扣减的归属依据）+ **当前相位到场计数** `arrived` + **到场去重槽**（`{(session, request_id) → 已计数}`——同请求重发/重放单计数，判例 v7 每会话去重槽与 v5 到场账簿）+ **换代窗口**（有界：仅保最近一次相位推进中已通知未重发的等待项集 `{(session, request_id), 了结相位}`——窗口下界由 `headReplyTimeoutMs` 压住，出窗迟到按新等待计，v5 D7 同款声明竞态）。等待集（`(session, request_id) → expected_phase` 到达序队列）为 Leader 本地裁决态：MUST NOT 进入日志与快照（边界条款由 replicated-state-machine 与 snapshot-recovery 能力钉定），条目锁内读写、与 `waiters` 类既有本地态同生命周期。

操作裁决的判定顺序与语义（条目锁内单关键区）：会话有效 → key 合法 → 家族匹配 → 操作分支：

- **REGISTER(count)**：无条目时创建并定型 PHASER 家族（判例 v3 `permits_total`/v7 非零主张建条目——注册是 phaser 唯一的建条目入口）；`count` 加计入调用会话配额与 `registeredParties`；加计后超 `max-parties-per-phaser` → `REJECT_PHASER_PARTIES`（线路 `OVERLOADED`），配额账簿零扰动（在带拒绝，既有参与者零影响——判例 `REJECT_SUBSCRIBERS` 形态）；回显到场相位。注册不触发合拢判定（新参与者进入**当前**相位的应到集合——JDK 语义：注册者须在当相位到场方可合拢）。
- **ARRIVE / ARRIVE_AND_AWAIT 的到场半程 / ARRIVE_AND_DEREGISTER 的到场半程**：`(session, request_id)` 命中去重槽 → 直接回显首计时的到场相位、不双计数（幂等）；未命中则 `arrived++` 入槽；`ARRIVE_AND_DEREGISTER` 在同一关键区先扣调用会话配额 1（配额为零 → `REJECT_PHASER_QUOTA` 线路 `INVALID_REQUEST`——JDK 匿名 party 此处本属未定义行为，我们显式从严并声明差异，扣减发生在合拢判定**前**：本次到场+离场可使剩余应到恰满足）；随后合拢判定。
- **合拢判定（每一处状态迁移后执行）**：`arrived ≥ registeredParties` → `phase++`、`arrived` 清零、去重槽清空（新相位重新计次）、换代窗口滚动为"本轮被唤醒等待项集"，并对等待集中 `expected_phase < 新相位` 的全部项发唤醒通知（`AWAIT_NOTIFY` ref=其 request_id，经调用方收集在锁外触发——判例 `BarrierEntry.await` 的 notify 收集纪律）；`registeredParties == 0` 时合拢判定恒不成立（空转不推进、不终止——注册归零不是终止事件，见生命周期条款）。
- **AWAIT_ADVANCE(expected_phase)**：不触达复制账簿——等待项以 `(session, request_id)` 入等待集（幂等：同 id 重复到达不二次入集，重挂即重演），回执 `QUEUED`；`expected_phase < 当前相位` 时在受理点直接 `OK` 了结（即刻可见已推进，不发生等待）。等待项登记 MUST NOT 要求调用者持有任何注册配额（`awaitAdvance` 可对旁观相位开放——JDK 同款：任意线程可 await 已见相位号）。
- **CANCEL(await_request_id)**：按 `(session, await_request_id)` 从等待集摘除（幂等，未存在亦 `OK`——判例 v9 LEAVE），MUST NOT 触碰任何复制账簿；已通知未重发的 ghost 由 `headReplyTimeoutMs` 清扫与换代窗口窗口兜底。
- **QUERY**：纯读回显 `phase`/`registered`/`arrived` 三计数，零迁移、零推进（观察不得使应到集合、合拢或唤醒时序发生变化）。

非 REGISTER 操作命中不存在条目 → `REJECT_NO_ENTRY`（线路 `INVALID_REQUEST`——phaser 不做隐式建条目：无 parties 断言可依赖，"key 在而账簿空"的歧义窗与 topic 撞 key 尽力而为探测相反，此处机制互斥更严）。条目观察面：`waiterCount` 与明细快照计入 phaser 等待项（等待就是等待——v9 条件等待者计入口径延伸；对照 topic 订阅者不计入的分轨随行注释）。

#### Scenario: 合拢单推进与去重重放

- **WHEN** registered=3 的 phaser 上依次到场 2 个不同请求、随后对第 2 个到场以同 (会话, request_id) 重发
- **THEN** 重发命中去重槽：`arrived` 保持 2、回显同一到场相位、不发生推进；第 3 个不同请求到场 → `arrived==registered` 合拢：相位 +1、`arrived` 清零、去重槽清空
- **AND** 下一轮对已清空槽的同 id 再次到场（新到达事件）正常计数（去重仅约束同一在场周期的重发）

#### Scenario: arriveAndDeregister 配额扣减与提前合拢

- **WHEN** registered=2、当前相位 arrived=1 时，持 1 配额的会话 S 发起 `ARRIVE_AND_DEREGISTER`
- **THEN** 同一关键区内到场计数与配额扣减一并生效（arrived 1→2、registered 2→1）→ 合拢判定 `arrived ≥ registered` 成立 → 推进并唤醒；调用方随后不再是任何相位的应到者（离场与到场的先后不影响终态，两序皆推进——确定性由 apply 全序承载）
- **AND** 对配额为零的会话重复 `ARRIVE_AND_DEREGISTER` → `INVALID_REQUEST`，账簿零扰动

#### Scenario: 中途注册进入当相位应到集

- **WHEN** registered=2、当相位 arrived=1（一方已到场未合拢）时，新会话 REGISTER(1)
- **THEN** registered 变 3、相位不推进；已到场方与新注册方各自到场后（arrived=3）方合拢——注册者**不**因注册即视为到场（JDK 同判：register 后仍须 arrive）

#### Scenario: 空转与复活

- **WHEN** 全部参与者 `arriveAndDeregister` 离场（registered=0）后，新会话对该 key REGISTER(2)
- **THEN** 离场末次相位照常（arrived≥0 判定按规则执行，不终止、无粘滞态）；REGISTER(2) 在**当前相位号**续起、应到 2 方到场后合拢——key 的 phaser 身份与相位单调性跨空转存续（对照 JDK 归零终止：差异显式声明）

#### Scenario: 无条目操作拒绝与家族定型

- **WHEN** 对从未 REGISTER 的 key 直接发起 ARRIVE/AWAIT_ADVANCE/QUERY
- **THEN** `INVALID_REQUEST`（无条目拒绝，MUST NOT 隐式建条目、MUST NOT 落日志副作用）；随后 REGISTER(1) 成功且 key 定型 PHASER，BARRIER/QUEUE 等其余家族请求此后对该 key 恒 `REJECT_TYPE_MISMATCH`

#### Scenario: CANCEL 幂等与 ghost 收敛

- **WHEN** 等待项 CANCEL 后其原请求重发 CANCEL（应答丢失重演），或 CANCEL 在途丢失而通知已发出
- **THEN** 重复 CANCEL 幂等 `OK` 零扰动；ghost 已通知项经换代窗口窗口收敛——重发命中 `OK`（若恰在窗内）或按新等待入集（出窗声明），`headReplyTimeoutMs` 清扫不吞活人位次

### Requirement: PHASER 条目生命周期与会话清理

PHASER 条目 SHALL 定型后存续不回收（判例 v5 D9 BARRIER：key 即 phaser 身份、相位单调与配额账簿依赖条目存续；回收再重建丢账）；无租约家族到期字段恒 0、不入租约到期堆、亦不被租约到期清扫回收（v6/v7 同款，判例队列"到期清扫与租约机制对队列零触碰"逐字对齐）——"不被清扫回收"及于副本影子镜像：镜像条目存续不依赖该 key 的后续变异，静置 phaser 键不因任一租约到期自管理面消失，清扫摘除集 MUST NOT 含无租约家族镜像条目（到期释放计数仅按实际释放计，幻影键 MUST NOT 入集）。常驻内存量 = O(key 数)、maxKeys 护栏兜底。会话死亡（`SESSION_CLOSE` 应用点与断连清理在条目侧的同一收口）SHALL 触发**隐式配额摘除**：该会话在全部 phaser key 的注册配额自 `registeredParties` 减除、配额账簿行删除；**已到场事实不撤销**（当前相位 `arrived` 不回退——到场是复制态事实，与 v7"死亡不吞元素"对偶：发生过的计数不因主体消亡而改写）；摘除后同关键区执行合拢判定（可因"应到集合缩小"而即时推进并唤醒——死亡不空转的机制本体）。摘除 MUST NOT 触碰其他会话的配额、已到场计数与在集等待项（除合拢唤醒的正常效应外零扰动）；MUST NOT 使条目进入任何终止/破相形态（无此概念）。该会话在 phaser 等待集中的登记随同摘除（等待项无租约、随会话灭——判例 v8"死亡即退订"与 v9"等待者无租约"两口径的合流）。`registeredParties` 归零属常态中间态而非生命周期终点（空转条款见合拢需求）；条目不设显式销毁操作（与 BARRIER/LATCH/QUEUE 同款，运维面经 key 治理而非 API）。

恢复路径：`CoreStateRestore` 增 PHASER 分支、`PhaserEntry.restored` 工厂——复制账簿（相位、registered、配额表、arrived、去重槽、换代窗口窗口）自快照逐字段回灌，等待集恢复为空（Leader 易失、客户端重挂补登记——重挂非双登记）；重启后旧请求重发命中恢复的去重槽与换代窗口照常裁决。

#### Scenario: 已到场者死亡不撤销其到场

- **WHEN** registered=3、arrived=2（含会话 S 的一个已到场身份）时 S 进程被杀（`SESSION_CLOSE` 应用）
- **THEN** registered 3→2、arrived 保持 2 → 合拢成立、相位推进并唤醒在集等待项；恢复/回放该条目日志的副本同判（摘除是 apply 内确定性迁移）

#### Scenario: 未到场者死亡摘除应到集合

- **WHEN** registered=3、arrived=1 时未到场的会话 S（配额 1）死亡
- **THEN** registered 3→2、arrived 保持 1、相位不推进；剩余两名未到场者到场后（arrived=3>registered=2）照常合拢，无饿死（死亡不空转）
- **AND** 同 key 其他会话的配额、等待集登记逐项不变

#### Scenario: 死亡摘除不入等待者计数

- **WHEN** S 死亡时其有 2 个在集等待项（AWAIT_ADVANCE 挂起）
- **THEN** 等待集摘除 S 的两项、`waiterCount` 相应回落；复制账簿仅 registered 变化——等待集无账可碰（非复制态）

#### Scenario: 重启恢复后重发与重挂照常裁决

- **WHEN** 含 phaser 账簿（推进若干相位、有在场去重槽与换代窗口）的状态生成快照、重启加载、旧等待方以原 request_id 重发
- **THEN** 相位号、配额、arrived、槽与换代窗口逐字段一致；命中槽/换代窗口的重发按幂等/了结裁决不双计数；等待集为空、旧唤醒不投递，客户端重挂后续唤醒照常

#### Scenario: 到期清扫与租约机制对 phaser 零触碰

- **WHEN** phaser 键经注册/到场建立账簿与镜像后，日志后续含另一租约有持锁 key 的到期条目，且该 phaser 键此后再无任何操作（静置）
- **THEN** 副本影子镜像 phaser 条目存续：相位/registered/arrived 逐项保真，LIST_KEYS/KEY_DETAIL 对该键照常命中、MUST NOT 以未命中壳呈现；清扫摘除的到期集仅含实际到期的锁 key，到期释放计数仅按实际释放计——静置键存续不依赖"后续变异事件自愈"路径

### Requirement: TimerEntry 时钟谓词账簿与到期判定

引擎 SHALL 提供 `TimerEntry`（`KeyFamily.TIMER`）实现"定时单次标记"：账簿 = **代次号**（`generation`，自 1 起每次 SCHEDULE 严格 +1，单调递增、不取模、重启/换主不回退）+ **装载态**（`armed` 与绝对到期时刻 `fire_at_ms`：`PENDING@fire_at_ms` 或 `DISARMED` 代终结）+ **每会话装载去重槽**（单槽 `{request_id, op, 回执回声}`，判例 v7 队列交付槽覆盖式语义）+ Leader 易失等待集（`(session, request_id) → 登记时刻`，到达序）。**到期判定为纯函数**：当代 `marked = armed ∧ 判定时刻 ≥ fire_at_ms`——MUST NOT 存在"已到期"驻留位或任何需由时钟写入的状态迁移（"到期不是迁移，是时间的兑现"——v7 就绪驱动"到期是可见性判定"公式的账簿侧对偶；`fire_at_ms` 由 SCHEDULE 条目的应用点以条目携带时刻折算为绝对值，Leader 切换与副本回放不改判，判例 v7 队列延时形态逐字适用）。判定矩阵：

- **SCHEDULE**（变异）：无条目时创建并定型 TIMER 家族（判例 v5/v10 首建定型）；有条目时**换代清钟**——`generation++`、`armed=true`、`fire_at_ms = 条目时刻 + delay`，粘滞 `marked` 随换代归伪（重装载即开新代，round 语义由"粘滞 + 换代清零"承载）；同 (会话, request_id) 重发命中去重槽回放回声（同代次、不双换代、不双改态）；旧 rid 在槽被覆盖后的迟到重发按新变异执行（声明竞态，判例 v7 槽覆盖与 v5 D7 窗口口径同族）；`delay` 值域 `[0, max-timer-horizon-ms]`（受理点钳制，越界内部拒绝码 `REJECT_TIMER_DELAY_OVER` 线路映射 `INVALID_REQUEST`——租约越界判例；判定唯一在受理点，配置漂移不撕裂账簿）。
- **DISARM**（变异）：当代转 `DISARMED`（代终结粘滞——当代永不再可 marked，直至下一次 SCHEDULE 开新代；`fire_at_ms` 保持撤销前值不重写，历史观察值如实存续）；同关键区收集全体在等旁观者的唤醒（`DENIED` 终态——撤销唤醒靠事件即时、不等到期 tick）；已 `DISARMED` 再 DISARM → 幂等回声零迁移零唤醒（判例 v9 LEAVE）；去重槽承载 DISARM 回声（同 rid 重发不双迁移）。
- **AWAIT**（Leader 本地、零日志）：以判定时刻重评谓词——当代 `PENDING` 且已到期 → 即刻 `OK{marked=true}`（粘滞共见：fire 之后到达的等待者即刻通过）；当代 `DISARMED` → 即刻 `DENIED`（等待不可满足的显式终态）；当代 `PENDING` 未到期 → `QUEUED` 登记（幂等：同 (会话, request_id) 重发命中在集项返回登记回执不二次入集；改期使在等旁观者以最新代为准——续等新代或到期先响皆合法形态，契约声明）；无条目 → `INVALID_REQUEST`（`REJECT_TIMER_NO_ENTRY` 映射，MUST NOT 隐式建钟）。
- **CANCEL**（Leader 本地、零日志）：按 (会话, await_request_id) 摘除等待项，幂等 `OK`（不存在即无操作——判例 v9 LEAVE/v10 CANCEL）；不改账簿。
- **QUERY**（Leader 本地、零日志、零迁移）：回当代 `{generation, armed, fire_at_ms, marked}` 抓取读数（marked 按判定时刻折算——advisory 观察，即刻过期是契约，判例 v10 `getPhase()` 语言事实句）；MUST NOT 推进代次、装载态、去重槽或清扫时序；无条目 → `INVALID_REQUEST`。

条目锁内读写、单关键区：DISARM 的迁移与唤醒收集同关键区完成（判例 v9 D4"账簿迁移→唤醒筛选单锁域"）；到期唤醒由扫描侧读谓词收集（等待集摘除随通知窗口纪律，ghost 由 `headReplyTimeoutMs` 清扫兜底）；等待集为进程易失态不入 digest、不入快照。跨副本判定确定性：SCHEDULE/DISARM 的账簿迁移仅依赖条目携带时刻与账簿内值——MUST NOT 依赖墙钟、Leader 身份或本地随机数；查询/等待的 marked 折算使用判定节点本地时钟，此为声明的读面降级（跨节点可见偏差 ≤ 时钟偏移），不构成账簿迁移输入。

#### Scenario: 装载幂等重发单换代

- **WHEN** 同会话以同一 request_id 重发 SCHEDULE（首次回执丢失），其间无其他装载
- **THEN** 命中去重槽回放首次回声：代次不前进、到期时刻不重写，应答与首次逐项一致

#### Scenario: 到期谓词三时点判定

- **WHEN** 以注入时刻依次在 `fire_at_ms` 之前、恰等、之后调用 AWAIT/QUERY
- **THEN** 之前 → `QUEUED`（登记在集）；恰等与之后 → `OK` 且 `marked=true`（粘滞共见，此后任何到达的 AWAIT 恒即刻通过直至换代或撤销）

#### Scenario: 重装载换代清钟

- **WHEN** 当代已到期共见（marked 粘滞为真）后再次 SCHEDULE
- **THEN** 代次 +1、`armed=true`、`fire_at_ms` 为新折算值、marked 归伪；此前到达的等待者谓词重评不再以旧代到期成立——以最新代为准

#### Scenario: 撤销终结与唤醒了结

- **WHEN** 三名等待者挂起于 PENDING 未到期当代，随后 DISARM 经提交应用
- **THEN** 三名等待者被同关键区收集唤醒；其重发 AWAIT 恒以 `DENIED` 终态了结（等待即终结，无二次 `QUEUED`）；后续新到 AWAIT 即刻 `DENIED`；再次 DISARM 幂等 `OK` 零唤醒零迁移

#### Scenario: 撤销与到期竞态以当值为准

- **WHEN** 到期 tick 唤醒与 DISARM 应用交叠，等待者在两者之间重发
- **THEN** 终态以重发判定时刻的当值为准：DISARMED 先落则 `DENIED`、到期先判且未撤则 `OK`——两序皆合法、皆无"既非 OK 又非 DENIED"的悬挂形态；代终结粘滞使 DISARMED 优先于同代到期成立

#### Scenario: 无条目操作拒绝且不隐式建钟

- **WHEN** 对从未装载的 key 发起 AWAIT/DISARM/CANCEL/QUERY
- **THEN** 全部 `INVALID_REQUEST` 映射拒绝、零条目零建条目；仅 SCHEDULE 可创建条目并定型家族

#### Scenario: 恢复与导出确定性

- **WHEN** 经 `restored` 工厂回灌账簿（代次/装载态/到期时刻/去重槽）并序列化导出，同态双副本各导一次
- **THEN** 逐字段保真、代次单调不回退、命中槽重发仍单换代；去重槽表按会话 id 升序、等待集不入序列化面——同账簿终态不同等待集构造导出字节等

### Requirement: TIMER 条目生命周期与会话无关

timer 账簿 SHALL 绑定 key 而非会话：装载者与等待者会话的生死 MUST NOT 改变 `{generation, armed, fire_at_ms}` 任何值——**创建者死亡钟照响**（`SESSION_CLOSE` 对 timer 账簿零扰动，判例 v7"死亡不吞元素"同轴：触发一旦装载即脱离发起方归属；此为本原语相对进程本地 `java.util.Timer` 的核心增量，契约显式声明）。死亡摘除仅及于辅助层：该会话在 Leader 等待集的挂起项与其装载去重槽行随会话摘除（等待是订阅必随会话灭；去重槽是幂等回声辅助结构、回声已无重放方——判例 v10 配额行随死亡删除的同构，恢复侧孤儿槽过滤与导出因此对称）。timer 条目录家族死亡的三种形态 MUST 并置成文、互不混读：Barrier——死亡即破障（定型应到集合不可逆）；Phaser——死亡隐式摘除配额且已到场事实不撤销（动态应到集合）；Timer——死亡零扰动（无应到集合概念，触发不依赖任何在场者）。条目存续不回收（判例 v5 D9：key 即 timer 身份、代次单调依赖条目存续，DISARMED 终态亦保留账簿供观察，常驻由 maxKeys 兜底）。

#### Scenario: 装载者死亡钟照响

- **WHEN** 会话 A 装载 `delay=T` 后进程死亡（`SESSION_CLOSE` 应用），会话 B 于 T 后 AWAIT/QUERY
- **THEN** B 见当代 `marked=true` 即刻 `OK`——账簿在死亡前后逐字段一致；A 若在等待集挂有旁观项则随会话摘除，不阻 B 共见

#### Scenario: 死亡三形态互不串扰

- **WHEN** 同一集群内 barrier key 参与者死亡、phaser key 参与者死亡、timer key 装载者死亡三事并列发生
- **THEN** barrier 破障、phaser 配额摘除可推进、timer 零扰动——三者各自按本家族契约收敛，互不影响对方的账簿与等待面

#### Scenario: 条目常驻与重复关闭幂等

- **WHEN** timer 条目装载、到期、撤销、再装载循环若干轮后经历同会话重复 `SESSION_CLOSE`
- **THEN** 条目存续可读（代次累计），会话重复关闭无二次副作用、账簿逐字段不变
