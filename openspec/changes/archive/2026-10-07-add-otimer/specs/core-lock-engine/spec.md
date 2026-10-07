# Spec Delta

## MODIFIED Requirements

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

## ADDED Requirements

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
