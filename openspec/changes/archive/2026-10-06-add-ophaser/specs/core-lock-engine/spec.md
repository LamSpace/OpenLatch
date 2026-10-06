# Spec Delta

## MODIFIED Requirements

### Requirement: 条目家族定型与类型不匹配拒绝

key 条目 SHALL 由首次创建它的请求定型所属家族：LOCK（`REENTRANT`/`SIMPLE`/`READ`/`WRITE`/`FAIR`）、SEMAPHORE、LATCH、ATOMIC（`ATOMIC_LONG`/`ATOMIC_INTEGER`/`ATOMIC_BOOLEAN`/`ATOMIC_REFERENCE` 四形态同族共享条目类型，形态间互斥——条目定型后形态不可变更）、BARRIER、QUEUE（`LOCK_TYPE_QUEUE`/`LOCK_TYPE_DELAY_QUEUE` 两形态同族共享条目类型，形态间互斥——出队规则由定型形态决定：到达序 vs 最早到期序）、PHASER（`LOCK_TYPE_PHASER` 单形态家族——phaser 无子形态判别，动态注册使"参与者构成"成为账簿内容而非定型属性，对照 BARRIER 的 parties 定型断言为刻意分轨）。`FAIR` 与 `REENTRANT` 同为锁家族且语义等价（互通互认，可相互重入）。跨家族请求 MUST 返回结果 `REJECT_TYPE_MISMATCH` 且 MUST NOT 改变条目任何状态；同跨形态的 ATOMIC 请求（如 `ATOMIC_LONG` 条目上请求 `ATOMIC_INTEGER`，或标量形态条目上请求 `ATOMIC_REFERENCE` 及反向）与同跨形态的 QUEUE 请求（`QUEUE` 条目上请求 `DELAY_QUEUE` 及反向）MUST 同样返回 `REJECT_TYPE_MISMATCH`；server 层将该结果映射为协议 `INVALID_REQUEST`。锁家族既有条目的定型规则（可重入性由首次请求决定）MUST NOT 变化。

#### Scenario: 跨家族请求被拒

- **WHEN** 以 `REENTRANT` 建立并持有的 key 上发起 `SEMAPHORE`、`LATCH_AWAIT`、`ATOMIC_OP`、`BARRIER_AWAIT`、`QUEUE_OP` 或 `PHASER_OP` 请求
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

## ADDED Requirements

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

PHASER 条目 SHALL 定型后存续不回收（判例 v5 D9 BARRIER：key 即 phaser 身份、相位单调与配额账簿依赖条目存续；回收再重建丢账）；无租约家族不入到期堆（v6/v7 同款），常驻内存量 = O(key 数)、maxKeys 护栏兜底。会话死亡（`SESSION_CLOSE` 应用点与断连清理在条目侧的同一收口）SHALL 触发**隐式配额摘除**：该会话在全部 phaser key 的注册配额自 `registeredParties` 减除、配额账簿行删除；**已到场事实不撤销**（当前相位 `arrived` 不回退——到场是复制态事实，与 v7"死亡不吞元素"对偶：发生过的计数不因主体消亡而改写）；摘除后同关键区执行合拢判定（可因"应到集合缩小"而即时推进并唤醒——死亡不空转的机制本体）。摘除 MUST NOT 触碰其他会话的配额、已到场计数与在集等待项（除合拢唤醒的正常效应外零扰动）；MUST NOT 使条目进入任何终止/破相形态（无此概念）。该会话在 phaser 等待集中的登记随同摘除（等待项无租约、随会话灭——判例 v8"死亡即退订"与 v9"等待者无租约"两口径的合流）。`registeredParties` 归零属常态中间态而非生命周期终点（空转条款见合拢需求）；条目不设显式销毁操作（与 BARRIER/LATCH/QUEUE 同款，运维面经 key 治理而非 API）。

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
