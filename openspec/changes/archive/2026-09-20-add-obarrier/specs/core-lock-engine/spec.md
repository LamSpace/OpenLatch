## MODIFIED Requirements

### Requirement: 条目家族定型与类型不匹配拒绝

key 条目 SHALL 由首次创建它的请求定型所属家族：LOCK（`REENTRANT`/`SIMPLE`/`READ`/`WRITE`/`FAIR`）、SEMAPHORE、LATCH、ATOMIC（`ATOMIC_LONG`/`ATOMIC_INTEGER`/`ATOMIC_BOOLEAN` 三形态同族共享条目类型，形态间互斥——条目定型后形态不可变更）、BARRIER。`FAIR` 与 `REENTRANT` 同为锁家族且语义等价（互通互认，可相互重入）。跨家族请求 MUST 返回结果 `REJECT_TYPE_MISMATCH` 且 MUST NOT 改变条目任何状态；同跨形态的 ATOMIC 请求（如 `ATOMIC_LONG` 条目上请求 `ATOMIC_INTEGER`）MUST 同样返回 `REJECT_TYPE_MISMATCH`；server 层将该结果映射为协议 `INVALID_REQUEST`。锁家族既有条目的定型规则（可重入性由首次请求决定）MUST NOT 变化。

#### Scenario: 跨家族请求被拒

- **WHEN** 以 `REENTRANT` 建立并持有的 key 上发起 `SEMAPHORE`、`LATCH_AWAIT`、`ATOMIC_OP` 或 `BARRIER_AWAIT` 请求
- **THEN** 返回类型不匹配拒绝，原持有者、租约与等待队列逐项不变

#### Scenario: FAIR 与 REENTRANT 互通

- **WHEN** 归属已以 `REENTRANT` 持有 key，再以 `FAIR` 类型重复获取
- **THEN** 按可重入语义授予（计数 +1），视同同类型重入

#### Scenario: 同族跨形态被拒

- **WHEN** 已以 `ATOMIC_LONG` 建立并写入的 key 上发起 `lock_type = ATOMIC_INTEGER` 的 ATOMIC 操作
- **THEN** 返回 `REJECT_TYPE_MISMATCH`，值与版本戳逐项不变

#### Scenario: LATCH 与 BARRIER 互斥

- **WHEN** 已以 `LATCH_AWAIT` 定型的 key 上发起 `BARRIER_AWAIT`，或反向
- **THEN** 返回 `REJECT_TYPE_MISMATCH`，既有条目状态零扰动

## ADDED Requirements

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
