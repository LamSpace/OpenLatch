## ADDED Requirements

### Requirement: 条目家族定型与类型不匹配拒绝

key 条目 SHALL 由首次创建它的请求定型所属家族：LOCK（`REENTRANT`/`SIMPLE`/`READ`/`WRITE`/`FAIR`）、SEMAPHORE、LATCH。`FAIR` 与 `REENTRANT` 同为锁家族且语义等价（互通互认，可相互重入）。跨家族请求 MUST 返回新结果 `REJECT_TYPE_MISMATCH` 且 MUST NOT 改变条目任何状态；server 层将该结果映射为协议 `INVALID_REQUEST`。锁家族既有条目的定型规则（可重入性由首次请求决定）MUST NOT 变化。

#### Scenario: 跨家族请求被拒

- **WHEN** 以 `REENTRANT` 建立并持有的 key 上发起 `SEMAPHORE` 或 `LATCH_AWAIT` 请求
- **THEN** 返回类型不匹配拒绝，原持有者、租约与等待队列逐项不变

#### Scenario: FAIR 与 REENTRANT 互通

- **WHEN** 归属已以 `REENTRANT` 持有 key，再以 `FAIR` 类型重复获取
- **THEN** 按可重入语义授予（计数 +1），视同同类型重入

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

`LatchEntry` SHALL 承载倒计数：初始值由首个 `await` 请求的 `count` 定型；`countDown(n)` 以 `count = max(0, count − n)` 更新，归零瞬间 MUST 对**全部**等待者发出通知（全体放行）；归零后 `await` MUST 立即通过、`countDown` MUST 为无操作。Latch 的等待者 MUST NOT 登记租约（不参与到期堆与续租），会话关闭仅摘除其等待者身份且 MUST NOT 影响计数。计数归零且无等待者后条目 SHALL 可被回收，回收即丧失一次性护栏（该 key 的后续带值 await 将新建屏障）。

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
