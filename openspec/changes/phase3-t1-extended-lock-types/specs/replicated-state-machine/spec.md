## ADDED Requirements

### Requirement: 扩展原语复制边界

Semaphore 的授予、释放、租约到期与 Latch 的 `countDown` SHALL 经复制日志多数派提交后应用，与锁的 `LOCK_ACQUIRE_ENTRY` 等既有条目模式一致（新增条目类型仅以新增枚举值表达，既有编号不变）；未达多数派时 MUST NOT 产生授予/扣减效果。Latch 的 `await` MUST NOT 进入日志：其为 Leader 内存队列表述 + 状态读取（计数已归零即答 OK），Leader 切换时在队 awaiter 的处置与锁等待队列同口径（不迁移、客户端重发现后重发）。

#### Scenario: 少数派分区不扣许可

- **WHEN** Leader 被隔离失去多数派时处理 Semaphore `acquire(2)`
- **THEN** 请求失败（无提交），全集群可用许可数不变

#### Scenario: countDown 经复制全副本生效

- **WHEN** `countDown` 提交后查询任一回放副本的 latch 状态
- **THEN** 计数一致下降；归零后各副本状态一致为放行态

#### Scenario: 池回收后的队首重发（Semaphore 集群生命周期）

- **WHEN** Semaphore 池随持有清零被回收后，Leader 队列中的队首以不携带总量断言的请求重发获取
- **THEN** 该请求以 `INVALID_REQUEST` 显式拒绝（纯加入者 MUST NOT 隐式重建池）；携带相符总量断言的重发则重建同规模池并按队首授予，位次序保持

#### Scenario: await 零日志增长

- **WHEN** 多个客户端在 Leader 上 await 同一未归零屏障
- **THEN** 复制日志无任何新条目

### Requirement: 许可数感知的队首推进

集群等待队列条目 SHALL 携带其请求许可数；Leader 在授予/释放/到期应用后 MUST 仅当可用许可数满足队首请求时通知队首（AWAIT_NOTIFY），队首不满足时 MUST NOT 通知队列中任何后续条目（与单机 SemaphoreEntry 判定语义等价）。Latch 归零时 Leader MUST 向该 key 全部 awaiter 广播通知。

#### Scenario: 队首不足不通知

- **WHEN** 集群模式下队首请求 2 个许可、归还 1 个后不足
- **THEN** 无任何等待者收到通知；再归还至满足队首时仅队首收到通知

#### Scenario: failover 后 latch 计数不丢

- **WHEN** latch 计数为 1 时 Leader 被杀，新 Leader 完成日志回放
- **THEN** 新 Leader 上计数为 1，`countDown(1)` 后向重连后重新 await 的等待者广播放行
