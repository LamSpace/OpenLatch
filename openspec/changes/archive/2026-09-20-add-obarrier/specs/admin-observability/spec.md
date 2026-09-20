## MODIFIED Requirements

### Requirement: 管理查询应答契约

服务端 SHALL 对已握手且连接协商版本 ≥3 的会话提供四类只读管理查询，应答内容以本节点视角如实呈现：

- `ADMIN_SUMMARY`：按家族持有条目数（锁/Semaphore，Latch、ATOMIC 与 BARRIER 无持有语义单列条目数）、等待者总数、本节点活跃会话数、节点角色（单机/Leader/Follower）、自启动运行时长、服务端版本；
- `ADMIN_LIST_KEYS`：分页返回 key、家族、持有者（会话/线程/计数；ATOMIC 与 BARRIER 恒为空）、剩余租约毫秒（未持有为 0；ATOMIC 与 BARRIER 恒为 0）、等待者数（ATOMIC 恒为 0；BARRIER 为当前世代在队等待者数，仅 Leader 视角非零）与 BARRIER 的 parties/当前世代号/当前世代到场数（非 BARRIER 恒缺省），支持前缀过滤，`total_matched` 为过滤后总条数；
- `ADMIN_KEY_DETAIL`：单 key 的持有明细（会话、线程、重入计数或持有许可数、读/写角色）、等待队列（位次、会话、请求 id、已等待时长）、剩余租约到期时刻、Latch 的总量/剩余、ATOMIC 的形态/当前值/版本戳/定型初值、BARRIER 的 parties/当前世代/当前到场数/动作挂账有无/最近完结世代的了结形态（`tripped`/`broken`/`none`）；key 不存在回明确的未命中状态；
- `ADMIN_LIST_SESSIONS`：本节点接入的会话列表——id、接入节点（由会话 id 高位导出）、建连时刻、持锁 key 数、在队等待数。

管理查询 MUST NOT 变更任何锁、租约、等待队列或会话状态（纯观察；BARRIER 的观察 MUST NOT 推进到场数、世代号或了结记录）；应答 MUST NOT 依赖任何写路径成功（负载处理阻塞时查询仍可作答，读数允许弱一致）。

#### Scenario: 预置状态摘要与明细一致

- **WHEN** 预置多类型条目（重入/读写/信号量/Latch/原子变量/循环屏障、含等待者）后依次请求 SUMMARY、LIST_KEYS、KEY_DETAIL、LIST_SESSIONS
- **THEN** 各应答逐字段与预置状态吻合，SUMMARY 聚合与 LIST_KEYS 明细、KEY_DETAIL 与 LIST_SESSIONS 的持锁/等待计数互相自洽；ATOMIC 条目在 SUMMARY 单列条目数、在 KEY_DETAIL 呈现形态/值/版本戳；BARRIER 条目在 SUMMARY 单列条目数、在 KEY_DETAIL 呈现 parties/世代/到场数/动作挂账与了结形态

#### Scenario: 管理查询零扰动

- **WHEN** 反复执行全量管理查询后，业务客户端对同批 key 执行获取/释放/续租、ATOMIC 操作与 BARRIER await
- **THEN** 全部业务语义与不执行管理查询时逐项一致（含租约时刻不被观察行为刷新、ATOMIC 版本戳不因读取而推进、BARRIER 世代号与到场账簿不因读取而推进）

#### Scenario: 未知 key 详情明确未命中

- **WHEN** 对不存在的 key 请求 ADMIN_KEY_DETAIL
- **THEN** 应答携带明确的未命中状态码，MUST NOT 回空壳成功应答

#### Scenario: Follower 视角屏障等待者数为零

- **WHEN** 在 Follower 节点请求含在队等待者的 BARRIER key 的 LIST_KEYS/KEY_DETAIL
- **THEN** 等待者数为 0 且 `wait_queue_leader_only=true`，parties/世代/到场数照常呈现（复制状态）
