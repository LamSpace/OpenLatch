## MODIFIED Requirements

### Requirement: 管理查询应答契约

服务端 SHALL 对已握手且连接协商版本 ≥3 的会话提供四类只读管理查询，应答内容以本节点视角如实呈现：

- `ADMIN_SUMMARY`：按家族持有条目数（锁/Semaphore，Latch、ATOMIC、BARRIER 与 QUEUE 无持有语义单列条目数；ATOMIC 计数含标量与有值引用两形态，QUEUE 计数含两形态）、等待者总数、本节点活跃会话数、节点角色（单机/Leader/Follower）、自启动运行时长、服务端版本；
- `ADMIN_LIST_KEYS`：分页返回 key、家族、持有者（会话/线程/计数；ATOMIC、BARRIER 与 QUEUE 恒为空）、剩余租约毫秒（未持有为 0；ATOMIC、BARRIER 与 QUEUE 恒为 0）、等待者数（ATOMIC 恒为 0；BARRIER 为当前世代在队等待者数；QUEUE 为双轨挂起等待者合计，后两者仅 Leader 视角非零）、BARRIER 的 parties/当前世代号/当前世代到场数、有值引用条目的载荷字节大小（`atomic_payload_size`）及截断预览（`atomic_payload_preview`）与队列条目的定型容量（`queue_capacity`）、当前深度（`queue_depth`）、首元素字节大小（`queue_head_payload_size`）及截断预览（`queue_head_payload_preview`）（非对应家族恒缺省零值/空串），支持前缀过滤，`total_matched` 为过滤后总条数；
- `ADMIN_KEY_DETAIL`：单 key 的持有明细（会话、线程、重入计数或持有许可数、读/写角色）、等待队列（位次、会话、请求 id、已等待时长；QUEUE 含 put/take 双轨，以 `queue_track` 判别——1=等容量、2=等元素、0=非队列）、剩余租约到期时刻、Latch 的总量/剩余、ATOMIC 的形态/当前值/版本戳/定型初值（有值引用形态的初值与当前值以 `atomic_payload_size` + `atomic_payload_preview` 承载：预览为服务端构造的截断转义文本——可打印前缀原样、不可打印字节转义呈现，预览长度恒定不随 `maxValueBytes` 配置或实际载荷大小膨胀，null 值如实以缺省预览+零大小表达且与空串可区分——后者预览为空引号形态或等价显式标记）、BARRIER 的 parties/当前世代/当前到场数/动作挂账有无/最近完结世代的了结形态（`tripped`/`broken`/`none`）、QUEUE 的定型容量/当前深度/首元素到期时刻（`queue_head_expiry_ms`，DELAY 形态、受理节点时钟读数，非 DELAY 或空队为 0）/总驻留字节（`queue_total_payload_bytes`，元素载荷字节之和，观察驻留治理）/首元素大小与截断预览（`queue_head_payload_size`/`queue_head_payload_preview`，同一截断转义纪律）；key 不存在回明确的未命中状态；
- `ADMIN_LIST_SESSIONS`：本节点接入的会话列表——id、接入节点（由会话 id 高位导出）、建连时刻、持锁 key 数、在队等待数。

管理查询 MUST NOT 变更任何锁、租约、等待队列或会话状态（纯观察；BARRIER 的观察 MUST NOT 推进到场数、世代号或了结记录；队列的观察 MUST NOT 推进元素、深度、到期判定或去重槽）；应答 MUST NOT 依赖任何写路径成功（负载处理阻塞时查询仍可作答，读数允许弱一致）。观察面 MUST NOT 成为载荷放大器：全量载荷字节 MUST NOT 出现在任何管理应答中（仅大小+截断预览）；**全量元素列表 MUST NOT 出现在任何管理应答中**（仅深度、驻留总字节与首元素预览）。

#### Scenario: 预置状态摘要与明细一致

- **WHEN** 预置多类型条目（重入/读写/信号量/Latch/标量原子/有值引用（含 null、空串与恰限大载荷三种值态）/循环屏障/队列（含满容量、未到期延时元素与双轨挂起等待者）、含等待者）后依次请求 SUMMARY、LIST_KEYS、KEY_DETAIL、LIST_SESSIONS
- **THEN** 各应答逐字段与预置状态吻合，SUMMARY 聚合与 LIST_KEYS 明细、KEY_DETAIL 与 LIST_SESSIONS 的持锁/等待计数互相自洽；ATOMIC 条目在 SUMMARY 单列条目数（两形态合并计数）、在 KEY_DETAIL 呈现形态/值/版本戳；有值引用条目呈现字节大小与截断预览且预览不随配置膨胀；BARRIER 条目在 SUMMARY 单列条目数、在 KEY_DETAIL 呈现 parties/世代/到场数/动作挂账与了结形态；QUEUE 条目在 SUMMARY 单列条目数、在 LIST_KEYS 呈现容量/深度/首元素预览、在 KEY_DETAIL 呈现容量/深度/驻留总字节/到期时刻/首元素预览与双轨等待位次

#### Scenario: 大载荷观察零放大

- **WHEN** 对持有恰限字节载荷的有值引用 key 与满容量恰限元素的队列 key 分别请求 LIST_KEYS 与 KEY_DETAIL
- **THEN** 引用应答仅含大小与恒定长度截断预览；队列应答仅含深度/驻留总字节/首元素截断预览——**全量元素字节不出现在应答中**；反复观察不改变条目任何状态

#### Scenario: 管理查询零扰动

- **WHEN** 反复执行全量管理查询后，业务客户端对同批 key 执行获取/释放/续租、ATOMIC 操作（含引用形态）、BARRIER await 与队列 put/take/drainTo
- **THEN** 全部业务语义与不执行管理查询时逐项一致（含租约时刻不被观察行为刷新、ATOMIC 版本戳不因读取而推进、BARRIER 世代号与到场账簿不因读取而推进、队列元素/深度/去重槽不因观察变动）

#### Scenario: 未知 key 详情明确未命中

- **WHEN** 对不存在的 key 请求 ADMIN_KEY_DETAIL
- **THEN** 应答携带明确的未命中状态码，MUST NOT 回空壳成功应答

#### Scenario: Follower 视角屏障等待者数为零

- **WHEN** 在 Follower 节点请求含在队等待者的 BARRIER key 或含挂起等待者的 QUEUE key 的 LIST_KEYS/KEY_DETAIL
- **THEN** 等待者数为 0 且 `wait_queue_leader_only=true`，BARRIER 的 parties/世代/到场数与队列的容量/深度/驻留字节/到期时刻照常呈现（复制状态）
