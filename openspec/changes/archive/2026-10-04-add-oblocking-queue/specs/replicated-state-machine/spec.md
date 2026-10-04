## ADDED Requirements

### Requirement: 队列操作复制边界与重放确定性

QUEUE 家族的队列操作 SHALL 以新条目类型 `RaftEntryType.QUEUE_OP_ENTRY = 13` 承载（`command_payload` 为 `QueueOpPayload{session_id, request_id, request}` 序列化，复用既有请求消息，MUST NOT 变更已发布编号）；复制边界：写（PUT/TAKE/DRAIN）与读（PEEK/SIZE）皆经多数派提交后应用（判例 v4 ATOMIC 读写全量经提交；读条目应用为零迁移），未达多数派 MUST NOT 产生元素入队/出队/去重槽变更。**挂起与唤醒不入日志**：阻塞式 PUT/TAKE 的等待登记（Leader 本地 `WaitQueue` 双轨）、`AWAIT_NOTIFY` 推送、已通知标记与超时清扫均为 Leader 本地态，MUST NOT 产生日志条目（判例 Latch/BARRIER"等待队列不入日志"）。应用点竞态回弹：预检放行提交后在应用点不可满足（元素被并发消费者摘走、容量被并发写入占满）的立即式与阻塞式命令 MUST 以 `ApplyStatus.DENIED` 回执、条目应用为**零迁移**（不改元素、不写去重槽），Leader 据该回执把阻塞式等待者**原位重新挂回**本地等待队列（位次不丢、不推错误给客户端）——回弹不引入副本分歧（DENIED 回执非复制状态）。到期时刻 MUST 于应用点以条目携带时刻折算（`expires_at_ms = wall_clock_ms + delay_ms`，加法溢出收纳至 `Long.MAX_VALUE`）——与租约 `expires_at_ms` 同构，任何副本任何物理时刻重放得出同一到期时刻；物理时钟与节点本地配置 MUST NOT 参与 apply 判定。`DRAIN` 的提取上限以**接入层钳定后的 N** 随条目进日志，应用点摘出 `min(N, 自队首起连续满足出队谓词段)`，全副本一致。每会话去重槽（`(session) → (op_seq, op, 回执)`）跨副本重放一致：同 `(session, op_seq)` 重放在任何副本得出**字节级相同**的回执（TAKE/DRAIN 重放交付同一份字节、不重复摘取），条目侧 MUST NOT 复核 `maxValueBytes`/`maxQueueCapacity`/`maxDrainBytes`（超限与钳制命令在接入层即拒、永不入日志，入日志者视为已钳制——判例 v6 配置漂移红线）。`ApplyResult` SHALL 以纯增量字段承载队列回执：`optional bytes queue_element_bytes = 20`、`repeated bytes queue_drained_bytes = 21`、`sint64 queue_size = 22`（仅队列条目择用，其余条目 MUST NOT 携带）。

#### Scenario: 并发队列写经复制全副本一致

- **WHEN** 多会话对同 key 并发提交 PUT/TAKE（含交错的元素字节），全部条目提交后在各副本比对队列状态
- **THEN** 各副本元素队列（载荷字节、到期时刻、顺序）、深度与每会话去重槽逐字段一致，全量摘要相等

#### Scenario: 延时条目跨副本同一到期时刻

- **WHEN** 携带 `delay_ms=3000` 的 PUT 在 Leader 应用后，落后副本于完全不同的物理时刻回放该条目
- **THEN** 回放副本得出的元素到期时刻与 Leader 逐毫秒一致（条目携带时刻折算，不读本地时钟）

#### Scenario: 读条目重放零迁移

- **WHEN** 含 PEEK/SIZE 条目的日志在新增副本上全量回放
- **THEN** 回放后该 key 元素与去重槽状态与无读条目时逐字节一致（摘要相等）

#### Scenario: 换主后重发经槽不双插不偷吃

- **WHEN** PUT 提交成功但应答丢失，Leader 切换后客户端向新 Leader 以同 `(session, op_seq)` 重发；另一场景为 TAKE 交付后应答丢失重发
- **THEN** PUT 重放命中回放所得槽、返回字节级一致回执且元素不双插；TAKE 重放重发**同一份已交付元素字节**、不重复摘取队首

#### Scenario: DENIED 回弹零迁移

- **WHEN** 两个阻塞 TAKE 的预检先后放行、仅一个元素在队，二者提交后一者应用点授予、一者应用点 `DENIED`
- **THEN** DENIED 条目不改任何复制状态（元素恰被摘取一次），被回弹的等待者仍在 Leader 本地队列原位等待下一次唤醒；各副本回放终态一致

#### Scenario: 配置漂移不分歧

- **WHEN** 一节点 `maxValueBytes`/`maxQueueCapacity` 配置低于写入时的值，该节点回放含大元素与大容量定型的日志
- **THEN** 回放照常落态、与 Leader 终态逐字节一致（条目侧不复核），副本间无分歧
