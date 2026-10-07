# Spec Delta

## ADDED Requirements

### Requirement: timer 操作复制边界与重放确定性

timer 采用**装载/撤销入日志、到期与观察零日志**的复制边界（半入日志家族：沿 v5/v10"迁移入日志"纪律——装载决定"未来某刻对谁可见"是复制态迁移，落地纪律第 2 条对本原语不适用豁免；**到期是派生谓词不是迁移**——`marked = armed ∧ 判定时刻 ≥ fire_at_ms` 为复制数据与钟的纯函数，v7 就绪驱动"到期是可见性判定而非复制状态迁移"公式的类目化，与 v3 租约到期"到期是状态迁移故入日志"构成判例族两端对照）：SCHEDULE/DISARM 每一变异操作 MUST 恰产生**一条 `TIMER_OP_ENTRY`**（`command_payload` 为 `TimerOpRequest` 序列化+会话包装，判例既有命令条目载荷形态）；AWAIT/CANCEL/QUERY MUST NOT 产生任何日志条目、MUST NOT 触达提交通道与 apply 路径（"等待是订阅不是状态"与"观察不是迁移"两判例族直接延伸）；**到期（时钟越过 `fire_at_ms`）MUST NOT 产生任何日志条目**——不存在"fire 事件"的复制形态，唤醒为 Leader 本地扫描提示、终态为等待方重发时的谓词重评。由此推导的常驻守卫断言（改动本条款 MUST 先经 ROADMAP 决策记录登记）：

1. 代次、装载态与绝对到期时刻为纯确定性 apply 迁移：SCHEDULE 的 `fire_at_ms` 由**条目携带时刻**（`wall_clock_ms`）加 `delay` 在应用点折算——回放与 live 应用同值（判例 v7 队列到期折算逐字适用），MUST NOT 依赖接收时刻、Leader 身份或本地随机数；跨副本 digest MUST 逐字节一致；
2. **到期零条目反向守卫**：纯 timer 流量下，日志仅在 SCHEDULE/DISARM 受理时新增；让时钟越过全体到期点后日志条数、digest 与快照 MUST 恒不变（等待/查询读数变化不进入任何复制面）——"到期误入日志即红"与"等待误入日志即红"构成对偶双守卫；
3. 重放幂等：每会话装载去重槽随账簿回放恢复——重启/追赶后命中槽的旧 rid 重发 MUST NOT 双换代、MUST NOT 双改态；槽被同会话新装载覆盖后的迟到重发按新变异执行（声明竞态与 v7 交付槽同口径）；代次 MUST 单调不回退（快照+尾部回放终态与不中断运行逐字段一致）；
4. 等待集为 Leader 进程易失态：换主/重启后恒空、由客户端重挂补登记——与"唤醒谓词在复制账簿"合流使重挂无损耗（已到期即刻 `OK` 了结、未到期续挂；对照 v9 signal 事件灭失窗：timer 无第三方事件可丢失，与 phaser 同侧证据再强化）；
5. 应用点 MUST NOT 依赖墙钟外生输入：`armed`/代次/`fire_at_ms` 迁移全部由条目数据决定；marked 折算仅发生在 Leader 应答读面（AWAIT 终态/QUERY），MUST NOT 回写账簿（无 `TRIPPED` 驻留位——对照 `LEASE_EXPIRE_ENTRY` 必须入日志的释放迁移：timer 到期无任何位需被置上）；
6. 编号证据：`TIMER_OP_ENTRY = 15` 为 v11 新增且仅 timer 占用；1–14 既有值语义逐项不变、MUST NOT 复用为 timer 变体；守卫基线由 v10 时代的"RaftEntryType 止于 14（14 为 phaser 专用）"更替为"止于 15（15 为 timer 专用）"（更替随本 change 常驻、断言按当前版本口径表述——防止两代上界证据同时锚死过期绝对值）。

#### Scenario: 变异单条目、等待查询零条目

- **WHEN** 三节点集群稳定 Leader 下执行 SCHEDULE × a、DISARM × b、AWAIT × c、CANCEL × d、QUERY × e，全程无其他业务
- **THEN** 日志新增条目数恰为 a+b 且全部为 `TIMER_OP_ENTRY`；等待/取消/查询贡献零条；三副本 digest 逐字节相等

#### Scenario: 时钟越过到期点日志与摘要恒不变

- **WHEN** 装载 `delay=T` 后推进集群时钟越过 T+tick×k（k≥2），期间执行若干 AWAIT 了结与 QUERY 读数，无新装载/撤销
- **THEN** `TIMER_OP_ENTRY` 计数与最后一条变异条目时刻相比零新增、三副本 digest 与快照字节不变、各副本对 `fire_at_ms` 的账簿读数不变——到期"发生"于谓词而不留复制足迹

#### Scenario: 到期时刻应用点折算跨副本与回放不改判

- **WHEN** 含 SCHEDULE 条目的日志在 Follower 回放、并经快照+尾部回放重建
- **THEN** 各副本对同一 key 的 `fire_at_ms` 逐字节相等（=条目 `wall_clock_ms`+`delay`，与各节点本地时钟无关）；换主后新 Leader 不改判既有到期时刻（判例 v7 确定化条款）

#### Scenario: 重启不双换代与迟到重发声明竞态

- **WHEN** 节点自含去重槽的快照重启并回放尾部 SCHEDULE/DISARM 条目，其间客户端以已覆盖槽位的旧 request_id 重发装载
- **THEN** 命中恢复槽的重发回放声（同代次）；槽被覆盖后的迟到重发按新变异执行（新代次、清钟——v5 D7/v7 同口径的声明竞态，测试钉两形态可观测边界）；代次全程单调不回退

#### Scenario: 换主等待清零重挂了结无损耗

- **WHEN** 等待者挂起于未到期当代期间换主、时钟于换主窗内越过 `fire_at_ms`，客户端经保活重发 `AWAIT(原 request_id)`
- **THEN** 新 Leader 账簿含原代次/到期时刻（复制态存续）、等待集仅含重挂登记（旧集合随进程灭）；重发即刻 `OK{marked=true}` 了结——"换主窗内已响的钟"由谓词自愈，MUST NOT 出现任何丢失触发的形态

#### Scenario: 到期误入日志即构建红

- **WHEN** 未来变更在到期判定/驱动扫描路径引入日志条目提交（如引入 `TIMER_FIRE_ENTRY` 型驻留位）
- **THEN** 本条款"时钟越过到期点日志恒零新增"断言转红，变更被打回规格修订流程（派生裁决的常驻守卫）
