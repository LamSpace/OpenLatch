# Spec Delta

## ADDED Requirements

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
