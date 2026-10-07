# Spec Delta

## MODIFIED Requirements

### Requirement: PHASER 条目生命周期与会话清理

PHASER 条目 SHALL 定型后存续不回收（判例 v5 D9 BARRIER：key 即 phaser 身份、相位单调与配额账簿依赖条目存续；回收再重建丢账）；无租约家族到期字段恒 0、不入租约到期堆、亦不被租约到期清扫回收（v6/v7 同款，判例队列"到期清扫与租约机制对队列零触碰"逐字对齐）——"不被清扫回收"及于副本影子镜像：镜像条目存续不依赖该 key 的后续变异，静置 phaser 键不因任一租约到期自管理面消失，清扫摘除集 MUST NOT 含无租约家族镜像条目（到期释放计数仅按实际释放计，幻影键 MUST NOT 入集）。常驻内存量 = O(key 数)、maxKeys 护栏兜底。会话死亡（`SESSION_CLOSE` 应用点与断连清理在条目侧的同一收口）SHALL 触发**隐式配额摘除**：该会话在全部 phaser key 的注册配额自 `registeredParties` 减除、配额账簿行删除；**已到场事实不撤销**（当前相位 `arrived` 不回退——到场是复制态事实，与 v7"死亡不吞元素"对偶：发生过的计数不因主体消亡而改写）；摘除后同关键区执行合拢判定（可因"应到集合缩小"而即时推进并唤醒——死亡不空转的机制本体）。摘除 MUST NOT 触碰其他会话的配额、已到场计数与在集等待项（除合拢唤醒的正常效应外零扰动）；MUST NOT 使条目进入任何终止/破相形态（无此概念）。该会话在 phaser 等待集中的登记随同摘除（等待项无租约、随会话灭——判例 v8"死亡即退订"与 v9"等待者无租约"两口径的合流）。`registeredParties` 归零属常态中间态而非生命周期终点（空转条款见合拢需求）；条目不设显式销毁操作（与 BARRIER/LATCH/QUEUE 同款，运维面经 key 治理而非 API）。

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

#### Scenario: 到期清扫与租约机制对 phaser 零触碰

- **WHEN** phaser 键经注册/到场建立账簿与镜像后，日志后续含另一租约有持锁 key 的到期条目，且该 phaser 键此后再无任何操作（静置）
- **THEN** 副本影子镜像 phaser 条目存续：相位/registered/arrived 逐项保真，LIST_KEYS/KEY_DETAIL 对该键照常命中、MUST NOT 以未命中壳呈现；清扫摘除的到期集仅含实际到期的锁 key，到期释放计数仅按实际释放计——静置键存续不依赖"后续变异事件自愈"路径
