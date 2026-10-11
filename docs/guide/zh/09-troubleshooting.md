# 09 · 故障排查与 FAQ

## 1. 错误码语义总表

| 错误（协议码 / 客户端异常） | 出自哪里 | 含义 | 你该怎么做 |
|---|---|---|---|
| `QUEUED` | 服务端 | 已入队，等通知重发 | 正常排队，无需动作 |
| `LOCK_HELD` | 服务端 | 立即式获取时锁在他人手 | 按业务重试或走 `lock()` |
| `NOT_LEADER` | 服务端（转发/角色道） | 本节点非 Leader，**可重试**，随附提示 | 交给客户端自动改道；持续出现看第 3 节 |
| `NOT_HELD` | 服务端（权威裁决） | 该会话确未持有此锁（多数派提交后判定）；v9 起**双义**——释放/归还路径=失锁，`CONDITION_OP`（signal 家族）路径=signal 权限被拒，以**请求类型**判别 | 按请求类型分流：锁操作视为失锁（中止提交、重新竞争）；signal 映射为 `IllegalMonitorStateException`（修调用点） |
| `INVALID_TOKEN` | 服务端（权威裁决） | 释放/续租凭据与当前归属不符（常见：failover 回滚/租约已过期） | 同上，走失锁处理 |
| `SESSION_EXPIRED` | 服务端 | 会话已关闭 | 重连后重新竞争（客户端自动，业务收失锁回调） |
| `BARRIER_BROKEN` | 服务端（在带裁决） | 等待项所属循环屏障世代已破障（他方离场/死亡/超时/`breakBarrier()`） | 视为会合失败：本方中止本轮协作，可择机重入新世代 |
| `INVALID_REQUEST` | 服务端 | 协议违例：字段非法、握手前业务请求、版本越界、认证失败（不区分原因） | 检查调用方/配置；认证类查令牌 |
| `INTERNAL_ERROR` | 服务端 | 非预期内部失败 | 可重试；持续出现带日志找运维 |
| `OBrokenBarrierException` | 客户端 | 所等待世代被破障（离场即破障） | 中止本轮协作；若为误伤，排查网络抖动导致的参与者超时 |
| `LockAcquisitionTimeoutException` | 客户端 | 等待预算（默认 30s）耗尽 | 视业务：加大预算/拆临界区/降级 |
| `OpenLatchTimeoutException` | 客户端 | 单请求 5s 无应答（连接活着） | 检查节点负载/时钟；瞬时一次可容忍 |
| `ServerUnavailableException` | 客户端 | 连接不可用（含切换窗口快速失败） | 重试；检查种子配置 |
| `TimeoutException`（phaser `awaitAdvanceInterruptibly` 到期） | 客户端 | 等待预算内相位未推进越过已见值（先尽力 CANCEL 撤销） | 与 `awaitAdvance` 的 `OpenLatchTimeoutException` 同属有界等待收束；需更长等待调大预算或循环重入（v10） |
| `IllegalMonitorStateException` | 客户端 | 未持有而解锁/归还/条件 signal（v9 起 signal 双源同型：本地先行，或服务端 `NOT_HELD` 映射） | 修代码路径（生命周期管理） |
| `LockLostException`（回调） | 客户端 | 锁被剥夺 | 中止临界区提交——这是设计必答题 |

## 2. 故障转移行为基线（实测数据）

进程级演练在参考硬件（3 节点同机）上的量级，供告警阈值校准：

- `kill -9` Leader → 新授予恢复：**实测 1.6–1.7s**（判据上限 10s）；
- 杀 Follower → 业务无感（多数派在）；
- 滚动重启逐台 → 应用可见错误集中于每台 2–3s 窗口，末窗 + 45s 自愈预算后**残留必须为 0**；
- 等待队列位次在 Leader 切换后重排（预期，非故障）；
- **改道收敛**：客户端在 home 会话驻留期间按周期（1s）经 `CLUSTER_VIEW` 核对当值 Leader——home 活着停在非权威节点、或启动窗提示滞后时，队列/原子/屏障/闩/订阅/相位器/定时器请求会在数秒内改道至真主，而不是耗尽等待预算显式超时。该核对为纯提示刷新且静默降级：单机形态、服务端不支持该消息、探测失败或无当值 Leader 时都不发起或不动作，绝不计为业务失败。收敛只解决**路由**；在此基础上，等待类原语对 `NOT_LEADER` **应答状态**另有**有界重道**——闩/屏障的 `await` 在 2s 内等待改道、仅在路由会话确已变化时以新会话重发，预算耗尽仍以 `NOT_LEADER` 显式失败（既不假绿返回"已放行"，也不悬挂至等待总预算）；相位器/定时器的等待面沿用状态码终态抛出。

## 3. 反复 `NOT_LEADER` / 找不到 Leader

排查顺序：

1. **种子给全了吗**——只提供单地址且该地址宕机 = 无法自动恢复；
2. `client-addresses` 与实际接入地址是否一致（配错会让提示指向不可达地址，靠种子自报兜底但多绕一圈）；
3. 三节点是否真的多数派存活（`curl :9412/metrics | grep is_leader`，全零=无主）；
4. 无主且选举频繁 → 时钟漂移或网络分区（NTP 要求漂移 ≤1s）；
5. 有 Leader 但写持续失败且日志见 `replication stall detected` → 第 4 节。

## 4. 复制停摆（写面冻结）与自愈日志

服务端看门狗的日志序列（每节点）：

```
replication stall detected          # 判定：任期超阈值 + commitIndex 连续零推进
...abdicate/让位事件                 # 第一处置：向最新对侧让位，多数派重选恢复
escalating to process restart       # 观察窗仍冻结 → 进程退出码 1 自杀，等 supervisor 拉起
```

运维动作：

- **有 supervisor（systemd/K8s）**：确认重启退避 ≥ 5 分钟（进程内冷却窗），事件本身自愈，记录即可；
- **无 supervisor**：让位段仍会自动恢复；`escalating to process restart` 后**需人工重启该节点**，
  生产集群强烈建议配 supervisor（详见 [05 停摆自愈](05-cluster-deployment.md)）；
- 停摆窗口内的客户端表现：获取超时/`ServerUnavailable` 类错误聚集，恢复后归零——
  若"末窗+预算后仍有错误"，这不是运维问题而是产品回归，带日志报障。

## 5. 部署/构建类常见问题

| 症状 | 原因 | 处置 |
|---|---|---|
| 启动失败：`管理端口启动失败（端口 9412 可能被占用）` | 同机多节点指标口冲突（fail-fast 设计） | 每节点配互异 `metrics.port` 或 `0` |
| 演练"通过"但什么都没发生 | 缺 shaded jar / 无 sudo 时 assume-**skip**（不是失败） | 先 `mvn -pl openlatch-server -am package`；查 `Skipped: 0` |
| Spring 注解不生效 | 未开 `-parameters`；或自调用绕过代理 | 补编译器参数；经代理调用 |
| 同线程第二次 `lock()` 卡死到超时 | 用了 SIMPLE 锁（非可重入，属语义） | 换 REENTRANT |
| 客户端启动即报错 | 服务端未起/地址不通——首连失败不影响 bean 创建，异常在首次调用 | 检查 9410 连通性 |
| 锁"莫名消失" | 临界区超过续租能力（长 GC/停顿）或会话被清理 | 看失锁回调与 `locks.lost` 指标；缩短临界区/调大租约 |
| failover 后个别锁失效 | 切换窗口未复制达多数派的授予回滚 | 设计内（[05 一致性声明](05-cluster-deployment.md)），失锁回调必须实现 |

## 6. 演练判据速查（自检集群健康）

```bash
mvn -pl openlatch-server -am package
mvn -pl openlatch-client verify -Pdrill        # 杀主/滚动/分区（分区需 passwordless sudo）
mvn -pl openlatch-server verify -Pdrill        # 停摆采样（仪器类）
# 报告：各模块 target/drill-reports/<套件>-<日期>.md
```

判据：failover 恢复 <10s 且无双授；rolling 两序"末窗+45s 预算后残留=0"；分区少数派
授予/释放全拒且锁存活、撤除后自动收敛；采样轮非全 ABORTED。

## 队列（v7）排查速查

- **`offer` 返回 `false` 与抛 `OVERLOADED` 的区别**：前者是**元素满**的正常立即式
  终态（队满即拒，等价 JDK `offer`）；后者是**等待挂起队列满**（本 key 在队等待者
  达 `max-queue-depth-per-key`），挂起请求被拒——两者都指示消费侧跟不上生产侧，
  但严重度不同（后者说明挂起已经堆积）；
- **`put`/`take` 长时间无返回值**：查三点——①对端是否真的会腾位/投递（控制台队列
  区段看深度与双轨等待数）；②Leader 是否稳定（切换窗口的挂起会随任期清零、由
  客户端重挂自愈，但每次换主会重置等待位次）；③延时队列的到点唤醒精度为
  `ready-tick-ms`（默认 200ms）量级，若消费一直不来且深度含未到期元素——查注入
  时刻与 `delay` 是否被业务侧算错（绝对到期以服务端条目时刻折算）；
- **`OpenLatchException(INVALID_REQUEST)` 的队列含义**：元素超限（>4KB 默认）、
  容量主张与定型不符、同 key 队列/延时形态互拒、低版本会话——异常消息携带状态码
  名，逐项对照服务端入口裁决表定位；
- **元素"没被消费却一直占内存"**：契约即如此——元素绑定 key 不绑定会话，服务端无
  自动回收；排期清理靠业务侧键命名（轮次/租户前缀）与控制台驻留读数。

## topic（v8）排查速查

- **"换主后收不到消息"的判别三分法**：① 先查 SDK 侧 home/车道重连日志——订阅重挂
  依赖连接重建（30s 保活周期兜底"同节点换主而连接未断"的情形），断档期消息本就不
  补投（至多一次契约）；② 再看服务端 Leader 订阅登记（控制台/`ADMIN_KEY_DETAIL` 的
  topic 区段）是否恢复该订阅者的登记；③ 若登记在、消息仍不达，查
  `openlatch_server_topic_dropped_total` 增速与订阅侧 `droppedCount()`——丢弃是
  缓冲满的 drop-newest，不是故障；
- **`REJECT_SUBSCRIBERS`**：该 key 订阅数达 `max-subscribers-per-key`（默认 64）——
  优先排查订阅者泄漏（进程活着但忘 `unsubscribe`/句柄换代未清理），限额确属业务
  容量则上调配置；既有订阅不受该拒绝影响（不挤占、不断开）；
- **`droppedCount()` 持续增长**：慢消费者信号（服务端或 SDK 本地两级缓冲满）。
  处置：监听器内不做重活（转业务线程池）、扩消费并行度或拆 topic 键降扇出；
  注意它是同任期 gap 推断 + 本地溢出计数，换主/重挂后基线重置，跨任期不累计；
- **`OpenLatchException(INVALID_REQUEST)` 的 topic 含义**：消息体超限（>4KB 默认）/
  缺省、形状违例（SUBSCRIBE 携带载荷或 `op_seq`）、撞 key（与锁/队列等同名，
  尽力而为拒绝）、低版本会话（v≤7 发 topic 消息）——异常消息携带状态码名对照；
- **疑似"消息双投"**：至多一次承诺下唯一双投来源是**跨换主的发布重试**（旧 Leader
  已扇出、新 Leader 去重槽为空再扇出）。核对两次交付的 `publisherSessionId` 与
  任期边界；消费侧幂等是契约义务（按业务键去重），不是服务端缺陷；
- **topic 在 Follower 上的管理读数**：订阅登记为 Leader 本地态——Follower 的
  SUMMARY `topic_entries` 恒 0、LIST_KEYS 无 topic 行、KEY_DETAIL 明确未命中，
  属如实呈现而非数据缺失。

## 条件（v9）排查速查

- **"await 不醒"的判别三分法**：① **signal 权限被拒**——查
  `openlatch_server_condition_total{status="NOT_HELD"}` 线：signal 调用者当时
  并非持有者（未持有时本地即抛、不产生请求；请求到了线路说明持有已丢的窗口，
  如失锁后仍在 signal），修权限路径；② **换主窗 signal 丢失**——比对 await
  重挂日志与换主时点：等待是承诺、signal 是事件，窗内 signal 不重放不补偿，
  等待者重挂后等下一次 signal 或超时——吻合即属契约面而非故障，应用侧以
  `await(timeout, unit)` 自救并按 WATCHLIST W13 观察丢失率与水位；③ **缺
  guard loop**——虚假唤醒本就允许（队首超时清扫促醒、LEAVE/SIGNAL 竞态收敛），
  裸 await 把唤醒误当谓词成立，外观像"丢了唤醒"：先审查调用方 `while (!谓词)`
  守卫；
- **`NOT_HELD` 的双义判别**：同码两性质——释放/归还路径上是**失锁**（中止提交、
  重新竞争）；`CONDITION_OP` 路径上是 **signal 权限被拒**（映射
  `IllegalMonitorStateException`，修调用点）。唯一判别面是请求类型，对照
  `acquire.total` 与 `condition.total` 两条分线计数即可定位；
- **await 抛 `OVERLOADED`**：条件等待者 + 等待队列合计达 `max-queue-depth-per-key`
  （零新增配置、共用护栏）——与队列"等待满"同口径：减挂起量或上调限额；
  注意该异常收束时调用线程**不持有**锁；
- **持有者进程死亡、等待者仍在睡**：契约即如此——租约到期/死亡 sweep 只唤醒
  等待队列入队者，**不代为 signal 条件等待者**；生产代码用 timed await
  （见 [03 条件变量](03-client-sdk.md)）；
- **`INVALID_REQUEST` / `UnsupportedOperationException` 的条件含义**：条件名空串或
  超 `max-key-length`、读/写形态获取携带 `condition` 字段、非 LOCK 家族 key 上发
  `CONDITION_OP`、形状矩阵违例（`thread_id`/`await_request_id` 与 op 不匹配）、
  低版本会话（v≤8 发条件消息）——异常消息携带状态码名对照；读写锁句柄调
  `newCondition` 为**本地** `UnsupportedOperationException`（零请求）；
- **条件读数在 Follower 上为零**：等待集为 Leader 本地态——Follower 的
  KEY_DETAIL 条件区段如实为零（与等待队列 Leader-only 口径同源），属如实呈现
  而非数据缺失。

## phaser（v10）排查速查

- **"相位不推进"的判别三分法**（`arriveAndAwaitAdvance`/`awaitAdvance` 久候不决）：
  ① **到场不足**——查 `getArrivedParties()`/管理面 phaser 区段比对 `getRegisteredParties()`：
  有注册方未到场（缺席者活着但在别的工作里）→ 等它到场或由其 `arriveAndDeregister`
  离场；缺席者进程已死但注册配额尚未摘除 → 检查其会话是否真已关闭（SESSION_CLOSE
  未达时账簿仍按其在册应到——失联探针窗内属预期，非缺陷）；② **车道窗超时**——变异
  操作（注册/到场/离场）经 Leader 直发道，换主窗可能显式超时（W11 型）：客户端以
  同请求标识重发幂等安全，持续超时看第 3 节；等待侧分片重发自愈、不构成第二暴露面；
  ③ **应用簿记错**——`arriveAndDeregister` 透支被拒（该会话配额已尽）后调用方仍按
  "会合会完成"等待：核对每会话注册/离场计数；
- **`OVERLOADED` 两超限以请求分轨**：REGISTER 线=超 `max-parties-per-phaser`
  （既有配额零扰动，处置为拆键或调上限）；AWAIT_ADVANCE 线=挂起等待达
  `max-queue-depth-per-key` 合并口径（`arriveAndAwaitAdvance` 的挂起半程不受拒）——
  看请求类型即知是哪道闸；
- **"定时标记不生效"的判别三分法**（`await` 久候不响或 `isFired` 恒假）：
  ① **tick 滞后与门控拒绝**——到期唤醒以 `timer-ready-tick-ms`（默认 200ms）为
  精度窗，刚过点在等者未醒属契约内（判读 `timer.total` 线：v≤10 会话的
  `TIMER_OP` 计入 `INVALID_REQUEST` 门控线——确认双端 ≥ v11；`{await,OVERLOADED}`
  高=合并深度护栏在挡新等待）；② **改期换代**——以管理面三元组原始读数核对：
  `timer_generation` 已前进而 `timer_fire_at_ms` 晚于预期 = 有人重装载推后了钟
  （"以最新代为准"契约；"从未装载"的形态是键未命中/代次 0，二者判别不同）；
  `timer_armed=false` = 当代已 `disarm`（新 `await` 即刻 DENIED/异常收束，不存在
  "永睡已撤钟"形态）；③ **换主窗重挂**——唤醒谓词在复制账簿、重挂自愈（对照
  条件 signal 丢失窗：timer 的"不生效"根因在"钟被改了/精度窗"而非"事件丢失"）；
  两台机器醒来先后不一 = 节点钟偏移声明面（判据取管理面原始 `fire_at_ms`，
  非各机 `isFired` 瞬时值；v11）；
- **换代窗口迟到重发按新到场计**：应答丢失很迟（跨两次推进）才重发的到场会计入
  新相位——显式声明竞态（窗口同屏障了结记录口径），重发要快（SDK 自动重发在
  `requestTimeout` 量级内）；应用不应以"到场必然恰好一次"编排正确性——以
  `getArrivedParties()`/`getPhase()` 读数对账；
- **等待"没醒"先排除谓词**：phaser 无 signal 概念，唤醒谓词是账簿相位——若确实
  未推进那是到场问题（上一条①），若已推进而调用方没收到，那是等待总超时收束
  （`awaitAdvance(int)` 有界，30s 后抛异常属契约），循环重入即续等。**与条件
  "换主窗 signal 丢失"的甄别**：phaser 的等待谓词在复制账簿，重挂即刻了结，
  不存在事件丢失窗——若外观像"丢了唤醒"，根因必在①/②/③而非契约面；
- **读数与在途变更交叠**：`getPhase()` 与 `getArrivedParties()` 是各自独立的
  一次往返（无事务性合并读数），先后两读之间相位可能已推进——对账逻辑须容忍
  "读数回退"的观感（以单调相位为准绳，不缓存跨读数做差）。

## 7. FAQ

**Q：能当分布式事务用吗？** 不能。锁是协调原语，不提供隔离级别；关键状态请配 fencing
凭据（如自增版本号经同一锁保护写入）或改走真共识存储。

**Q：key 数量有上限吗？** 无硬上限（单连接在途与单键队列深度有限额，见配置面）。
锁/信号量条目随最后持有释放回收；屏障条目一次性定型后存续至节点重启——废弃屏障靠键
命名约定隔离（见 [03](03-client-sdk.md)）。

**Q：能主动让位吗？** 可以——运行时 API 的 `transferLeadership` 将 Leader 让给日志最新的
健康成员；计划内运维窗口建议先让位再动原 Leader 所在节点。

**Q：观察请求也走 Leader 吗？** 不需要——统计/明细/`CLUSTER_VIEW` 等只读观察由所连节点
本地应答（弱一致，允许短暂陈旧）；改变锁状态的写路径必须经 Leader 复制提交。
