# 附录 · 术语表

| 中文 | 英文 | 释义 |
|---|---|---|
| 租约 | lease | 授予的有效期限，未续持到期即回收；默认 30s，配置区间钳制 |
| 看门狗 | watchdog | 客户端后台续租器，按 `lease/3` 周期为持有的锁续命 |
| 授予 | grant | 一次成功的锁获取结果（含凭据 leaseToken 与到期时刻） |
| 续租 | renew | 持有期内延长租约的请求 |
| 会话 | session | 一条握手成功的连接的服务端簿记身份；断连即清理、其锁释放 |
| 等待者 | waiter | 获取失败后入队的请求登记 |
| 等待-通知-重发 | wait–notify–resend | 排队机制：入队即答 `QUEUED`，轮到推送通知、客户端重发同请求取授予 |
| 队头应答超时 | head-reply timeout | 队头等待者获通知后的回应期限（默认 5s），超时跳过该席位 |
| FIFO 公平 | FIFO fairness | 单键队列严格按到达序授予；Leader 更替时队列重排 |
| 惊群 | thundering herd | 唤醒全部等待者的反模式；OpenLatch 仅通知队头，无惊群 |
| 失锁 | lock lost | 租约到期/会话关闭/failover 回滚致持有权丧失，经 `LockLostListener` 通知 |
| Leader 提示 | leader hint | HELLO/`NOT_LEADER` 应答随附的当值 Leader 地址，驱动客户端改道 |
| 种子发现 | seed discovery | 对配置的种子列表并发 `CLUSTER_VIEW` 探测以定位 Leader 的兜底机制 |
| 转发车道 | forwarding lane | Follower 将 RELEASE/RENEW 等写请求经内部通道转交当值 Leader 执行的路径 |
| 改道 | reroute | 客户端从 Follower 迁移连接到 Leader 的动作 |
| 多数派 | quorum | 集群存活节点数 > N/2；写提交与选主的最低要求 |
| 复制状态机 | replicated state machine | Raft 核心模型：日志复制 + 确定性应用 = 各副本状态一致 |
| 快照 | snapshot | 定期把状态机全量固化，用于压缩日志与新节点追赶 |
| 日志截断 | log truncation | 快照位点之前的 Raft 日志被清理 |
| 摘要 | digest | 状态机跨副本一致性摘要（SHA-256），演练用于验证收敛 |
| 双授 | double-award | 同一时刻同键多个权威持有者——OpenLatch 的核心不变式承诺其永不发生 |
| 复制停摆 | replication stall | Leader 任期内提交冻结的缺陷形态；服务端看门狗检测并自愈（让位→退出升级） |
| 选举风暴 | election storm | 无在任 Leader 的持续缺位形态；看门狗无从让位，需退避重启恢复 |
| 管理令牌 | admin token | `ADMIN_*` 只读观察面的逐消息凭据，与业务令牌相互独立 |
| 业务令牌 | business auth token | 握手承载的连接级认证凭据（`auth_token`），失败即断连 |
| 弱一致读数 | weakly consistent read | 观察面取自单副本本地态，允许短暂滞后于多数派提交 |
| 世代 | generation | 循环屏障的一次会合周期；每 key 单调递增，破障/合拢后回卷开新世代 |
| 离场即破障 | leave-breaks | 任一已到场方超时/中断/死亡/显式 break 即时打破其当前世代的契约（增强于 JDK） |
| 版本戳 | version stamp | 原子变量每次成功写恰 +1 的单调计数；超时复判与 ABA 消除的依据 |
| 去重槽 | dedup slot | 原子条目记录的最近已应用写操作 (会话, 序号, 应答)，保证超时同序号重发不双加 |
| 初值主张 | initial claim | 原子句柄携带的非零初值断言：首建生效、既有条目不符即拒（判例：屏障 total）；引用形态换为 presence 主张（缺省=不主张、零长度=空串主张） |
| 有值引用 | atomic reference | ATOMIC 家族的载荷形态（v6）：一段不透明字节 + 版本戳，get/set/版本 CAS，服务端永不反序列化 |
| 载荷钳制 | payload clamp | 有值引用载荷字节上限（`max-value-bytes`，默认 4KB）：仅在接入层判定，超限命令不入日志、零生效；下调不追溯存量 |
| 截断预览 | truncated preview | 管理观察面对引用载荷的呈现形态：恒定长度（≤64B）转义前缀 + 真实字节数，全量载荷不出现在管理应答中 |
| 定型容量 | declared capacity | 队列首建写入主张并定格的容量上限（v7，受服务端 `max-queue-capacity` 钳制）；后续非零主张不符即拒 |
| 出队谓词 | dequeue predicate | 队首元素可否被消费之判定：QUEUE 形态恒真；DELAY 形态要求队首绝对到期不晚于判定时刻（未到期不得被越过） |
| 到期折算 | expiry folding | 延时元素绝对到期时刻在应用点以条目携带时刻折算（判例租约 `expires_at`）；跨副本回放逐毫秒一致 |
| 双轨等待 | dual-track waiting | 队列挂起的两种身份：等容量（put-waiter）与等元素（take-waiter），按轨独立计位次与唤醒（v7） |
| 应用点回弹 | apply-point bounce | 预检放行提交后应用点不可满足：阻塞请求经改写回 QUEUED 原位续挂，对调用者透明（v7） |
| 队首门 | head gate | 非队首的挂起者重发不得越过在队前辈直接生效（单机条目与 Leader 预检两路同判），保证唤醒授予序=挂起到达序（v7） |
| 广播 term | broadcast term | 一任 Leader 任期的广播域：`topic_seq` 仅在 term 内单调有序，换主后重新起算、跨 term 不可比（v8） |
| topic_seq | topic sequence number | 广播消息在 term 内的受理序号（Leader 内存分配、不入日志）；单订阅视角严格升序，其缺口即订阅者丢弃推断依据（v8） |
| 弱背压 | weak backpressure | 广播的显式背压契约：每订阅两级缓冲（服务端 + SDK 本地）满则 drop-newest——不反压发布者、不断开订阅，丢弃仅计数不通知（v8） |
| drop-newest | drop-newest | 缓冲溢出裁决：丢最新一条并计数，既有缓冲照常交付（区别于 JDK `SubmissionPublisher` 的 overflow-close 判例）（v8） |
| gap 推断 | gap inference | 订阅侧丢弃计数：同 term 内 `topic_seq` 跳号数 + 本地缓冲溢出数，`droppedCount()` 的构成（v8） |
| 死亡即退订 | death-as-unsubscribe | topic 订阅与存活会话绑定：订阅者进程死亡即回收登记、缓冲与去重槽（与队列"死亡不吞元素"刻意相反）（v8） |
| 条件等待集 | condition wait set | 锁 key 之下按条件名分组的到达序等待者登记（v9）：Leader 进程易失、不入日志不入快照，三路回收（LEAVE/会话死亡/换主），与等待队列合并计数受 `max-queue-depth-per-key` 护栏 |
| 命名寻址 | named addressing | 条件身份 = (锁 key, 条件名) 而非句柄身份（v9）：同名句柄进程内重复创建与跨进程创建均绑定同一服务端等待集——JDK 句柄身份的分布式适配，跨进程等价是能力面 |
| 释放折叠 | release folding | await 的"全量释放 + 入等待集登记"两半程以一条既有 ACQUIRE 条目原子承载（v9）：登记先于释放可见，同关键区无丢唤醒窗 |
| 搬运 | carry | SIGNAL/SIGNAL_ALL 将条件等待项按到达序从等待集移入锁等待队列的动作（v9）：搬运后走既有队首授予纪律，不再计入条件等待集读数 |
| signal 事件性 | signal-as-event | signal 家族是事件不是状态（v9）：不入日志、不补偿、不重放——换主窗内发出的 signal 丢失，等待者以 timed await 自救 |
| 返回时持锁 | returns holding the lock | await 无论唤醒、超时还是中断收束，返回（或抛出）前必须重新持有锁的 JDK 保真契约（v9）：重入计数从 1 级起 |
| guard loop 义务 | guard-loop obligation | 虚假唤醒允许且调用方 MUST 以谓词复查循环包裹 await（v9）：唤醒不证明谓词为真，促醒来源清单属契约面 |
| 等待是承诺、signal 是事件 | wait-as-promise, signal-as-event | 条件的换主分层语义（v9）：等待承诺经日志重放与客户端自动重挂幸存；signal 为即时事件，换主窗内丢失即丢失（与 topic"至多一次"同句不同域） |
| 到场相位 | arrival phase | 到场操作发生时相位器账簿所处的相位号（v10）：`arrive` 族的返回值语义，恰触发合拢的回显仍为到场时相位 |
| 应到集合 | expected party set | 当前相位需到场的注册总数（v10）：注册跨相位存续、离场/死亡才缩容；合拢判据"到场数 > 0 且 ≥ 应到数" |
| 注册配额 | registration quota | 各会话在相位器中未离场的注册参与数（v10）：`arriveAndDeregister` 仅可扣本会话、透支拒绝；会话死亡整行隐式摘除——JDK 匿名 party 的显式化收紧 |
| 隐式摘除 | implicit quota removal | 参与者会话死亡时服务端自动减其注册配额、不撤销其已到场事实的死亡语义（v10）：应到集合缩小可当场合拢（不空转），与屏障"死亡即破障"刻意对照（单死者不炸锅） |
| 空转复活 | idle-then-revive | 相位器注册归零的非终止语义（v10）：账簿空转（相位保持、配额 0），后续注册自当前相位恢复运转——与 JDK"归零即终止粘滞"相反 |
| 换代窗口 | previous-generation window | 相位推进时保留的上一周期到场身份集与到场相位（v10）：跨推进的同请求重发据此终态回显、不双计数；再推进即滚出，更早迟到按新到场计（声明竞态，窗口下界同屏障了结记录口径） |
| 装载与清钟 | schedule & clock clear | timer 每次 `schedule` 产生新代次并把上一代的粘滞标记归伪（v11）：round 语义由"粘滞 + 换代清零"承载，改期以最新代为准 |
| 到期派生谓词 | derived expiry predicate | `marked = armed ∧ 判定时刻 ≥ fire_at_ms`——到期不是状态迁移而是账簿与钟的纯函数（v11）：装载/撤销入日志、时钟过点日志恒零新增，与租约"到期是状态迁移故入日志"构成判例族两端 |
| 共见标记 | shared mark | timer 到期后标记对一切到达者可见、不被消耗（v11）：与队列"一元素一消费者"的交接形态相对，广播单次标记语义的本体 |
| 代终结 | generation terminal | `disarm` 使当代永不再可标记直至下一次装载（v11）：在等旁观者以 `DENIED` 终态/异常即时收束（撤销靠事件唤醒、不等到期 tick），新到达者同样即刻 DENIED |
| 唤醒面与到期面之辨 | wake-side vs fire-side | `timer.fired.total` 只在"扫描到点且当批有在等者可唤醒"时计数（v11）：无观察者的静默到期在线路上零足迹——到期发生于谓词，fired 是唤醒面计数，判读勿与到期总数混读 |
| 呈现面时钟无关 | clock-free projection | timer 管理呈现恒呈原始 `{generation, armed, fire_at_ms}` 不折算 marked（v11）：各节点逐字节等；"已否到期"的折算只发生在 Leader 应答线，跨节点偏差 ≤ 时钟偏移为声明契约 |
| 双速呈现 | two-speed projection | phaser 管理观察的 Follower 口径（v10）：账簿三计数与配额为复制态照常可读、挂起等待明细为 Leader 本地态如实零——两区速度不同属如实呈现，MUST NOT 以计数可读误判"无人等待" |
