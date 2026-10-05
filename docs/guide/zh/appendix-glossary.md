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
