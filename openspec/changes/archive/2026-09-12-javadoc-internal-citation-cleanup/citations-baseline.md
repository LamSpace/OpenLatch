# Citations Baseline: javadoc-internal-citation-cleanup

- 首跑时间：2026-09-12（模式集定稿后）
- 扫描：`bash scripts/check-source-citations.sh`，命中 648 处 / 110 文件（含 5 条 pom description）
- 用途：清理台账与进度基线；收口判定 = 脚本退出码 0

```
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreStats.java:20: * 引擎统计观察面的一次性快照（Phase 3 T2，spec"只读统计观察面"）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/session/SessionRegistry.java:23: * 会话登记表：{@code sessionId → 该会话触及的 key 集合}，加速断连清理（设计说明书 §4.7）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/session/SessionRegistry.java:83:     * 已登记会话数（统计观察面，Phase 3 T2；弱一致读数）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/snapshot/CoreStateRestore.java:26: * 快照状态重建的输入值对象（详设 §7.1，S4/design D1）：复制状态全集的
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/snapshot/CoreStateRestore.java:31: * "纯 Java、零外部依赖"的模块隔离（core-lock-engine spec"无网络与零依赖"）；
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/snapshot/CoreStateRestore.java:90:         * 锁家族便捷构造（Phase 1/2 既有形态）：许可与屏障字段取缺省 0。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/snapshot/CoreStateRestore.java:105:         * 构造并校验条目形态自洽性（按家族分支）：锁条目沿用 Phase 1 规则
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:80:    /** key → 状态条目映射与条目生命周期（Phase 3 T1 后按家族承载锁/Semaphore/Latch 条目）。 */
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:105:     * 快照状态重建（详设 §7.1，S4/design D1）：以 {@link CoreStateRestore}
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:127:     *       使 {@link #expireDue} 能回收快照继承的租约（spec"到期扫描覆盖
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:131:     *       （spec"发号不复用继承凭证"），也与未截断副本对同一尾部日志
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:203:     * 本读数的凭证。供快照生成侧读取发号水位（详设 §7.1 / s4 design D10），
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:303:        // LATCH 不经获取通道（详设 §2.1"走独立通道"）：ACQUIRE 携带 LATCH
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:309:        // Semaphore 建条目预检：条目不存在时总量主张必须 > 0（design D1）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:321:                    continue; // 条目在等待期间被移除，重试（design.md D4）
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:334:                // 条目内规则按实现类分派（锁规则集 / Semaphore 规则集，详设 §2.3）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:356:     * 及 Phase 3 起的 FAIR 别名）落 {@link KeyFamily#LOCK}；SEMAPHORE/LATCH
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:373:     * （建条目预检已保证 &gt; 0）；Latch 家族 P3-05 接入前不可达。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:483:     * 等待屏障（Phase 3 详设 §2.4 / P3-05）：校验会话与 key 后，
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:516:                    continue; // 条目竞态移除，重试（design.md D4 同机制）
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:537:     * 倒计数（Phase 3 详设 §2.4 / P3-05）：条目定位与会话校验同
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:647:     * 只读统计观察面（Phase 3 T2，spec"只读统计观察面"）：弱一致遍历
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:692:     * 明细只读观察面（Phase 3 T3，spec"明细只读观察面"）：弱一致遍历
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEngine.java:745:            // 与命令路径同一 D4 纪律——非当前值即视作不存在。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lease/LeaseManager.java:24: * 到期最小堆。只入不删：释放/续租不删堆记录，靠到期时的陈旧校验跳过（设计说明书 §4.6）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lease/LeaseManager.java:29: * 两向不成环（设计说明书 §4.9.3）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockTable.java:26: * 避免移除/创建竞态（设计说明书 §4.9.1）。值为 {@link KeyEntry} 抽象——
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockTable.java:27:Phase 3 T1 后同一张表按家族承载锁/Semaphore/Latch 条目，创建侧的
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/KeyEntry.java:31: * {@link #family()} 完成（Phase 3 T1 design D2）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/KeyEntry.java:120:     * 等待队列条目读数（统计观察面，Phase 3 T2）：锁/Semaphore 为等待队列
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockEntry.java:53: * <p><b>并发模型</b>（设计说明书 §4.9）：条目内所有状态迁移都在
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockEntry.java:106:     * 快照重建工厂（详设 §7.1，仅供 {@code CoreEngine.restoreFrom} 在加载快照时
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockEntry.java:139:     * 状态迁移：按下列规则顺序授予、排队或拒绝（设计说明书 §4.3 规则集，
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockEntry.java:164:     * {@code effectiveLeaseMs} 整段刷新，口径统一（design D2：请求值 0 取
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockEntry.java:182:        // 请求值钳制（0 取默认），重入者由此获得延长/缩短租约的通道（design D2）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockEntry.java:235:        // 幂等去重：同 (sessionId, requestId) 已在队 → 返回当前位次，不二次入队（§4.8）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockEntry.java:539:     * 等待队列长度读数（统计观察面，Phase 3 T2）。须在持有条目锁时调用。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LockEntry.java:549:     * 明细只读快照（Phase 3 T3，spec"明细只读观察面"）：条目锁内拷贝
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/SemaphoreEntry.java:40: * 单 key 的许可门闸状态机（Phase 3 详设 §2.3 / P3-03）：N 个许可的共享
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/SemaphoreEntry.java:60: * 杜绝大请求饥饿（详设 §2.3）。同归属重入是唯一例外：它不改变队列
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/SemaphoreEntry.java:96:     * 快照重建工厂（详设 §7.1 推广，供 {@code CoreEngine.restoreFrom} 加载
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/SemaphoreEntry.java:98:     * 规则；等待队列恒空（集群等待队列不进复制状态，design D9）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/SemaphoreEntry.java:140:     * 路径的租约以 {@code effectiveLeaseMs} 整段刷新（design D2 口径）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/SemaphoreEntry.java:242:     * 该归属持有数 → {@code OVER_RELEASE}（池零扰动，详设 §2.3 对称扣减
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/SemaphoreEntry.java:425:     * 等待队列长度读数（统计观察面，Phase 3 T2）。须在持有条目锁时调用。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/SemaphoreEntry.java:435:     * 明细只读快照（Phase 3 T3，spec"明细只读观察面"）：条目锁内拷贝
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LatchEntry.java:35: * 单 key 的一次性倒计数屏障状态机（Phase 3 详设 §2.4 / P3-05）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LatchEntry.java:59: * 带值请求新建屏障、纯加入被拒，见 design D5）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LatchEntry.java:87:     * 快照重建工厂（详设 §7.1 推广）：以计数快照直接装配，不经迁移规则；
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LatchEntry.java:278:     * 扣减，已归零屏障以条目存续承载一次性护栏（见类注释与 design D5 修订）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LatchEntry.java:306:     * awaiter 队列长度读数（统计观察面，Phase 3 T2；与锁家族统一为
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/LatchEntry.java:317:     * 明细只读快照（Phase 3 T3，spec"明细只读观察面"）：条目锁内拷贝
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/Waiter.java:22: * 等待者。{@code notifyDeadlineMs > 0} 表示"已通知、待重发"状态（设计说明书 §4.5）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/Waiter.java:43:     * 锁等待者便捷构造（Phase 1/2 既有形态）：许可数取缺省 1。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/lock/Owner.java:20: * 锁归属，由 {@code (sessionId, threadId)} 唯一确定（概要设计 §6.4）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/LockType.java:46:     * 显式公平承诺互斥（Phase 3 T1）：语义与 {@link #REENTRANT} 逐项等价
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/LockType.java:48:     * 顺序的保证由 FIFO 队列本体承载（详设 §2.2）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/LockType.java:52:     * 许可门闸（Phase 3 T1）：非锁家族类型，仅作请求定型判别——携带此
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/LockType.java:53:     * 类型的命令由门面分派至 {@code SemaphoreEntry}（详设 §2.3），不参与
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/LockType.java:58:     * 倒计数屏障（Phase 3 T1）：非锁家族类型，仅作 key 定型判别——屏障
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/LockType.java:60:     * {@code LatchEntry}（详设 §2.4），不经获取/释放/续租命令通道。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/result/ReleaseStatus.java:41:     * P3-03 起生效；锁与 Latch 的释放路径永不返回此值），归属持有与
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/result/Outcome.java:22: * （设计说明书 v1.2 §4.2 已同步为两值），使 server 层无需重校验即可映射到
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/result/Outcome.java:63:     * （Phase 3 T1 详设 §2.3 / design D1）；条目状态零扰动，server 层
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/result/Outcome.java:70:     * 扣减/挂起），或既有屏障上非零主张与定型值不符（Phase 3 详设 §2.4 /
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/result/Outcome.java:71:     * design D1）；条目状态零扰动，server 层映射协议 {@code INVALID_REQUEST}。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreEventListener.java:22: * 回调在条目锁之外触发（见设计说明书 §4.9.5）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreInspection.java:22: * 引擎明细只读观察面的一次性快照（Phase 3 T3，spec"明细只读观察面"）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/CoreConfig.java:20: * 核心引擎限额与租约配置。各默认值与设计说明书 §5.7 对齐。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/KeyFamily.java:29: *   <li>{@link #SEMAPHORE}——许可门闸家族（{@code SemaphoreEntry} 承载，Phase 3 T1 引入）；</li>
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/KeyFamily.java:30: *   <li>{@link #LATCH}——倒计数屏障家族（{@code LatchEntry} 承载，Phase 3 T1 引入）。</li>
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/KeyFamily.java:42:    /** 许可门闸家族（{@code SemaphoreEntry}，Phase 3 T1）。 */
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/KeyFamily.java:45:    /** 倒计数屏障家族（{@code LatchEntry}，Phase 3 T1）。 */
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/command/LatchCountDownCommand.java:20: * 屏障倒计数命令（Phase 3 详设 §2.4 / design D1）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/command/LatchAwaitCommand.java:20: * 屏障等待命令（Phase 3 详设 §2.4 / design D1）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/command/ReleaseCommand.java:37:     * 锁家族便捷构造（Phase 1/2 既有调用形态）：归还数取缺省 1。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/command/AcquireCommand.java:34: *                         （等待模式折算见详设 §3.2.2）。
openlatch-core/src/main/java/io/github/lamspace/openlatch/core/command/AcquireCommand.java:54:     * 锁家族便捷构造（Phase 1/2 既有调用形态）：许可参数取缺省
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:30: * 业务令牌认证配置（Phase 3 详设 §5.2/§10.4 P3-16，spec"业务令牌认证与默认
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:39: * 携带（Phase 1 预留字段，P3-16 起启用），一次完成于握手；会话生命周期内不做
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:40: * 逐请求鉴权（连接即身份，详设 §5.3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:42: * <p><b>默认值口径</b>：{@code enabled=false}（默认）= Phase 1 兼容守卫（HELLO
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:45: * 即放行——多令牌支持轮换期双活（spec"轮换期多令牌双活"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:53:    /** 配置键前缀（详设 §5.2）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:56:    /** 默认认证开关（关闭——Phase 1 兼容守卫）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AuthConfig.java:81:     * 关闭形态（Phase 1 兼容守卫）——兼容构造重载与库内嵌缺省。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AdminConfig.java:26: * 管理观察配置（Phase 3 详设 §4.2 T3，spec"管理令牌认证"；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AdminConfig.java:33: * <p><b>令牌承载通道（change design D1）</b>：管理令牌经每条 ADMIN 请求
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AdminConfig.java:35: * {@code auth_token}（Phase 1"非空即断连"规则保持，T4 业务令牌通道
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/AdminConfig.java:46:    /** 配置键前缀（详设 §4.2）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/session/ServerSession.java:25: * Channel 绑定的会话簿记：握手状态、sessionId、inflight 计数（设计说明书 §5.1）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/session/ServerSession.java:47:     * {@code ADMIN_LIST_SESSIONS} 的"建连时间"来源（Phase 3 T3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/session/ServerSession.java:57:     * 出站）。未握手连接的兜底值为 1（Phase 1 默认；推送仅可能发生在握手后，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/session/ServerSession.java:138:     * CAS 实现（代码侧待办，登记于变更 design D5）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/session/ServerSessionRegistry.java:24: * sessionId → 会话的反向索引（design.md D2）：core 的队首通知事件只携带
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/session/ServerSessionRegistry.java:27: * （Phase 3 T3，经 {@link #snapshot()} 弱一致取用）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/session/ServerSessionRegistry.java:84:     * 当前登记会话的不可变快照（Phase 3 T3，ADMIN_LIST_SESSIONS 数据源；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/TlsConfig.java:26: * 传输层 TLS 配置（Phase 3 详设 §5.1/§10.4 T4，spec"服务端 TLS 传输层"；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/TlsConfig.java:40: * 对齐"端口占用启动失败"语义，design D1）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/TlsConfig.java:43: * {@link #disabled()}——明文协议栈，服务端行为与现状逐字节一致（spec"默认
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/TlsConfig.java:55:    /** 配置键前缀（详设 §5.1）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/TlsConfig.java:58:    /** 默认 TLS 开关（关闭——明文栈，§6 兼容性策略"默认关闭"）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/TlsConfig.java:91:     * 一致（spec"默认关闭明文照常"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:30: * 集群配置（详设 §9，spec"集群配置体系"；{@code openlatch.cluster.*} 键族）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:34: * （Phase 1 既有装配与测试不受扰动）。默认实例 {@link #disabled()} 即
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:35: * "关闭集群 = Phase 1 单机行为"（同一二进制，§9）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:42: * 客户端以种子发现兜底，见 s3 design D4）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:44: * @param enabled           是否启用集群（{@code false} 即 Phase 1 单机）
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:51: * @param snapshotThreshold 快照触发条目数（S4 起接入 Ratis 自动触发阈值）
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:54: *                          {@code 0} 取库默认；S4 追赶用例用小值驱动截断与
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:105:    /** 配置键前缀（§9）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:178:     * 全量校验（spec"缺必填项启动失败"）：{@code enabled=true} 时
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ClusterConfig.java:367:     *         客户端以种子发现兜底，design D4）
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/MetricsConfig.java:26: * 监控指标配置（Phase 3 详设 §3.1/§10.2 T2，spec"指标配置与管理端点生命周期"；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/MetricsConfig.java:48:    /** 配置键前缀（详设 §3.1）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/MetricsHttpServer.java:48: * 管理端口 HTTP 服务（Phase 3 详设 §3.1，spec"管理端点仅两路径"）：在独立
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:37: * 服务端指标词表与埋点门面（Phase 3 详设 §3.2，spec"服务端指标清单与线路命名"/
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:40: * <p><b>单一命名点</b>：§3.2 全部指标的逻辑名/标签常量收编于本类（详设 §3.4
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:41: * 勘误后的双路径共用组件，design D1）——{@code RequestDispatcher}（单机）与
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:46: * {@code ServerMetricsVocabularyTest} 为唯一权威断言点（design D2）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:48: * <p><b>消息 → 指标映射口径</b>（spec"耗时与到期计数口径"）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:57: * 对外服务，埋点照常累积进内存注册表（design D1"关闭即不抓取"而非
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:175:     *                     受理至写回，含 Raft 提交等待，spec"耗时与到期计数口径"）
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:184:     * 线路形态钉为 {@code _seconds_bucket/_count/_sum}（design D2 词表口径；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:185:     * 桶位取 Micrometer 默认集，非配置项——详设 §9 遗留"桶位可配置"不在 T2）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:246:     * 绑定集群形态 gauge（spec"Gauge 取值语义与采样安全"）：held 读影子表
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/metrics/ServerMetrics.java:281:     * 注册集群角色 gauge（spec"Gauge 取值语义与采样安全"：仅集群启用时注册，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:61: * 管理观察请求处理器（Phase 3 详设 §4.2 T3 / P3-11，spec
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:63: * 日志条目——与 T2 {@code ServerMetrics} 同构的"单组件双数据源"形态，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:75: *       （不泄露原因，spec"管理令牌认证"）。</li>
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:89: * 污染（spec"管理流量与业务面隔离"）；仍处单连接在途限额记账内。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:101:    /** 分页页大小上限（防观察面自身成为放大攻击源，spec"分页、过滤与排序语义"）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:179:            // 分发兜底与业务路径同纪律：绝不静默悬挂（详设 §4.2 只读观察面）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:195:     * {@link ConstantTime}（等长补齐常量时间，防时序/长度侧信道，§5.2
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:216:        // 常量时间原语（Phase 3 T4，spec"常量时间比较"）：与业务令牌共用单一
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:266:     * 选举空窗如实 {@code UNKNOWN}，MUST NOT 虚报（spec"双形态数据源"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:283:     * 非 Leader 一律不呈现等待数据（spec"等待队列仅 Leader 可见"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/admin/AdminRequestHandler.java:478:            // 接入节点：逻辑会话 id 高位（nodeId<<32|localSeq，Phase 2 会话
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:55: * 请求分发（设计说明书 §5.4）：{@code Envelope} → core 命令，core 结果 → {@code Envelope}。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:56: * 映射为纯函数（design.md D5），可脱离 Netty 单测；{@link #dispatch} 为入口。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:64: * <p><b>指标埋点</b>（Phase 3 T2，详设 §3.4 勘误后口径）：每条产出应答的
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:133:     * 分发屏障倒计数（Phase 3 P3-05，详设 §2.4）：负参数在协议层拒绝；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:157:     * 分发屏障等待（Phase 3 P3-05）：判定与门控同上；QUEUED 携带位次，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:182:     * GRANTED=OK、拒绝细分同码，design D3 协议面不新增状态码）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:205:     * {@code -1} 与正数均映射为可排队（设计说明书 §3.2.2）；租约到期时刻
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:216:        // v3 门控（详设 §6）：新锁类型仅对握手中声明 v3 的会话开放，v1/v2
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:236:                req.getWaitMs() != 0,   // wait_ms == 0 立即式；-1 与 >0 均可排队（设计说明书 §3.2.2）
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:244:     * 获取请求的许可参数合法性（Phase 3 详设 §2.1 / P3-03）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:330:     * 拒绝（详设 §6 兼容性策略）。已接入：{@code FAIR}（P3-02）、
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:331:     * {@code SEMAPHORE}（P3-03）；{@code LATCH} 随 P3-05 增列。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:343:     * core 授予结果 → 协议响应（design.md D5 全表映射）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:416:            // 超额归还是请求参数与持有不符，非租约问题（design D3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/dispatch/RequestDispatcher.java:472:            // v3-T3：ADMIN 消息同规则——认证/门控/限额拒绝的状态码在线路可见
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/ServerConfig.java:28: * 服务器配置。配置键与默认值对齐设计说明书 §5.7。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:51: * OpenLatch 服务器入口（详设 §3.1：每个节点 = Raft 副本 + 完整接入层）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:53: * 经 {@link ClusterRuntime}）→ 启动租约扫描调度与 Netty 监听（设计说明书
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:54: * §5.2）。{@code cluster.enabled=false} 时行为与 Phase 1 逐用例一致
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:55: * （spec"单机模式回退保证"，同一二进制）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:63: *       EventLoop 写回（design D4），见 {@code ReplicationGateway} 类注释；</li>
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:67: *       {@code LeaseExpiryDriver} 驱动到期条目（不直驱引擎，design D12）；</li>
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:77: * Phase 3 T2）、按 {@link AdminConfig} 装配 {@code ADMIN_*} 管理观察处理器
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:78: * （Phase 3 T3，业务端口上的只读观察通道，未配置令牌即一律拒绝）；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:85:     * v3（Phase 3 T1）起握手接受 {@value #MIN_CLIENT_PROTOCOL_VERSION}–
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:107:     * 服务端版本字符串（ADMIN_SUMMARY 的 {@code version} 来源，Phase 3 T3
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:108:     * design D8）：优先取 jar manifest 的 {@code Implementation-Version}；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:137:    /** 集群配置（不可变；{@code enabled=false} 即 Phase 1 单机）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:154:     * design D12——避免双引擎持有者视图）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:167:     * pom 项目版本同步维护（Phase 3 T3 design D8）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:175:    /** 启动成功时刻（epoch 毫秒，0=未启动）——ADMIN_SUMMARY 运行时长基准（Phase 3 T3）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:197:     * （同一二进制回退保证，spec"单机模式回退保证"）；{@code true} 时不组装
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:224:     * 管理观察通道由 {@code adminConfig} 决定（Phase 3 T3：未配置令牌即
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:239:     * （Phase 1 兼容守卫）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:255:     * 构造服务器（全配置形态，Phase 3 T4）：业务认证由 {@code authConfig} 决定
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:257:     * 维持 Phase 1"非空即拒"兼容守卫）；TLS 由 {@code tlsConfig} 决定——
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:260:     * 装配 {@code SslHandler}（明文连接拒绝、握手超时 5s 断开，spec"服务端
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:284:            // 单机 gauge 数据源：统计观察面与会话注册表的弱一致回调读数（T2）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:296:            // spec"先完成 Raft 组网与状态机初始化，后开放客户端接入端口"。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:307:        // 传输层 TLS（Phase 3 T4）：enabled 时构造 SslContext——PEM 文件不可读
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:309:        // 不进入半启动，对齐"端口占用启动失败"语义，design D1）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:319:        // 管理观察处理器（Phase 3 T3）：数据源按装配形态二选一，未配置
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:343:            // spec"指标配置与管理端点生命周期"：管理端口冲突与锁端口同策略——
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:366:     * 由 TLS 配置构造服务端 {@link SslContext}（Phase 3 T4，design D1）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:387:            // 包装为启动失败的 IllegalStateException（design D1 快速失败）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:395:     * 管理端口实际监听端口（Phase 3 T2）。未启用或尚未 {@link #start()}
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:438:     * 经复制路径迁移，design D12），此时返回 {@code null}，测试请改用
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:466:     * 关停序列（设计说明书 §5.6）：先解除管理端口监听（Phase 3 T2，spec
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:469:     * 探针/扫描线程停止、Raft 服务关闭，spec"关停无悬挂请求"）。幂等，可重复调用。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/OpenLatchServer.java:524:                // 到期计数走扫描返回值（expireDue 即"本轮实际释放数"，零 core 侵入，T2 design D3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/security/ConstantTime.java:23: * 令牌常量时间比较原语（Phase 3 详设 §5.2/§10.4 P3-16，spec"常量时间比较"）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/NotifyEventBridge.java:29: * core 事件出口 → 协议推送（设计说明书 §5.1）：按 {@code sessionId} 反查连接
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/NotifyEventBridge.java:30: * （design.md D2），写回 {@code AWAIT_NOTIFY}（{@code request_id = 0}，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:55: * {@link ShadowTable} 双写核算的唯一入口（详设 §4.2/§4.3；S1 PoC 内核转正，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:56: * design D1/D2/D9/D12）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:63: * {@link #apply} 内（design D12 的"引擎状态变更唯一漏斗"不变式）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:65: * <p><b>时间语义（§4.3）</b>：应用期间经 {@link EntryClock} 注入条目携带时刻，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:69: * <p><b>会话映射</b>：逻辑会话 id（{@code (nodeId<<32)|localSeq}，§5.2）在
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:77: * <p><b>快照通道（S4，design D1/D2/D10）</b>：{@link #snapshotState()} 在
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:87: * Ratis {@code StateMachineUpdater}，design D10）调用；{@code applyLock} 兜底
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:109:     * 二者之外不得变更引擎状态（design D12 不变式的 S4 扩展）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:127:     * 装配一个零状态引擎：集群引擎恒不登记等待项（design D9），
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:236:     * 进引擎，design D9），授予时镜像影子表；需排队时回
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:237:     * {@link ApplyStatus#DENIED}（排队裁决由 Leader 侧在应用回调中完成，§4.5/D3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:261:                // Semaphore 授予按许可数镜像持有增量；锁家族恒 1（P3-03/P3-07）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:276:            // v3 形状拒绝（家族误用/总量断言）：参数非法回执，P3-04 码形接正。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:312:            // 超额归还：参数与持有不符，回执 INVALID_REQUEST（P3-07 码形接正）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:361:     * §4.3/P2-09）。守卫匹配时 {@code expireDue()} 顺带收敛同一时刻到期的
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:372:        // 回放守卫（spec"过期条目不误杀新持有者"）：条目 token 与当前持有
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:422:     * 供 Leader 侧归零广播判定（design D5/D6）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:513:     * 产出当前复制状态的一致性快照形态（详设 §7.1/§7.2，S4/P2-15）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:515:     * {@code next_lease_token}，design D10——缺它则重建副本对同一尾部日志
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachineCore.java:533:     * 安装一份快照并整体替换状态（详设 §7.3，S4/P2-16）：启动加载（本地
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationStallWatchdog.java:32: * 复制停摆自愈看门狗（phase2-leader-stall-followup T2/2B.1，design D2/D3/D6）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationStallWatchdog.java:36: * {@code openspec/changes/phase2-leader-stall-followup/observations-leader-stall-rootcause.md}），
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationStallWatchdog.java:41: * <p><b>检测信号（D2 双条件，防误伤；窗口键＝连续在任段）</b>：仅当「本节点
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationStallWatchdog.java:55: * <p><b>可配性（D6）</b>：全部阈值为装配层钉死常量（由 election-timeout 折算），
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationStallWatchdog.java:56: * 不引入运维配置键（详设 §9 运维配置最小面口径）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationStallWatchdog.java:75:    /** {@code T_stall} 下限（毫秒）：max(10s, 5×election-timeout) 的钉死底（D2）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationStallWatchdog.java:77:    /** 零推进连续样本数阈值 M：与失联判定容忍数同族（D2，3 个采样周期）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationStallWatchdog.java:232:     * 生产装配：以子系统实况构建探针与动作组，阈值按 D2/D6 由 election-timeout
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaderTracker.java:25: * Leader 提示的单源视图（详设 §3.2 {@code LeaderTracker}，s3 design D3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaderTracker.java:30: * 身份恒一致（spec"单一数据源一致性"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaderTracker.java:35: * 发现），MUST NOT 使非 Leader 节点误受理其不该受理的写（spec"降级不误受理"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaderTracker.java:90:     * 依本地视图构造 {@code CLUSTER_VIEW} 载荷（详设 §6.2，任意节点可答，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaderTracker.java:93:     * （缺项空串——客户端以各节点自报兜底，design D4），{@code is_leader}
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/EntryClock.java:22: * 条目时刻时间源（详设 §4.3.4，design D2）：在状态机应用线程内返回
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/EntryClock.java:30: *       见 design D10——仅应用已提交条目且逐条串行）；</li>
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:36: * 影子状态表（详设 §4.1 复制边界的逻辑镜像）：状态机应用路径同步维护的
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:39: * <p><b>职责</b>：①跨副本一致性摘要的载体（{@link #digest()}，P2-10 退出门与
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:40: * S4 快照比对共用）；②快照序列化结构（{@link #toProto()}/{@link #load}，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:41: * §7.1 内容 = 锁条目 + 会话注册表，<b>不含</b>等待队列与本地配置，design D9）；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:44: * （{@link #adminEntry}/{@link #adminEntries()}，Phase 3 T3——逐应用点整体
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:57: * 扫描周期的延后，正确性裁决恒在应用路径（§4.5"以应用结果为准"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:77:     * 随条目定型不变，T2 gauge 按家族聚合的读数来源）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:87:     * 管理观察的条目全量投影（Phase 3 T3，spec"双形态数据源与集群视角口径"）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:163:     * 管理观察明细投影（Phase 3 T3）：key → 不可变全字段视图，应用线程在
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:220:     * 授予登记（计数增量形态，Phase 3 T1）：Semaphore 一次授予归还多许可，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:302:     * 释放登记（计数增量形态，Phase 3 T1）：归还 {@code releaseDelta} 个
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:394:                // 屏障条目存续不随参与者散尽而回收（一次性护栏，design D5
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:454:     * 指定归属当前是否持有该 key（Phase 3 T1 重入预检通道）：读取
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:506:     * 从 proto 全量恢复（替换当前内容；S4 快照加载使用）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:552:     * 全量摘要（SHA-256 hex）：跨副本一致性比对基准（P2-10 退出门、
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:553:     * 故障演练"锁不丢"断言与 S4 快照比对的公共判据）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:567:     * 屏障倒计数应用点镜像（Phase 3 T1）：条目不存在则按 {@code total}
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:666:     * 指定 key 的管理观察视图（Phase 3 T3，ADMIN_KEY_DETAIL 集群数据源）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:678:     * 全部条目的管理观察视图（Phase 3 T3，ADMIN_LIST_KEYS 集群数据源）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:687:     * 按家族聚合的持有中条目数（Phase 3 T2 gauge 读数）：弱一致遍历无锁
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ShadowTable.java:718:     * 摘要输入的字节长度（digest 前序列化开销的观测口，P2-10 记录用）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:44: * 集群模式写请求处理器（详设 §4.5，design D3/D9）：Phase 1 的"同步函数
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:52: *       均不写日志（§4.5"排队不是复制状态"）；</li>
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:55: *       EventLoop（design D4）；</li>
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:60: * <p><b>Follower 分车道（S3/P2-12，spec"Follower 写请求分车道"）</b>：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:81: * <p><b>指标埋点</b>（Phase 3 T2，详设 §3.4 勘误后与单机路径共用
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:84: * 受理至应答生成（spec"耗时与到期计数口径"：集群档含 Raft 提交等待）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:100:    /** Leader 提示单源（NOT_LEADER 应答随附提示，s3 design D3）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:145:     * 角色（Follower 即 {@code NOT_LEADER}+提示改连，S3 分车道）→
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:146:     * 载荷与键合法性 → 会话登记 → 排队裁决（§4.5 预演）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:164:        // v3 门控（详设 §6）：与单机分发器同规则——v3 专属类型对低版本会话
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:170:        // 许可参数合法性（与单机分发器共用判定，P3-03）：非法不入日志。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:179:        // 重入豁免（Phase 3 T1）：请求归属已在持有集内时不得被 busy 拦截——
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:184:        // 许可感知 busy（design D4）：Semaphore 的"占用"是池不足而非有人持有
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:190:        // 走复制授予路径（AWAIT_NOTIFY 后重发的 Phase 1 语义等价形态；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:218:        // 补登记并改写回执（gateway.leaderSideEffects，design D3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:244:        // 归还数为负属参数非法（与单机分发器同规则，P3-03），不入日志。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:297:     * LATCH_COUNT_DOWN 集群路径（Phase 3 T1，转发车道）：计数变更属复制状态，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:299:     * 复制执行；等待队列不在日志内（design D9 同构）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:331:     * LATCH_AWAIT 集群路径（Phase 3 T1，ACQUIRE 车道 + Leader 本地裁决）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:333:     * 先经 {@code LATCH_COUNT_DOWN_ENTRY(count=0)} 复制定型（design D1/D6
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:530:     * 写回前经指标出口记录一次（T2）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:544:     * 异步应答弹回连接 EventLoop 写回（design D4；断连后 writeAndFlush 自动丢弃，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:546:     * 起算、含 Raft 提交等待（spec"耗时与到期计数口径"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:561:     * 指标出口（T2）：按应答信封家族计数并记耗时样本；测试夹具装配
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:574:     * 提交失败的应答拆分（S3/P2-12，不再共享混叠码）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRequestHandler.java:597:     * 未配置地址映射时为空串（客户端种子发现兜底，design D4）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:29: * 集群运行时装配（design D6：{@code RaftSubsystem} 与 §9 配置并入 gateway
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:34: * 调用方 MUST 在开放客户端接入端口之前完成（spec"先组网后开端口"）；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:58:    /** Leader 提示单源视图（s3 design D3：HELLO/NOT_LEADER/CLUSTER_VIEW 共用）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:60:    /** 复制停摆自愈看门狗（T2/2B.1，design D2/D3/D6：让位一次→超时升级重启）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:105:     * 全部挂接调用方共用的 {@link ServerMetrics}，T2/详设 §3.4 勘误口径）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:118:        // Leader 提示单源（s3 design D3）：监听器须在 RaftServer 启动前挂上，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:135:            // 复制态 gauge 与角色指标绑定（T2）：抓取线程弱一致读，不触碰应用锁。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:139:        // 复制停摆自愈看门狗（T2）：装配末位——全部判据通道（division/gateway/
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:140:        // client 池）此时均已就绪；阈值由 election-timeout 折算钉死（D6）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:148:     * Leader 提示视图（HELLO/NOT_LEADER/CLUSTER_VIEW 消费，s3 design D3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:215:     * 成员变更运维入口：移除一个投票者并清理其会话（详设 §7.4，S4/P2-17，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:216:     * design D6）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:220:     * 单步变更语义）；随后立即触发被移除节点会话的批量清理（§5.2 规则 4
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ClusterRuntime.java:244:     * 复制状态摘要（退出门与演练断言入口）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ApplyObserver.java:26: * （详设 §4.5 应用结果收口，design D3/D9）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ApplyObserver.java:28: * <p><b>回调线程</b>：状态机应用线程（单线程、条目间无并发，design D10）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ApplyObserver.java:50:     * <p>失去 Leadership 时实现须让全部在途回执以可重试错误完成（§8 切换窗口
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ApplyObserver.java:52:     * §4.4/design D9）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:26: * Leader 侧等待队列（详设 §4.4/§4.5 的集群承载结构，design D9）：FIFO 排队
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:36: *       队首唤醒须"可用许可 ≥ 队首请求"方可通知（防大请求饥饿，Phase 3 T1
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:37: *       design D4——与单机 {@code SemaphoreEntry} 判定语义等价）；屏障归零
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:42: *       同 Phase 1 规则）；</li>
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:48: * {@link #clear()}——"单个 Leader 任期内的严格 FIFO"（§4.4 公平性表述）由
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:85:        /** 入队时刻（epoch 毫秒，管理观察 waited_ms 折算基准，Phase 3 T3）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:101:     * 管理观察的等待项视图（Phase 3 T3）：位次、归属与已等待时长的不可变
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:189:     * 许可感知的队首推进（Phase 3 T1 design D4）：仅当可用许可满足队首请求
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:215:     * 屏障归零全体广播（Phase 3 T1）：标记全部未通知等待项为已通知并返回
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:271:     * 返回新队首供通知（同 Phase 1 的摘除级联语义）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:339:     * 全部队列条目总数（Phase 3 T2 gauge 读数，含锁/Semaphore 等待与
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:354:     * 单 key 队列深度的当时最大值（Phase 3 T2 gauge 采样读数，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:370:     * 指定 key 的等待队列明细快照（Phase 3 T3，ADMIN_KEY_DETAIL Leader 侧
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:395:     * 按逻辑会话聚合的在队等待数（Phase 3 T3，ADMIN_LIST_SESSIONS 的
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/WaitQueue.java:440:     * 清空全部队列（WinLeadership 任期边界调用，design D9）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:45: * 会话集群协调器（详设 §5.2，P2-08）：会话的集群登记、断连传播与接入
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:48: * <p><b>会话 id 组合（§5.2 规则 1）</b>：接入节点 HELLO 时分配
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:52: * <p><b>HELLO 语义（design D12）</b>：先提交 {@code SESSION_OPEN} 并等待
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:57: * <p><b>断连传播（§5.2 规则 3）</b>：接入节点检测到断开提交
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:61: * <p><b>失联批量清理（§5.2 规则 4，design D5 + S4 加固）</b>：Leader 以
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:66: * {@code SESSION_CLOSE}。S4 加固的判据说明：仅"未越过 Leader 位点"不够——
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:69: * （违 §11-2"存活会话锁不丢"）；真失联节点的定义性特征正是零推进。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:82:    /** 失联判定的连续停滞周期容忍数（design D5 防抖，M=3）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:104:    /** Leader 提示单源（HELLO 应答的 leader_hint/leader_address 来源，design D3）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:113:    /** peer 上次观测 commitIndex（零推进判据的记忆位，S4 加固）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:342:            // 零推进判据（S4 加固）：滞后但在推进（选举/回放修复中）的存活
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:356:     * 成员移除的显式失联清理（详设 §7.4，S4/P2-17）：被移除节点即刻从
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:358:     * 显式走同一批量清理车道（§5.2 规则 4 语义：每会话一条 SESSION_CLOSE
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/SessionCoordinator.java:376:     * （每会话一条条目，§5.2 规则 4）。同一节点只触发一轮（cleanedNodes
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaseExpiryDriver.java:37: * 租约到期驱动（详设 §4.3.1/P2-09）：到期判断只发生在 Leader——周期扫描
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaseExpiryDriver.java:43: * 只允许在应用线程、条目时刻下被调用（design D12"引擎唯一漏斗"）——
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaseExpiryDriver.java:49: * 自然重试。切换 Leader 时抑制集整体作废（design D5 的 NOOP 探针同此
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaseExpiryDriver.java:58: * （详设 §12 风险 2 的可接受声明）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaseExpiryDriver.java:73:    /** 指标门面（到期计数出口，T2）；{@code null} 表示不埋点。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaseExpiryDriver.java:135:            // Leader 侧等待队列的"已通知队首超时清扫"（Phase 3 T1 接上生产
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaseExpiryDriver.java:191:     * 并按回执实际释放量为 {@code lease.expired.total} 计数（T2，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LeaseExpiryDriver.java:192:     * spec"耗时与到期计数口径"——本回调每副本各执行一次，计数为本地观察值；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:45: * Raft 子系统装配（详设 §3.2 {@code RaftSubsystem}，design D6/D7/D11）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:55: * 调用——每分一次组，S2 单组、实例幂等复用），装配方经 {@link #core()} /
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:58: * <p><b>快照装配（S4，§7/P2-15）</b>：Ratis 自动触发按
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:60: * 即在应用线程产出快照）；快照文件保留数钉为 2（详设 §7.2"保留最近 2 份"，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:70: * <b>之前</b>调用 {@link #start()}（spec"Raft 子系统生命周期绑定"）；关停反序。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:77:    /** 内部提交客户端池大小（PoC 摩擦：同 ClientId 在途串行，池化摊开，design D11）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:100:    /** 内部提交客户端池（design D11），未启动为 {@code null}。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:149:        // S4：自动触发按 snapshot-threshold 开启；保留 2 份（详设 §7.2）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:156:        // 无上界增长且严重落后场景永远走不到安装流（§7.3-2 依赖截断制造位点差）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:170:            // 粒度落在测试可驱动的条目量级（S4 追赶用例）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:178:        // 摩擦档案（P2-02）：重启目录非空必须 RECOVER，否则 "Failed to FORMAT"。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:233:     * 手动触发一份快照（详设 §7.2"手动管理命令"落点，S4/design D6）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:252:     * 成员变更（详设 §7.4，S4/P2-17/design D6）：以目标投票者/监听者全集
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:260:     * <p><b>多数派护栏</b>（spec"成员变更运维"，机械拒绝而非仅文档约定）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:296:                            + ", removed=" + removed + "）——先加新节点并等待追赶完成，再移除旧节点（§7.4）");
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:373:     * 轮转取用一个内部提交客户端（design D11）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/RaftSubsystem.java:422:     * 分区的公开视图（peer commitInfos 轮询——失联检测入口，design D5）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:41: * 复制网关（详设 §3.2 {@code ReplicationGateway}）：节点内所有复制条目的
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:42: * 唯一提交通道与"提交 → 应用 → 应答"桥（§4.5，design D3/D4/D11/D12）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:47: * design D10）——因此"应答即多数派确认后"。Ratis 传输层回执本身仅用于
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:49: * {@link RetryableCommitException} 完成（spec"在途请求快速失败"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:51: * <p><b>Leader 侧应用副效应</b>（仅当本节点为当值 Leader，design D9）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:52: * 授予出队、"需排队"竞态的排队登记与 QUEUED 改写（§4.5/D3）、按
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:74:    /** Leader 侧等待队列（design D9）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:84:    /** 到期驱动（P2-09 装配后回挂；null 表示到期复制未启用）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:86:    /** 会话协调器（P2-08 装配后回挂；接收应用/角色事件转发）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:107:     * 回挂到期驱动（装配后期绑定，P2-09）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:116:     * 回挂会话协调器（装配后期绑定，P2-08）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:128:     * 时刻（Leader 发起时刻，§4.2 诊断与条目时刻语义的来源）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:169:                    // reply.isSuccess：应答不取自回执消息——完成点在 onApplied（design D10）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:206:     * 当选：清空上一任期等待队列并启动到期驱动首扫（P2-09/D9）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:242:     * Leader 侧应用副效应（design D3/D9 的落点）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:267:        // 预演失效改写（§4.5/D3）：提交时判定可授予、应用时锁已被占——
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:296:        // 许可感知（design D4）：Semaphore 按影子表可用数判定队首是否满足，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:323:        // 屏障归零：全体 awaiter 广播放行（design D5）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:352:     * 推送 AWAIT_NOTIFY（Leader 本地连接投递；跨接入节点转发挂 S3，design D9）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:361:            return; // 连接已不存在：等清扫路径兜底（与 Phase 1 静默丢弃同语义）
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:394:     * 关停：以可重试错误完成全部未决 future（spec"关停无悬挂请求"）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:408:     * 已通知队首超时清扫（Leader 侧周期驱动，Phase 3 T1）：摘除超时未重发
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/ReplicationGateway.java:470:     * 调用方 MUST 以可重试错误应答客户端（§6.3 快速失败优先，S3 语义的 S2 承载）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:51: * Ratis 状态机适配器（详设 §3.2 {@code LockStateMachine}）：把 Ratis 的
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:55: * <p><b>契约要点</b>（design D10）：Ratis 仅应用<b>多数派已提交</b>的条目
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:62: * <p><b>快照通道（S4，§7/P2-15~16，design D3/D4/D5）</b>：{@link #takeSnapshot()}
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:68: * 持久化的崩溃窗口，被 design D4 显式否决；写盘期间 updater 线程的短暂
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:69: * 停为既定代价，耗时由 §10 快照基准度量。安装侧走 Ratis 3.3 的
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:91:     * 本状态机经同一实例读写——不自管磁盘（design D3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:125:     * （§7.3-1：重建锁状态后由库从快照位点起重放日志）。生命周期经
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:126:     * {@code startAndTransition} 推进（NEW→STARTING→RUNNING）——S4 起
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:190:     * 任期队列清理的触发源，§4.4/§8），并把新 Leader 身份投递
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:191:     * {@link #setLeaderIdentityListener 领导身份监听器}（s3 design D3 的
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:215:     * 装配领导身份监听器（{@link LeaderTracker} 挂点，s3 design D3）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:243:     * 生成并落盘一份快照（详设 §7.2，S4/P2-15）。由 Ratis 应用线程在
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:247:     * <p><b>流程与语义</b>（design D4）：取已应用位点 → applyLock 内
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:268:        // 锁内一致性副本（applyLock 内序列化，design D4）——此后副本不可变，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/raft/LockStateMachine.java:292:     * 快照安装暂停（Ratis 3.3 安装流第一步，design D5）：库侧写完快照块后、
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerChannelInitializer.java:34: * pipeline 装配（设计说明书 §5.2；Phase 3 详设 §5.1 T4 增 TLS 首位置）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerChannelInitializer.java:41: * <p><b>TLS 语义（spec"服务端 TLS 传输层"）</b>：注入非空 {@link SslContext}
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerChannelInitializer.java:44: * {@value #TLS_HANDSHAKE_TIMEOUT_MS} 毫秒（设计 §5.1：开启后拒绝明文/未完成
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerChannelInitializer.java:53:    /** 最大帧长 1 MiB，超限断连（设计说明书 §3.1）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerChannelInitializer.java:56:    /** TLS 握手超时（毫秒）：开启 TLS 后未完成握手即断开（详设 §5.1 默认 5s）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerChannelInitializer.java:82:     * 构造 pipeline 装配器（Phase 3 T4）：{@code sslContext} 非空时开启 TLS。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:43: * 连接级业务入口：握手门闩（design.md D8）、请求分发、断连清理。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:58: *   └─ 版本不在支持区间 [1,3] 或认证未通过（Phase 3 T4：开启命中失败 /
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:59: *      关闭 Phase 1 守卫"auth_token 非空"）：回 INVALID_REQUEST 并断连
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:68: *   <li>{@code ADMIN_*}（Phase 3 T3 管理观察）：独立早退交
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:102:    /** 管理观察处理器（Phase 3 T3；恒非空——兼容构造回落"未配置令牌"形态）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:104:    /** 业务令牌认证配置（Phase 3 T4 P3-16；兼容构造回落 {@link AuthConfig#unconfigured()}）。 */
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:140:     * {@link AuthConfig#unconfigured()}（Phase 1 兼容守卫）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:157:     * 构造会话处理器（全装配形态，Phase 3 T4 P3-16）：注入管理观察处理器与
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:159:     * 由 {@code authConfig} 门控（spec"业务令牌认证与默认兼容守卫"：开启校验
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:160:     * 命中任一令牌，关闭维持 Phase 1"非空即拒"守卫）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:203:     * 处理器独立早退（Phase 3 T3，不进指标埋点，在途记账由其写完成处终结）；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:224:        // 自我保护限额（设计说明书 §5.4，design.md D4）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:230:            // 管理观察独立早退（Phase 3 T3）：不进业务分发、不进指标埋点；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:236:            // 集群路径：应答异步于应用回执（design D4），endRequest 由
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:272:            // 分发兜底（design D1）：未预期异常回 INTERNAL_ERROR 并记 WARN 带堆栈，
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:294:        // 断连清理（design.md D3）：先摘注册表（通知不再路由到此连接），后清会话。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:303:                    // （§5.2 规则 3，含失败退避重试）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:334:     * 客户端协议版本不在支持区间 [1,3] 或认证未通过（Phase 3 T4：认证开启
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:335:     * 命中失败 / 关闭维持 Phase 1"非空即拒"守卫）回 {@code INVALID_REQUEST}
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:352:        // 业务令牌认证门控（Phase 3 T4 P3-16，spec"业务令牌认证与默认兼容守卫"）：
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:354:        // Phase 1 兼容守卫（非空 auth_token 即拒）。判定在 cluster/sessionOpened
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:361:            // 版本越界或认证未通过：拒绝并断连（不做隐式兼容，设计说明书 §3.2.1）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:367:            // 集群路径：SESSION_OPEN 经共识确认后回响应（design D12）。
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerSessionHandler.java:379:     * {@code protocol_version}（v1 客户端因此看到与 Phase 1 同形的响应）；
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerBootstrapFactory.java:25: * Netty ServerBootstrap 构建（设计说明书 §5.2）：boss/worker 线程组均由
openlatch-server/src/main/java/io/github/lamspace/openlatch/server/net/ServerBootstrapFactory.java:26: * 调用方传入并原样装配（本应用按 §5.2 取 boss=1、worker 可配置，选择
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/RemoteSemaphore.java:29: * {@link OSemaphore} 的远程实现（Phase 3 详设 §2.3 / P3-04）：许可裁决全部
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/AcquireSpec.java:22: * 异步获取请求参数（详设 §6.3 {@code acquireAsync} 入参）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/AcquireSpec.java:24: * <p><b>{@code waitMs} 语义</b>（详设 §3.2.2）：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/AcquireSpec.java:46:     * 锁家族便捷构造（Phase 1/2 既有形态）：许可参数取缺省
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OReadWriteLock.java:20: * 读写锁门面（详设 §6.3）：同一锁键上的读/写两个 {@link OLock} 句柄。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OReadWriteLock.java:22: * <p>读写互斥与严格 FIFO 由服务端裁决（详设 §4.4 规则 5）：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OReadWriteLock.java:24: * Phase 1 不支持持读升级写或持写降级读的特判。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OSemaphore.java:22: * 分布式信号量（Phase 3 详设 §2.3 / P3-04）：N 个许可的共享资源门闸，
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchException.java:22: * 客户端运行时异常基类（详设 §6.1）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/LockLostException.java:20: * 锁丢失的原因载体，作为 {@link LockLostListener} 回调参数传递（详设 §6.6）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/RemoteCountDownLatch.java:30: * {@link OCountDownLatch} 的远程实现（Phase 3 详设 §2.4 / P3-06）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/RemoteCountDownLatch.java:36: * （改连换 id 的幂等口径与 {@code OLock} 等待一致，详设 §6.3）。通知
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/RemoteLock.java:30: * 阻塞桥接（详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/LockAcquisitionTimeoutException.java:21: * 到达而仍未被授予时抛出（详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/LockType.java:20: * 锁类型（详设 §3.2 协议 {@code LockType} 的客户端公开映射）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/LockType.java:28: *       （详设 §4.4"SimpleLock 的自锁问题"）；</li>
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/LockType.java:34: * Phase 1 不支持持读升级写或持写降级读的特判，一律走通用排队规则。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/LockType.java:50:     * 许可门闸（v3，Phase 3 T1）：非锁类型，仅 {@link OSemaphore} 内部
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchTimeoutException.java:21: * （详设 §6.4"每个请求必有超时"）。对应概要设计 §4.3 标准 3：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:49: * 连接与重连状态机（详设 §6.2）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:66: * <p><b>目标选择（S3，详设 §6.3）</b>：连接目标为可轮换的 {@code (currentHost,
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:108:    /** v3 协议版本（Phase 3 T1 起），握手请求固定携带（服务端兼容 v1–v3，应答回显本版本）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:110:    /** 入站帧最大长度（1 MiB），与服务端帧长限制一致（详设 §3.1）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:154:    /** 客户端指标门面（T2 重连计数；默认禁用，装配阶段经 {@link #setMetrics} 注入）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:168:     * 做启动直连发现（详设 §6.3）。在 EventLoop 线程调用，MUST NOT 阻塞。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:187:     * <p><b>单种子（Phase 1 语义，逐字节不变）</b>：每次失败即指数倍增
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:223:     * 全参构造（S3 多车道，design D6）：车道以显式初始目标建连。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:258:     * Builder 校验保证）。主动断连不推进——重连先试原地址（详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:281:     * 注入指标门面（T2；仅由客户端装配阶段调用）。断连重连与连接失败重试
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:502:        // 客户端 TLS（Phase 3 T4）：启用时惰性构造 SslContext 并缓存（跨重连复用，
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:504:        // + WARN 日志，不崩溃 EventLoop），与 spec"错误 trust-store 清晰失败"一致。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:524:                        // TLS 必居 pipeline 首位（Phase 3 T4）：握手/加解密先于
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:595:        // 业务令牌（Phase 3 T4）：配置非空即随 HELLO 携带（认证开启的服务端校验；
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:630:                // 推进游标，关闭后的退避重连换下一种子（详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:647:            // 锁外回调：首连/重连成功通知（重连裁决经此触发，详设 §6.2）；
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ConnectionManager.java:648:            // 握手监听携带 v2 leader 提示，客户端启动发现据此直连（§6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/HeldLockRegistry.java:28: * 本地持锁簿记（详设 §6.1/§6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/HeldLockRegistry.java:30: * <p><b>核心约束（design.md D4）：只记归属、不记重入计数。</b>
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/HeldLockRegistry.java:183:         * 失锁时刻：上次成功续租 + 实际生效租约（详设 §6.2）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/HeldLockRegistry.java:255:     * 避免双账本漂移（详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/HeldLockRegistry.java:310:     * 判否，供客户端丢弃该键的附属登记（如锁丢失监听器，详设 §6.3、
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/HeldLockRegistry.java:311:     * 变更 phase1-audit-remediation design D4）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/SessionContext.java:23: * {@code requestId} 分配器（详设 §6.2/§6.4）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/SeedDiscovery.java:48: * 种子扇出发现（详设 §6.3"连续 N 次 NOT_LEADER → 强制走一次种子列表发现"，
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/SeedDiscovery.java:49: * s3 design D4/D6 的降级路径实现体）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/SeedDiscovery.java:134:        // 探针与主连接同安全配置（Phase 3 T4，spec"种子发现探针附令牌"）：TLS
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/SeedDiscovery.java:177:            // 业务令牌（Phase 3 T4）：与主连接一致，配置即随探针 HELLO 携带。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/SeedDiscovery.java:206:                // 地址未配置：CLUSTER_VIEW 取 Leader 自报地址（design D4 降级路径）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientSecurity.java:26: * 客户端安全装配工具（Phase 3 详设 §5.1/§5.2，spec"客户端 TLS 与认证消费"）：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:34: * 单连接请求多路复用器（详设 §6.4）：全部出站请求的唯一收口。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:41: *   <li><b>每个请求必有超时</b>（概要设计 §4.3 标准 3）：超时任务触发时以
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:47: *       （变更 phase1-audit-remediation design D3）；</li>
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:49: *       组件处理补偿归还（详设 §6.5、design.md D3）。</li>
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:58:    /** v3 协议版本（Phase 3 T1 起），业务出站信封固定携带（服务端应答回显此版本）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:69:    /** 客户端指标门面（T2）；禁用形态下观测回调整体不注册（零额外路径）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:76:     * 出站门（测试注入口，design.md D7）：谓词返回 {@code false} 时请求
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:105:     * 不影响任何完成语义与返回值，T2）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:147:     * 保证两个调用方的 future 均有界完成（design D3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:163:        // 同 id 交叠时先到的超时不会误杀后登记的条目（design D3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:172:            // 杜绝"覆盖后旧 future 永不完成"的悬挂（design D3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:184:     * 观测挂载（T2）：在返回的 future 上注册旁路完成回调记录请求终局。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:222:     * 以给定原因使全部挂起请求失败（断连快速失败路径，详设 §6.2）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/RequestMultiplexer.java:247:     * 设置出站门（测试注入口，design.md D7）。谓词返回 {@code false} 的信封
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientConfig.java:23: * 客户端配置（不可变，详设 §6.7 默认值表；v2 增种子列表，详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientConfig.java:30: * <p><b>种子语义（S3）</b>：{@code host/port} 即种子列表首项（Phase 1 单地址入口的
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientConfig.java:35: * <p><b>安全配置（Phase 3 详设 §5.1/§5.2，spec"客户端 TLS 与认证消费"）</b>：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientConfig.java:74:     * 兼容构造：种子列表 + 全部安全项默认关闭的形态（旧 9 参签名，S3 前
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientConfig.java:96:     * 兼容构造：Phase 1 的 8 参形态（单地址 = 一元种子表，安全项默认关闭）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientChannelHandler.java:28: * 客户端入站唯一收口（详设 §6.1）：解码后的 {@link Envelope} 全部经
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/LatchNotifyRegistry.java:24: * 屏障等待的通知路由登记表（Phase 3 P3-06）：{@code (会话, 请求 id) →
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:43: * 等待跟踪器（详设 §6.5）：管理排队中的获取请求全生命周期。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:53: *   ├─ AWAIT_NOTIFY → 以同一 requestId 重发（服务端幂等，§4.8）
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:55: *   ├─ 重发请求超时 → 仅结束该次重发，保持挂起等待下一次通知（design.md D1）
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:59: * <p><b>补偿归还（design.md D3）</b>：等待以任何方式结束后，其 {@code requestId}
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:91:     * v2 {@code NOT_LEADER} 接管钩子（S3 客户端重定向，详设 §6.3）：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:152:        /** 是否曾收到 QUEUED：重发超时保持挂起（D1）的判定依据。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:225:     * 未命中（等待已超时/失败/完成）则忽略（详设 §6.5 边界场景）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:248:     *   <li>等待已结束且在保留窗口内 → 对授予发送补偿释放（D3）。</li>
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:276:     * 断连清空：全部挂起等待以给定原因快速失败（详设 §6.2）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:305:     * 可迁移等待快照（S3 Leader 改道：本车道降级时挂起项向新主车道的
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:371:     * 重发阶段收到错型响应 → 保持挂起等待下一次通知（与 D1 同调：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:396:     * 重发、保持挂起等待下一次通知（design.md D1），由等待总超时兜底。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/AwaitTracker.java:510:     * 失败用户 future。服务端队列条目按 §6.3 惰性回收。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:35: * 看门狗：持锁期间的自动续租调度（详设 §6.6）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:37: * <p><b>续租周期</b>：{@code grantedLeaseMs / 3}（概要设计 §6.3）。续租请求
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:49: * <p><b>断连正交化（design.md D5）</b>：条目的归属车道（单连接形态即唯一
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:50: * 连接；S3 多车道按 sessionId 解析）非 ACTIVE 或不可得时跳过本次续租发送
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:67:    /** 会话 → 多路复用器解析器（S3 多车道：存量锁的续租回其归属车道）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:73:    /** 归属车道可用性判断（按 sessionId）：断连跳过续租（D5）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:96:     * 创建看门狗（多车道形态，S3 design D6）：条目按其归属会话解析
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:150:     * 续租周期执行：条目已不在册 → 终止；断连 → 跳过不计数（D5）；
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/Watchdog.java:159:        // 归属车道解析（S3 多车道）：会话所在连接不可用即跳过不计数（D5 同源语义）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientMetrics.java:28: * 客户端可选指标门面（Phase 3 详设 §3.3，spec"客户端可选监控指标"）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/internal/ClientMetrics.java:36: * <p><b>指标词表</b>（命名与详设 §3.3 逐项对齐；{@code type} 取
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OLock.java:23: * JUC 风格的同步锁句柄（详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OLock.java:29: *       （概要设计 §4.2）；</li>
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OLock.java:41: * 断连时按失锁时刻定时裁决（详设 §6.2/§6.6）。{@code lock()} 的持有语义
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OLock.java:110:     * 丢锁时旧监听器不触发，需重新注册（详设 §6.3，design D4）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:50: * OpenLatch 客户端入口（详设 §6.1/§6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:56: * <p><b>连接车道（S3，design D6）</b>：稳态单连接（home 即 Leader，或单机
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:64: * 改连产生的新会话使用新 {@code requestId} 空间（幂等口径，详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:66: * <p><b>线程模型</b>（详设 §6.8）：全部网络读写在客户端 EventLoop 线程；
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:96:    /** 请求多路复用：home 车道全部出站请求与入站响应的收口（详设 §6.4）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:98:    /** home 车道的连接与重连状态机（测试注入与故障裁决入口，Phase 1 语义保持）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:102:    /** 本地持锁簿记：只记归属不记重入计数（详设 §6.3）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:104:    /** 屏障等待的通知信号登记表（Phase 3 P3-06，按到达连接会话路由）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:107:     * 获取车道（Leader 车道，design D6）：{@code null} 即稳态单连接——home 即
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:112:    /** 会话 id → 归属车道路由表：存量锁续租/释放回其获取车道（design D6）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:118:    /** 连续 NOT_LEADER 计数：达阈值触发种子扇出强制发现（详设 §6.3）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:121:    /** 强制发现阈值（详设 §6.3"连续 N 次（默认 3）"，常量取向不配置化）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:125:    /** 看门狗：持锁期间的自动续租与失锁判定（详设 §6.6）。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:127:    /** 客户端可选指标门面（详设 §3.3，T2）：未注入注册表即禁用形态、零观测路径。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:150:     * （Phase 3 T2，详设 §3.3——旁路观测，不改变任何行为契约）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:205:    // ==================== S3：Leader 发现与故障转移（design D6） ====================
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:208:     * 获取车道：指向当值 Leader 的第二连接（详设 §6.3，design D6）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:305:     * 会话已被服务端清理，其持锁必然失效（详设 §6.2 断连裁决）。仅裁决
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:327:     * home 车道握手：按 v2 提示做启动直连发现（详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:385:        // 有界窗口内未 ACTIVE 即关道并降级到种子发现（design D3——陈旧/不可达
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:452:     * NOT_LEADER 接管（详设 §6.3）：按提示改道重放 / 空窗原地退避 / 阈值强制发现。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:498:            forceDiscover(req); // 提示有主但无地址：种子扇出自报兜底（design D4）
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:555:     * 获取车道上的新会话重放（改连即新 requestId 空间，详设 §6.3 幂等口径）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:638:     * 会话归属车道可用性（D5 正交化：不可得按 home 连接状态）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:650:     * 车道退役（design D6 收口规则）：车道不再是当前获取车道时，无在册持锁
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:683:     * 持锁登记失锁时刻裁决（详设 §6.2，按会话隔离不误伤他车道锁）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:717:     * 创建构建器。服务地址为唯一必填项，其余参数取详设 §6.7 默认值。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:755:     * 等待获取车道建立（S3 测试钩子，包私有）：启动直连发现/重定向为异步
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:780:     * 异步获取锁（详设 §6.3）。行为按 {@link AcquireSpec#waitMs()} 分支：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:798:        // 获取车道优先（design D6）：存在指向 Leader 的车道时新获取以其会话
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:822:                        // Phase 3 T1 许可参数：锁家族恒 permits=1 / total=0，
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:841:     * 异步释放锁（详设 §6.3）：以获取时签发的租约凭据发送释放请求。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:857:     * 异步释放（Phase 3 T1 扩展）：携带归还许可数（{@code 0} 为缺省单许可，
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:872:        // 存量锁跟家（design D6）：释放按其获取车道路由——home 会话的锁在
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:894:                        // design D4：该键无人重持时丢弃监听器登记——监听表
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:917:     * <p>关停前对本地登记持锁尽力释放（详设 §6.3，见
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:946:     * 并尝试收口因此清空的待退车道（design D6"存量清零→连接收口"）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:956:        // 失锁裁决恰发生在簿记移除成功处 → 计数同条目至多一次（T2）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:968:     * 关停前尽力释放本地持锁（详设 §6.3）：对每个条目发送释放请求并等待
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:995:     * 授予回调：登记本地持锁归属（只记归属不记重入计数，design.md D4）
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1003:     * 计数，design.md D4）并为新登记条目启动看门狗续租；重入授予命中既有
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1022:     * 会话路由表摘除（design D6"存量清零→连接收口"）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1111:     * 请求多路复用器，测试经此装配出站门（半开连接注入，design.md D7）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1130:     * （design D4：完全释放后丢弃登记）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1141:     * 调用方需重新注册（详设 §6.3，design D4）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1154:     * 单个回调异常被捕获并记录，不影响其他监听器（详设 §6.6）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1189:     * 创建可重入互斥锁句柄（详设 §6.3）。同一锁键可创建多个句柄，
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1200:     * 创建显式公平承诺互斥锁句柄（Phase 3 详设 §2.2/P3-02）。语义与
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1207:     * v3（Phase 3 服务端）时可用；旧服务端会以 {@code INVALID_REQUEST}
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1218:     * 创建不可重入互斥锁句柄（详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1221:     * （详设 §4.4"SimpleLock 的自锁问题"）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1231:     * 创建读写锁门面（详设 §6.3）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1244:     * 创建分布式信号量句柄并主张许可总量（Phase 3 详设 §2.3 / P3-04）：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1274:     * 创建分布式倒计数屏障句柄（创建者/断言形态，Phase 3 详设 §2.4 /
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1275:     * P3-06）：该句柄的每次 await/countDown 请求携带 {@code total = count}
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1305:     * 屏障通道路由快照（P3-06）：获取车道优先、回落 home（与
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1307:     * 下 MUST 经车道承载，深编排归 P3-07）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1387:     * 客户端构建器：收集配置并校验（详设 §6.7 默认值表；§6.3 种子列表）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1389:     * <p>服务地址必填：{@code address}（Phase 1 单地址入口）与 {@code seeds}
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1411:        /** 宿主度量注册表（Phase 3 T2）；{@code null}=客户端指标默认关闭。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1413:        /** 是否启用 TLS（Phase 3 T4，spec"客户端 TLS 与认证消费"）；默认 false=明文。 */
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1442:         * 设置种子地址列表（集群形态，详设 §6.3：任一可建连、断连轮询、
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1540:         * 注入宿主度量注册表，启用客户端可选指标（Phase 3 T2，详设 §3.3：
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1556:         * 启用/关闭 TLS（Phase 3 T4，spec"客户端 TLS 与认证消费"）。开启后客户端
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1604:         * 设置业务令牌（服务端业务认证开启时 HELLO 校验；spec"客户端 TLS 与
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OpenLatchClient.java:1648:            // mTLS cert/key 成对约束（Phase 3 T4）：单独配置其一即误配，快速失败。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/LockLostListener.java:20: * 锁丢失监听器（详设 §6.1/§6.6）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OCountDownLatch.java:22: * 分布式倒计数屏障（Phase 3 详设 §2.4 / P3-06）：一次性倒计数栅栏，
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/OCountDownLatch.java:32: * 无操作；不支持重置——新一轮屏障使用新 key（详设 §2.4 文档化约定）。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/LockGrant.java:20: * 锁授予结果（详设 §6.1）：获取成功时由异步接口返回。
openlatch-client/src/main/java/io/github/lamspace/openlatch/client/ServerUnavailableException.java:20: * 连接不可用时抛出：断连瞬间对全部挂起请求的快速失败（详设 §6.2），
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchClientBuilderCustomizer.java:22: * {@link OpenLatchClient.Builder} 的装配期扩展点（Phase 3 T2）。
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchProperties.java:25: * {@code openlatch.*} 配置属性（详设 §8.2 全表）。
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchProperties.java:27: * <p><b>职责</b>：绑定并承载 starter 的全部可配置项，缺省值即详设 §8.2
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchProperties.java:35: * 装配（design D4）。时长类属性支持标准 Duration 写法（如 {@code 5s}、
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAspect.java:52: * {@link OpenLatch} 注解的锁切面（详设 §8.3，M4 定案见 design D7）。
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAspect.java:61: * 客户端锁丢失通道通知，design D3；会话过期仅计入释放侧，获取侧原样
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAspect.java:196:     * 业务执行后的守卫式释放（design D3）：锁已丢失（失效状态码
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatch.java:29: * 声明式分布式锁（详设 §8.3，M4 定案：{@code type} 直接复用客户端公开枚举
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatch.java:30: * {@link LockType}，不另造 {@code LockMode}，design D2）。
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatch.java:59: *       方法会排队等待自身，直至租约到期（详设 §4.4 自锁警示）；</li>
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatch.java:63: *   <li>Phase 1 不支持持读升级写 / 持写降级读特判；{@code READ}/{@code WRITE}
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatch.java:77:     * 锁键的 SpEL 表达式（详设 §8.3）。求值上下文注入方法形参
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatch.java:87:     * 锁类型（客户端公开枚举，design D2）。
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAutoConfiguration.java:29: * OpenLatch 自动装配入口（详设 §8.1）。
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAutoConfiguration.java:45: * <p><b>客户端指标（Phase 3 T2）</b>：上下文存在 {@code MeterRegistry} Bean
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAutoConfiguration.java:65:     * {@code connectTimeout}/{@code workerThreads} 不在 §8.2 属性表内，
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAutoConfiguration.java:71:     * @param customizers 构建期定制器（按 {@code @Order} 升序应用；T2 度量
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAutoConfiguration.java:86:        // TLS/认证属性（Phase 3 T4，spec spring-boot-starter"配置属性绑定与默认值"）：
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAutoConfiguration.java:115:     * 度量注册表注入分支（T2，spec"度量注册表自动注入"）：类级
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAspectConfiguration.java:29: * {@link OpenLatchAspect} 切面的条件装配（详设 §8.1，design D4/D7）。
openlatch-spring-boot-starter/src/main/java/io/github/lamspace/openlatch/spring/OpenLatchAspectConfiguration.java:35: * design D4）；{@code @ConditionalOnBean(OpenLatchClient)} 覆盖"客户端
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/metrics/MetricsScraper.java:35: * 节点指标端点代理抓取器（Phase 3 T3 design D7，spec"轮询刷新与指标趋势"）：
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClientPool.java:34: * 节点连接池（Phase 3 T3 design D6，spec"节点连接管理与故障降级"）：
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClientPool.java:40: * 各客户端连接、再优雅终止线程组（spec"关闭时优雅释放全部对节点的连接"）。
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClientPool.java:64:        // 节点连接安全（Phase 3 T4，spec admin-console"部署形态与配置"）：TLS 开启
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClient.java:55: * 单节点管理客户端（Phase 3 T3 design D6）：懒连接、HELLO(v3) 后同步
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClient.java:71: * 连接被服务端断开（令牌被拒的线路形态，spec"管理令牌认证"）折算 kind=AUTH，
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClient.java:73: * （spec"节点连接管理与故障降级"）。
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClient.java:94:    /** 节点 TLS 上下文（Phase 3 T4）；{@code null} 即明文（明文形态与 T3 一致）。 */
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClient.java:109:     * 构造单节点客户端（不连接；明文、无业务令牌——T4 前的既有形态）。
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClient.java:122:     * 构造单节点客户端（Phase 3 T4 全形态：可选节点 TLS 与业务令牌）。
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClient.java:291:                            // TLS 必居 pipeline 首位（Phase 3 T4）：握手/加解密先于
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminClient.java:318:        // 认证开启下逐消息 admin-token 不构成 HELLO 放行依据，spec"不可借道"）；
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminUnavailableException.java:20: * 管理通道不可用异常（Phase 3 T3）：控制台侧对"节点查询失败"的单一收口
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/admin/AdminUnavailableException.java:24: * 受检异常会把降级逻辑撕成样板（详设 §4.3 只读容错取向）。
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/ConsoleConfig.java:28: * 控制台配置（Phase 3 详设 §4.4，spec"部署形态与配置"；
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/ConsoleConfig.java:35: * （spec"坏配置快速失败"），MUST NOT 静默回落。
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/ConsoleConfig.java:43: * @param security         节点连接安全配置（Phase 3 T4，spec admin-console"部署形态与
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/ConsoleConfig.java:59:     * 节点连接安全配置（Phase 3 详设 §5.1/§5.2，spec admin-console 增量）：
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/ConsoleConfig.java:132:    /** 默认 HTTP 监听端口（详设 §4.4）。 */
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/web/ConsoleController.java:42: * 控制台五页面路由（Phase 3 详设 §4.3，spec"五页面只读呈现"）：全部为
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/web/ConsoleController.java:47: * 逐一查询并按节点分块呈现——服务端"仅本节点视角"的语义（spec"双形态
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/web/WebModels.java:31:     * 单节点单查询的尽力而为结果（spec"任一节点故障 MUST NOT 阻断对其余
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/OpenLatchConsoleApplication.java:31: * 控制台入口（Phase 3 详设 §4.4，spec"部署形态与配置"）：独立部署的
openlatch-console/src/main/java/io/github/lamspace/openlatch/console/OpenLatchConsoleApplication.java:44: * 抛 {@link IllegalArgumentException}——容器启动失败退出（spec"坏配置快速
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/ExampleServers.java:23: * 示例共享夹具：进程内内嵌服务器（design D6）。
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/BenchmarkMain.java:36: * 基准 harness（详设 §9/§10.5，design D5 手写方案）：
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/BenchmarkMain.java:40: * <p><b>定位</b>：记录为基线防退化参考，<b>不作发布门槛</b>（§10.5）；
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/BenchmarkMain.java:372:        sb.append("# OpenLatch Phase 1 基准基线\n\n");
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/BenchmarkMain.java:374:                .append("　来源：`BenchmarkMain`（design D5 手写 harness）\n\n");
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/BenchmarkMain.java:402:                .append("本基线仅作防退化参考，不作发布门槛（详设 §10.5）。\n");
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/QuickStartExample.java:26: * 示例 1：编程式 API 最小闭环（详设 §9）——lock / tryLock / unlock。
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/WatchdogExample.java:28: * 示例 4：看门狗续租与锁丢失回调（详设 §9）。
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/WatchdogExample.java:37: *       连续续租超时判定，详设 §6.2/§6.6）；若客户端稍后重连成功，
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/ReadWriteExample.java:28: * 示例 3：读写锁并发矩阵（详设 §9）——读读共享、读写互斥。
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/ConcurrencyExample.java:31: * 示例 2：多线程竞争同一互斥锁，打印入队与授予顺序（详设 §9，
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/SpringAnnotationExample.java:33: * 示例 5：Spring Boot 应用 + {@code @OpenLatch}（SpEL key，详设 §9），
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/SpringAnnotationExample.java:34: * 兼作验收标准 4 的活证据——除 starter 依赖与注解外零接入代码。
openlatch-examples/src/main/java/io/github/lamspace/openlatch/examples/SpringAnnotationExample.java:38: * 服务器以进程内内嵌方式启动（design D6，演示夹具，生产请独立部署）。
openlatch-server/pom.xml:15:    <description>OpenLatch Netty 单节点服务器（M2 交付）</description>
openlatch-client/pom.xml:15:    <description>OpenLatch 客户端 SDK（M3 交付）：异步内核 + 同步包装 + 看门狗 + 重连</description>
openlatch-spring-boot-starter/pom.xml:15:    <description>OpenLatch Spring Boot 自动装配与 @OpenLatch 声明式锁（详设 §8，M4 交付）</description>
openlatch-console/pom.xml:15:    <description>OpenLatch 管理控制台（Phase 3 详设 §4 T3：只读观察 Web 应用，概要设计 §5.2）</description>
openlatch-examples/pom.xml:15:    <description>OpenLatch 示例与基准（详设 §9，M4 交付；不发布到 Maven Central）</description>
```
