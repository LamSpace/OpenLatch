# Tasks: phase2-s4-snapshot-recovery

对应详设 §13.4 子任务：组 1=P2-15，组 2=P2-16，组 3=P2-17，组 4=P2-18，组 5=P2-19（Phase 2 发布门），组 6=文档收口。设计决策 D1–D12 见 design.md。

## 1. 快照生成（P2-15）

- [x] 1.1 core 重建入口（D1/D2）：新增 `CoreStateRestore` 值对象（core 原生类型，无协议依赖）+ `CoreEngine.restoreFrom`——锁条目直写（持有计数/凭证/到期）、`leaseTokenCounter` 跳至 max(快照凭证)、`LeaseManager` 逐继承条目回填、`SessionRegistry` 登记；Javadoc 契约级撰写（限定唯一用途与调用时序）；`CoreEngineTest` 侧补 core-lock-engine spec 新 4 场景（继承态操作正确/发号跳界/到期堆覆盖/单机路径零扰动）
- [x] 1.2 `LockStateMachineCore.installSnapshot(SnapshotState)`：proto→record 翻译 + `shadow.load()` + `sidMap` 重建，applyLock 内与引擎重建原子推进；单测——`toProto()→load()` round-trip digest 相等；**快照切割点不变性属性测试**：随机条目序列"直接回放" vs "前缀快照重建 + 后缀回放"终态 digest 一致（≥100 组，沿 S2 属性测试基架与口径）
- [x] 1.3 `LockStateMachine.takeSnapshot` 实装（D3/D4）：applyLock 内 `toProto().toByteArray()` 一致性副本 → 放锁 → `getSnapshotStorage().newSnapshot()` 写盘提交，返回 lastApplied 位点；stall 语义写入类级 Javadoc；`RaftSubsystem` 接配置：`setAutoTriggerEnabled(true)` + `snapshot-threshold` → 自动触发阈值 + 保留 2 份，新增 `triggerSnapshot()` 手动通道（D6）
- [x] 1.4 集成用例（`ClusterHarness`，低阈值）：并发 ACQUIRE/RELEASE 负载下触发——负载零错误、快照文件数 ≤2、快照位点==当时已应用位点、快照+尾部回放后与在役副本 digest 收敛（spec 触发/保留/不中断三 Requirement 场景）

## 2. 快照加载与追赶（P2-16）

- [x] 2.1 spike（时限半天，沿 S3 3.1 先例）：Ratis 3.3.0 快照安装流行为——3 节点低阈值、杀 Follower 落后至触发 `pause`/`loadSnapshot`，验证时序与追赶期写拒答表现；结论回写 design D5、追加 `observations-ratis-3.3.0.md` 摩擦档案
- [x] 2.2 启动加载与安装实现：`initialize` 读 `getLatestSnapshot()` → 反序列化 → 1.2 重建入口 → `notifyTermIndexUpdated`；`loadSnapshot(SnapshotInfo, InputStream)` 安装流实现（读尽即安装点，applyLock 内重建 + `setLastAppliedTermIndex`）；文件内容 = `SnapshotState` 纯字节（D3，不造 term/index 信封）
- [x] 2.3 恢复路径集成用例：① 快照点前后重启 Follower（`restartNode` 保留 data-dir）→ 装快照+回放尾部 → `awaitDigestsAgree`；② 制造严重落后（快照截断历史日志）重启 → 断言经安装流而非本地全量回放（观测安装事件/本地日志起点）；③ 追赶窗口：写请求一律 `NOT_LEADER`、`CLUSTER_VIEW`/HELLO 正常应答（spec 三场景）
- [x] 2.4 10 万条目基准（D9）：apply 直灌构造 → 度量快照大小/落盘耗时/加载+追赶耗时 → 恢复总时限 <30s 断言（§2.4）→ `StateComparisons.diff` 全量结构级比对（MUST NOT 抽样）；度量数据入库报告

## 3. 成员变更（P2-17）

- [x] 3.1 `RaftSubsystem` 成员变更封装（D6）：`setConfiguration`——新节点 listener 加入、追赶收敛后升 voter、移除出组；同批变更越界（触多数派）入参校验拒绝；Javadoc 契约级
- [x] 3.2 删节点会话清理集成用例：移除持有活跃会话/锁的节点 → 其会话经 §5.2 规则 4 批量 `SESSION_CLOSE` 车道落地 → 存活副本三路 digest 一致、其持锁可被重新授予
- [x] 3.3 加节点追赶 IT：空 data-dir 新节点 → 快照安装+增量追赶 → 纳入后可当选并服务（transferLeadership 验证）；变更进行中对存量锁续租/释放语义不破坏（spec"变更期间已确认锁不丢"）
- [x] 3.4 运维文档：`docs/OpenLatch-Phase2-集群部署与故障转移.md` 新增成员变更章节——"先加后删、等追赶完成"流程、多数派变更禁令、回滚限制（快照启用后不可回退 S2 二进制）

## 4. 分区与滚动重启演练（P2-18）

- [x] 4.1 netns 真分区演练脚本（D7 主轨，`-Pdrill` + `@Tag("drill")`，需特权、不进 surefire）：3 节点独立 netns、双向切断少数派↔多数派——断言少数派无主且全部写请求超时/拒绝、恢复联通后自动收敛且全量比对一致；报告入库 `docs/partition-drill-<date>.md`（含失败轮次如实记录，沿 `failover-drill-2026-08-31.md` 格式）
- [x] 4.2 in-JVM 辅轨用例（附带发现并修复失联判定误伤存活副本的真实缺陷：design D12 + spec"失联判定的进度保护"）：transferLeadership 让主单侧后停其余两节点，少数派侧写全灭断言（常规回归；证据口径标注"近似：失联非分区"）
- [x] 4.3 滚动重启演练：逐台重启（先主后从/先从后主两序），期间持续混合负载客户端统计错误率 <1% 且切换窗口外零错误；报告入库 docs

## 5. 混沌测试与验收闭环（P2-19 · Phase 2 发布门）

- [x] 5.1 混沌用例：随机杀节点 + ≥10 分钟持续负载 + 低阈值令快照窗口与杀点交错；不变式检查器——任意时刻同 key 至多一写持有者、停载+一租约期后全副本锁表空、快照恢复后不变式复验（检查器通道按 design Open Question 实现期定夺并记录）
- [x] 5.2 §11 验收证据收集：第 3–6 项（分区/快照一致/滚动重启/无死锁泄漏）逐项勾对 + 基准数据汇总为 `s4-exit-checklist.md`；`s4-zero-touch-evidence`（D8 口径：core diff 仅 §7.1 授权纯新增、既有方法体逐字节不变的审查记录）
- [x] 5.3 全量门禁：`mvn -s /home/lam/repo/settings.xml clean verify` 全 reactor 绿（默认不含 drill）+ `-Pdrill` 演练全套通过 → S4 退出、Phase 2 验收清单收口

## 6. 文档收口

- [x] 6.1 详设 v1.4 回写：§7 实现口径（D4"锁内副本+锁外写盘"、手动触发落点、安装流）、§1.4/§2.4/§13.4 core 口径切换（D8）、§13.4 各任务完成标记与证据链接
