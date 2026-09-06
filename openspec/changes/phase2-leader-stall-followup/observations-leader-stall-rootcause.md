# observations：leader 复制停摆根因定位（任务 1.2）

- 登记：2026-09-06，`phase2-leader-stall-followup` 任务 1.2。
- 输入：①双 jstack 现场（热机进程级滚动重放，r5 轮停摆态，间隔 10s 两次快照 ×3 节点）；
  ②停摆轮三节点全量日志；③Ratis 3.3.0 sources 对账（本地仓库 sources jar）。
- 频率矩阵（本次重放 8 轮 ×双序，driveSeconds=60 短窗；r4 中断无数据）：

| 轮 | 测试① | 测试② |
|---|---|---|
| r1 | 先从后主 21.36%（末错 15227ms 收敛，强度超线） | **先主后从 26.13%（末错 72894ms≈全程末，停摆❌）** |
| r2 | 先从后主 4.82%（末错 9925ms 收敛） | **先主后从 22.17%（末错 73015ms，停摆❌）** |
| r3 | 先主后从 1.00% ✅ | 先从后主 3.76%（收敛） |
| r5 | **先从后主 23.31%（末错 73243ms，停摆❌，jstack 现场已捕）** | 先主后从 1.27% ✅ |
| r6 | 先从后主 1.01% ✅ | 先主后从 16.11%（17.3s 收敛；风暴簇误触发两包对照取证） |
| r7 | 先从后主 1.95% | 先主后从 1.38% |
| r8 | 先从后主 3.07%（收敛） | 先主后从 0.00% ✅ |

  **停摆 3/14 有效测试（r1L、r2L、r5F：先主后从 2/7≈29%，先从后主 1/7≈14%）——两序
  皆可停**：档案"先从不先主"推荐序的
  安全前提是"先主后从 4/4、先从后主 0"，本次 r5 在**先从后主**序打出同形停摆
  （机理吻合：归来旧主以更高任期当选→其新 LeaderState 的 conf 条目压在两
  快照边界 follower 之上——旧主归群竞态与顺序无关，仅概率有别）。运维推荐序
  降级为"降概率"而非"免疫"（5.1/5.2 同步修订）；非停摆轮的窗内突发强度
  亦普遍高于档案历史值（短窗分母 + 本机并发负载，如实记录）。

## 复现路径记录（任务 1.1 verify 的完整过程）

1. **in-JVM 构造不中（28 轮）**：`LeaderStallReproDrillIT` v1（12 轮，等新主 ready 再归群）
   与 v2（16 轮，脏尾停主 + 400–1600ms 竞速归群 + 后台写载）全部 RECOVERED
   （`docs/leader-stall-repro-2026-09-06.md` 三 Run 节）。机理性负结果：选举先收敛则
   新主任期 conf 已由双节点多数派提交，归群者无法冻结不需要它的多数派——停摆需要
   **两 follower 的 matchIndex 同时低于新主任期 conf 条目**，而该条件要求归群节点
   以"自身日志被截断至快照边界"的深度落后形态参与选举时序，in-JVM 秒级窗内未命中。
2. **回退滚动演练重放（D1 预案）**：进程级 `RollingRestartDrillIT` 背靠背 ≥4 轮，
   先主后从 3/3 停摆命中，r5 轮完成双 jstack + 全量日志取证 → 复现基座建立 ✅。

## 根因陈述（三选一收口：候选② `GrpcLogAppender` INCONSISTENCY 回退后推进不收敛）

### 前件（产品配置 × 库行为的复合，非单库 bug）

1. 每节点关停时 `raft.server.snapshot.trigger-when-stop.enabled`（**库默认 true**）在
   最后 applied 位点产出快照；本产品的 `purge.upto.snapshot.index=true`（S4 §7.3 决策）
   使重启装载时日志截断/清除至**本节点快照位点**。停摆轮实测：n1
   `snapshotIndexFromStateMachine=244`（其关停瞬间 applied=244，日志留 245+ 旧任期
   脏尾），n3 `=398`。滚动重启把"边界深度落后"同时刻放进两节点。
2. 新/再任 leader（r5 轮 n2，term 4）的每个新 `LeaderStateImpl` 为其两 follower 建
   **全新 FollowerInfo**：`nextIndex=本节点 nextIndex(≈553)`、`matchIndex=-1`。任期
   conf 条目（`startupLogEntry`）落 553+，`isLeaderReady()` 要求其提交。

### 停滞环（库内单点：INCONSISTENCY 回退被并发/错误分支撤销，净推进为零）

现场日志三节点拼合的稳态环（每 follower 每 ~5s 一圈，永不收敛）：

```
leader 发 append：prev=(t:4,i:400)（或 (t:3,i:248)）
follower:  "Failed appendEntries, previous log entry (t:4,i:400) not found"
follower:  INCONSISTENCY 回 nextIndex=399（=自身快照边界+1）
leader:    setNextIndex updateUnconditionally 553→399      （gRPC executor 线程A）
leader:    同刻另一路 "request=null" 错误回调分支：
           "Follower failed (request=null, errorCount=N); keep nextIndex (400/401) unchanged"
           （GrpcLogAppender.resetClient 的 request==null 分支不回退——回退被抵消）
leader:    sleepForErrors（errorRetryWaitPolicy 末段 5s；注释"won't be waked up by
           signal"——信号唤醒不能提前结束回退等待）
leader:    下一圈仍以 nextIndex≈401 起步 → 同上，errorCount 17→18 单调爬升
```

两 follower 的 matchIndex 永停在各自边界（−1→首次成功前不动），多数派
`updateCommit` 的中位数被自身 flush + 无 ack 拉不住 conf@553 —— **任期 conf 条目
永不提交 → `LeaderStateImpl.isReady()` 恒假 → 一切 RW 于 Leader 侧即时
`LeaderNotReadyException` 拒回**（写面症状与档案一致：200+ 秒不自愈）。

### 不自愈的原因（为什么没有任何逃生通道）

- **重选不会发生**：心跳通道健康（HEARTBEAT 的 INCONSISTENCY 仅回退复制面，
  `getLastRpcResponseTime` 仍刷新），`checkLeadership` 多数派租约成立——n2 以
  `reject PRE_VOTE from n3: this server is the leader and still has leadership` 持续
  拒绝 n3 的每轮竞选（n3 亦因此 CANDIDATE→REJECTED 循环）；
- **appender 无健康复位**：`LeaderStateImpl.checkHealth` 只做
  `notifyFollowerSlowness` + 指标，不重启/替换 appender；`LogAppenderDaemon` 仅在
  `EXCEPTION` 时 restart，活环非异常；
- **产品探针 NOOP 每 800ms 提交即被拒**（pending 队列堆积），无法自行推进提交。

### 双 jstack 佐证（10s 间隔对比）

- 停摆 leader 的 `->n1/n3-GrpcLogAppender-LogAppenderDaemon` 两线程 **RUNNABLE⇄parking
  交替、CPU 时间增长**（195→394ms / 171→360ms）：线程活着、在环里睡/发——排除死锁与
  线程熄灭假说；
- `LeaderStateImpl`/daemon 线程 elapsed≈7.3s < 进程 elapsed——确认为**同任期再任**的
  新 `LeaderStateImpl`（其 startupLogEntry 位于两 follower 边界之上，正是要提交而
  永不可提交的条目）。

### 三候选判定

| 候选 | 判定 | 依据 |
|---|---|---|
| ① `LeaderStateImpl` sender 激活 | **排除** | `changeToLeader()` 同方法内恒 `leader.start()`；jstack 两 daemon 在环中活跃 |
| ② `GrpcLogAppender` INCONSISTENCY 回退后推进 | **收口于此** | 上环：回退被 `request=null` keep-分支与并发 updateUnconditionally 抵消 + `sleepForErrors` 不可唤醒，净推进恒零 |
| ③ `startupLogEntry` 生命周期 | **非病灶，是被冻结的门** | MemoizedSupplier per-LeaderState 正确；ready@399 首次任期正常点亮；二次再任的门卡死是②的后果 |

### 对照包（r6 误触发，反向证据）

r6 两包（`evidence-r6-control-1/` `-2/`，已入库本目录）：同形 INCONSISTENCY/keep-nextIndex 风暴簇
（watcher 误判持续），但 `is ready since` 随后点亮、窗后 17.3s 收敛——**风暴是停摆的
前件而非充分条件**；停摆轮（r5）中 errorCount 爬升与 `request=null` keep 撤销形成
自持环，60s+ 无一次成功交付。此对照支持 2B 检测阈值按"零推进持续时间"而非
"存在回退"判定（D2 方向正确）。

### 喂给 D3 的复位判定

- **让位（transferLeadership）：不保证复位**——腐坏态在 leader 侧 per-LeaderState 的
  FollowerInfo 环，让位后目标新主的 conf 位点更高（≥当前 nextIndex），落后两节点
  边界依旧在其 conf 之下——同构死局可复现（目标节点的 appender 面对同一边界差）；
- **自杀式重启（进程级复位）：可复位**——stalled leader 重启后以纯 follower 身份从
  多数派追平（多数派此刻由对侧组成即可提交），换干净 LeaderState 重选；与档案运维
  缓解"先从不先主"同理（不让归群者参与其 conf 之下的复制）。
- **结论：D3 定稿取"让位一次失败→兜底重启"或直接重启**；2B 实现以 1.1 用例
  （进程级重放形态）复跑验收"全部自愈"。

## 第二形态：选举风暴（selfheal-r6 取证补充，看门狗 v2 设计输入）

看门狗上线后的重放复核（12 轮 24 测试，`/tmp/selfheal-round-*.log`）发现与形态 A
相邻的**形态 B**：末段错误贯穿至驱动尾（复核第 6 轮先主后从 82/318=25.79%、
末错 103902ms≈全程末），但该轮三节点日志
`LeaderNotReadyException` **计数为零**、角色链全部是 `FOLLOWER→CANDIDATE→(REJECTED)`
循环、无节点进入稳定 LEADER——**选举风暴**（档案互拒链的放大形态）：所有节点任期
递增互斥竞选，集群级提交冻结但没有任何"当值 Leader"可供判定/让位。r6 风暴自行
平息于 ~100s 量级（终态健康断言通过）。判读：

- 形态 B 与 A 同根（快照边界冲突使各次竞选产生的任期条目均不可提交），A 是
  "一个节点抱住 lease 不放"的定格，B 是"谁也抱不住"的 churn——同一病的两个
  时间切片；
- 看门狗 v1 的窗口键（任期）在 B 下不可见（无 Leader 样本），在 A 的
  "同任期再任/在任中任期跃迁"变体下也会重置逃逸——v2 已改为**连续在任段**
  键（跃迁不弃窗、弃任才弃段），A 系全形态捕获（e2e IT 实证：真实冻结 Leader
  15.78s 内检测+让位成功），B 仍不覆盖（无在任者）；
- **B 的持续时长无上界保证**：selfheal-r6 观测 ~100s 自然收敛；5.2 门禁 gate2
  复跑中再现 B 形态轮**贯穿 200s 驱动全程未恢复**（该轮分段判据 red 是门禁的
  正确指示，非误伤——无人值守下 B 属可用性残余风险）。B 的根治只能依赖上游
  （提报草稿已含两形态）；
- 无 supervisor 部署下 A 的自愈止于让位段——让位在 B 中同样不可用（无让位者）。
  5.1 运维指引与验收报告复核按此如实表述：⚠️ 维持，注记 A 已承载/B 残余。

## 附：Ratis 3.3.1 上游状态（任务 1.3 三态收口：**未发布**）

- Maven Central/release 事实：最新 `ratis-3.3.0`（tag `ratis-3.3.0` 2026-07-30，
  announce 2026-08-12）；3.3.1 仅 `[DISCUSS]` 线程与 master `3.3.1-…-SNAPSHOT`，无工件。
- tag 后 master 相关路径 diff 对账：`LeaderStateImpl`/`LeaderElection`/
  `GrpcLogAppender`/`LogAppender*` **零变更**；`RaftServerImpl`/`ServerState` 仅
  RATIS-2661（bootstrap conf 持久化——"全节点在初始 conf 持久化前重启→组卡
  STARTING"，与本签名"LEADER 态 startupLogEntry 永不应用"不同病）。RATIS-2500
  （InstallSnapshot 通知环）在快照安装路径，本环未触及安装流（leader 直接向边界
  追发，`shouldInstallSnapshot` 需 follower 回 REINSTALL，实测只见 INCONSISTENCY）。
- **决策（D4）**：不等待升级，组 2 走 **2B 看门狗**。组 3 经定夺**不提报**（2026-09-06，
  用户）：提报不改变实现效果与 B 形态实际风险等级；成稿英文正文与复现配方保留于
  `upstream-ratis-issue-draft.md`，取证包已入库本目录（`evidence-r5-jstack/`、
  `evidence-r6-control-{1,2}/`），后续时点即取即用；B 形态的根治跟踪改由我方
  升级评估以本文三条库内路径做 changelog 对账承载。
