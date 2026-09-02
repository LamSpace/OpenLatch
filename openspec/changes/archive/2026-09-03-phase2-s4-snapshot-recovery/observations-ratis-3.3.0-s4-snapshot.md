# Ratis 3.3.0 快照/安装流观察（S4/P2-16 spike 产出，design D5）

沿 S2 `observations-ratis-3.3.0.md` 先例。spike 以 `ClusterSnapshotRecoveryTest#severelyLaggingFollowerInstallsSnapshotFromLeader`
固化（3 节点、threshold=40、`logSegmentBytes=4096`、杀 Follower→推 400+ 条目→重启→安装→追平），
全绿于本机，用时 ~16s。

## 1. 3.3.0 的 API 面与常见教程（3.1/3.2 时代）不同

- **不存在** `SnapshotStorage`/`SnapshotOutputStream`/`StreamStateMachine`/`StateMachine.loadSnapshot(SnapshotInfo, InputStream)`。
- 快照持久化载体是 `StateMachine.getStateMachineStorage()`（覆写返回 `SimpleStateMachineStorage` 实例）：
  - `init(raftStorage)` 绑定 `stateMachineDir`（`<storageDir>/<group>/<peer>/sm`）；
  - `getSnapshotFile(term, index)` 给库命名（`snapshot.T_I`），`updateLatestSnapshot(SingleFileSnapshotInfo)` 维护引用；
  - **文件由状态机自己写**（takeSnapshot 侧），库只负责命名约定、清理与读取下发。
- `getLatestSnapshot()` 基类实现即 `getStateMachineStorage().getLatestSnapshot()`，覆写 storage 后无需单独覆写。

## 2. 安装流（落后/新节点）由库完成，SM 只提供两个钩子

`SnapshotInstallationHandler`（默认 `install.snapshot.enabled=true`，Leader 直发 chunk）：

```
Leader 端：LogAppender 发现 followerNext != snapshotIndex+1
  → InstallSnapshotRequests 读 SM getLatestSnapshot() 文件分块下发
Follower 端：收 chunk → SnapshotManager 写 SM storage 的 tmp 目录
  → done 块：stateMachine.pause()            ← SM 必须自行迁移生命周期到 PAUSED
  → snapshotManager.finalizeSnapshot()        ← 库把 tmp 目录 rename 入 sm 目录（含 MD5 伴随）
  → StateMachineUpdater.reload()：
      断言 getLifeCycleState()==PAUSED → stateMachine.reinitialize()
      → 读 SM getLatestSnapshot() 位点，appliedIndex 无条件钉到快照位点 → RUNNING
```

要点：
- `BaseStateMachine.pause()/reinitialize()` 是**空实现**；必须自行做
  `getLifeCycle().transition(PAUSING); transition(PAUSED)`（pause）与
  `STARTING→RUNNING`（reinitialize 尾部）。`initialize` 需以
  `getLifeCycle().startAndTransition(...)` 包裹（S2/S3 未迁移生命周期、停在 NEW，
  无人断言故无碍；S4 的 reload 断言使其成为硬要求）。
- `finalizeSnapshot` **不更新** SM storage 的 latest 引用——reinitialize 里必须
  `storage.loadLatestSnapshot()` 盘上重扫，`getLatestSnapshot()` 会读到旧引用。
- 快照装载没有独立回调：安装的内容加载就发生在 `reinitialize()` 自己身上
  （本项目经 `loadSnapshot(SingleFileSnapshotInfo)` 与启动路径共用）。

## 3. 快照文件的 MD5 伴随是硬要求

`SimpleStateMachineStorage.findLatestSnapshot/cleanup` 均以 `snapshot.T_I.md5`
判定有效与保留数；`MD5FileUtil.computeAndSaveMd5ForFile` 必须在 takeSnapshot 写盘后
执行（安装端 `checkAndInstallSnapshot` 亦按 MD5 校验，不符 rename 为 `.corrupt` 重试）。

## 4. 日志截断默认被任一落后节点卡死

`StateMachineUpdater` 的 `purgeIndex = min(快照位点, 全体 peer 提交位)`
（`purge.upto.snapshot.index` 默认 false）。单节点停机期间 Leader 的截断不推进→
永不产生"库无历史可发、必须装快照"的位点差。装配层取
`setPurgeUptoSnapshotIndex(true)`：快照位点恒为已应用⊆已提交，对多数派安全；
代价（落后过多节点只能走安装流）恰是 §7.3-2 的预期路径。

## 5. 测试驱动的位点差构造

默认 segment 4MB 下，数百条小条目挤在单一未关闭 segment、截断不动。
新增 `openlatch.cluster.log-segment-bytes`（Raft 库语义透传，同
`election-timeout-ms` 口径；0=库默认）令用例可以 4KB segment 制造滚动+截断。

## 6. 与 S2 观察的衔接

- S2 记录"同 ClientId 在途串行"（池化缓解）——本轮无新增客户端面。
- S2 记录 RECOVER 启动必选——本轮补：RECOVER 下 `initialize` 会收到已有
  storage 目录，`loadLatestSnapshot` 与 Ratis 段回放（从快照位点起）由库自动衔接，
  无需状态机干预截断/回放边界。
