# T1 验收证据（Phase 3 详设 §8 第 1/2/6 项，2026-09-06）

构建命令一律 `mvn -s /home/lam/repo/settings.xml …`；演练档 `-Pdrill`。

## 1. FairLock 顺序回归套件 CI 常开 ✅

- 三档套件：`openlatch-core` `FairOrderingSuiteTest`（8 例）、
  `openlatch-server` `FairOrderingSuiteE2ETest`（真实端口 2 例）、
  `openlatch-server/raft` `FairOrderingSuiteClusterTest`（真三节点 2×参数化）；
  `FAIR`/`REENTRANT` 参数化跑同一"授予序==排队序"矩阵（手工时钟，无 sleep）。
- 常开判据：三类均为 surefire 默认 include 的 `*Test`，无 `@Tag("drill")`；
  全量 `clean verify` 即执行（本轮 359 server 用例含以上）。
- 门控与别名：`FairLockGatingTest`（v1/v2 会话 `INVALID_REQUEST` 不断连、
  FAIR≡REENTRANT 重入互认）；starter `type=FAIR`（`OpenLatchAspectTest.fairTypeMapsToAcquireSpec`）。

## 2. Semaphore/CDL 语义测试全绿（含集群切换场景） ✅

| 层 | 用例 | 结果 |
|---|---|---|
| core Semaphore | `CoreEngineSemaphoreTest` 10 例（计数/队首防饥饿/重入/到期整体归还/超额释放/断连/定型/不匹配矩阵/深度限额） | 全绿 |
| core Latch | `CoreEngineLatchTest` 9 例（total 双通道定型/纯初始化/归零广播/一次性/无租约零触及/断连摘除/不匹配/幂等去重/通知超时清扫） | 全绿 |
| 协议单机 | `SemaphoreGatingTest` 3 例、`LatchGatingTest` 3 例（门控、非法参数、双会话线路往返含 AWAIT_NOTIFY） | 全绿 |
| 客户端 | `ClientSemaphoreIT` 4 例（含裸 socket 断连归还）、`ClientLatchIT` 5 例 | 全绿 |
| 集群矩阵 | `ClusterSemaphoreLatchTest` 3 例（许可感知队首推进/池回收纯加入显式拒+带断言重建、Latch 复制计数/本地等待/归零广播、failover 存续+副本摘要一致） | 全绿 |
| 确定性 | `StateMachineDeterminismTest` 新增 2 例（Semaphore 池镜像、Latch 计数与拒绝码形）+ 原 116 例 | 全绿 |
| 快照 | `SnapshotFamilyRoundTripTest` 2 例（新条目恢复往返、锁序列化字节零扰动） | 全绿 |
| 演练(-Pdrill) | `LeaderKillDrillIT.killLeaderPreservesExtendedPrimitiveState`（9.6s）、`RollingRestartDrillIT.extendedPrimitivesSurviveRollingRestart`（14.7s）、`ClientSemaphoreClusterIT` 2 例 | 全绿 |
| 回归底线 | 全反应堆 `clean verify`：protocol 17 / core 72 / server 359 / client 63 / client-IT 29(1 既存条件跳过) / starter 22 / examples 9，BUILD SUCCESS；互斥锁既有测试一字未改 | 全绿 |

## 6. 新功能默认不改变既有行为（v1 客户端兼容性） ✅

- 协议：v3 仅新增枚举值/字段/消息（golden 契约文件同步，`OpenlatchProtoContractFreezeTest`
  /`RaftProtoContractFreezeTest` 钉既有编号）；`HandshakeTest` 新增 `v3_hello_accepted`，
  v1/v2 回归用例原样通过（未知版本界移至 4）。
- v3-only 门控：`FAIR/SEMAPHORE` 类型与 `LATCH_*` 消息对握版本 <3 会话消息级
  `INVALID_REQUEST`、不断连（单机分发器与集群受理层同规则，先于日志提案）。
- 客户端出站信封升至 v3、许可字段锁路径携带缺省值（permits=1/total=0），
  服务端归一与锁路径不读该字段——锁行为逐字段不变。
- 附带修复：集群重入预检（Phase 2 遗留：`isHeld` 一刀切 busy 阻断重入提案）
  与 `WaitQueue` 生产清扫调用边——均为既有语义的补正，非新功能开关。

## 遗留记录（转 §9 流程评估）

- 集群 Semaphore 池回收后纯加入者显式拒（design D4 边界，spec 已钉两分支）。
- Latch 条目存续至节点重启（design D5 修订；泄漏面=每定型 key 一条小条目）。
- `REJECT_SEMAPHORE_TOTAL/OVER_RELEASE` 集群回执码形经 `INVALID_REQUEST` 统一
  （P3-04/07 接正，`mapAcquire/mapRelease` 同步）。
