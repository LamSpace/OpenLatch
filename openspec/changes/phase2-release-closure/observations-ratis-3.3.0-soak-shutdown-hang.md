# Ratis 3.3.0 观察档案：soak 暴露的优雅关停无限挂起（cached proxy 池零 worker）

日期：2026-09-05。来源：`phase2-release-closure` 任务 3.1 字面 10 分钟混沌 soak 首跑。
证据：`evidence/soak-hang-jstack-t+27min.txt`、`evidence/soak-hang-jstack-t+28min.txt`（间隔 60s 双 dump，栈位零移动、SMU 线程 CPU 持续增长）。

## 现象

`ClientChaosIT` soak 档（`-Dopenlatch.chaos.soak-minutes=10`）运行至负载窗口第 ~7 分钟，测试主线程卡死：

```
"main" WAITING (parking)
  at ThreadPoolExecutor.awaitTermination
  at ConcurrentUtils.shutdownAndWait(ConcurrentUtils.java:144)          ← 无超时入参的公开重载
  at ConcurrentUtils.shutdownAndWait(ConcurrentUtils.java:136)
  at RaftServerProxy.lambda$close$9(RaftServerProxy.java:448)           ← 关停序列最后一步
  at RaftServerProxy.close(RaftServerProxy.java:424)
  at io.github.lamspace.openlatch.server.raft.RaftSubsystem.close(RaftSubsystem.java:205)
  at io.github.lamspace.openlatch.server.OpenLatchServer.stop(OpenLatchServer.java:277)
  at ClientChaosIT.stopNode → runChaos(ClientChaosIT.java:229)          ← 混沌循环内的随机杀
```

同 dump 中挂起对象 n3 的关键线程状态：

- `n3@group-...-StateMachineUpdater`：仍在 `waitForCommit` 的 100ms 定时循环里空转（CPU 时间持续增长），说明其**从未收到停止信号**（`stopIndex` 未设）；
- **不存在任何 `n3-impl-*` 工作线程**（proxy 池前缀，`RaftServerProxy` 构造处以 `id + "-impl"` 命名）；
- `n3@group-...-SegmentedRaftLogWorker` 为非守护线程，随之永驻。

## 机制链（源码引用为 ratis-server/ratis-common/ratis-server-api 3.3.0，本地 sources jar 核对）

1. `RaftServerProxy.close()` 第 434 行调 `impls.close()`；该方法是 **fire-and-forget**：`ConcurrentUtils.parallelForEachAsync(map.entrySet(), entry -> close(...), executor.get())`——返回的 future 无人 join（proxy 内部类 `close()`，源 122–131 行；`parallelForEachAsync` 实现即 `executor.execute(...)` 逐条派发，源 176–191 行）。
2. proxy `executor` 池由 `ConcurrentUtils.newThreadPoolWithMax(proxyCached, proxySize, id+"-impl")` 创建；`RaftServerConfigKeys.ThreadPool` 默认 `PROXY_CACHED_DEFAULT=true`、`PROXY_SIZE_DEFAULT=0` → `Executors.newCachedThreadPool` 语义：core=0、worker 空闲 **60s** 全部回收。
3. 池内唯一确定的任务集是启动期 `parallelForEachAsync(getImpls(), RaftServerImpl::start, executor).join()`（源 413 行）。此后该池进入长期空闲——**启动 60s 后 worker 数归零**（除非再有组管理事件）。
4. 于是"节点启动 ≥60s 后执行优雅关停"的派发落入零 worker 的池：关停任务入队后无（或极窄窗口内失去）worker 执行——`RaftServerImpl.close`/`ServerState.close`/`StateMachineUpdater.stopAndJoin` 整条链从未被触发（与 SMU 空转、无 `n3-impl` 线程吻合）。
5. `close()` 的最后一步 `ConcurrentUtils.shutdownAndWait(executor.get())` 采用**无超时入参的公开重载**：语义为 `shutdown()` + `awaitTermination(TimeDuration.ONE_DAY)`（`ConcurrentUtils.java:133–144`）→ 主线程挂 24 小时，测试语境等效永久挂起；进程不退出。

（JDK `ThreadPoolExecutor` 在 offer/worker 生灭/重查之间的确切微观竞态分支未再深究——修复从构造上消除"零 worker 时刻派发"前件，不依赖对微观分支的归因。）

## 触发条件算式（为何 S1–S4 全部既有用例从未命中）

命中条件 = **某节点的优雅 stop 发生在其 proxy 池最后一任务（启动派发）后 >60s**。

| 场景 | 启动→关停间隔 | 判定 |
|---|---|---|
| S2–S4 集成测试（MiniRaftCluster/ClusterSnapshot/…） | 数秒 | 不中 |
| 常规混沌 ~18s 窗口 | ≤18s+杀点间隔 | 不中 |
| `soak-minutes=1` 冒烟 | 窗口 60s，杀点在 60s 内 | 擦边不中（实测绿） |
| 滚动重启演练 | 节点重启间隔内启动→再停 ≤数十秒 | 不中 |
| **字面 10 分钟 soak** | 首分钟后的任一杀点 ⇒ **必中** | 中（本次） |
| **生产：空闲集群的 SIGTERM 优雅停机** | 常态 >60s | **必中——发布级 P0** |

数据面/故障转移面不受影响：SIGKILL、杀主演练、复制与授予路径均不经此关停链；但"滚动重启不中断"（§11-5）与一切运维优雅停机会永久挂死——S4 滚动演练未触发只因窗口恰好短于 60s。

## 修复（D7，产品侧装配钉死）

`RaftSubsystem.start()` 将 proxy/server/client 三组池钉为非缓存固定池（size=4，契约与尺寸论证入方法 Javadoc）：固定池 core 线程常驻不回收，"零 worker 时刻派发"前件在构造上消失；同时 `RaftServerImpl.close` 内同构的 `shutdownAndWait(clientExecutor/serverExecutor)`（同样 cached 默认）一并消除。成本为每节点 ≤12 常驻空闲线程。

回归：`IdleNodeGracefulStopIT`（`@Tag("drill")`）——三节点建群→静默 65s→逐节点 stop 断言有界（<15s）。修复前语义由本档案的 soak 现场实证（无需重放红跑）；修复后实测 `Tests run: 1, Failures: 0`，elapsed 69.81s（其中静默 65s），三次 stop 均秒级。

规格化：`cluster-node-lifecycle`"Raft 子系统生命周期绑定"增补有界关停条款与"长空闲后优雅关停有界"场景（本 change delta spec）。

## 验证状态

- [x] 修复编译 + `IdleNodeGracefulStopIT` 绿（2026-09-05 22:2x）
- [ ] 10 分钟 soak 复跑绿（进行中，任务 3.1）——兼作"字面达标"发布证据与全关停链复核
- 上游检索无同签名 JIRA 定案（RATIS-2245/RATIS-502 近邻）；3.3.1 讨论中（lists.apache.org "DISCUSS Ratis 3.3.1"）若发布可复核该链路是否上游根治，届时可评估解除钉死。
