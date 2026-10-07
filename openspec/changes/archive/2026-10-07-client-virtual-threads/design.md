# Design

## Context

动机与可行性事实链见 proposal.md - Why（JDK 25 基线 + JEP 491 消灭 monitored 区钉扎 + 调用线程路径零 JNI）。此处只沉淀影响设计的现状约束：

- 客户端线程拓扑（src/main 实测）：`openlatch-client-io`（Netty EventLoop，默认 1）、`openlatch-client-timer`（共享 `HashedWheelTimer`，驱动看门狗续租/请求超时/保活/重发全部超时面，1 条）、`openlatch-client-lock-lost`（失锁回调专用单线程执行器，1 条）、`openlatch-topic-delivery-*`（**每订阅 1 条平台线程**，`RemoteTopic.Subscription` 构造时创建，`deliveryLoop` 以 `queue.poll(200ms)` 挂起等消息）。前两者无随实体规模增长的形态；唯一热点是 topic 派发线程。
- 看门狗"线程模型评估"项的实情：`Watchdog.tick` 纯异步链（`mux.send().whenComplete()`），零按锁线程——评估结论即"现形态已最优，保持不动"，本设计将其钉为显式决定。
- 调用方桥接形态：`CompletableFuture.get(timeout)` 限时等待（锁/信号量/屏障等）+ `Thread.sleep(≤SESSION_POLL_MS)` 会话轮询（条件/timer/原子/队列等）+ `writeAndFlush` 非阻塞写出；全部对虚拟线程原生友好。
- 服务端既有治理限额：`max-queue-depth-per-key`、`max-subscribers-per-key`、`max-subscription-buffer` 等——大规模等待者/扇出测试必须跨 key 摊开以尊重默认限额，不以此为由改服务端配置。
- 双语指南既有线程契约句（失锁回调"专用单线程、勿阻塞"、topic"单订阅串行回调在 SDK dispatcher 线程"、异步链"回调在网络线程勿阻塞"）为文档改动的锚点。

## Goals / Non-Goals

**Goals**：把已成立的虚拟线程亲和钉为 client-sdk 规格条款并常驻测试绊线；消灭 SDK 内唯一按实体 1:1 的平台线程消耗（OTopic 派发）；交付可度量的规模证据（基准 + 万级等待者 IT）。

**Non-Goals**（除 proposal 所列外的设计级边界）：
- 不把同步桥接改造成全异步形态（撕公开 API 面，与本项收益不成比）。
- 不动 `deliveryLoop` 的 `poll(200ms)` 挂起形态（换 `take()+interrupt` 属关停时序面改造，超范围）。
- 不新增对外指标（虚拟线程观测走 JVM 基建：JFR/ThreadMXBean，不自造 Gauge）。
- 不触 wire/proto/服务端/core/console/starter 代码；同 key 同类型约定登记不触发（零新原语）。

## Decisions

**D1 唯一代码改动点：`RemoteTopic.Subscription` 派发线程改虚拟线程。**
以 `Thread.ofVirtual().name("openlatch-topic-delivery-", seq)`（AtomicLong 顺序号，替掉现 `ThreadLocalRandom` 随机后缀——虚拟线程名仅服务 thread dump 可读性，顺序号更利诊断）创建；守护属性由虚拟线程语义天然满足（不随 JVM 退出阻塞），原 `setDaemon(true)` 行随形态删除。**替代方案**：共享单派发器 + 每订阅串行队列——否决，因每订阅独立线程天然实现"慢 handler 不拖累他订阅"的隔离性，共享派发器会让一个阻塞 handler 卡住全体交付，属语义倒退；且改动面远超本 change 预算。

**D2 三类基建线程保持平台形态（Q2 用户裁决）。**
EventLoop（Netty 纪律）、HWT（单条、定时精度、全 SDK 超时面共用）、lockLostExecutor（"专用单线程"序列化契约已入 client-sdk 规格与双语指南，1 条 parked 平台线程成本≈0，改虚拟只赔契约稳定）。ROADMAP"看门狗线程模型评估"即以此为结论文本。

**D3 钉扎验证形态：测试内程序化 JFR，不开 CI 全局开关。**
`ClientVirtualThreadsIT` 以 `jdk.jfr.Recording` 启用 `jdk.VirtualThreadPinned` 事件覆盖工作负载窗口，断言事件数 0——自包含、本地/CI 同判、不依赖 stderr 抓取。JDK 25 上真实钉扎源仅剩 native 帧（critical JNI），本项目调用路径不存在，理论上零事件成立；夹具价值在"未来改动重新引入钉扎即红"。**替代**：failsafe JVM 参数 `-Djdk.tracePinnedThreads=full` + CI 日志 grep 门——否决（脆弱面大、噪声污染全部 IT 日志）。apply 时若沙箱 JFR 受限，退路为 fork JVM 抓 stderr 断言（记入 tasks 的验证步骤，不改变任务结构）。

**D4 万级等待者 IT 进默认 verify 矩阵（Q3 裁决，荐案即定案）。**
判由：全程进程内（`ClientTestServers` 同款 in-process 服务端），虚拟线程 park 内存开销十 MB 级、无环境门槛依赖（区别于 `ClientChaosIT` 的跳过门槛）；钉扎/扇出绊线要常驻每次 verify 才有防腐价值。形态：1 万虚拟线程 × 每者一次 `lock()/unlock()` 争用，摊 100 key × 100 等待者（尊重默认每 key 等待深度限额，apply 时先实测服务端默认值定摊开参数，不放宽服务端配置）；单周期争用总时长预算 <90s。若实测 CI 压力不可接受，降规模至 5k 仍保"同规模平台线程不可启动"的存在性证明（该退让须记入归档过程记录）。

**D5 OTopic 扇出绊线断言平台线程数恒定。**
500 订阅摊 500 个独立 key（每 key 恰一订阅；apply 期实测更正——服务端 `TopicRegistry` 按 `(session, key)` 唯一登记幂等覆盖，同会话同 key 多订阅只存续一路由，原"10 key × 50 订阅"前提不成立；跨会话扇出需多客户端，与"单客户端进程资源形态"的测量目的不符），`ThreadMXBean` 计数在订阅建立前取基线、交付稳定后断言增量落在常数区间（JDK 21+ `getThreadCount()` 只计平台线程，虚拟线程不入账）。**红先钉住**：改动前该断言在 500 平台线程增量下必红（实测红值恰 +500，见 evidence/red-tripwire-fanout.md），改后绿——与影子修复的全值矩阵绊线同哲学（复发型夹具而非一次性验证）。

**D6 基准对比（Q4 前置的度量面）。**
`BenchmarkMain` 以 `-pl openlatch-examples`（不带 `-am`，判例：exec:java 落聚合 pom 报 ClassNotFound）改动前后同参各跑一轮，记录线程数/RSS/p99；改动前后对比留档进 change 证据。

**D7 `-Pdrill` 跑一轮（Q4 用户裁决：跑）。**
单轮次复跑纪律（防压测伪证判例）；演练日志入 drill-logs 留证。预期全绿——本改动纯客户端本地线程资源面，无 wire/服务端时序面变化；若演练出现客户端侧异常，视为派发线程形态化的真实回归信号处理。

**D8 文档同步面（改动清单）。**
- `docs/guide/{zh,en}/03-client-sdk.md`：新增"虚拟线程"小节（调用方支持声明、JDK≥24 无 monitored 钉扎一句、`jdk.VirtualThreadPinned`/`tracePinnedThreads` 诊断推荐、失锁回调与 EventLoop 线程形态不变）；OTopic"监听器线程模型"条目补"派发线程为虚拟线程、串行承诺不变"半句。
- `docs/guide/{zh,en}/10-compatibility.md`：兼容表补"调用方线程形态：平台/虚拟线程均受支持（SDK 自 v1.0.x 起契约化）"一行；运行时要求不变（仍 JDK 25）。
- Javadoc：`OpenLatchClient` 类级线程模型概括段、`RemoteTopic.Subscription` 构造器与 `deliveryLoop`、`OTopic`/`OTopicSubscription` 接口线程模型句——按 CLAUDE.md 注释纪律同步，措辞只读自洽（对外文本禁内部引用，check-source-citations.sh 自查）。
- `ROADMAP.md` 工程改进第二行：立项即 `进行中`+change 链接，归档时 `已落地`+归档链接（维护规约既有纪律）。

**D9 协议与发布面零触碰。**
零新 wire 类型 → 不升 v12 门（ROADMAP 版本门纪律不触发）；client artifact 运行时要求不变，Maven Central 发布链无感知。

## Risks / Trade-offs

- [万级等待者 IT 拖慢 CI] → 单周期最小工作量 + 跨 key 摊开 + <90s 预算；实测超标按 D4 退让条款降规模并留痕。
- [沙箱 JVM 禁 JFR] → D3 退路（fork JVM stderr 断言）；两路均不可行时降级为 `tracePinnedThreads` 参数跑单轮留证 + design 记录。
- [ThreadMXBean 断言受 JUnit 环境线程噪声干扰] → 基线差值 + 常数区间容忍，独立 client 实例，不与其他 IT 并缸。
- [慢 handler 在虚拟线程上的阻塞从"占用一条平台线程"变为"unmount 等待"] → 对应用无契约变化（指南"耗时处理请转交业务线程池"措辞保留）；隔离性由"每订阅一线程"形态保有。
- [close/关停路径原依赖平台线程 interrupt 语义] → 虚拟线程对 interrupt 的 park 解除裁决同构，既有 `ClientTopicIT`/`RemoteTopicScriptedTest` 全量回归兜底。

## Migration Plan

客户端库内行为变更随版本发布，无部署次序约束（服务端零变化）；回滚 = 还原 `RemoteTopic` 一处改动与文档行，无数据/协议迁移。

## Open Questions

- 每 key 等待深度与每 key 订阅数默认限额的具体数值（apply 时以 `ClientTestServers` 实测确定摊开参数）——不影响规格、方案与任务拆分，可安全后置。
