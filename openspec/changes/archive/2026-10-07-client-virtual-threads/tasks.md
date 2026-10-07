# Tasks

## 1. 基线与红先钉住

- [x] 1.1 以 `ClientTestServers` 实测服务端默认 `max-queue-depth-per-key`/`max-subscribers-per-key` 数值，据此定万级等待者（100 key × 100 等待者量级）与 500 订阅扇出（≥10 key 摊开）的摊开参数，写为测试常量并注出处 — 验证：实测值与参数留档一致，规模不触任何默认限额
- [x] 1.2 新建 `ClientVirtualThreadsIT` 骨架，实现 OTopic 扇出平台线程数绊线（500 订阅建立并稳定交付后，`ThreadMXBean` 平台线程计数相对基线增量落在常数区间），在未改动代码上运行确认**精确红于线程计数增量**（交付正确性断言绿）— 验证：红因钉住为 +500 量级平台线程增长而非其他断言，红先证据留档
- [x] 1.3 于当前 HEAD 采集基准基线：`mvn -s /home/lam/repo/settings.xml exec:java -pl openlatch-examples`（不带 `-am`，判例：加 `-am` 时 exec:java 落聚合 pom ClassNotFound）跑 `BenchmarkMain`，记录线程数/RSS/p99 — 验证：基线数据表落入 change 证据目录

## 2. 核心改动：OTopic 派发线程虚拟线程化

- [x] 2.1 `RemoteTopic.Subscription` 派发线程改 `Thread.ofVirtual().name("openlatch-topic-delivery-", seq)`（AtomicLong 顺序号替代 ThreadLocalRandom 随机后缀），删除 `setDaemon(true)` 行，`deliveryLoop` 的 `poll(200ms)` 形态不动；同步更新构造器/`deliveryLoop`/`Subscription` 类 Javadoc（线程形态、串行承诺不变、dump 可读性说明）— 验证：任务 1.2 绊线转绿，`ClientTopicIT`/`RemoteTopicScriptedTest` 全绿（关停/重挂/串行回归锚点）
- [x] 2.2 `OTopic`/`OTopicSubscription` 接口 Javadoc 线程模型句与 `OpenLatchClient` 类级线程模型概括段补虚拟线程亲和描述（对外措辞只读自洽，禁内部引用）— 验证：`mvn -s /home/lam/repo/settings.xml -pl openlatch-client javadoc:javadoc`（或 verify 的 javadoc 门）通过，`bash scripts/check-source-citations.sh` 零命中

## 3. 虚拟线程亲和测试面

- [x] 3.1 新增万级虚拟线程等待者规模见证用例（按 1.1 参数跨 key 并发 `lock()/unlock()` 单周期，等待总超时预算内全部了结、内存平稳；注释声明"同规模平台线程形态不可启动"的存在性论证；进默认 verify 矩阵，不设环境门槛）— 验证：用例绿且耗时 <90s；若实测 CI 压力不可接受按 design D4 退让条款降规模并在过程记录留痕
- [x] 3.2 新增 JFR `jdk.VirtualThreadPinned` 零事件断言用例（`jdk.jfr.Recording` 观测窗内以虚拟线程执行锁/信号量/屏障/条件阻塞等待、队列 `put`/`take` 轮询路径、原子族 CAS 循环）— 验证：钉扎事件数为 0，且观测窗有效性有证（同窗可读 `jdk.VirtualThreadStart` 等虚拟线程事件，证明 recording 在收）；若沙箱禁 JFR 按 design D3 退路（fork JVM `-Djdk.tracePinnedThreads=full` stderr 断言）并在过程记录说明
- [x] 3.3 新增归属矩阵与中断语义用例（虚拟线程：重入两次后两次 unlock、他线程 `isHeldByCurrentThread`、非持锁线程 unlock 抛 `IllegalMonitorStateException`、阻塞中 `await()`/`take()`/`acquire()` 被中断的既有裁决形态逐项复刻平台线程判例）— 验证：与对应平台线程既有用例断言逐条同构全绿

## 4. 文档同步

- [x] 4.1 `docs/guide/{zh,en}/03-client-sdk.md` 新增"虚拟线程"小节（调用方支持声明、JDK≥24 无 monitored 区钉扎原理一句、`jdk.VirtualThreadPinned`/`tracePinnedThreads` 诊断推荐、EventLoop/共享定时器/失锁回调线程形态不变声明）并在 OTopic"监听器线程模型"条目补"派发线程为虚拟线程、单订阅串行不变"半句 — 验证：zh/en 对称无缺页、`bash scripts/check-links.sh` 零死链
- [x] 4.2 `docs/guide/{zh,en}/10-compatibility.md` 兼容表补"调用方线程形态：平台/虚拟线程均受支持"行，运行时要求表述保持 JDK 25 不变 — 验证：zh/en 对称、check-links 零死链
- [x] 4.3 `ROADMAP.md` 工程改进第二行状态 `未启动` → `进行中` 附本 change 链接（归档时再转 `已落地`+归档链接并 sync 主规格，非本 apply 范围）— 验证：表格行列数与表头一致（awk 管数自检，防渲染回归）、维护规约条款符合

## 5. 集成验证

- [x] 5.1 全量回归：`mvn -s /home/lam/repo/settings.xml clean verify` — 验证：8/8 模块 BUILD SUCCESS、0 失败 0 错误（唯一允许跳过为 ClientChaosIT 既有环境门槛）、新增三组用例进矩阵且绿
- [x] 5.2 改动后基准对比：`BenchmarkMain` 同参复跑（`-pl openlatch-examples` 不带 `-am`），与 1.3 基线并表 — 验证：p99 无劣化；扇出/线程面指标不升（订阅场景平台线程数下降为预期收益）
- [x] 5.3 `-Pdrill` 演练一轮（用户裁决"跑"）：`mvn -s /home/lam/repo/settings.xml verify -Pdrill`，单轮次复跑纪律防压测伪证 — 验证：演练全绿、drill-logs 留证；任何客户端侧异常按 2.1 派发线程形态化的真实回归信号回查
- [x] 5.4 `openspec validate client-virtual-threads --strict` 通过 — 验证：校验输出 valid（主规格 sync 与双查归归档时点执行）
