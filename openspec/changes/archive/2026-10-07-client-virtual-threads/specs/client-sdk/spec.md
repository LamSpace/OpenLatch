# Spec Delta

## ADDED Requirements

### Requirement: 虚拟线程亲和

客户端 SDK 的阻塞调用面（`OLock`/`OSemaphore`/`OCountDownLatch`/`OBarrier`/`OCondition`/`OPhaser`/`OTimer`/`OBlockingQueue`/`ODelayQueue`/`OAtomic*` 族的全部同步方法与 JUC 风格同步包装）SHALL 可被调用方在虚拟线程上安全且高效地执行：桥接形态（future 限时等待 + 客户端本地计时轮询 + 非阻塞写出）MUST NOT 存在钉扎虚拟线程载体线程的阻塞源，等待期间的调用方虚拟线程 MUST NOT 占用平台载体线程。运行时基线 JDK ≥24（本项目为 25）已消除 "monitored 区内阻塞钉扎"，客户端 MUST NOT 在调用方线程路径引入新的 native 帧阻塞源。

线程归属键（`Thread.currentThread().threadId()` 承载的重入计数、`isHeldByCurrentThread`、许可持有计数、条件 signal 持有归属查询）在平台线程与虚拟线程两种调用方形态下 MUST 行为一致；虚拟线程 id 进程内全局唯一且终止后不复用，本地持锁登记 MUST NOT 因调用方线程形态产生归属漂移。中断语义 MUST 与形态无关：阻塞中的调用方被中断时，既有"恢复中断标志/抛 `InterruptedException`/离场摘除服务端等待项"裁决逐项原样成立。

`OTopic` 每订阅的交付派发线程 SHALL 为虚拟线程：单订阅内串行回调、回调不占用 Netty EventLoop、回调异常吞并记日志的既有承诺 MUST 逐项不变；同进程并发订阅数增长时，平台线程计数 MUST NOT 随订阅数线性增长（维持客户端内部基建的常数形态）。

形态保持承诺（显式非目标锚定）：Netty EventLoop 线程、驱动全部超时面（看门狗续租节奏、请求超时、保活、重发）的共享定时器线程、锁丢失回调专用单线程执行器 SHALL 保持平台线程形态与既有数量承诺；本要求 MUST NOT 被解读为改动看门狗线程模型（现形态无随持锁/等待者规模增长的线程消耗）或改为失锁回调的串行保证。

#### Scenario: 万级虚拟线程等待者闭环通过

- **WHEN** 一万个虚拟线程（同规模平台线程形态不可启动）跨多 key 并发阻塞在 `lock()`/`acquire()`/`put()` 类等待上
- **THEN** 全部等待者经受"等待-通知-重发"闭环在等待总超时预算内正常了结，客户端进程内存平稳、无泄漏；等待者数量不产生续租流量（等待不持租约，既有"await 无看门狗流量"承诺在虚拟线程形态下依旧成立）

#### Scenario: 钉扎事件零断言

- **WHEN** 在 JFR `jdk.VirtualThreadPinned` 事件观测窗内，以虚拟线程执行锁/信号量/屏障/条件的阻塞等待、队列 `put`/`take` 的轮询路径与原子族 CAS 循环
- **THEN** 观测窗内钉扎事件计数为 0（该断言常驻为回归绊线，未来任何改动重新引入钉扎即测试失败）

#### Scenario: 虚拟线程归属矩阵

- **WHEN** 虚拟线程对可重入锁连续获取两次后调用两次 `unlock()`；另一虚拟线程查询 `isHeldByCurrentThread()`；非持锁线程调用 `unlock()`
- **THEN** 重入计数、归属查询与 `IllegalMonitorStateException` 拒绝行为与平台线程形态逐项一致，服务端仅见计数归零后的恰一次释放生效

#### Scenario: 虚拟线程中断与平台线程判例一致

- **WHEN** 虚拟线程调用方阻塞在 `await()`/`take()`/`acquire()` 时被中断
- **THEN** 抛 `InterruptedException`（或既有的 boolean/标志恢复裁决形态）、服务端等待项经离场路径摘除、后续操作不受损——与平台线程中断判例逐项等价

#### Scenario: OTopic 扇出下平台线程数恒定

- **WHEN** 同一客户端进程并发建立 500 个订阅并持续收到消息交付
- **THEN** 每订阅回调仍串行执行、交付内容与顺序承诺不变；进程平台线程计数相对订阅建立前基线保持恒定区间（派发线程虚拟形态化，不随订阅数线性增长）

#### Scenario: 基建线程形态保持

- **WHEN** 检视构建后客户端的线程清单与关停行为
- **THEN** EventLoop 线程、共享定时器线程、锁丢失回调执行器线程均为平台线程且数量维持既有承诺；仅 OTopic 派发线程呈虚拟线程形态；`shutdown()` 对全部内部线程的收敛行为不变
