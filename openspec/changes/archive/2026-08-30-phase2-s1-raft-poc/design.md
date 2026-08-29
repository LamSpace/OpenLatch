## Context

Phase 1 已发布：六模块主干（protocol/core/server/client/starter/examples），Java 25、protobuf 3.25.5、Netty 4.1.137；`CoreEngine` 为纯 Java 构造注入模块（`CoreEngine(CoreConfig, Clock, CoreEventListener)`），单机基线见 `docs/benchmark-baseline-2026-08-29.md`。详设 §2 给出评估框架（§2.2 权重维度）与判定门槛（§2.4），§13.1 拆出 P2-01～P2-04，且声明 PoC 产出允许一次性。动机与范围见 proposal.md。

## Goals / Non-Goals

**Goals:**
- 两候选在同一测量口径下产出 §2.4 四门槛的可复现数据；
- 以最小胶水代码探明各库的集成摩擦面（状态机接入、快照自定义、传输共存），供 S2 实现规避；
- 选型报告 + 详设 v1.1 回写，S1 退出。

**Non-Goals（设计级边界）：**
- 不测 Phase 1 网络接入层（Netty pipeline / ForwardingProxy / 协议 v2）——延迟只考复制开销本身；
- 不测 QUEUED/AWAIT_NOTIFY（§4.5 明确排队不写日志，与选库无关）、成员变更联合共识细节（两库 API 存在性做桌面调研记录即可，§7.4 属 S4）；
- 不做跨机部署、性能调优竞赛——门槛是二元的，过线即止。

## Decisions

### D1：进程级节点拓扑 + 最小行协议（而非线程内嵌集群）
杀 Leader 必须是 `kill -9` 进程，故节点 = 独立 JVM。driver 走 TCP 最小行协议（`ACQ`/`REL`/`OPEN`/`STAT`/`SNAP`/`DUMP`），协议由 harness 统一定义、两库节点共用。备选"线程内嵌 + Runtime.halt 模拟杀"被否：无法覆盖真实进程终止后日志/快照恢复路径。

### D2：`poc/raft-selection/` 独立 reactor，不入根 `<modules>`
parent 指向根 pom（relativePath 解析），但仅 `mvn -f poc/... ` 手动构建。主干构建、javadoc 插件（show=private 会毒化 PoC 草稿代码的构建）、发布链路零污染。`openlatch-core` 以本地仓库构件依赖（`mvn -pl openlatch-core -am install`）。备选"根 reactor 加 poc 模块 + profile 隔离"被否：profile 防不住 CI 误跑，且违反"一次性"定位。

### D3：EntryClock 注入证明 core 零改动（而非提前实现 §4.3.4 参数）
适配器构造 `new CoreEngine(cfg, entryClock, listener)`；`entryClock.nowMs()` 在 apply 线程（Ratis applier / JRaft FSMCaller 均为单线程应用语义）读 thread-local 的"当前条目携带时刻"，无值回落系统时钟。Leader 提案路径同样以条目时刻计算租约，天然确定化。备选的 core 命令级时间参数改动推迟到 S2 正式做；若 PoC 发现某库 applier 不保证单线程，此结论本身作为门槛"状态机集成"的证据写入报告。

### D4：门槛一口径 = 客户端侧总延迟 < 20ms，floor 归因单列
按详设字面（"集群授予延迟 P99 < 20ms"）取严格口径：driver 端到端含 localhost 一跳。同时 no-raft 伪节点（SPI 直通 `CoreEngine` 的实现）同脚本跑 floor，报告呈现 floor/总延迟/delta，使评审可见"20ms 里网络与复制各占多少"。备选"纯进程内口径再手工加 5ms"被否：拼接数不可复现。

### D5：传输层各取库默认生产形态（Ratis → gRPC；JRaft → bolt/jrpc）
"传输层共存"维度考察的是**生产会引入什么冲突**：记录 Ratis gRPC 的 shaded grpc-netty 与主干 Netty 的共存、JRaft bolt 的 Netty/protobuf shading 情况，以及 ratis-netty transport 作为备选路线的分析注记。备选"两库都测裸 Netty 传输"被否：JRaft 无对等选项，反而造成口径不对称。

### D6：`raft.proto` 用 PoC 本地拷贝
§4.2 定义在 poc-harness 内复刻一份（仅 PoC 所需条目子集）。主干 protocol 在定案前不动——PoC 期间条目字段可能被库 API 倒逼调整，先污染主干不划算；S2（P2-05）再按定案形态正式入库。

### D7：配置冻结 + A/B/A/B 轮次公平协议
两库功能面全部通过后冻结配置：各取库默认值，任何手动调参（flush 策略、election timeout 等）在报告参数表中登记且两侧对称。基准 3 轮交替（A/B/A/B/A/B）取中位；同机、同 JVM flags、各节点独立 data-dir（同盘，公平）。任何一侧"只调自己"的优化视为违规轮次。

### D8：Java 25 兼容性 = 门槛外风险项
门槛表（§2.4）无兼容性行。若某候选起不来：登记风险随报告呈报评审定夺，不自动淘汰、不擅自降 JDK。PoC 第一周即做启动冒烟，给评审留出处置时间。

### D9：影子状态表（shadow map）承担快照载体与比对基准
适配器在 apply 时同步维护 `key → (mode, holders, counts, leaseToken, expiresAt)` 影子表：序列化为 PoC 快照文件（原型化 §7.1"自定义序列化"），`DUMP` 返回全量摘要供一致性比对。不动 `CoreEngine` 内部（其快照重建入口属 S4/P2-16 范围）。

### D10：10 万条目数据经集群路径自造
bulk 阶段 driver 以单会话 acquire 10 万个不同 key（全部走 Raft 日志），天然形成高吞吐复制负载 + 触发快照阈值，替代离线灌数工具——灌数路径与追赶路径的一致性校验因此同构。

### D11：杀主计时拆两段
`死亡→新主产生`（探测 STAT 角色变化）与 `新主→首个写成功`（含日志追赶与 NOOP 提交确认）分别记录，总时长对 10s 门槛。单一总数会掩盖"选主快、首写慢"型差异，而后者恰是 §4.5 NOOP 确认路径的库间差异点。

## Risks / Trade-offs

- [fsync 开销使两库都超 20ms，门槛失去区分度] → 报告仍出数据；若发生，登记"以 Ratis 3.x 默认 flush / JRaft common flush 参数重跑"为评审选项，调参两侧对称（D7 冻结协议允许评审豁免解冻）。
- [本机 8 核跑 3 节点 + driver + 两库负载，CPU 争抢扭曲 P99] → 轮次交替本身消偏（同一时刻拓扑相同）；报告记录 CPU 型号与负载窗口，不绑核、不美化。
- [JRaft 传递依赖与主干 protobuf 3.25.5 冲突（历史高发区）] → PoC 独立 reactor 天然隔离；冲突事实记入侵入度档案，作为 S2 依赖树预警，不强行 exclusion 后带病测量。
- [EntryClock 的 thread-local 假设在某库不成立] → 这正是 D3 要探明的信息，失败结论同样是门槛四的合格证据。
- [胶水代码 LOC 受实现者熟悉度偏差影响（先写的那侧偏高）] → 记录实现顺序，Ratis/JRaft 按详设表格顺序固定先 Ratis 后 JRaft，两轮数据并列呈现，不做"拉平"修正。
- [PoC 代码诱惑回流主干] → 主干 MR 审查规则：`poc/` 之外不得出现对 ratis/jraft 的依赖引用；S2 开工时 PoC 仅作参考文档。
