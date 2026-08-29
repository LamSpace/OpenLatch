## Purpose

为 Phase 2 Raft 库选型（Apache Ratis vs SOFAJRaft）提供可重复、可审计的对比测量能力：同一共享骨架下两候选各完成同一最小原型，产出四项判定门槛的实测数据与选型报告，使选型结论可被评审追溯与复核。

## ADDED Requirements

### Requirement: 同一最小原型双候选实现
PoC SHALL 为每个候选库各实现一个适配器，达成同一功能面：3 节点本机集群组网选主、以详设 §4.2 的 `RaftLogEntry` 条目类型（SESSION_OPEN / LOCK_ACQUIRE_ENTRY / LOCK_RELEASE_ENTRY / NOOP 的 PoC 子集）将获取/释放经多数派复制并应用到 `CoreEngine`、快照触发与落后节点追赶。两候选 MUST 经由同一 driver 与同一负载脚本驱动，功能面差异视为未通过。

#### Scenario: 3 节点组网选主
- **WHEN** 以任一候选适配器启动 3 个节点进程
- **THEN** 10 秒内产生唯一 Leader，其余 2 节点报告 Follower 角色，driver 探测集群健康通过

#### Scenario: 授予经复制生效
- **WHEN** driver 向 Leader 提交单键获取（会话先经 SESSION_OPEN 登记）
- **THEN** 该条目经多数派确认后应用到各节点状态机；对同 key 的第二次异会话获取 MUST NOT 返回授予成功

### Requirement: CoreEngine 零改动接入
PoC SHALL 证明 `CoreEngine` 不改锁语义代码即可挂入两候选状态机（详设 §2.4 门槛"状态机集成"）：适配器 MUST 通过构造注入的时间源实现"条目携带时刻"回放语义（apply 期间租约到期 = 条目时刻 + 租期，与回放物理时钟无关），主干模块（openlatch-core 及协议）在整个 PoC 期间 git diff 保持为空。

#### Scenario: 同一日志序列两次回放状态一致
- **WHEN** 同一 `RaftLogEntry` 序列在干净状态机上重放两次（条目时刻与回放物理时刻不同）
- **THEN** 两次应用后的影子状态表逐字段一致（key、mode、持有者、计数、leaseToken、到期时刻）

#### Scenario: 主干零侵入核验
- **WHEN** P2-04 出具报告前执行 `git status` / `git diff` 主干模块
- **THEN** openlatch-core、openlatch-protocol、openlatch-server 无任何改动；PoC 代码全部位于 `poc/` 且不出现在根 reactor 模块列表中

### Requirement: 门槛一——集群授予延迟 P99
测量系统 SHALL 在 3 节点本机集群上以客户端侧端到端口径（driver 发起请求至收到应用回执，含 localhost 一跳与序列化）对"单键获取+释放"混合负载连续计时 5 分钟，记录授予延迟 P99 与吞吐；P99 < 20ms 为通过。系统 SHALL 同时以同一负载脚本运行直通 `CoreEngine` 的 no-raft 伪节点，记录本机 client-side 延迟 floor，供报告做复制开销归因。

#### Scenario: 候选达标判定
- **WHEN** 某候选完成 5 分钟基准，P99 延迟汇总计算完毕
- **THEN** 报告记录其 P99 与吞吐，并对照 20ms 门槛标注通过/不通过，附 no-raft floor 同轮数据

#### Scenario: 轮次公平性
- **WHEN** 对两候选执行基准
- **THEN** 两候选使用同一负载脚本、同一 JVM 参数、同机运行，按 A/B/A/B 交替至少 3 轮，门槛判定取各轮中位数

### Requirement: 门槛二——杀 Leader 恢复计时
测量系统 SHALL 在持续负载下 `kill -9` 当值 Leader 进程，计时两段：死亡至新 Leader 产生、新 Leader 产生至 driver 首个写请求成功；总恢复时间（前者+后者）< 10s 为通过。恢复后 SHALL 断言切换前已授予并经多数派确认的锁在新 Leader 上仍有效（原会话 release 返回 OK），且等待中的后续获取不产生双授。

#### Scenario: 恢复计时与锁保留
- **WHEN** Leader 进程被 kill -9，driver 以固定间隔探测并记录恢复时间
- **THEN** 总恢复 < 10s，且切换前已授予的锁在新 Leader 上仍可被原会话正常释放

### Requirement: 门槛三——快照恢复与追赶
测量系统 SHALL 构造 ≥10 万把不同 key 的持锁状态（经集群日志路径达成），触发 Leader 快照（锁表序列化为 PoC 自定义二进制，原型化 §7.1 可自定义要求），随后终止一台 Follower 并以已截断日志方式重启，计时从启动到该节点影子状态与 Leader 一致且可服务；< 30s 为通过。一致性 SHALL 以全量影子状态摘要比对判定，MUST NOT 以抽样代替。

#### Scenario: 10 万条目快照追赶
- **WHEN** Follower 在 Leader 快照生成并压缩日志后重启
- **THEN** 该节点经快照安装 + 增量追赶在 30s 内与 Leader 全量状态一致，报告记录快照大小、安装耗时与追赶耗时

### Requirement: API 侵入度记录
每个候选 SHALL 产出一份侵入度档案：接入胶水代码行数（cloc 统计，不含共享 harness 与生成代码）、摩擦日志（被迫引入的抽象、绕过的 API、文档缺口，按条目记录）、依赖冲突清单（protobuf / Netty / 与 Java 25 的兼容性，含任何 `--add-opens` 类隐性要求）。

#### Scenario: 侵入度档案齐备
- **WHEN** 某候选完成全部四组实验
- **THEN** 其档案含 LOC 数、≥1 条摩擦日志（或显式记录"无"）、依赖冲突清单三项，缺项视为 PoC 未完成

### Requirement: 正确性不变式监视
driver SHALL 在全部实验期间持续校验不变式：任一时刻同一 key 至多一个写持有者（以各节点影子表与响应观察联合判定）；违例即时序记录并使该轮数据标记为无效。

#### Scenario: 双主授予检出
- **WHEN** 故障演练期间两个节点先后对同 key 向不同会话返回授予成功
- **THEN** 不变式监视器记录违例事件（节点、term、时间线），该轮所有门槛数据在报告中标注无效

### Requirement: 选型报告与定案回写
PoC SHALL 产出 `docs/raft-selection-report.md`：逐项门槛数据（含原始 results JSON 的路径引用）、§2.2 权重维度的定性评分表、淘汰/定案结论与理由；任一候选不满足任一门槛即淘汰，均满足则按权重综合评分定案。结论 SHALL 回写详设说明书修订版（v1.1，§2.1 定案、§12 风险 4 关闭），报告与详设修订 MUST 一并提交评审。

#### Scenario: 报告证据可追溯
- **WHEN** 评审者抽取报告中任一门槛数字
- **THEN** 能沿报告标注定位到 results 目录中对应轮次的原始 JSON 并由脚本复现汇总值

#### Scenario: Java 25 兼容性风险处置
- **WHEN** 某候选库在 Java 25 上无法启动集群
- **THEN** 该事实记入门槛外风险项并随报告呈报评审，MUST NOT 未经评审自行判为淘汰或自行降级 JDK 版本
