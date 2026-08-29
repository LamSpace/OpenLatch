# OpenLatch Phase 2 / S1：Raft 库选型报告

| 项目     | 内容                                   |
|----------|----------------------------------------|
| 文档类型 | 选型报告（PoC 结论，详设 §2 定案依据） |
| 版本     | v1.0（提请评审）                       |
| 日期     | 2026-08-30                             |
| 作者     | Lam Tong（S1 PoC 执行）                |
| 依据     | 《OpenLatch-Phase2-详细设计说明书》v1.0 §2/§13.1；`openspec/changes/phase2-s1-raft-poc` |
| 证据     | `poc/raft-selection/`（代码、`run-matrix.sh`、原始 results JSON、摩擦与冻结文档） |

> 本报告全部数字可由 `python3 poc/raft-selection/summarize.py` 从
> `poc/raft-selection/results/<candidate>-<exp>-<round>.json` 复现；
> 实验公平协议与对称调整登记见 `poc/raft-selection/config-freeze.md`（design D7）。

## 1. 结论

**定案：Apache Ratis。** 实现版本建议 **3.3.0**（2026-08-12 发布，含 linearizable read 改进；本报告实测数据基于 3.2.2，3.3.0 已补冒烟全绿：`results/ratis-smoke-9.json`）。

- **两候选均通过 §2.4 全部四门槛**（无淘汰项），按 §2.2 权重综合评分定案。
- 决定性差异在**传输层共存**（高权重）：Ratis 全量 shading（`ratis-thirdparty-misc`），与主干 protobuf 3.25.5 / netty-bom 4.1.137 零类路径交集；JRaft 不着色 protobuf/netty，主干一次 Netty/protobuf 升级即波及共识层——与"项目核心特色是 Netty"的定位直接冲突。
- JRaft 两项优势（快照追赶快 ~26%、批量复制带宽高 ~3×）不足以逆转：**复制带宽优势在锁复制小条目负载下无体现**（bench P99 双方持平 4.5 vs 4.9ms），而杀主恢复 Ratis 快 2.8×（Phase 2 验收第 2 条的体验面）。
- 许可证：Apache-2.0（双方均满足，门槛项通过，记录在案）。

## 2. 门槛逐项判定（详设 §2.4；3 轮 A/B 交替取中位，round-0 开发轮不计入）

| 候选 | 指标 | 轮值 | 中位 | 门槛 | 判定 |
|---|---|---|---|---|---|
| Ratis | 集群授予延迟 P99 (ms) | 4.783 / 5.125 / 4.865 | **4.865** | < 20 | ✅ 通过 |
| Ratis | 杀 Leader→可服务 (ms) | 548 / 983 / 351 | **548** | < 10s | ✅ 通过 |
| Ratis | 杀主存活会话锁保留 | 真/真/真 | 全真 | — | ✅ |
| Ratis | 10 万条目快照恢复+追赶 (ms) | 2514 / 2248 / 2583 | **2514** | < 30s | ✅ 通过 |
| JRaft | 集群授予延迟 P99 (ms) | 4.500 / 4.397 / 5.120 | **4.500** | < 20 | ✅ 通过 |
| JRaft | 杀 Leader→可服务 (ms) | 1359 / 2466 / 1508 | **1508** | < 10s | ✅ 通过 |
| JRaft | 杀主存活会话锁保留 | 真/真/真 | 全真 | — | ✅ |
| JRaft | 10 万条目快照恢复+追赶 (ms) | 1876 / 1846 / 1883 | **1876** | < 30s | ✅ 通过 |
| 双方 | 快照 digest 全量一致 + 走快照路径 | `rebuilds=1, failures=0` × 全部轮次 | — | — | ✅ |
| 双方 | 状态机集成：`CoreEngine` 零改动 | 主干 `git diff` 全程为空 | — | 不重写锁语义 | ✅ |

**归因（floor = no-raft 伪节点同脚本同机，排除网络/测量框架开销）**：

| 项 | floor | Ratis（delta） | JRaft（delta） |
|---|---|---|---|
| 授予 P50 | 0.035ms | 2.16ms（+2.13） | 2.86ms（+2.82） |
| 授予 P99 | 0.083ms | 4.87ms（+4.78） | 4.50ms（+4.42） |
| bench 吞吐 | 13468 对/s | 220 对/s | 176 对/s |
| 100k bulk 复制带宽 | — | 4310 条/s | 12988 条/s |
| 杀主选主段 tElect | — | 中位 354ms | 中位 1353ms |
| 快照文件（10 万锁） | — | 3.54–3.87 MB | 3.87 MB |

20ms 门槛语义说明（design D4）：P99 为 driver 客户端侧端到端（含 localhost 一跳）；复制纯开销为 delta 列。两候选均余量充足，**无需触发 D7 的 flush 调参豁免**。

**不变式**：全部 21 个正式实验轮次（bench×9 含 floor、kill×6、snapshot×6；含杀主/追赶窗口）**零双授违例**。

## 3. 评估维度综合（详设 §2.2）

| 维度 | 权重 | Ratis 3.2.2 | JRaft 1.4.1 | 优方 |
|---|---|---|---|---|
| 状态机集成方式 | 高 | `BaseStateMachine`+`SimpleStateMachineStorage` 即外部状态机形态；零改动达成；但存储接线陷阱多（StartupOption/RECOVER 探测/protected tmp/`getUuid` 目录名） | `StateMachine` 接口最直观，`onSnapshotSave/Load` 直给目录；零改动达成；applier 单线程语义双方均符合 | 略平（JRaft 接口手感略优，Ratis 集成正确性靠 storage 兜底） |
| 快照支持 | 高 | 自定义二进制 ✓（3.5MB@10万锁）；leader install pipeline 有标准存储接口（PoC 未测安装路径，记 S2 风险）；恢复 2514ms | 自定义二进制 ✓（3.9MB）；外部触发走 `CliService`（`snapshotSync` 限 SM 回调内，1.4.1 文档失真）；无阈值 setter→自动快照口径与旧文档不符；恢复 1876ms | 接近（实测恢复 JRaft 快；可控性双方各有坑） |
| 传输层共存 | 高 | **gRPC + protobuf + Netty 全着色**：与主干 4.1.137/3.25.5 零交集 | bolt 用**非着色 netty-all**（被主干 BOM 顶版本）+ protobuf 非着色（本次恰好同版 3.25.5=幸运）+ rocksdb/jna/hessian 足迹；Java 25 io_uring native-access 告警 | **Ratis 明显优** |
| 性能 | 中 | 杀主恢复 548ms（快 2.8×）；P99 持平；批量带宽低（单客户端串行坑，需客户端池绕行） | P99 持平略优；批量带宽 3×；杀主 1508ms | 各半：恢复=Ratis，带宽=JRaft；锁场景延迟/恢复更相关 → 偏 Ratis |
| 成员变更 | 中 | `AdminApi.setConfiguration`（联合共识自动） | `changePeers/resetPeers/addPeer/removePeer` + CliService 对等口 + 成熟运维 CLI 工具链 | 平（JRaft 运维工具略优） |
| 社区与维护 | 中 | ASF 顶级项目，3.3.0（2026-08）/3.2.2（2026-04）节奏稳定，Ozone 生产依赖 | 蚂蚁单厂商，1.4.1（2026-06）距 1.4.0（2025-07）一年，**官方文档/样例大面积落后 API**（PoC 8 条摩擦中 5 条源于此） | **Ratis 优** |
| 许可证 | 门槛 | Apache-2.0 ✓ | Apache-2.0 ✓ | 均过 |

综合：高权重 3 项 Ratis 拿下传输层、快照与集成基本打平；中权重 Ratis 社区+恢复占优。**Ratis 胜出，无一项"高权重明显失分"。**

## 4. Java 25 兼容性（门槛外风险项，D8 呈报）

| 候选 | 结论 | 遗留 |
|---|---|---|
| Ratis 3.2.2 / 3.3.0 | ✅ 冒烟、杀主、5k/100k 快照全绿，无 `--add-opens` 需求 | 无 |
| JRaft 1.4.1 | ✅ 全实验绿 | bolt io_uring `System::loadLibrary` restricted-method 告警（建议 `--enable-native-access=ALL-UNNAMED`，非阻塞） |

D8 未触发：无淘汰级兼容问题。

## 5. API 侵入度（详设 §2.3 第 5 项）

| 项 | Ratis | JRaft |
|---|---|---|
| 适配器胶水 LOC | 321 | 306 |
| 共享内核 LOC（对等） | harness 1806 | 同左 |
| 摩擦日志条数 | 9（`friction-ratis.md`） | 8（`friction-jraft.md`） |
| 被迫引入的抽象 | RaftClient 池（绕过单客户端串行）、tmp+rename 自造 | CliService 手动触发+轮询落盘、逻辑会话映射层（两库共用） |
| 依赖冲突 | 零 | protobuf/netty-all 结构性共存风险 |

实现顺序固定 Ratis 先（D7 备注）：后写的 JRaft 侧吸收了通用教训，LOC 对比仅作参考。

## 6. PoC 附带发现（回写详设 / S2 输入）

1. **leaseToken 天然确定**：engine 内部 `AtomicLong` 计数器，同序应用各副本值一致——§4.2 无需条目携带 token 字段。
2. **`sessionOpened()` 随机 sid 是唯一非确定面**：P2-08 的 `sessionId=(nodeId, localSeq)` 方案落地后，PoC 的映射层自然消失。
3. **§4.3.4 的 core 时间参数可以降级**：EntryClock（apply 线程 thread-local 条目时刻）在两库 applier 上均成立，`CoreEngine` 可保持零改动；若 S2 评审倾向显式参数化，改动量也已收敛为"可选"。
4. **快照重建需正式入口（P2-16 确认必要）**：回灌式重建要求 token 连续，授予-释放空洞会显式失败——§7.1 的包级私有重建入口必须做。
5. **leader 对单客户端在途串行（Ratis）**：ReplicationGateway 设计（P2-07）需评估多 client 或多连接提交路径。

## 7. 评审记录

- [ ] 评审通过本结论与 v1.1 详设回写
- 待议：S2 直接基于 Ratis 3.3.0（推荐）；3.3.0 仅冒烟级验证，集成级风险随 S2 暴露。

## 附录：证据与复现

| 证据 | 路径 |
|---|---|
| 原始数据（21 轮正式 + round-0 开发轮 + 3.3.0 冒烟） | `poc/raft-selection/results/*.json` |
| 实验矩阵脚本 | `poc/raft-selection/run-matrix.sh`（D7 协议） |
| 门槛汇总生成器 | `poc/raft-selection/summarize.py` |
| 摩擦日志 | `poc/raft-selection/friction-{ratis,jraft}.md` |
| 配置冻结登记 | `poc/raft-selection/config-freeze.md` |
| 零改动核验 | `git diff`（主干 tracked 文件）+ PoC 独立 reactor 不入根 `<modules>` |

```bash
mvn -s /home/lam/repo/settings.xml -pl openlatch-core,openlatch-protocol -am install -DskipTests
mvn -s /home/lam/repo/settings.xml -f poc/raft-selection/pom.xml install
bash poc/raft-selection/run-matrix.sh          # ~50min（bench 300s × 3 轮 × 2 候选 + floor + kill + snapshot 100k）
python3 poc/raft-selection/summarize.py
```

社区来源：[Ratis 3.3.0 公告](https://ratis.apache.org/post/3.3.0.html)、[3.2.2](https://ratis.apache.org/post/3.2.2.html)、[发布归档](https://ratis.apache.org/post/index.html)、[apache/ratis releases](https://github.com/apache/ratis/releases)；[sofastack/sofa-jraft](https://github.com/sofastack/sofa-jraft)、[JRaft release notes](https://www.sofastack.tech/en/projects/sofa-jraft/release-log/)、[jraft-core 版本列表](https://mvnrepository.com/artifact/com.alipay.sofa/jraft-core/versions)。
