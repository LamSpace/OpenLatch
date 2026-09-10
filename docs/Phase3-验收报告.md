# OpenLatch Phase 3 验收报告

| 项目     | 内容                                                                                  |
|----------|---------------------------------------------------------------------------------------|
| 项目名称 | **OpenLatch**                                                                         |
| 文档类型 | 阶段验收报告（Phase 3 / 功能完整平台）                                                |
| 依据文档 | 《OpenLatch-Phase3-详细设计说明书》v1.1、《OpenLatch-总体实施计划与验证方案》v1.0     |
| 版本     | v1.0（定稿；发布定夺见签署栏）                                                        |
| 日期     | 2026-09-10                                                                            |
| 作者     | Lam Tong                                                                              |
| 状态     | 待签署                                                                                |

本报告逐项闭环《总体实施计划与验证方案》§5.3 Phase 3 验收清单（六项）与 Phase 3 详设 §8（六项），并按 §5.4 完成定义（DoD）自检。证据以引用汇总，路径均相对仓库根。Phase 3 的实现单元为四个 change（T1–T4），各附验收证据文件，本报告汇总引用、不重复采集。

## §5.3 Phase 3 验收清单逐项闭环

### 标准 1：FairLock——并发竞争下授予顺序与排队顺序一致

- 判据：独立公平性套件常开，`FAIR`/`REENTRANT` 参数化跑同一"授予序 == 排队序"矩阵。
- 证据：`openspec/changes/archive/2026-09-06-phase3-t1-extended-lock-types/t1-acceptance-evidence.md` §1——`FairOrderingSuiteTest`（core 8 例）、`FairOrderingSuiteE2ETest`（真端口 2 例）、`FairOrderingSuiteClusterTest`（真三节点 2×参数化）；三类均为 surefire 默认 include、无 `@Tag("drill")`，全量 `clean verify` 即执行（P3-02 退出）。
- 判定：✅

### 标准 2：Semaphore——许可计数、超额等待、租约到期归还许可全部有测试

- 证据：`t1-acceptance-evidence.md` §2——`CoreEngineSemaphoreTest` 10 例（计数/队首防饥饿/重入/到期整体归还/超额释放/断连/定型/不匹配矩阵/深度限额）、`SemaphoreGatingTest` 3 例（协议门控与线路往返）、`ClientSemaphoreIT` 4 例（含裸 socket 断连归还）、`ClusterSemaphoreLatchTest`（许可感知队首推进、池回收纯加入显式拒）。
- 判定：✅

### 标准 3：CountDownLatch——countDown/await 语义、归零广播唤醒、一次性语义有测试

- 证据：`t1-acceptance-evidence.md` §2——`CoreEngineLatchTest` 9 例（total 双通道定型/纯初始化/归零全体广播/一次性/无租约零触及/断连摘除/不匹配/幂等去重/通知超时清扫）、`LatchGatingTest` 3 例、`ClientLatchIT` 5 例、`ClusterSemaphoreLatchTest`（Latch 复制计数/本地等待/归零广播/failover 存续 + 副本摘要一致）。
- 判定：✅

### 标准 4：指标——清单完整暴露，Prometheus 抓取成功

- 证据：`openspec/changes/archive/2026-09-07-phase3-t2-metrics/t2-acceptance-evidence.md`——§3.2 十项清单逐项 ↔ 断言用例对照表（L1 词表测试钉死逻辑名↔Prometheus 线名映射）；真 Prometheus（prom/prometheus:v2.53.4）抓取联调实录（`/api/v1/targets` up；四查询值与 `/metrics` exposition 逐项吻合；`histogram_quantile(0.99, rate(..._bucket[1m]))` 由桶线算出 p99，格式合规最强佐证）。
- 判定：✅

### 标准 5：控制台——可列出锁、持有者、等待者、会话与核心指标（端到端冒烟通过）

- 证据：`openspec/changes/archive/2026-09-09-phase3-t3-admin-console/t3-acceptance-evidence.md`——L2 `ConsoleStandaloneTest`（6 例：五页面 HTML 断言 + sparkline 非降级）、`ConsoleClusterTest`（3 例：角色分布恰一 Leader、follower 标注）、`ConsoleMetricsDownTest`/`ConsolePartialFailureTest`（降级）；L3 真实 jar + 进程走查（9413 五页面 + 反例 9414 令牌错 → 降级横幅且零泄露）；管理通道强制认证（L1 认证矩阵 + L2 `ConsoleAuthFailureTest`）。
- 判定：✅（唯「浏览器目检记录」栏留待用户回填观感；L1/L2 已自动化覆盖页面渲染正确性）

### 标准 6：安全——启用 TLS 后拒绝明文连接；错误/空 Token 的 HELLO 被拒绝并断开

- 证据：`openspec/changes/archive/2026-09-10-phase3-t4-security/t4-acceptance-evidence.md`——`TlsGatingTest`（明文客户端被拒零会话 / 正确证书握手 / mTLS 缺证书被拒 / 握手超时 <8s 断开 / 坏证书配置启动快速失败）、`AuthHandshakeTest`（空/错令牌同形断连不泄露、双令牌双活、摘旧令牌被拒、默认关保留 Phase 1 守卫、被拒零会话副作用）、`ClientTlsAuthIT` 6 例、`ConsoleTlsAuthTest` 2 例、`ConstantTimeTest` 4 例 + `AdminProtocolTest` 15 例回归。
- 判定：✅

## 详设 §8 六项对照

详设 §8 六项与上述 §5.3 同轴，映射如下（判据、证据、结论一致）：

| 详设 §8 | 对应上节 | 结果 |
|---|---|---|
| 1 FairLock 顺序回归套件在 CI 常开 | 标准 1 | ✅ |
| 2 Semaphore/CDL 语义测试全绿（含集群切换场景） | 标准 2、3 | ✅ |
| 3 指标清单逐项有断言用例，Prometheus 抓取联调记录在案 | 标准 4 | ✅ |
| 4 控制台冒烟通过，且管理通道强制认证 | 标准 5 | ✅ |
| 5 安全拒绝用例全绿（明文连接、错误令牌、未认证管理请求） | 标准 6 | ✅ |
| 6 全部新功能默认值关闭或不改变既有行为（v1 客户端兼容） | 见下「兼容性专项」 | ✅ |

### 兼容性专项（§8-6）

- **T1**：协议升至 v3，仅新增枚举值/字段/消息，golden 契约冻结测试钉既有编号；v1/v2 会话对新类型（`FAIR/SEMAPHORE`）与 `LATCH_*` 消息消息级 `INVALID_REQUEST`、不断连；锁路径不读许可字段，行为逐字段不变。
- **T2**：指标端点默认开启但走独立管理端口（9412），不影响锁端口；开/关应答逐字段一致（归一口径用例）。
- **T3**：`ADMIN_*` 走独立早退分支，不进 `RequestDispatcher`/`ClusterRequestHandler` 与指标词表（零污染消息级断言）。
- **T4**：TLS/auth 默认关；`authOffKeepsPhase1Guard` 保留 Phase 1"非空即拒"守卫；无协议/字段变更。
- 背书：全量 `clean verify` 含 v1/v2/v3 全部回归与协议冻结测试零失败（见「构建门禁复跑」）。

## 基准数据汇总（DoD §5.4-5）

| 组 | 指标 | 值 | 来源 |
|---|---|---|---|
| 单机基准（Phase 3） | 无竞争 tryLock ops/s / P99 | 11788 / 0.09ms | `docs/benchmark-baseline-2026-09-10.md` |
| 单机基准（Phase 3） | 64 线程竞争 ops/s / 授予延迟 P99 | 11549 / 8.93ms | 同上 |
| 对比 Phase 1 基线 | 吞吐 Δ / 延迟 Δ | +2.4%～+4.6% / ±10% 内，**无 >20% 退化** | 同上「与 Phase 1 基线对比」 |
| 选型 / 集群 | 集群授予 P99 / 杀主恢复 | 4.87ms / 548ms | `docs/raft-selection-report.md`（Phase 2） |
| 快照 | 10 万条目大小 / 恢复 | 4.48MB / 521ms（预算 30s） | `SnapshotBenchmarkTest`（Phase 2） |
| 混沌 soak | 字面 ≥10 分钟 | 510 杀 / 509 重启 / 2567 授予 / 0 冲突 / 0 泄漏 | `ClientChaosIT` soak 档（Phase 2） |

Phase 3 未引入锁热路径的性能相关变更（扩展锁类型与 TLS 均走默认关路径），单机基准相对 Phase 1 无显著退化。

## 总体计划 §5.4 DoD 五条款自检

1. **交付物合入主干、`mvn verify` 全绿**：Phase 3 四个 T 变更（P3-01～P3-17）全部实现并归档；全 reactor `clean verify` 结果见下「构建门禁复跑」。✅
2. **验收清单逐项有自动化用例或验收记录**：本报告 §5.3 六项 + 详设 §8 六项逐项勾对，证据路径见各项。✅
3. **无已知正确性缺陷（P0 清零；P1/P2 在案 + 跟进）**：Phase 3 实施期未引入 P0；T1 附带修复 Phase 2 遗留的集群重入预检缺陷与 `WaitQueue` 生产清扫调用边（均为既有语义补正，非新功能开关）；Phase 2 遗留 P1 leader 复制停摆已由 `phase2-leader-stall-followup` 根因收口 + 看门狗自愈承载（形态 B 选举风暴残余在案，跟踪由升级评估对账承载）。✅（按条款口径）
4. **文档同步**：Phase 3 详设升 v1.1（实现期各章勘误已回写，本次收口版本/状态/修订记录）；README 双语文档节与「已知局限」按现状改判；`transport-security` 等主规格随变更归档同步。✅
5. **基准记录在案、无显著退化**：见上「基准数据汇总」，单机基准相对 Phase 1 无 >20% 退化。✅

## 缺陷与遗留记录（如实）

1. **T4 集群档认证零副作用夹具未建**（`phase3-release-closure` design D1）：未认证 HELLO 零 `SESSION_OPEN` 副作用由结构保证（门闩分叉置于 `SessionCoordinator.handleHello` 之前）+ 单机档零副作用用例锁定；集群档实证需新建三节点 `OpenLatchServer` 夹具（现有 `ClusterHarness` 直驱 `ClusterRuntime`、不经门闩）。**另立小变更**，本报告不以结构论证冒充执行证据。
2. **L3 手工走查 / `openssl s_client` 抓取 / 真多进程滚动重启**：本环境为无 TTY 沙箱，未作为证据执行；步骤见 T4 design D9 与 README 安全章节，留运维/CI 环境补图补据。T3 的 L3 走查（真实 jar + 进程）已在 `t3-acceptance-evidence.md` 落地，唯「浏览器目检」栏待用户回填观感。
3. **`-Pdrill` 进程级演练**：按仓库收口纪律，本沙箱不可靠（无 TTY 下进程拉起 / kill-9 / 限时选举受限，clean-master A/B 判定为环境性）；等价进程内故障覆盖（LeaderFailover / MinorityQuorum / ClusterMetrics / AdminCluster / ClusterSemaphoreLatch）含于全量 `clean verify`。留 CI/运维环境复跑。
4. **§9 出阶段遗留项**（非 Phase 3 范围）：控制台写操作（强制解锁 / 踢会话）、读者批量授予优化、多租户 / 命名空间、客户端按 key 维度指标；另有 T2 直方图桶位可配置化、T3 跨页排序（服务端 `sort_by`，v3 协议小增量）、9412 / 9413 是否纳入 TLS。
5. **T1 遗留**（`t1-acceptance-evidence.md` 遗留段）：集群 Semaphore 池回收后纯加入者显式拒（design D4 边界，spec 已钉两分支）；Latch 条目存续至节点重启（design D5 修订，泄漏面 = 每定型 key 一条小条目，README「已知局限」已明示）。

## 构建门禁复跑

- **命令**：`mvn -s /home/lam/repo/settings.xml clean verify`（全 reactor）
- **结果**：**BUILD SUCCESS（exit 0）**，2026-09-10 22:22:03，总耗时 **07:29 min**
- **模块**：`OpenLatch` 0.7s / `openlatch-protocol` 7.1s / `openlatch-core` 4.7s / `openlatch-server` **04:18** / `openlatch-client` **02:28** / `openlatch-spring-boot-starter` 11.2s / `openlatch-console` 16.5s / `openlatch-examples` 1.5s —— 全部 SUCCESS
- **覆盖**：含 v1/v2/v3 全部回归、协议 golden 冻结测试、T1 公平性三档套件、T2 词表/端点/集群指标、T3 管理协议与 console 页面、T4 TLS/认证套件，以及 `maven-javadoc-plugin`（`show=private`，CLAUDE.md §5）校验。
- **未执行项**：`-Pdrill` 进程级演练（见「缺陷与遗留记录」第 3 条——无 TTY 沙箱不可靠，等价进程内故障覆盖已含于上述 `openlatch-server` 测试；留 CI/运维环境复跑）。

## 发布宣告

- **版本**：Phase 3（功能完整平台），协议 v3，模块 `openlatch-*` 1.0-SNAPSHOT（沿 Phase 2 定夺：维持 1.0-SNAPSHOT 与无 tag 现状，定版随后续发布窗口另行执行）。
- **判据**：《总体实施计划与验证方案》§5.3 六项——**六项 ✅**；DoD §5.4 五条自检 **✅**。
- **发布定夺**：Phase 3 功能交付发布；本变更（`phase3-release-closure`）归档提交即宣告。
- **签署**：评审人 ____________　日期 ____________
