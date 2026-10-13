# Tasks

## 1. 门控基础设施（红态可常驻而不使门禁变红）

- [x] 1.1 `openlatch-client/pom.xml`：failsafe 默认 `excludedGroups` 由 `drill` 改为 `drill,w11-repro`。验证=**已验**：`mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆 8/8 模块 SUCCESS（0 失败 0 错误）；默认构建全程未运行两枚红先夹具（日志零命中）。
- [x] 1.2 `openlatch-client/pom.xml`：新增 profile `w11-repro`，以 `combine.self="override"` 置 failsafe `<groups>w11-repro</groups>`（判例=既有 `drill` profile）。验证=**已验**：`help:active-profiles` 仅带 `-Pw11-repro` 时激活该 profile；该 profile 下 failsafe 只选中两枚夹具（`Tests run: 2`），不带则该 profile 不激活。

## 2. 夹具 A：启动窗提示滞后（脚本化服务端，无真集群）

- [x] 2.1 复活脚本化夹具（蓝本=`46bd28b` 的 `LeaderDiscoveryTest`，见归档 change `2026-10-10-fix-lane-reroute-convergence` tasks 1.1）：握手返回**无效** Leader 提示，随后对该会话的 `CLUSTER_VIEW` 查询报出当值 Leader；断言非获取请求在既有等待预算内**完成**。验证=**已验**：未改实现前该用例红，签名=`OpenLatchTimeoutException: put on queue 'repro-hint-lag' exceeded default wait budget`；不起真集群、不依赖真杀主竞速。
- [x] 2.2 夹具 A 并入守卫断言（design D5）：被读作成功的应答其值/状态**不得来自 proto3 默认实例**；显式超时与服务端 `SESSION_EXPIRED` **不判红**。验证=**已验（探针实测）**：home 配成缺码形应答且无当值 Leader 可达时，客户端 `put` 以显式超时收场、home 收到 84,533 次 `QUEUE_OP` 而**从未**被读作成功——静默成功路径在当前实现中不存在（这正是守卫要永久钉住的形态）；守卫断言只在成功路径求值，故超时/会话失效形态天然不判红。
- [x] 2.3 夹具 A 类级 Javadoc（含门控命令与"红态是预期"的声明）。验证=**已验**：类级 Javadoc 覆盖两窗语义、红态语义与守卫口径，其中给出的 `-Pw11-repro` 命令逐字执行通过。

## 3. 夹具 B：home 驻留非权威节点（进程内三节点集群）

- [x] 3.1 复活集群夹具（蓝本=`46bd28b` 的 `ClientClusterIT#residentNonAuthoritativeHomeConvergesQueueRequest`，见归档 tasks 1.2）：home 直连当值 Leader 并正常受理一次（无车道），随后令该节点**存活让位**（连接与会话保留、已非权威）并把 Leader 迁至第三节点；断言窗内非 ACQUIRE 请求**收敛**。验证=**已验**：未改实现前红，签名=`OpenLatchTimeoutException: put on queue 'repro-resident-queue' exceeded default wait budget`（29s＝预算耗尽）；窗口由"让位"单向构造。
- [x] 3.2 夹具 B 覆盖非 ACQUIRE 族抽样（队列为锚 + 原子一例）并入守卫断言（同 2.2 口径，原子侧重点= `addAndGet` 不得返回 0）。验证=**已验（部分为结构性）**：两例（队列 `put` + 原子 `addAndGet`）均在同一窗内断言；原子守卫为 `isEqualTo(10)`（返回 0 即该形态）。其"红路径"需实现在收敛后给出静默错值才可达——当前实现无法注入（结论同 2.2 探针），故该守卫按绊线（tripwire）性质交付，不由本轮执行证明。

## 4. 文档与登记

- [x] 4.1 夹具类级 Javadoc 记明：覆盖的两个根因窗、红态语义（无修复必红＝预期）、守卫判据、门控命令；**不得**引用内部过程代号（citation 门禁）。验证=**已验**：`bash scripts/check-source-citations.sh` 零命中（退出码 0）；夹具 Javadoc 未引用设计决策编号等内部代号。
- [x] 4.2 `WATCHLIST.md` W11 行的"记录位置"列追加本 change 与夹具类（判据已落地、红态可复跑），保持只增位置、不写长文。验证=**已验**：行内新增引用指向 change 目录与两枚夹具类名；表行校验＝14 行号集合不变、每行 6 列、零粘连。

## 5. 收口验证

- [x] 5.1 默认全量构建仍绿：`mvn -s /home/lam/repo/settings.xml clean verify` 通过（0 失败 0 错误），坐实红先夹具默认排除、任一既有门禁未被影响。验证=**已验**：全反应堆 8/8 模块 SUCCESS（server 8:02、client 4:10），两枚红先夹具默认零执行。
- [x] 5.2 红态稳定性：`-Pw11-repro` 连跑 ≥3 轮，两枚夹具恒红且同签名、无 flaky（达到本仓"真回归两侧都红"的判据强度，design D6）。验证=**已验**：三轮均 `Tests run: 2, Errors: 2`，两枚夹具的失败**签名与行号逐轮一致**（`ClientClusterIT:379` / `RerouteConvergenceReproIT:124`，均为 `OpenLatchTimeoutException` 显式超时），无 flaky。
- [x] 5.3 引用与链接门禁：`bash scripts/check-source-citations.sh` 与既有链接检查零命中/零死链。验证=**已验**：citation 门禁零命中（退出码 0）、链接检查死链 0（退出码 0）。
