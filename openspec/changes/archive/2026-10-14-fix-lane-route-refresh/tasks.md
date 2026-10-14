# Tasks

## 1. 刷新器（发现侧：惰性、共享、有界）

- [x] 1.1 新增 `RouteRefresh`（客户端内部）：单飞（同刻至多一次在飞）+ 限频（最小间隔）+ 在飞期间触发合并；复用既有 `CLUSTER_VIEW` 编解码与 `SeedDiscovery`，结果发布为**期望路由**并据此建/换获取车道。验证=单测覆盖三分支（在飞合并为一次探测、限频期内触发被抑制、探测失败/视图无主时静默无动作）。 验证=**已验**：`RouteRefresh` 落地（单飞 + 限频 1s + 在飞合并 + 补探）；单测 7 例全绿（在飞合并/限频抑制/探测失败静默/探测抛出静默/关停停探/合并补探/发布失败不上浮）。
- [x] 1.2 静默降级：单机形态（`HELLO` 无提示字段）、服务端无该消息能力、探测超时——一律不动作、不计为业务失败、不新增失败面。验证=单测断言上述形态下探测计数为 0 且业务请求行为与改动前逐项一致。 验证=**已验**：新增脚本用例 `RemoteBlockingQueueScriptedTest#singleMachineNeverProbesForRouteRefresh`（单机形态下请求一路吃 `NOT_LEADER`，断言 `countType(CLUSTER_VIEW) == 0` 且以显式超时收场）；队列脚本用例 11/11 绿。
- [x] 1.3 刷新结果**不得**直接促成对在途请求的干预：发布路径只写"期望路由"，MUST NOT 触碰任何在途等待/重发环。验证=代码审查 + 1.1 的单测断言发布路径无等待环调用面。 验证=**已验（含一处口径澄清）**：发布路径只经 `followLeaderTarget` 写"期望路由"，探测完成回调不触碰任何等待/重发环；`RouteRefresh` 单测断言发布路径无等待面调用。澄清：换道收口沿用既有 `retargetAcquireLane`，其中"旧车道→新车道"的等待迁移是**既有改道语义**（握手提示换道一直在做），非本路径新增的干预。

## 2. 采用侧（规则 #2：在途请求不因全局变更而重取）

- [x] 2.1 **原子族**（当前每轮迭代重取全局路由）：改为起手钉住 + 仅在自身不可用（明确零生效拒绝、连接断开、会话换代）时重取。验证=既有原子脚本用例全绿，且严格锚 `committedAtomicValueSurvivesLeaderKillWithoutDoubleApply` 不变。 验证=**已验**：`execute`/`executeRef` 两处标量+引用循环均改为起手钉住 + 三处自身不可用重取；`RemoteAtomicScriptedTest` 3/3、`RemoteAtomicFactoryTest` 5/5 绿；严格锚 `ClientClusterIT` 8/8 绿（含 `committedAtomicValueSurvivesLeaderKillWithoutDoubleApply`）。
- [x] 2.2 **条件 / 定时器**：同上口径（两族当前无换代守卫，须同时确认重取点位置正确）。验证=既有两族脚本用例全绿 + 换主窗用例不变。 验证=**已验**：`RemoteCondition` 的 exchange 环钉住 + 三处重取（缺码形/会话未就绪/零生效拒绝）；`RemoteTimer` 两处缺码形分支接刷新（其等待环本就起手捕获 + 分支重取，天然满足规则 #2）；两族脚本用例全绿。
- [x] 2.3 **闩 / 屏障**：同上口径（两族有严格守卫，重取后仍按采用规则 D3 判定续发）。验证=既有两族脚本用例全绿。 验证=**已验**：`RemoteCountDownLatch`/`RemoteBarrier` 等待环本就"起手捕获 + 分支重取"，补齐缺码形分支的刷新触发（屏障另含 action-done/leave 两处）；两族脚本用例全绿。
- [x] 2.4 **相位器**：同上口径（该族环内取点最多，逐点核）。验证=既有相位器脚本与集群用例全绿。 验证=**已验**：`RemotePhaser` 三处缺码形分支接刷新（其环起手捕获 + 分支重取）；相位器脚本用例全绿。
- [x] 2.5 **订阅发布**：同上口径（订阅侧另有 30s 保活重挂通道，须确认二者不互相干扰）。验证=既有订阅用例全绿。 验证=**已验**：`RemoteTopic` 订阅环与发布环**改为起手钉住**（原为每轮重取）+ 各零生效分支接刷新；订阅/发布用例全绿；订阅侧 30s 保活重挂通道未改动、二者不互相干扰（重挂走独立幂等 SUBSCRIBE）。
- [x] 2.6 **队列补 `NOT_LEADER` 分支重取**（design D4）：`parkLoop` 与 `exchange` 两处同环补齐，与同环瞬态/缺码形分支一致。验证=既有队列用例全绿 + 夹具 B 的队列例在窗内收敛。 验证=**已验**：`parkLoop` 与 `exchange` 两处 `NOT_LEADER` 分支补齐路由重取（原仅原地重试），与同环瞬态/缺码形分支一致；夹具 B 的队列例在窗内收敛。
- [ ] 2.7 **（不可行，前提被证伪——如实记录，未执行）** 原拟"在夹具 B 的窗内加一族等待类抽样（闩或屏障），证明等待面经同一路由入口同样受益"。核实结论：**该前提不成立**——`RemoteCountDownLatch:220` 与 `RemoteBarrier:305` 的状态分派对 `NOT_LEADER` 一律**终态抛出**（`throw new OpenLatchException(status, …)`），故等待面在该窗内经同一路由入口**不受益**，夹具必然红。这是**既定契约面**（在册 W11 行的"等待面 `NOT_LEADER` 终态"残余），不是本 change 的缺陷；其重做归"等待类改道语义"另立 change。本 change 的收敛面因此界定为**非获取类请求面**（队列/原子/条件/定时器/订阅），已在 W11 行如实记录。

## 3. 规格与文档

- [x] 3.1 规格对账（design D6，**实现前必做**）：核实条款 4「连续 3 次业务请求收到 `NOT_LEADER` MUST 触发发现」与实现面（仅获取路径计数、非获取族走刷新入口）的偏差，选定表述并把结论回写 delta 规格（另立一条描述非获取族，或按实际收窄条款 4）。验证=规格措辞与实现语义逐条一致，`openspec validate fix-lane-route-refresh --strict` 通过。 验证=**已验**：条款 4 已按实测收窄为"获取与等待路径"并指向第 7 条，MUST NOT 互相折算；design D6 已回写核实结论（`notLeaderStreak` 唯一计数点 `OpenLatchClient:532`，仅被两个 `AwaitTracker` 接入）。
- [x] 3.2 契约 Javadoc 同步：`latchRoute`/路由取用纪律（起手钉住 + 自身不可用时重取）、刷新器语义（单飞/限频/合并/静默降级）、与采用规则（零生效 ∧ 从未不确定）的关系，按 JDK `Lock` 接口级深度撰写。验证=javadoc 构建通过（`show=private`）。 验证=**已验**：`latchRoute` 追加"取用纪律"契约段（起手钉住 + 自身不可用时重取 + 不得因外部刷新重取）；新增 `RouteRefresh` 全成员 Javadoc（单飞/限频/合并/静默降级/协作契约/线程模型）；全量构建含 javadoc 目标。
- [x] 3.3 双语文档对账：`docs/guide/{zh,en}` 涉及改道与持续超时的段落（客户端 SDK 章、故障排查章）同步该增强与其降级条件。验证=中英双侧对应段落均更新、语义一致，无单侧更新。 验证=**已验**：`09-troubleshooting` 第 3 节新增"单连接驻留窗"排查项（中英双侧同步，含稳态零流量、单机不激活、在途不改道、显式失败合法四条）。

## 4. 收口验证

- [x] 4.1 **主判据**：`-Pw11-repro -pl openlatch-client verify` 下两枚红先夹具**翻绿**（各自守卫断言仍成立：成功来自当值 Leader、静默错值即红），≥3 轮不 flaky。 验证=**已验**：`-Pw11-repro` 下两枚夹具**连续 3 轮全绿**（各 `Tests run: 2, Failures: 0, Errors: 0`），守卫断言（成功来自当值 Leader、静默错值即红）仍在。
- [x] 4.2 回归底线：`mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆通过（0 失败 0 错误），严格锚与队列锚不回退；默认构建仍零执行 `w11-repro` 夹具。 验证=**已验**：`mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆 8/8 SUCCESS；客户端 142 单测 + 89 IT 全绿（1 跳过）；`ClientClusterIT` 严格锚 8/8 绿；默认构建仍零执行 `w11-repro` 夹具。
- [x] 4.3 门禁：`bash scripts/check-source-citations.sh` 零命中、`bash scripts/check-links.sh` 零死链。 验证=**已验**：citation 门禁零命中（退出码 0）、链接检查死链 0（退出码 0）。
- [x] 4.4 `WATCHLIST.md` W11 行更新：记录本 change 落地（收敛缺口修复）与先行 change 的关系；若缺口确已消除，按维护规约销账退役。验证=行内容与实测判据一致，不写长文。
