# Tasks

## 1. 协议 v10 面

- [x] 1.1 `openlatch.proto` 新增 `PHASER_OP = 22`、`PhaserOp` 枚举（0–6 七操作）、`PhaserOpRequest { key=1, op=2, parties=3, expected_phase=4(sint64 presence), await_request_id=5 }`、`PhaserOpResponse { status=1, op=2, phase=3, registered=4, arrived=5 }`、`Envelope.payload` 槽 47/48、`LockType.LOCK_TYPE_PHASER = 14` 与管理面 `phaser_phase`/`phaser_registered`/`phaser_arrived`/`AdminPhaserWaiterInfo`/`AdminPhaserPartyInfo`/`phaser_entries` 续接字段；`StatusCode` 零新增；`raft.proto` 增 `PHASER_OP_ENTRY = 14` 与 `SnapshotLock` phaser 字段（35 起续接）。验证：protocol 模块编译，生成代码字段与 design D11 冻结表逐项一致
- [x] 1.2 契约冻结测试扩 v10 面（22 消息类型/15 锁类型/14 状态码/39 payload 消息全量编号表、v1 基线零变更、`AWAIT_NOTIFY` 定义零变更声明）；**编号证据基线更替：既有"RaftEntryType 止于 13"守卫（`StateMachineTopicTest`/`StateMachineConditionTest` 等）改为"止于 14、14 为 phaser 专用、1–13 既有值语义不变"的版本相对口径，topic/condition 零占用证据随之改为相对表述**。验证：冻结测试与更替后的编号证据测试绿
- [x] 1.3 `ProtocolCodecTest` 扩 presence 往返：`PhaserOpRequest` 七操作全形状（含矩阵违例形：REGISTER 携 `expected_phase`、`AWAIT_ADVANCE` 携 `parties`、`CANCEL` 携 `expected_phase` 等）与 `expected_phase` 未主张/0 主张二态可判别、应答字节级保真。验证：往返测试绿

## 2. core：PhaserEntry 相位账簿与裁决

- [x] 2.1 新建 `PhaserEntry`（`KeyFamily.PHASER`）：账簿=相位号/`registered`/每会话配额表/当前 `arrived`/`(session,request_id)` 去重槽/有界了结记录，条目锁内读写；判定矩阵实现——REGISTER 建条目+配额加计+上限拒绝（`REJECT_PHASER_PARTIES`）、ARRIVE 去重与计数、`ARRIVE_AND_AWAIT`/`ARRIVE_AND_DEREGISTER` 同关键区双半程（扣配额先于合拢判定、零配额 `REJECT_PHASER_QUOTA`）、合拢 `arrived ≥ registered` 单推进（phase++/arrived 清零/槽清空/了结滚动/notify 收集锁外触发）、归零空转不终止、非 REGISTER 无条目 `REJECT_NO_ENTRY`。验证：`PhaserEntryTest` 判定矩阵（合拢单推进/去重重放单计数/中途注册入当相位不触发判定/扣减-到场双序同终态/幽灵到场存续/空转复活/无条目拒绝/恢复 `restored` 往返）绿
- [x] 2.2 等待集与三路回收入 `PhaserEntry`：`(session,request_id) → (expected_phase, 登记时刻)` 到达序队列；`AWAIT_ADVANCE` 幂等登记与"已推进即刻了结"直回；`CANCEL` 幂等摘除；`SESSION_CLOSE` 同关键区执行配额摘除+等待位摘除+合拢重判（死亡摘除测试：已到场者死/未到场者死/归零空转三形态，其他会话零扰动）；`headReplyTimeoutMs` ghost 清扫覆盖已通知未重发项。验证：死亡与回收测试矩阵绿
- [x] 2.3 `CoreEngine.phaserOp` 门面 + 分派表接入（`LockType.PHASER`/`KeyFamily.PHASER` 注册、跨家族互拒 `REJECT_TYPE_MISMATCH` 含 BARRIER↔PHASER 双向）；`QUERY` 门面走纯读路径（零迁移断言）；`waiterCount`/明细观察面计入 phaser 挂起等待项（了结窗项不计——对照条件"已搬运不重复"随行注释）。验证：`CoreEnginePhaserTest`（门面分支全覆盖+观察口径+Follower 语义下等待集空）绿
- [x] 2.4 恢复路径：`CoreStateRestore` 增 PHASER 分支（账簿逐字段回灌、等待集恢复为空）、`SnapshotLock` phaser 字段序列化/反序列化（配额表会话 id 升序、槽/了结记录确定性序导出）。验证：恢复往返测试 + 序列化确定性（同态双副本字节等）绿
- [x] 2.5 上述全部 core 侧 Javadoc（条目锁单关键区线程模型、Leader-only 等待集声明、死亡摘除与"到场是事实"因果叙事、空转不终止与 JDK 差异、了结窗口声明竞态）。验证：`maven-javadoc-plugin`（show=private）构建通过

## 3. 服务端：接入层、复制边界与可观测

- [x] 3.1 `OpenLatchServer.PROTOCOL_VERSION` 9→10；`RequestDispatcher`/`ClusterRequestHandler` v10 门（v≤9 会话 `PHASER_OP` 同型 `INVALID_REQUEST` 不断连）与 PHASER_OP 分发分轨：四变异操作经 `PHASER_OP_ENTRY` 提交通道（受理点预检=门控/形状/上限快速拒绝，零条目）、`AWAIT_ADVANCE`/`CANCEL`/`QUERY` Leader 本地直受理零提交；Follower 对七操作词全量同型 `NOT_LEADER` 零副作用（无提示字段判例）；形状互斥矩阵入口裁决唯一在接入层。验证：`PhaserGatingTest`（门+七 op 形状矩阵+parties 钳制+家族互拒+QUERY 形状）绿
- [x] 3.2 apply 路径接线：`StateMachine`/apply 分派新增 `PHASER_OP_ENTRY` → `CoreEngine.phaserOp` 变异应用（合拢唤醒与死亡摘除的 notify 收集经既有条目锁外通知桥投递 `AWAIT_NOTIFY`，ref=等待项 request_id，零新推送类型）；`SESSION_CLOSE` 复制应用点触达 2.2 摘除钩子；换主事件后新 Leader 等待集空、接纳重挂（幂等登记）与即刻了结路径。验证：装配/角色测试 + 模拟 kill 参与者进程后 `arrived`/`registered`/等待数三读数收敛断言绿
- [x] 3.3 配置 `max-parties-per-phaser`（默认 1024、[1,65536]）：非法域启动快速失败、启动日志护栏行扩展、`CoreConfig`/`ServerConfig` 接线。验证：config 校验与边界夹具绿
- [x] 3.4 `ServerMetrics` 新命名点：`phaser.total{op,status}`（七 op 词表；两超限 `OVERLOADED` 以 op 分轨——`{register,OVERLOADED}` 配额线 vs `{await_advance,OVERLOADED}` 深度线，`DENIED`/`BARRIER_BROKEN`/`REJECT_SUBSCRIBERS`/`NOT_HELD` 恒不可达钉死）、`phaser.parties.registered.max`；`waiters` Gauge 计入 phaser 挂起等待项；四口径注释互引扩五口径（命名点源码互注）。验证：指标夹具按线归数（含重放计 OK、两超限分轨对照场景）绿
- [x] 3.5 `AdminRequestHandler` phaser 读数：SUMMARY `phaser_entries` + 等待者合计含 phaser 项；LIST_KEYS 三计数与等待数；KEY_DETAIL phaser 区段（三计数+配额明细会话升序+等待明细到达序，`wait_queue_leader_only` 同口径，**双速呈现：Follower 账簿可读而等待明细如实零**）；观察零扰动（不推进相位/到场/清扫）。验证：admin 夹具扩 phaser 维（Leader/Follower 双视角 + 换主过渡 case + 与 topic 未命中分轨断言）绿
- [x] 3.6 控制台 phaser 呈现：概览条目数与等待口径注记、锁列表 PHASER 行（三计数+等待数 Leader/Follower 标注）、锁详情 phaser 区段（三计数/配额列表/等待列表，"无持有语义"如实标注不空壳）。验证：console HTML 测试扩维绿
- [x] 3.7 server 侧全部新类/改动 Javadoc（提交轨与本地轨分界、Follower 零副作用、五口径互引注）。验证：javadoc 构建通过

## 4. 客户端 SDK

- [x] 4.1 `OPhaser` 公开契约与 `OClient.newPhaser(key)` / `newPhaser(key, initialParties)` 工厂、`RemotePhaser` 实现：API 面 register/bulkRegister/arrive/arriveAndDeregister/arriveAndAwaitAdvance/awaitAdvance/awaitAdvanceInterruptibly(phase,timeout,unit)/getPhase/getRegistered·Arrived·UnarrivedParties；接口级 Javadoc 全量三清单（保真/降级/增强：三砍差异与返回值自实现替代惯用法、配额严格归属与 JDK 匿名 party 对照、查询 Leader 本地非线性化、等待跨换主自愈与条件 signal 丢失窗对照防混读句、每操作 RTT 成本模型、构造零网络与 initialParties 会话作用域注册、重连重执与超调保守方向声明、句柄无状态跨进程等价）。验证：契约审读清单齐备、javadoc 构建通过
- [x] 4.2 直发车道与等待闭环：`PHASER_OP` 请求-应答（`NOT_LEADER` 既有退避改道）；等待收 `QUEUED` 后按 request_id 过滤 `AWAIT_NOTIFY` 挂起、原 request_id 重发 `AWAIT_ADVANCE` 取数了结（重复通知以首个为准纪律复用）；`awaitAdvanceInterruptibly` 超时/中断 → `CANCEL` fire-and-forget → 抛对应异常（`TimeoutException`/`InterruptedException`）；活跃等待项双通道自动重挂（车道激活事件+周期保活，判例 v8；重执 REGISTER(initialParties) 随会话重建，2.2 幂等承接）。验证：`RemotePhaserScriptedTest`（合拢矩阵、超时 CANCEL、旁观 await、重挂即刻了结形与续挂形、initialParties 重执）绿
- [x] 4.3 握手常量 9→10（`ConnectionManager`/`RequestMultiplexer`）；`ClientMetrics.statusCodeOf` 扩 PHASER_OP 归线。验证：握手回归（v≤9 既有行为逐项不变）与指标归线测试绿

## 5. 集群 E2E 与守卫回归

- [x] 5.1 `ClusterPhaserTest`（共享 harness，守 W2 fork 端口纪律，合并负载控制 harness 数）：跨进程 2×N/3×N 合拢矩阵与返回值保真、中途 REGISTER 入当相位应到集、`arriveAndDeregister` 提前合拢、**kill 参与者进程死亡摘除不空转（头号验收：已到场者死即时推进形 + 未到场者死缩应到形，对标 Barrier 破障 E2E 形态）**、换主重挂无损耗（有界总超时+eventually 续醒形态，禁裸时序断言——v9 D12/W10 教训）+ D10 重执双时序形态（重建快于失联判定→超调保守方向、摘除后收敛）、同 request_id 重发单到场、Follower 七操作词零副作用、并发到场不丢不重。验证：E2E 全测试绿
- [x] 5.2 `StateMachinePhaserTest`（复制边界守卫常驻）：纯 phaser 流量每变异恰一条 `PHASER_OP_ENTRY`、等待/取消/查询贡献零条目、三副本 digest 逐字节（含死亡摘除跨副本同判、配置漂移不分歧）、重启/追赶后命中去重槽重发不双计数、相位单调不回退、换主等待集清零重挂无陈旧唤醒；**误塞日志即红的对照断言**与 1.2 编号证据基线（止于 14）联动。验证：确定性测试绿
- [x] 5.3 快照夹具扩维：`SnapshotFamilyRoundTripTest` phaser 分支（推进相位+配额+在场槽+了结记录逐字段保真）、尺寸落界断言（N 键×配额上限×多轮推进字节 ≤ N×(常数+配额行上限+在场窗上限) 不随轮次增长）、**等待集零足迹断言（同账簿终态不同等待集/通知时序构造字节等）**。验证：三夹具绿
- [x] 5.4 `RejectCodecTableTest` 扩 PHASER_OP 同型拒绝行（NOT_LEADER/门控/形状三码形不得呈"成功空应答"，W10 判例常驻）；协议冻结表、词表/config/指标 vocabulary/控制台断言扩 phaser 维；`FairOrderingSuite` 豁免登记（v5 D6 正文同款，豁免清单落测试代码）。验证：四夹具绿
- [x] 5.5 `ClientPhaserIT`（单机内嵌+集群档：register/arriveAndAwaitAdvance 闭环、超时自救、配额透支拒绝映射）；既有公平性/混沌/换主锚零新增红确认；`BenchmarkMain` 扩 phaser 相（到场受理 ops/s、K 方合拢推进延迟 P50/P99），报告落 `target/benchmark/` 供 W14 引用（exec 注意 `-pl` 不带 `-am`，记忆档）。验证：IT、回归锚状态与基线数据产出

## 6. 文档与治理

- [x] 6.1 ROADMAP：决策记录补"2026-10-06 三档立项裁决"行（OPhaser 需求信号未至提前立项复刻、放行依据登记、OCondition 行格式对齐）；三档 OPhaser 行 未启动→进行中附本提案链接（归档时流转已落地）。验证：表格与决策行落盘
- [x] 6.2 WATCHLIST：登记 W14（热点 phaser key 条目率——每 party 每相位一条日志，触发=生产观测条目率/日志规模成运维压力 → 另立 change 评估读路径折案或批量到场合并，沿 W6/W7/W9 口径，记录位置=本行+design 风险节+基准 phaser 相）；W11 行扩 v10 注记（PHASER 直发车道加入非 ACQUIRE 暴露面+1，等待侧双通道自愈与变异/查询侧显式超时的非对称声明，沿 v8 topic 注记口径）。验证：两表行落盘
- [x] 6.3 指南 zh/en 对称扩章：01 概念（§12 相位与配额、合拢规则、死亡摘除与破障对照、三不提供）+ 速查行；03 SDK OPhaser 章（三砍差异清单、配额严格化、替代惯用法示例、成本模型、自愈与 signal 丢失窗对照）；05 部署（`max-parties-per-phaser` 配置行、回滚窗第四口径——phaser 有持久账簿、屏障/队列同型"降级前清在途流量"，与 topic/condition 天然干净并列防混读、W14/W11 治理注记）；07 呈现（phaser 区段+双速呈现标注）；08（`phaser.total` op 词表与两超限 op 分轨判读、五口径之辨、水位与增速告警建议）；09 排查（相位不推进三分法：到场不足查配额明细→车道窗 W11 判别→应用簿记核对；区别于 await 不醒的"根因在到场不足非事件丢失"对照句）；10（v1–v10 矩阵、门纪律行、升级序服务端先行、回滚第四口径）；术语表新词（到场相位/应到集合/注册配额/隐式摘除/空转复活/双速呈现）。验证：双语对账无单边缺章、user-documentation delta 场景逐项过
- [x] 6.4 提交前 `bash scripts/check-source-citations.sh` 全源零命中（含 `--selftest`）+ Javadoc 无内部文档引用。验证：脚本退出码 0

## 7. 全量验证（集成）

- [x] 7.1 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆 8/8 模块 BUILD SUCCESS（0 失败 0 错误；跳过项逐一注记原因，基线 = 既有 1916 测试 + phaser 全族新增）。验证：构建输出与模块计数
- [x] 7.2 `-Pdrill` 全量复跑（用户在非沙箱单轮执行，负载段扩 phaser 相：滚动重启/杀主下重挂自愈 + 死亡摘除 E2E；红先保 `target/drill-logs/` 再清理，记忆档单轮防压测伪证）。验证：演练全绿并留存报告
- [x] 7.3 `openspec validate add-ophaser --strict` 通过；实现与规格偏差回写 delta 后再归档（sync --strict + grep 双查段头残留，记忆档纪律）。验证：validate 输出与归档时主规格状态
