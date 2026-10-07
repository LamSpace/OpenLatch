# Tasks

## 1. 协议 v11 面

- [x] 1.1 `openlatch.proto` 新增 `TIMER_OP = 23`、`TimerOp` 枚举（0–4 五操作：SCHEDULE/DISARM/AWAIT/CANCEL/QUERY）、`TimerOpRequest { key=1, op=2, delay_ms=3(int64), await_request_id=4(int64) }`、`TimerOpResponse { status=1, op=2, generation=3(sint64), armed=4(bool), fire_at_ms=5(sint64), marked=6(bool) }`、`Envelope.payload` 槽 49/50、`LockType.LOCK_TYPE_TIMER = 15` 与管理面 `timer_generation`/`timer_armed`/`timer_fire_at_ms`（`AdminKeyInfo` 自 22 续接）、`AdminTimerWaiterInfo` 与 `timer_waiters_info`（`AdminKeyDetailResponse` 自 38 续接）、`timer_entries`（`AdminSummaryResponse` 续接）字段；`StatusCode` 零新增；`raft.proto` 增 `TIMER_OP_ENTRY = 15`、`TimerOpPayload`、`SnapshotLock` timer 字段（自 42 续接）+ `SnapshotTimerSlot`、`ApplyResult` timer 回执三元组（自 27 续接）。验证：protocol 模块编译，生成代码字段与 design D10 冻结表逐项一致（descriptor diff 复核续接位无错槽）
- [x] 1.2 契约冻结测试扩 v11 面（23 消息类型/16 锁类型/14 状态码/41 payload 消息全量编号表、v1 基线零变更、`AWAIT_NOTIFY` 定义零变更声明）；**编号证据基线更替：既有"RaftEntryType 止于 14、14 为 phaser 专用"与"LockType 止于 14"守卫（`StateMachinePhaserTest`/`StateMachineTopicTest`/`StateMachineConditionTest` 等）改为版本相对口径"止于 15、15 为 timer 专用、1–14 既有值语义逐项不变"，phaser 的 14 号证据随本提案改相对表述**。验证：冻结测试与更替后的编号证据测试绿
- [x] 1.3 `ProtocolCodecTest` 扩 presence 往返：`TimerOpRequest` 五操作全形状（含矩阵违例形：AWAIT 携 `delay_ms`、SCHEDULE 携 `await_request_id`、`CANCEL` 携 `delay_ms` 等）与 `delay_ms=0` 主张/缺省可判别、`TimerOpResponse` 各终态形（QUEUED 观察值/OK 终态/DENIED 终态）字节级保真。验证：往返测试绿

## 2. core：TimerEntry 时钟谓词账簿与裁决

- [x] 2.1 新建 `TimerEntry`（`KeyFamily.TIMER`）：账簿=代次号/装载态（`armed` + 绝对 `fire_at_ms`）/每会话装载去重槽（单槽覆盖式 `{request_id, op, 回声三元组}`）/Leader 易失等待集，条目锁内读写；判定矩阵实现——SCHEDULE 建条目+换代清钟+应用点折算（`fire_at_ms = 条目时刻 + delay`）、去重槽命中回放声零迁移、horizon 受理点钳制（`REJECT_TIMER_DELAY_OVER`）、DISARM 代终结粘滞+同关键区收集唤醒+幂等回声、AWAIT/CANCEL/QUERY 本地谓词重评（到期即刻 OK/代终结即刻 DENIED/未到期 QUEUED 幂等登记/无条目 `REJECT_TIMER_NO_ENTRY`）。验证：`TimerEntryTest` 判定矩阵（装载幂等重发单换代/到期谓词三时点注入 now 判定/重装载换代清钟/撤销终结与唤醒了结/撤销-到期竞态当值裁决/无条目拒绝零建条目/恢复与导出确定性）绿
- [x] 2.2 等待集与三路回收：`(session, request_id) → 登记时刻` 到达序队列住条目内；AWAIT 同 rid 重发命中在集项幂等回执；CANCEL 幂等摘除；`SESSION_CLOSE` 摘除本会话等待位且**账簿零扰动断言**（与 phaser 配额摘除测试刻意反向）；DISARM apply 点收集全体唤醒；`headReplyTimeoutMs` ghost 清扫覆盖已通知未重发项（此项仍在等待集、计入合计——与 phaser 了结窗排除口径的差异常驻注释）。验证：死亡与回收测试矩阵绿
- [x] 2.3 `CoreEngine.timerOp` 门面 + 分派表接入（`LockType.TIMER`/`KeyFamily.TIMER` 注册、跨家族互拒 `REJECT_TYPE_MISMATCH` 含 TIMER↔DELAY_QUEUE/PHASER 双向）；QUERY/到期判定走纯读路径（零迁移断言——无 marked 驻留位写入）；`waiterCount`/明细观察面计入 timer 挂起等待项。验证：`CoreEngineTimerTest`（门面分支全覆盖+观察口径+Follower 语义下等待集空）绿
- [x] 2.4 恢复路径：`CoreStateRestore` 增 TIMER 分支（代次/装载态/到期时刻/去重槽逐字段回灌、等待集恢复为空）、`SnapshotLock` timer 字段序列化/反序列化（去重槽按会话 id 升序确定性导出；同账簿终态不同等待集构造字节等断言夹具）。验证：恢复往返测试 + 序列化确定性（同态双副本字节等）绿
- [x] 2.5 上述全部 core 侧 Javadoc（条目锁单关键区线程模型、"到期不是迁移是时间的兑现"派生谓词声明、死亡零扰动与队列/屏障/phaser 三形态对照句、改期换代对在等者的最新代为准声明、DISARMED 代终结粘滞语义、声明竞态窗口）。验证：`maven-javadoc-plugin`（show=private）构建通过

## 3. 服务端：接入层、复制边界、驱动与可观测

- [x] 3.1 `OpenLatchServer.PROTOCOL_VERSION` 10→11；`RequestDispatcher`/`ClusterRequestHandler` v11 门（v≤10 会话 `TIMER_OP` 同型 `INVALID_REQUEST` 不断连）与 TIMER_OP 分发分轨：SCHEDULE/DISARM 经 `TIMER_OP_ENTRY` 提交通道（受理点预检=门控/形状/delay 上限快速拒绝，零条目）、AWAIT/CANCEL/QUERY Leader 本地直受理零提交；Follower 对五操作词全量同型 `NOT_LEADER` 零副作用（无提示字段判例）；形状互斥矩阵入口裁决唯一在接入层。验证：`TimerGatingTest`（门+五 op 形状矩阵+horizon 钳制+家族互拒含 DELAY_QUEUE 撞 key+单机闭环）绿
- [x] 3.2 新类 `TimerReadyDriver`（判例 `QueueReadyDriver` 逐件同型：Leader 角色短路+网关内再核、单守护线程 `timer-ready-tick-ms` 固定周期、当选立即首扫补偿换任窗、仅对有在集等待者且 PENDING 已越点的 key 全员推 `AWAIT_NOTIFY`、MUST NOT 提交条目/MUST NOT 改复制状态）；`ClusterRuntime`/`ReplicationGateway` 装配接线与 `sweepTimerReady(now)` 网关臂；DISARM 唤醒经 apply 点事件直发（不经本驱动）。验证：装配/角色测试 + 时钟推后到期后唤醒-重发-终态链路测试绿；扫描全程 `TIMER_OP_ENTRY` 零贡献断言
- [x] 3.3 apply 路径接线：`StateMachine`/apply 分派新增 `TIMER_OP_ENTRY` → `CoreEngine.timerOp` 变异应用（SCHEDULE 换代清钟、DISARM 代终结与唤醒收集经既有条目锁外通知桥投递，ref=等待项 request_id，零新推送类型）；`SESSION_CLOSE` 复制应用点触达 2.2 等待摘除钩子（账簿零扰动）；换主事件后新 Leader 等待集空、接纳重挂（幂等登记）与即刻了结路径。验证：模拟 kill 装载者进程后 timer 三读数（代次/armed/fire_at）不变 + 后续 AWAIT 共见断言绿
- [x] 3.4 配置两枚：`max-timer-horizon-ms`（默认 86400000、[1000, 604800000]）与 `timer-ready-tick-ms`（默认 200、≥10）：非法域启动快速失败、启动日志护栏行扩展、`CoreConfig`/`ServerConfig`/`ClusterConfig` 接线；等待深度骑 `max-queue-depth-per-key`（timer 等待项计入合并口径）。验证：config 校验与边界夹具绿
- [x] 3.5 `ServerMetrics` 新命名点：`timer.total{op,status}`（五 op 词表；`{await,DENIED}` 撤销终态线首可达、`{await,OVERLOADED}` 深度线、`{schedule,INVALID_REQUEST}` 越界与门控归线；`BARRIER_BROKEN`/`REJECT_SUBSCRIBERS`/`NOT_HELD` 恒不可达钉死）、`timer.fired.total`（唤醒集合事件口径——无等待者静默到期零计数的夹具断言）；`waiters` Gauge 计入 timer 挂起等待项（含已通知未重发项——与 phaser 了结窗排除的差分注释）；"五口径无 timer 成员"避让注记于命名点源码互注。验证：指标夹具按线归数（含回声重放计 OK、fired 唤醒面口径、静默到期零计数）绿
- [x] 3.6 `AdminRequestHandler` timer 读数：SUMMARY `timer_entries` + 等待者合计含 timer 项；LIST_KEYS 三元组与等待数；KEY_DETAIL timer 区段（三元组+等待明细到达序，`wait_queue_leader_only` 同口径，**呈现面不折算 marked**——原始 `fire_at_ms` 各节点等值断言）；观察零扰动（不推进代次/装载态/到期/清扫）。验证：admin 夹具扩 timer 维（Leader/Follower 双视角 + 三形态钟 + 换主过渡 case + 时钟无关呈现断言 + 与 topic 未命中分轨断言）绿
- [x] 3.7 控制台 timer 呈现：概览条目数与等待口径注记、锁列表 TIMER 行（三元组+等待数 Leader/Follower 标注）、锁详情 timer 区段（三元组/等待列表，"无持有语义"与"到期不折算"如实标注不空壳）。验证：console HTML 测试扩维绿
- [x] 3.8 server 侧全部新类/改动 Javadoc（提交轨与本地轨分界、驱动零日志职责边界与租约到期入日志的对照句、Follower 零副作用、fired 口径注）。验证：javadoc 构建通过

## 4. 客户端 SDK

- [x] 4.1 `OTimer` 公开契约与 `OClient.newTimer(key)` 工厂、`RemoteTimer` 实现：API 面 schedule(delay, unit)→代次回显/disarm()/await()/await(timeout, unit)→true|false、撤销异常收束/isFired()/isArmed()/getRemainingMillis()；接口级 Javadoc 全量三清单（保真/降级/增强：三不提供与替代惯用法、改期最新代为准、死亡不撤钟增强、钟偏移可见性降级、tick 精度契约句、每操作 RTT 成本模型、构造零网络、与 ODelayQueue 选型分界随行注释）。验证：契约审读清单齐备、javadoc 构建通过
- [x] 4.2 直发车道与等待闭环：`TIMER_OP` 请求-应答（`NOT_LEADER` 既有退避改道）；AWAIT 收 `QUEUED` 后按 request_id 过滤 `AWAIT_NOTIFY` 挂起、**原 request_id** 重发取 `OK`/`DENIED` 终态（重复通知以首个终态为准纪律复用）；`await`/`await(timeout)` 超时/中断 → `CANCEL` fire-and-forget → `TimeoutException`/`InterruptedException`；当代撤销唤醒 → `OpenLatchException`（与超时 false 两形态不混读）；活跃等待项双通道自动重挂（车道激活事件+周期保活，判例 v8/v10；重挂恒原 rid——装载去重槽假象双换代防御，**v10 真缺陷教训的同-rid 回归用例本任务同轮落地**）。验证：`RemoteTimerScriptedTest`（到期共见矩阵、超时 CANCEL、撤销异常形态、重挂即刻了结形与续挂形、重发回声单换代）绿
- [x] 4.3 握手常量 10→11（`ConnectionManager`/`RequestMultiplexer`）；`ClientMetrics.statusCodeOf` 扩 TIMER_OP 归线（含 DENIED 终态）。验证：握手回归（v≤10 既有行为逐项不变）与指标归线测试绿

## 5. 集群 E2E 与守卫回归

- [x] 5.1 `ClusterTimerTest`（共享 harness，守 W2 fork 端口纪律，合并负载控制 harness 数）：一到 N 等待者同刻齐过矩阵（广播共见——队列不可表达格的正向证明）、**kill 装载者进程钟照响（头号验收：对标破障/摘除 E2E 形态，三读数不变+后续等待者共见）**、改期双方向（推后在等者续睡、提前先响）、DISARM 唤醒 DENIED 全员了结、换主账簿存续+重挂了结+新主首扫补偿（有界总超时+eventually 续醒形态，禁裸时序断言——v9 D12/W10 教训）、同 request_id 重发单换代、Follower 五操作词零副作用、并发装载竞态（最后写者赢且等待者终见当代判定）。验证：E2E 全测试绿
- [x] 5.2 `StateMachineTimerTest`（复制边界守卫常驻）：纯 timer 流量每变异恰一条 `TIMER_OP_ENTRY`、等待/取消/查询零条目、**时钟推过全体到期点后日志条数/digest/快照恒不变（"到期误入日志即红"反向守卫，与"等待误入日志即红"对偶）**、三副本 digest 逐字节（含到期后查询窗口与 DISARMED 形态）、重启/追赶后命中去重槽重发不双换代、代次单调不回退、到期时刻应用点折算回放不改判、换主等待集清零重挂无损耗；与 1.2 编号证据基线（止于 15）联动。验证：确定性测试绿
- [x] 5.3 快照夹具扩维：`SnapshotFamilyRoundTripTest` timer 分支（三形态钟+多会话槽逐字段保真、恢复无"补 fire"环节断言）、尺寸落界断言（N 键×(常数+会话槽行上限) 不随装载轮次增长）、**等待集零足迹断言（同账簿终态不同等待集/通知时序构造字节等；"已到期"不使两份构造产生字节差）**。验证：三夹具绿
- [x] 5.4 `RejectCodecTableTest` 扩 TIMER_OP 同型拒绝行（NOT_LEADER/门控/形状三码形不得呈"成功空应答"，W10 判例常驻）；协议冻结表、词表/config/指标 vocabulary/控制台断言扩 timer 维；`FairOrderingSuite` 豁免登记（v5 D6/v10 正文同款：AWAIT 广播谓词等待非队首批授，豁免清单落测试代码）。验证：四夹具绿
- [x] 5.5 `ClientTimerIT`（单机内嵌+集群档：schedule→await 闭环、超时自救、撤销异常映射、horizon 拒绝映射）；既有公平性/混沌/换主锚零新增红确认；`BenchmarkMain` 扩 timer 相（装载受理 ops/s、K 等待者同刻了结端到端延迟 P50/P99 含 tick 滞后基线），报告落 `target/benchmark/` 供 W15 引用（exec 注意 `-pl` 不带 `-am`，记忆档）。验证：IT、回归锚状态与基线数据产出

## 6. 文档与治理

- [x] 6.1 ROADMAP：决策记录补"2026-10-06 三档立项裁决（OTimer）"行（覆盖矩阵核验结论——"覆盖即可"收窄为"延时交接由队列覆盖、广播单次标记与改期撤销由本原语承载"、放行依据登记、OCondition/OPhaser 行格式对齐、到期派生零条目裁决与死亡不撤钟契约成行）；三档"延时触发(定时单次标记)"行状态 `挂起`→`进行中` 附本提案链接并改写备注消除"挂起 vs 已兑现"打架（归档时流转已落地）。验证：表格与决策行落盘
- [x] 6.2 WATCHLIST：登记 W15（延时触发唤醒滞后与 fired 速率——tick 精度与在集规模出现运维压力 → 另立 change 评估时间轮/最小堆/按需定时升级，记录位置=本行+design 风险节+基准 timer 相滞后分布）；W11 行扩 v11 注记（TIMER_OP 直发车道加入非 ACQUIRE 暴露面+1，等待侧谓词自愈与装载/查询侧显式超时的非对称声明，沿 v8/v10 注记口径）。验证：两表行落盘
- [x] 6.3 指南 zh/en 对称扩章：01 概念（§13 定时单次标记：派生到期/粘滞共见/代终结/死亡三形态对照表 + 速查行）；03 SDK OTimer 章（三不提供清单、与 ODelayQueue 选型分界矩阵、改期/撤销/超时三形态语义与惯用示例、成本模型）；05 部署（两配置行、回滚窗并列口径——timer 有持久账簿、屏障/队列/phaser 同型"降级前清在途流量"，与 topic/condition 天然干净并列防混读、W15/W11 治理注记）；07 呈现（timer 区段+双速呈现标注+"到期不折算"呈现纪律）；08（`timer.total` 词表与 `{await,DENIED}`/`{await,OVERLOADED}` 判读、`fired.total` 唤醒面之辨、静默到期零计数解释、告警建议）；09 排查（定时标记不生效三分法：tick/门控 → 改期换代核对原始三元组 → 换主重挂；"两机醒来不一先后=钟偏移"判读句）；10（v1–v11 矩阵、门纪律行、升级序服务端先行、回滚并列口径）；术语表新词（装载与清钟/代次/到期派生谓词/共见标记/代终结/唤醒面与到期面之辨）。验证：双语对账无单边缺章、user-documentation delta 场景逐项过
- [x] 6.4 README（双语）协议徽章 v10→v11、原语清单补 OTimer 行（沿上次徽章同步口径）；提交前 `bash scripts/check-source-citations.sh` 全源零命中（含 `--selftest`）+ Javadoc 无内部文档引用。验证：脚本退出码 0、check-links 零死链

## 7. 全量验证（集成）

- [x] 7.1 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆 8/8 模块 BUILD SUCCESS（0 失败 0 错误；跳过项逐一注记原因，基线 = 既有全量测试 + timer 全族新增；既有 v3–v10 门控/矩阵/冻结用例一行不改地绿——回归锚）。验证：构建输出与模块计数
- [x] 7.2 `-Pdrill` 全量复跑（用户在非沙箱单轮执行，负载段扩 timer 相：滚动重启/杀主下重挂了结 + kill 装载者钟照响 E2E；红先保 `target/drill-logs/` 再清理，记忆档单轮防压测伪证、W8 停摆族误判先例）。验证：演练全绿并留存报告
- [x] 7.3 `openspec validate add-otimer --strict` 通过；实现与规格偏差回写 delta 后再归档（sync --strict + grep 双查段头残留，记忆档纪律：delta MODIFIED 须逐字保留全部既有场景——本 change 已按此撰写，validate 即门禁）。验证：validate 输出与归档时主规格状态
