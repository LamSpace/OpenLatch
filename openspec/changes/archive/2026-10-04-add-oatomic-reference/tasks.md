## 1. 协议层（openlatch-protocol）

- [x] 1.1 `openlatch.proto`：`LockType` 加 `LOCK_TYPE_ATOMIC_REFERENCE = 11`；`AtomicOpRequest` 加 `optional bytes operand_bytes=9/expected_bytes=10/initial_bytes=11`；`AtomicOpResponse` 加 `optional bytes old_value_bytes=7/value_bytes=8`；管理面 `AdminKeyInfo` 加 11/12、`AdminKeyDetailResponse` 加 21/22（字段编号与 specs/wire-protocol 增量逐项一致；`MessageType`/`payload` 槽/`StatusCode` 零新增）
- [x] 1.2 `raft.proto`：`ApplyResult` 加 `optional bytes atomic_old_value_bytes=18/atomic_value_bytes=19`；`SnapshotLock` 加 28–31 四字节字段（去重槽骨架与 version 复用既有编号，design D7）
- [x] 1.3 协议回归：两份契约冻结测试（`OpenlatchProtoContractFreezeTest`/`RaftProtoContractFreezeTest`）扩至 v6 面——既有编号零变更断言 + 新增编号钉定（RED→GREEN）；`ProtocolCodecTest` 扩载荷往返：三形态 presence（缺省/零长度/非空）逐格可判别、≥4KB 二进制往返字节级保真
- [x] 1.4 握手版本常量升至 6（`OpenLatchServer.PROTOCOL_VERSION`、客户端 `ConnectionManager`/`RequestMultiplexer` 三处；`HandshakeTest` v6 接受用例 + 越界用例改 7，RED→GREEN）

## 2. core 状态机（openlatch-core）

- [x] 2.1 新建 `AtomicRefEntry implements KeyEntry`（`KeyFamily.ATOMIC`、形态 `ATOMIC_REFERENCE`）：`(value: byte[]+null, version, initial, 去重槽(session,opSeq,载荷应答四元组))`，五操作 `synchronized` 判定顺序与标量条目逐规则同构（形态互拒→ADD 值域外拒绝→初值 presence 断言→GET 短路→同槽重放→执行），CAS 族字节内容相等判定；逐方法 Javadoc 写明"条目不复核载荷尺寸（钳制属接入层）""null≠空串"契约
- [x] 2.2 `LockType` 加 `ATOMIC_REFERENCE`；`CoreEngine` 原子门面按形态分派两条目类（家族定型/跨形态互拒既有规则直扩——四形态同族）、`CoreInspection`/明细观察面扩引用读数；`Outcome` 类注释原子通道口径更新（引用形态 presence 主张与 ADD 拒绝）
- [x] 2.3 `AtomicRefEntryTest`：操作矩阵逐格（五操作 × null/空串/非空字节、命中/不命中、初值主张（含零长度主张）/冲突、版本断言、同槽重放字节级一致、GET 不建条目回 (null,0)）；`CoreEngineAtomicTest` 扩标量↔引用跨形态互拒、会话关闭载荷零触碰、条目侧超限回放照常落值（不复核配置的判据用例）
- [x] 2.4 `CoreStateRestore` 加 `AtomicRefState`（十字段自包含）与双向序列化；`AtomicRefEntry.restored(...)` 重建工厂；`CoreEngine.restoreFrom` 扩引用形态分支

## 3. 复制路径（openlatch-server/raft）

- [x] 3.1 `LockStateMachineCore.applyAtomicOp` 按 `lock_type` 分派引用形态入引擎门面；`ApplyResult` 载荷回执 18/19 装配；`toAtomicOpResponse` presence 回显（拒绝态 bytes 全缺省）
- [x] 3.2 `ShadowTable`：`SLock`/`AdminEntryView` 扩引用字段（size/preview 源数据、载荷值）与登记点；无租约不入到期堆断言沿用
- [x] 3.3 应用确定性测试：含载荷写/GET 的同一命令序列 Leader/新副本回放终态逐字节一致（摘要含载荷字节）；配置漂移用例——回放节点 `maxValueBytes` 调小仍与 Leader 终态一致（分歧不可能性的可执行证明）

## 4. 分发、门控与钳制（openlatch-server/dispatch + net）

- [x] 4.1 `ServerConfig` 加 `maxValueBytes`（`openlatch.server.limit.max-value-bytes`，默认 4096，validate [1, 512KiB]）+ 启动日志关键限额列出；不进 `CoreConfig`
- [x] 4.2 `RequestDispatcher.validateAtomicRequest` 扩形：引用形态标量槽位 MUST 零、非引用形态 bytes MUST 缺省、引用携 ADD 拒绝（形状矩阵逐格）；单机分发路径引擎调用前载荷钳制（超限 INVALID_REQUEST 零触碰）
- [x] 4.3 `ClusterRequestHandler.handleAtomicOp`：v6 门（`session.protocolVersion() < 6` 携引用形态 → INVALID_REQUEST 消息级拒绝、不断连，判例 v4/v5 门）+ `gateway.submit` 前载荷钳制——超限零日志条目（断言提交前后日志位点不动）
- [x] 4.4 门控与钳制测试：`AtomicGatingTest`（判例 `LatchGatingTest`）扩 v5 会话引用形态拒不断连、v≤4 既有回归不变、ACQUIRE/其余消息携 11 号拒绝；`MessageLegalityTest` 扩形状矩阵；钳制边界矩阵（恰限/超一字节/下探）× 两部署形态；配置下调后存量读不变/新写拒
- [x] 4.5 指标：`ServerMetrics` kind 词表加 `reference`（单一命名点）；`ServerMetricsVocabularyTest` 扩表；超限拒绝走 `status=INVALID_REQUEST` 线断言

## 5. 快照与恢复（openlatch-server/snapshot）

- [x] 5.1 `SnapshotLock` 引用形态生成/加载双向（28–31 + 骨架复用；presence 口径：null 缺省、空串零长度、标量值字段恒缺省）；序列化确定性含载荷字节
- [x] 5.2 恢复测试：载荷/null/空串/恰限四态快照→重启→回放逐字段保真；重启后命中槽重发不双写；v5 旧夹具（无引用字段）加载兼容回归
- [x] 5.3 快照膨胀治理回归夹具：N 个引用 key × 恰限载荷多轮版本推进 → 生成快照断言尺寸 ≤ N×(2×maxValueBytes+常数)×安全系数且不随轮次增长 → 安装新副本逐字段保真、追赶回放正常

## 6. 管理面与控制台

- [x] 6.1 `AdminRequestHandler`：SUMMARY `atomic_entries` 含引用断言；LIST_KEYS/KEY_DETAIL 装配 `atomic_payload_size` + `atomic_payload_preview`（服务端截断转义至 64B、恒定长度不随配置膨胀、null 与空串呈现可区分；全量载荷零外发）
- [x] 6.2 console 五页面扩呈现（列表行 reference 形态大小+预览、详情卡引用值区段、null 态如实）；`AdminClusterTest`/`AdminProtocolTest`/控制台页面断言扩用例（含恰限大载荷预览截断断言）

## 7. 客户端 SDK（openlatch-client）

- [x] 7.1 公开接口 `OAtomicReference` + 内嵌 `Stamped`：byte[] 主形态 + String(UTF-8) 便利族（get/getAsString/getStamped/getVersion/set/getAndSet/compareAndSet/compareAndSetStamped 各形态）；无 ADD/accumulate 面；契约 Javadoc 按 Lock 接口级撰写降级/增强清单（RTT、不确定窗、ABA 仅 stamped、载荷不透明服务端永不反序列化、null/空串两态、服务端权威钳制默认 4KB、条目常驻不回收、需 v6 握手）
- [x] 7.2 `RemoteAtomicReference extends RemoteAtomicBase`：bytes 通道请求构造、op_seq 与同序号重发继承既有车道；超限/低版本拒绝归"不可重试→显式异常"分类（不重试不截断）
- [x] 7.3 `OpenLatchClient.newAtomicReference` 工厂（无主张/byte[] 主张/String 主张；null 主张=不主张契约）；握手版本声明升 6
- [x] 7.4 客户端单测：工厂类型与参数校验、presence 三形态请求装配断言、String null 语义

## 8. 集群 E2E 与混沌

- [x] 8.1 `ClusterAtomicRefTest`（判例 `ClusterAtomicTest`）：多客户端并发 CAS/CAS_STAMPED 矩阵（Σ成功 CAS == Δversion、终值 ∈ 候选字节集且字节级一致）、4KB 热键并发写不丢不双加、`getAndSet` 双端交错
- [x] 8.2 kill 进程裁决 E2E：引用写入的会话进程被杀 → 载荷值与版本戳纹丝不动（归属无关锚引用形态版）；换主窗口 SDK 层同 `op_seq` 重发不双写锚（判例 `ClientClusterIT` 原子锚）
- [x] 8.3 兼容矩阵 E2E：新客户端 × v5 服务端不可在单仓实例化（旧 jar 不共存，判例 tasks 1.3），以 `AtomicGatingTest.v5SessionRefRejectedWithoutDisconnectScalarRegressionUnchanged`（v5 会话引用拒不断连+标量回归不变）+ SDK INVALID_REQUEST 不重发车道 + 指南 v1–v6 兼容矩阵注记承担；v≤5 既有全量回归由本轮 `clean verify` 全绿承担
- [x] 8.4 公平套件豁免记录（本条即豁免正文）：`FairOrderingSuite*` 对引用形态不适用——同标量原子判例（无等待队列/无挂起/即时线性化应答），豁免理由随 ATOMIC 家族既有条目一并扩写
- [x] 8.5 混沌对齐：`ClientChaosIT` 混合负载按既有 ATOMIC 口径不扩引用面（针对性场景由 8.1/8.2 承担，判例同标量原子）

## 9. 基准与文档

- [x] 9.1 `BenchmarkMain` 扩引用相：热键 4KB SET 吞吐、CAS 争用放大、GET 延迟基线、快照生成时长对比（运行 `-pl` 不带 `-am`，报告标题按阶段手改）
- [x] 9.2 双语指南：01-concepts"有值引用与小载荷边界"节、03-client-sdk `OAtomicReference` 用法与语义降级/增强声明清单、05-cluster-deployment 载荷快照尺寸治理与回滚窗口注记、08-observability kind 词表、10-compatibility v1–v6 矩阵、术语表（载荷钳制/截断预览/有值引用）；zh/en 对称
- [x] 9.3 ROADMAP.md 本行状态 → 已落地（归档时），提案立项即置"进行中"并附提案链接（如尚未更新则先补）；载荷通道基建行注记承载 change；同 key 同类型约定登记（ATOMIC 家族四形态含 REFERENCE）
- [x] 9.4 全量验证：`mvn -s /home/lam/repo/settings.xml clean verify` 全绿 + `-Pdrill` 单轮复跑（非沙箱终端）+ `bash scripts/check-source-citations.sh --selftest` 后自查零命中。注记：演练复跑暴露进程级夹具哑弹缺陷——`LeaderKillDrillIT`/`PartitionDrillIT`/`RollingRestartDrillIT`/`ClientProcessKillIT` 钉死 `1.0-SNAPSHOT-executable.jar` 文件名，1.0.0 发版起查找恒 null、用例静默跳过假绿（与 v6 无关，发现于本验证轮）；已改前后缀版本无关匹配并以 `ClientProcessKillIT` 实跑转绿为证，`-Pdrill` 须再复跑一轮以让三个进程级演练真实生效。复跑记录（2026-10-03）：进程级三套件全部真跑——LeaderKill 3/3、RollingRestart 3/3（写风暴错误 0.61%/1.82% 全落重启窗、tailErrors=0）、Idle 绿；PartitionDrillIT 首跑以 300s 超时红——定性为夹具双重缺陷（`Sudo.exec` 漏 `-n` 违反其自身 passwordless 契约，凭证 5 分钟过期后 teardown 直开 /dev/tty 抢终端挂人工；`run()` 无界 readAllBytes 先于 waitFor，30s 超时失效；当轮功能判据全绿，非产品回归、与 v6 无关），单点修复后本机真跑全场景绿（4.4s、报告落盘、netns/iptables/bridge 零残留）。用户侧已复跑（sudo 正常，未挂人工）：PartitionDrillIT 真跑 4.4s 绿（修复在真环境闭环）；同轮 `LeaderKillDrillIT` 场景 A 端到端恢复观察窗 1 红（该轮整机高负载、60s 观察窗无授予完成）——复验 3 独立 + 2 顺序复刻（partition→kill 同 fork）全绿，产品不变式判据（少数派不授予/无双主/失锁不误判/重启后可授予）历轮皆绿，定性为负载敏感间歇红（重试环不抛弃在途请求系已知放大窗口），未获确定性复现证据不改产品码；登记 WATCHLIST 观察（kill→新主端到端恢复观察窗高负载间歇红），本 change 据此收口。00:57 二轮复跑：LeaderKill 3/3 绿（印证间歇定性）、PartitionDrillIT 真跑绿；RollingRestart 两红（先主后从 tailErrors=59 持续至 214s；extendedPrimitives 首请求 8s 超时）——现场 drill-logs 取证定性为 Phase 2 已签署接受的选举风暴形态 B 再显形（PRE_VOTE REJECTED 风暴：落后候选以高频改选持续重置最先进节点的选举计时，log-precedence 拒绝成立、无人可胜；stallEvents=0 排除形态 A），非 v6 触达面、不改本 change 产品码；已登 WATCHLIST W8（复现并入 W4 Ratis 升级对账）。9.4 据此闭环：v6 门禁内全绿，红点均归因已知接受残余并以 W 表承载
