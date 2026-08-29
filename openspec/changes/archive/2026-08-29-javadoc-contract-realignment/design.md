# Design: javadoc-contract-realignment

## Context

动机与范围见 proposal.md（Why / What Changes）。此处只记录影响实施方式的事实：

- **机械门现状**：根 `pom.xml` 的 `maven-javadoc-plugin` 3.12.0 以 `doclint=all` + `show=private` + `failOnWarnings=true` 绑定 `verify`，仅扫描 `src/main`；`openlatch-protocol` 模块 `<skip>true</skip>`（生成代码）。全量 `javadoc:javadoc` 当前 BUILD SUCCESS——本变更不新增任何机械层缺失，纯质量层修复。
- **发现来源**：六路并行审计（core/client/server+protocol/starter+examples 四路主源码 + 两路测试源码），逐条对码取证（本报告引用其行号级证据）。实施时对任一条发现先复核再改写，防止点时快照失真。
- **标准史**：`2026-08-26-normalize-javadoc-and-license` D2 曾将测试注释定为"类级 + 公共夹具方法标签"精简档；`2026-08-26-deepen-javadoc-contract-detail` D3 将 `show=private` 强制范围限定 `src/main`。本次由用户裁决：**测试源码按字面 CLAUDE.md §5 全覆盖**（见 D1）。
- **漂移根因**：「枚举式承诺」句式无机械防线，且行为演进未同步注释（§5.2 失守样本：`ServerSessionTest` 的"限额不可触发"陈述早于 phase1-audit-remediation 引入的写完成背压路径）。

## Goals / Non-Goals

**Goals:**

- 全部注释断言与实现一致（§5.2），尤其公开/wire 语义零假陈述。
- 契约承载类达到"只读注释即可懂契约"（§5.1 Lock 级四维度：职责/线程模型/状态机/契约边界）。
- 测试源码私有成员注释覆盖与主源码同标准（字面 §5）。
- 零行为变化：diff 中除注释字符外不出现任何代码行改动。

**Non-Goals:**

- 不改任何代码逻辑（含 `markClosed` CAS 化等代码侧改进——见 D5 待办清单，仅记录）。
- 不润色已核实为真的注释措辞（延续 normalize"只补缺不润色"）；不重写正确注释。
- 不触碰 protobuf 生成代码；不引入 checkstyle 等新工具（维持 normalize D4 裁定）。
- 不修改构建配置与 `openspec/specs/` 主规格（无行为变化）。

## Decisions

### D1｜标准统一：测试源码按字面 CLAUDE.md §5，废弃归档精简档

用户裁定（2026-08-29）：CLAUDE.md §5 为项目最高注释权威，42 个测试文件的私有字段/方法/构造器/内部 record 一律补 Javadoc；`normalize` D2 测试行与 `deepen` D3 的 src/main 限定仅保留为**构建强制范围**描述（插件确实只扫 main），不再作为内容标准。
替代方案（否决）：维持精简档——与项目现行指示冲突；折中档（仅补"承载断言前提"的 helper）——留下一套无文档背书的二义判例。
**深度仍按 §5.1 比例原则**：测试私有辅助取简洁档（一句话职责 + 隐藏前提，如 `acquire` 工厂必须注明固定 `leaseMs=30_000` 与 `queue=true`），不制造深度。`@Override` 维持"有注释即可、内容允许 `{@inheritDoc}` + 实现差异说明"——但协议可见入口不适用该豁免（见 D4）。

### D2｜同族失真统一 canonical 措辞（防止三处改写互相漂移）

`DENIED` 触发条件的 canonical 句（用于 core `Outcome` 类注释 + `DENIED` 常量 + `CoreEngine.acquire` 排队语义段 + `AcquireCommand.queueIfBusy`，client `LockType.SIMPLE` 同族补齐）：

> 立即式请求（`queueIfBusy=false`/`wait_ms=0`）在**不存在快路径**时返回 `DENIED`：锁被占用，**或**虽无持有者但等待队列非空（队首已通知、待重发窗口——规则 3 禁止越过在队者）。

`OpenLatchException.status()` canonical 枚举（替换"锁丢失不携带状态码"段）：

> 超时、断连类本地失败不携带状态码（`status()` 为 `null`）；**锁丢失可能携带服务端状态码**（`INVALID_TOKEN`/`NOT_HELD`/`SESSION_EXPIRED`，来源：续租被拒、解锁前发现丢失），仅断连/宽限到期路径为 `null`。调用方应判 `status() == null` 为"本地原因"而非"非丢失"。

`shutdown()` canonical 现状句（类级生命周期段与方法注释同步）：

> 关停前对本地登记持锁尽力释放：逐条目发送释放请求，单条目至多等待一个 requestTimeout；失败不阻塞关停，由服务端租约到期兜底。随后执行关停序列并进入终态（幂等）。

`helloResponse` canonical 句：

> 恒携带 `server_protocol_version` 与 `default_lease_ms`（供客户端参考，与结果码无关）；失败路径 `session_id` 为 0。

### D3｜`OLock` 契约面 JDK Lock 级补全内容清单

类级新增「租约与续租」段：授予后由看门狗以 `grantedLeaseMs/3` 周期自动续租、连续 2 次续租超时判失锁、`lock()` 的持有语义以客户端进程存活为前提、锁丢失经 `onLockLost` 通知。方法级补 `@throws`：`lock()`/`tryLock()`/`tryLock(t,unit)` 增补 `ServerUnavailableException`（非 ACTIVE 即时拒绝）、`OpenLatchException`（服务端错误码/传输失败）；`tryLock(t,unit)` 与 `tryLockAsync` 增补 `IllegalArgumentException`（负 `waitTime`，与 `RemoteLock` 校验实现对齐）。`RemoteLock` 各 override 的 `{@inheritDoc}` 实现差异句保持，衍字"以待等待"两处一并改"以等待"。

### D4｜线程模型段与协议入口注释的最低完备要求

- 会话/连接层 7 类（`ServerSession`、`ServerSessionHandler`、`ConnectionManager`、`RequestDispatcher`、`NotifyEventBridge`、`ServerSessionRegistry`、`HeldLockRegistry`）类级必须含 T 段：**各共享字段的保护方式与访问线程**（EventLoop / 扫描线程 / 调用方线程）、串行性前提。`ConnectionManager.stateLock` 字段注释改为如实的受锁字段清单（state/connectFuture/currentBackoffMs/reconnectTask 受锁；channel/session/pendingSession 走 volatile），禁止"全部读写在此锁内"式过强声明。
- 协议可见入口方法（D4 豁免之例外）：`ServerSessionHandler` 四个 override、`NotifyEventBridge.notifyHead`、`EnvelopeCodecHandler.exceptionCaught`、`ServerChannelInitializer.initChannel` 各补方法级 Javadoc——一句话职责 + 触发线程 + 关键分支（endRequest 记账、markClosed 去重、request_id=0、丢弃/关闭语义）；类级矩阵文字保留，方法级不重复展开。
- `exceptionCaught` 类级触发源同步修正为"分帧/反序列化失败**及下游入站异常向 head 传播**"。
- `markClosed` 注释限定前提："仅由该连接 EventLoop 串行调用，故读-判-写即足够幂等"；**不**改代码。

### D5｜注释修复 vs 代码待办的边界

对"注释错但代码可疑"的三处分歧，本变更一律**以代码为准修注释**（§5.2 的"注释语义以代码实现为准"），代码侧改进登记为待办（不在本变更实施）：
1. `markClosed` 若未来允许多线程调用需 `AtomicBoolean` CAS 化；2. `ClientWatchdogIT.HOLD_MS` 死常量与 `ClientFaultInjectionIT.fastExpiry` 重复、`OpenLatchServerTest.configOnPort` 与 `TestServers.config` 重复——清理归后续测试维护变更。
理由：单一职责 diff（纯注释）换取可评审性与零回归风险，深循仓库 M4 先例（6413bd6 "零行为变更"）。

### D6｜实施纪律：防失真三重保险

1. **先复核后改写**：每条发现实施前重读涉事代码分支（审计取证为点时快照，且 git 工作树此后可能演进）；发现不成立者标记"不修复 + 理由"，不盲改。
2. **canonical 句逐字复制**：D2 四句为唯一版本，实施时不得临场改写措辞。
3. **分批 + 模块内 verify**：按 core→client→server→starter/examples→测试（core+client→server+starter+protocol）六批，每批后单模块 `mvn -s /home/lam/repo/settings.xml verify`；最终全量 clean verify。

### D7｜验证门

- `mvn -s /home/lam/repo/settings.xml clean verify` 全绿（机械层防回归 + 测试通过证明零行为影响）。
- **纯注释 diff 断言**：`git diff --unified=0 | grep -E '^[+-]' | grep -vE '^(\+\+\+|---)' | grep -vE '^[+-]\s*(/\*\*|\*|\*/|//)' ` 输出为空（工作树级校验，含内联行尾注释改动时逐行人工确认语义仅为注释）。
- **反向对码清单**：每条被改写的枚举式承诺登记「新注释断言 → 代码行号证据」对照表（附于 tasks G7），HIGH 级 7 点必全列，MED/LOW 改写句抽验；此表即本变更的验收工件。

## Risks / Tradeoffs

- [改写本身引入新失真（deepen 同款风险复发）] → D6-①/② 先复核 + canonical 句 + D7 反向对码清单。
- [测试全覆盖新增 ≈24 条注释产生低价值文本] → 接受（用户裁决 D1）；以简洁档一句话控制体积。
- [审计发现存在假阳性（代理单文件视角局限）] → D6-① 复核环节兜底，不成立者显式记录"不修复"。
- [diff 混入代码行（编辑器自动格式化）] → D7 纯注释断言机检。
- [三处以上同族措辞漂移（DENIED 四处复制后失同步）] → canonical 全文进 design 而非只进 diff；后续维护可 grep 定位同族。
- [约 47 文件单次变更评审量大] → D6-③ 六批分次合入，每批独立 verify（沿用 normalize/deepen 的按模块拆分经验）。

## Migration Plan

纯注释变更，无部署/回滚事项：任一冲突直接 revert 即恢复。合入顺序即 D6-③ 六批。

## Open Questions

无——测试标准档位（D1）与代码/注释边界（D5）均已由用户裁定或按先例定案。
