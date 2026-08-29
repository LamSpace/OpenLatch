# Design: phase1-audit-remediation

## Context

Phase 1 全量交付后的逐条核对（详设 v1.1 ↔ 代码）确认功能无缺失，但暴露 5 个实现缺陷（A1–A5）、测试缺口与约 14 处文档滞后。动机与范围见 proposal.md - Why/What；本文件记录修复的技术决策。仓库约束：Java 25、core 纯 Java 无依赖、mvn 必须 `-s /home/lam/repo/settings.xml`、Javadoc 按 CLAUDE.md §5 分级同步。

## Goals / Non-Goals

**Goals:**
- 消除两类挂等路径（server 未知 type 不回包、client 同 id 覆盖丢 future），使 §10.2/§6.4 的"无静默悬挂"承诺在异常输入下也成立。
- 收敛 core 重入/加入路径的租约刷新口径至单一规则。
- 堵监听器表无界增长；闭环 port=0 配置矛盾。
- B 类测试补强 + C 类文档回写，使详设 v1.2 与代码/openspec 定案三方一致。

**Non-Goals:**
- 不引入 `ACQUIRE_CANCEL`（§12-2 维持"队首响应超时回收"方案）。
- 不做 §5.5 `IdempotencyWindow`（YAGNI 定案不变）。
- 不动协议 `.proto`、公开 API 签名、错误码语义；不重构核对中判定"可接受"的偏差（如孤儿补偿 RELEASE、D1/D5 行为——只回写文档，不改代码）。
- 读锁"加入者刷新全体读者租约"是 A3 口径统一后的直接推论，不另做读者间租约隔离（超出一段租约模型）。

## Decisions

### D1（A1）未知 MessageType：分发层兜底，而非 codec 层断连

`RequestDispatcher.errorResponse` 回显未知 type 时以 `MESSAGE_TYPE_UNKNOWN` 占位；`ServerSessionHandler.channelRead0` 对分发抛出的任何未预期异常兜底回 `INTERNAL_ERROR`（可回显时）或仅记日志，MUST NOT 让异常沉到 Netty tail 造成"不回包不断连"。
备选：在 `EnvelopeCodecHandler` 之前加 type 白名单守卫——否决：解码成功的 Envelope 已无法区分"业务异常"与"未知类型"，白名单与 MessageType 枚举演进耦合（Phase 2 新类型会误杀），兜底在分发层一处收口最小。

### D2（A3）重入/加入统一按"请求值钳制刷整段"

`LockEntry` 写侧重入与读侧加入两条路径均改为 `refresh = clamp(requestedLeaseMs==0 ? default : requestedLeaseMs)`，与规则 4"整段新租期"字面一致；条目内不再区分新旧值来源。
备选 B：两路径都用条目现有值——否决：重入者无法延长短租约，与看门狗"grantedLeaseMs/3"续约模型冲突（客户端永远拿不到延长通道）。
备选 C：读侧加入不碰共享租约——否决：共享到期时刻是单 leaseToken/单租约三元组模型，读者间隔离需改数据结构，超出 Phase 1。
代价（记录于 spec 场景）：最后加入者决定全体读者租约；由服务端 min/max 钳制兜底，且与 Redisson `readLock` 行为兼容性可接受。

### D3（A2）multiplexer 条目身份校验

`PendingRequest` 增加唯一序列号（id 内 generation 计数）；`sendWithId` 遇同 id 已存在时不覆盖：旧条目立即以 `OpenLatchException("superseded")` 完成后替换登记（重发场景旧请求本就应失败让位于新请求）；`onTimeout`/响应摘除一律 `inflight.remove(id, pending)` 双参 CAS。
备选：重发换 requestId——否决：§3.2.3 幂等契约要求"以同一 request_id 重发"，换 id 会二次排队。
`AwaitTracker` 的 waits 表已保证同请求不会并发双发重发（sendOrResend 单飞门闩），本决策是纵深防御 + 消除 onTimeout 误杀面。

### D4（A4）监听表随 fullyReleased 清理

`OpenLatchClient` 在收到 `fullyReleased=true` 的释放响应、且 `HeldLockRegistry` 中该 key 无重持条目时，移除 `keyLockLostListeners` 中该 key 条目。语义按 spec：不复活、需重注册；`OLock.onLockLost` Javadoc 同步声明。
备选：引用计数延迟清理——否决：监听器与持锁生命周期一致即可，重持场景罕见且重注册成本低。

### D5（A5）validate 放行 port=0

`ServerConfig.validate()` 下界改 `port >= 0`；其余校验不动。port=0 的实际端口经 `serverChannel.localAddress()` 已在启动日志输出（`OpenLatchServer:127-130`），无额外改动。

### D6（B 类）测试补强落点

按核对报告清单逐条映射：core `CoreEngineReadWriteFairnessTest`/`CoreEngineSessionIdempotencyLimitTest` 补 WRITE-WRITE 互斥、sessionClosed 新队首补通知、规则 7 防御分支、异 requestId 重发；server `FramingTest`/新建 `UnknownTypeTest` 补 D1 两用例与 mismatch 存活、inflight OVERLOADED 端到端；client `RequestMultiplexerTest` 补 D3 双登记竞争、`WatchdogTest`/`OLockSyncWrapperTest` 补 OVERLOADED 计数与 RELEASE INVALID_TOKEN 分支、`ClientTestServers`/CI 脚本确认 kill IT 不被 skip（skip 时打印 warning 而非静默）。

### D7（C 类）文档回写以代码为准，不反向改代码

核对中所有"代码正确、文档滞后"项（§4.4/§4.6 矛盾、§3.2.2 lease 口径、§4.2/§5.1/§6.1 类清单、D1/D3/D5/D7 定案回写、LockType 命名、§9 Boot 4 等）一律修文档；详设升 v1.2 并在修订记录列 P1-xx 追溯。规则 7 匹配键（sessionId+requestId）与读侧重入 FIFO 例外按现状写入 §4.4/§4.5 正文。

## Risks / Trade-offs

- [D2 改变读侧加入刷新来源] → 现网语义自由度收敛，集成测试（ClientReadWriteIT）与 core 读锁用例回归验证；钳制区间保证不会被请求方拉到无界。
- [D1 兜底路径吞异常可能掩盖真 bug] → INTERNAL_ERROR 回包同时 WARN 日志带堆栈；测试断言"回包 + 连接存活"两条。
- [D3 supersede 完成旧 future 与 AwaitTracker 单飞门闩交互] → superseded 异常仅出现在"同 id 双发"这一本不应发生的场景；AwaitTrackerTest 双通知用例改为经真实 multiplexer 断言两条均有界完成。
- [文档回写量大易引入新漂移] → 纯 docs 批次独立提交，`git diff --stat` 验证零代码变更；对照核对报告 14 项清单逐项勾验。
- [kill IT 在本地无 shade jar 时仍 skip] → 改为显式 warning + 文档说明 CI 需先 `package` server 模块；不强制 skip→fail 以免破坏单模块开发流。

## Migration Plan

无数据/协议迁移：全部为行为收敛与兼容性修复，滚动发布即可；回滚 = revert 提交，无状态残留。发布顺序无跨模块依赖（单仓库原子提交）。

## Open Questions

（无——A3 口径已由 D2 定案；监听器复活语义由 spec 定案为"不复活"。）
