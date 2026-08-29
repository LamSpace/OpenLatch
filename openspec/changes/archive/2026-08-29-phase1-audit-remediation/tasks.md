# Tasks: phase1-audit-remediation

约定：所有 Maven 命令必须带 `-s /home/lam/repo/settings.xml`；每组代码任务按 CLAUDE.md §5 同步 Javadoc；每组收尾跑该模块 `mvn -s /home/lam/repo/settings.xml -pl <module> -am verify` 全绿后方可勾选下一组。

## 1. 正确性修复（A1/A2，server + client）

- [x] 1.1 server：`RequestDispatcher.errorResponse` 未知 type 以 `MESSAGE_TYPE_UNKNOWN` 占位回 `INVALID_REQUEST`；`ServerSessionHandler.channelRead0` 增加未预期异常兜底（回 `INTERNAL_ERROR` + WARN 带堆栈，不断连）（design D1）
- [x] 1.2 server：补测试——未知 type 数值帧回 INVALID_REQUEST 且连接存活、后续请求正常；mismatch 在 pipeline 层"不断连"直接断言（§10.2）
- [x] 1.3 client：`RequestMultiplexer` PendingRequest 加 generation；`sendWithId` 同 id 重复登记时旧条目以 superseded 异常完成后替换；超时/响应摘除改 `inflight.remove(id, pending)` CAS（design D3）
- [x] 1.4 client：补测试——同 id 双发经真实 multiplexer 双定时器竞争，两条均有界完成、互不误伤；`AwaitTrackerTest` 双通知用例改造走真实路径（§6.4/§6.5）

## 2. 语义收敛与资源修复（A3/A4/A5，core + client + server）

- [x] 2.1 core：`LockEntry` 写侧重入与读侧加入统一 `clamp(requestedLeaseMs==0?default:requestedLeaseMs)` 刷整段；Javadoc 声明口径与"最后加入者决定全体读者租约"推论（design D2，spec 场景「重入按请求租约值刷新」「读侧加入按请求租约值刷新」）
- [x] 2.2 core：补测试——2.1 两个新场景用例；WRITE-WRITE 互斥排队断言；`sessionClosed` 摘除等待项后新队首补通知断言；规则 7 防御分支（队首但类型冲突→保持 QUEUED）；队首异 requestId 重发→排队尾（现状行为固化）（注：规则 7 防御分支经论证为公开 API 不可达的理论不可达路径，以异 requestId 用例固化匹配键语义代替，防御分支保 Javadoc 声明）
- [x] 2.3 client：`OpenLatchClient` 在 `fullyReleased` 且无重持时移除 `keyLockLostListeners` 该 key 条目；`OLock.onLockLost` Javadoc 声明"释放后不复活、重持需重注册"（design D4）
- [x] 2.4 client：补测试——监听表不随历史 key 无界增长（多 key 注册→全释放→表空）；OVERLOADED 计入 watchdog 连续失败（含单次后恢复）；RELEASE 收 INVALID_TOKEN 时 `unlock()` 已丢锁通知分支（实现本已存在于 Watchdog:189-196/RemoteLock:136-143，补的是用例）
- [x] 2.5 server：`ServerConfig.validate()` 下界改 `port >= 0`；port=0 配置加载用例（启动成功 + 实际端口日志断言）（design D5）
- [x] 2.6 server：inflight→OVERLOADED handler 级端到端用例（§5.4）

## 3. 全量回归与 CI 核查

- [x] 3.1 `mvn -s /home/lam/repo/settings.xml clean verify` 全仓绿（7 模块全 SUCCESS、零跳过、约 202 项）；核对无既有用例因 D2/D3 语义收敛而需改断言（`AwaitTrackerTest.duplicateGrantCompensatedWithRelease` 属断言增强而非语义迫使修改）
- [x] 3.2 `ClientProcessKillIT` 缺 shade jar 时输出显式 warning（不再纯静默 skip）；类 Javadoc 说明需先 `package` server 模块；全仓 verify 下实测该 IT 真实执行（2.08s 非 skip）
- [x] 3.3 `BenchmarkMain` 对比基线：无竞争 10860(-3.7%)、16 线程 11294(-1.1%)/P99 2.12(+5.0%)、64 线程 10796(-4.1%)/P99 9.48(+9.2%)，均在 ±15% 内无退化，基线文件不覆盖（重跑件留存 /tmp/benchmark-recheck.md）

## 4. 详设回写 v1.2（纯文档，零代码变更）

- [x] 4.1 修文档内部矛盾 2 处：§4.4 释放规则 4 删"从 LeaseManager 摘除"改陈旧校验口径（对齐 §4.6）；§3.2.2 `lease_ms` 注释改"钳制"口径（对齐 §4.6，定案 A1 疑点 2）
- [x] 4.2 §4 回写：Outcome 细分 `REJECT_KEY_EMPTY/REJECT_KEY_TOO_LONG`；`AcquireCommand.queueIfBusy` 字段与 server 层映射说明；§4.2 清单补 `Owner`/`ReleaseStatus`/core `LockType`；§4.7 sessionClosed"先移除记录后清理"措辞及理由；§4.4 规则 4 写入 D2 刷新口径；规则 5 读侧重入 FIFO 例外声明；规则 7 匹配键 (sessionId, requestId) 字面化；§4.9 论证补条目锁→堆锁嵌套与 DENIED 登记 key 两点
- [x] 4.3 §5/§3 回写：§5.1 删 `IdleEventHandler`（并入 ServerSessionHandler 说明）、补 `ServerSessionRegistry`；§5.2 pipeline 图补 `EnvelopeCodecHandler`；§3.2.1 补 auth_token 非空处置；§5.4 补未知 type 数值行为（1.1 修复后）；§5.7 port 允许 0 说明
- [x] 4.4 §6 回写：§6.1 类树补 `AcquireSpec`/`OpenLatchTimeoutException`/`LockType`/`ClientChannelHandler`/`RemoteLock`/`RemoteReadWriteLock`/`LockDeniedException`；§6.2 孤儿响应补偿语义（D3 定案）；§6.5 重发超时保持挂起（D1 定案）；§6.6 断连期跳过续租不计数（D5 定案）与 OVERLOADED 计数（2.4 修复后）；§6.3 未 ACTIVE 即拒契约与 connect/close 生命周期方法；§6.1/§6.3 监听器不复活语义（D4 修复后）
- [x] 4.5 §8/§9 回写：`LockMode`→`LockType`；切面流程改 `acquireAsync` 直通（D7 定案，删 OLock 句柄缓存/newReadWriteLock 表述）；SpEL"编译缓存"改"解析缓存（解释模式）"；§8.1 注明切面 Bean 拆至 `OpenLatchAspectConfiguration`；§9 "SB3 应用"→Boot 4；文档头版本 v1.1→v1.2 + 修订记录
- [x] 4.6 `git diff --stat` 验证第 4 组仅 `docs/` 变更；对照核对报告 14 项清单逐项勾验
