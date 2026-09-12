# 09 · 故障排查与 FAQ

## 1. 错误码语义总表

| 错误（协议码 / 客户端异常） | 出自哪里 | 含义 | 你该怎么做 |
|---|---|---|---|
| `QUEUED` | 服务端 | 已入队，等通知重发 | 正常排队，无需动作 |
| `LOCK_HELD` | 服务端 | 立即式获取时锁在他人手 | 按业务重试或走 `lock()` |
| `NOT_LEADER` | 服务端（转发/角色道） | 本节点非 Leader，**可重试**，随附提示 | 交给客户端自动改道；持续出现看第 3 节 |
| `NOT_HELD` | 服务端（权威裁决） | 该会话确未持有此锁（多数派提交后判定） | 视为失锁：中止提交、重新竞争 |
| `INVALID_TOKEN` | 服务端（权威裁决） | 释放/续租凭据与当前归属不符（常见：failover 回滚/租约已过期） | 同上，走失锁处理 |
| `SESSION_EXPIRED` | 服务端 | 会话已关闭 | 重连后重新竞争（客户端自动，业务收失锁回调） |
| `INVALID_REQUEST` | 服务端 | 协议违例：字段非法、握手前业务请求、版本越界、认证失败（不区分原因） | 检查调用方/配置；认证类查令牌 |
| `INTERNAL_ERROR` | 服务端 | 非预期内部失败 | 可重试；持续出现带日志找运维 |
| `LockAcquisitionTimeoutException` | 客户端 | 等待预算（默认 30s）耗尽 | 视业务：加大预算/拆临界区/降级 |
| `OpenLatchTimeoutException` | 客户端 | 单请求 5s 无应答（连接活着） | 检查节点负载/时钟；瞬时一次可容忍 |
| `ServerUnavailableException` | 客户端 | 连接不可用（含切换窗口快速失败） | 重试；检查种子配置 |
| `IllegalMonitorStateException` | 客户端 | 未持有而解锁/归还 | 修代码路径（生命周期管理） |
| `LockLostException`（回调） | 客户端 | 锁被剥夺 | 中止临界区提交——这是设计必答题 |

## 2. 故障转移行为基线（实测数据）

进程级演练在参考硬件（3 节点同机）上的量级，供告警阈值校准：

- `kill -9` Leader → 新授予恢复：**实测 1.6–1.7s**（判据上限 10s）；
- 杀 Follower → 业务无感（多数派在）；
- 滚动重启逐台 → 应用可见错误集中于每台 2–3s 窗口，末窗 + 45s 自愈预算后**残留必须为 0**；
- 等待队列位次在 Leader 切换后重排（预期，非故障）。

## 3. 反复 `NOT_LEADER` / 找不到 Leader

排查顺序：

1. **种子给全了吗**——只提供单地址且该地址宕机 = 无法自动恢复；
2. `client-addresses` 与实际接入地址是否一致（配错会让提示指向不可达地址，靠种子自报兜底但多绕一圈）；
3. 三节点是否真的多数派存活（`curl :9412/metrics | grep is_leader`，全零=无主）；
4. 无主且选举频繁 → 时钟漂移或网络分区（NTP 要求漂移 ≤1s）；
5. 有 Leader 但写持续失败且日志见 `replication stall detected` → 第 4 节。

## 4. 复制停摆（写面冻结）与自愈日志

服务端看门狗的日志序列（每节点）：

```
replication stall detected          # 判定：任期超阈值 + commitIndex 连续零推进
...abdicate/让位事件                 # 第一处置：向最新对侧让位，多数派重选恢复
escalating to process restart       # 观察窗仍冻结 → 进程退出码 1 自杀，等 supervisor 拉起
```

运维动作：

- **有 supervisor（systemd/K8s）**：确认重启退避 ≥ 5 分钟（进程内冷却窗），事件本身自愈，记录即可；
- **无 supervisor**：让位段仍会自动恢复；`escalating to process restart` 后**需人工重启该节点**，
  生产集群强烈建议配 supervisor（详见 [05 停摆自愈](05-cluster-deployment.md)）；
- 停摆窗口内的客户端表现：获取超时/`ServerUnavailable` 类错误聚集，恢复后归零——
  若"末窗+预算后仍有错误"，这不是运维问题而是产品回归，带日志报障。

## 5. 部署/构建类常见问题

| 症状 | 原因 | 处置 |
|---|---|---|
| 启动失败：`管理端口启动失败（端口 9412 可能被占用）` | 同机多节点指标口冲突（fail-fast 设计） | 每节点配互异 `metrics.port` 或 `0` |
| 演练"通过"但什么都没发生 | 缺 shaded jar / 无 sudo 时 assume-**skip**（不是失败） | 先 `mvn -pl openlatch-server -am package`；查 `Skipped: 0` |
| Spring 注解不生效 | 未开 `-parameters`；或自调用绕过代理 | 补编译器参数；经代理调用 |
| 同线程第二次 `lock()` 卡死到超时 | 用了 SIMPLE 锁（非可重入，属语义） | 换 REENTRANT |
| 客户端启动即报错 | 服务端未起/地址不通——首连失败不影响 bean 创建，异常在首次调用 | 检查 9410 连通性 |
| 锁"莫名消失" | 临界区超过续租能力（长 GC/停顿）或会话被清理 | 看失锁回调与 `locks.lost` 指标；缩短临界区/调大租约 |
| failover 后个别锁失效 | 切换窗口未复制达多数派的授予回滚 | 设计内（[05 一致性声明](05-cluster-deployment.md)），失锁回调必须实现 |

## 6. 演练判据速查（自检集群健康）

```bash
mvn -pl openlatch-server -am package
mvn -pl openlatch-client verify -Pdrill        # 杀主/滚动/分区（分区需 passwordless sudo）
mvn -pl openlatch-server verify -Pdrill        # 停摆采样（仪器类）
# 报告：各模块 target/drill-reports/<套件>-<日期>.md
```

判据：failover 恢复 <10s 且无双授；rolling 两序"末窗+45s 预算后残留=0"；分区少数派
授予/释放全拒且锁存活、撤除后自动收敛；采样轮非全 ABORTED。

## 7. FAQ

**Q：能当分布式事务用吗？** 不能。锁是协调原语，不提供隔离级别；关键状态请配 fencing
凭据（如自增版本号经同一锁保护写入）或改走真共识存储。

**Q：key 数量有上限吗？** 无硬上限（单连接在途与单键队列深度有限额，见配置面）。
锁/信号量条目随最后持有释放回收；屏障条目一次性定型后存续至节点重启——废弃屏障靠键
命名约定隔离（见 [03](03-client-sdk.md)）。

**Q：能主动让位吗？** 可以——运行时 API 的 `transferLeadership` 将 Leader 让给日志最新的
健康成员；计划内运维窗口建议先让位再动原 Leader 所在节点。

**Q：观察请求也走 Leader 吗？** 不需要——统计/明细/`CLUSTER_VIEW` 等只读观察由所连节点
本地应答（弱一致，允许短暂陈旧）；改变锁状态的写路径必须经 Leader 复制提交。
