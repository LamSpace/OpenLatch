# 配置冻结登记（design D7，P2-04 前置检查）

## 统一条件

| 项 | 值 |
|---|---|
| 机器 | 本机 8 核，同轮同拓扑（3 JVM 节点 + 1 driver） |
| JVM | `java -jar`，无额外 flags，同 JDK 25.0.3 |
| 选举超时 | 两库均取**库默认**（driver 不传 `--election-timeout-ms`） |
| 租约 | bench：请求 300s（clamp 内）；bulk：1h；对称 |
| 数据盘 | 同一物理盘，每节点独立 data-dir |
| 轮次 | 3 轮，奇数轮 ratis→jraft、偶数轮反向（A/B/A/B），逐指标取中位 |
| 门槛一测量点 | driver 客户端侧端到端（含 localhost 一跳）；noraft floor 同脚本 |

## 对称性调整登记（逐项说明动机与对等性）

| # | 调整 | 原因 | 对等性论证 |
|---|---|---|---|
| 1 | Ratis 提案走 `async().send`（非 `io()`） | io() 阻塞线程，与 JRaft `node.apply` 异步链口径不对等 | 两侧都是"提交即返回、apply 完成回执" |
| 2 | Ratis 8 路 `RaftClient` 池 | leader 对单 ClientId 在途串行（API 无提示，摩擦日志 #8） | JRaft 单 `node.apply` 天然批处理；目的同为"测饱和复制带宽而非客户端排队" |
| 3 | 快照实验统一"全节点手动 SNAP → delta 200 条 → 杀 Follower → 原地重启 → digest 追平" | JRaft 1.4.1 无外部 snapshotSync、无阈值 setter；Ratis 自动触发口径不同 | 两候选走完全相同的实验脚本与判定（rebuild=1 + digestEqual + catchupMs） |
| 4 | Ratis `RECOVER/FORMAT` 按目录探测 | 单组默认 FORMAT 重启即崩（摩擦日志 #4/#5） | 属可用性修复，不改变复制/延迟语义 |

## 未调整（维持库默认，防止调优竞赛）

- Ratis：segment 大小、flush 策略、retry policy、gRPC 参数；
- JRaft：electionTimeout（默认）、log segment、flush、bolt 线程池。
- 若 4.2 出现双候选均逼近 20ms 门槛的情况，是否允许对称调 flush 参数 → 提请评审豁免后再动（design D7 冻结协议）。
