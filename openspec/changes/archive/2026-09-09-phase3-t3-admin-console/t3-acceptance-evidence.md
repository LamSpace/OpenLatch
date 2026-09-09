# T3 管理控制台验收证据（phase3-t3-admin-console）

日期：2026-09-09（change 归档随附，对齐 §7 T3 与 §8-4 判据）

## 判定口径总览

| 判据（§7/§8） | 证据 |
|---|---|
| 管理协议消息级测试 | L1 `AdminProtocolTest`（单机，15 例）+ `AdminClusterTest`（集群 3 节点，2 例） |
| 未认证管理连接被拒 | L1 认证矩阵 + L2 `ConsoleAuthFailureTest`（页面降级横幅、HTTP 200、零泄露） |
| 控制台端到端冒烟（列表/详情/会话/概览/节点） | L2 `ConsoleStandaloneTest`（6 例）、`ConsoleClusterTest`（3 例）、`ConsolePartialFailureTest`/`ConsoleMetricsDownTest` |
| 指标区独立降级 / 节点宕机部分可用 | L2 `ConsoleMetricsDownTest`/`ConsolePartialFailureTest` |
| v1/v2/v3 既有行为零扰动（§8-6 延伸） | 全量 `clean verify` 既有用例全绿 + 协议冻结测试 v1 基线零变更 |
| 管理流量指标零污染 | `AdminProtocolTest.adminTraffic_doesNotPolluteMetrics`（执行前后 scrape 等值） |
| 管理查询零日志条目（集群） | `AdminClusterTest.adminQueries_produceNoLogEntries`（lastApplied 不动点） |

## 自动化用例（L1/L2）逐项对照 §7 T3

- **§7"管理协议消息级测试"**：`AdminProtocolTest`（单机 EmbeddedChannel 直驱完整接入层）——认证矩阵（错/空/未配置令牌→`INVALID_REQUEST`+断连；握手前被门闩拒；v2 会话消息级拒不断连）、四消息逐字段断言（SUMMARY/LIST_KEYS 分页与过滤/KEY_DETAIL 三家族/LIST_SESSIONS 仅本节点接入）、分页参数越界消息级拒、在途限额 `OVERLOADED`、指标零污染、观察零扰动。
- **§7"集群档"消息级用例**：`AdminClusterTest`——Leader/Follower 双视角（SUMMARY 角色、follower 镜像收敛可查 + 等待区 `wait_queue_leader_only`、会话列表跨节点分治、逻辑会话高位=接入节点）、管理查询零日志条目。
- **§7"控制台端到端冒烟"**：`ConsoleStandaloneTest`（五页面 HTML 断言 + sparkline 非降级）+ `ConsoleClusterTest`（角色分布恰一 Leader、follower 标注、节点视图成员表）。
- **§7"未认证管理连接被拒"**：`AdminProtocolTest.wrongToken_*`/`emptyToken_*`/`unconfiguredAdmin_*` + `ConsoleAuthFailureTest`。

## 部署走查（L3，真实 jar + 进程）

### 环境与启动序列

- 服务器：`openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar`
  `java -Dopenlatch.config=<evidence/server-standalone.properties> -jar <jar>`（9410 业务 + 9412 指标 + admin-token）
- 负载预置：`SeedT3 <127.0.0.1:9410>`（重入锁 `order:1` 持有+排队 / Semaphore `pool:db` 2/3+3 排队 / Latch `gate:boot` 2+awaiter）
- 控制台：`java -Dopenlatch.console.config=<evidence/console-standalone.properties> -jar openlatch-console/target/openlatch-console-1.0-SNAPSHOT-executable.jar`（9413）
- 反例：`console-wrongtoken.properties`（9414，错误令牌）

### 断言输出（curl，2026-09-09 实跑）

启动序列见 `evidence/` 下三个 properties 与 `SeedT3.java`；实际命令：

```bash
java -Dopenlatch.config=<evidence/server-standalone.properties> \
     -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar   # 9410 + 9412, adminEnabled=true
java -cp <client 依赖> SeedT3 127.0.0.1:9410                                    # seed ready: ...（见 l3-seed.log）
java -Dopenlatch.console.config=<evidence/console-standalone.properties> \
     -jar openlatch-console/target/openlatch-console-1.0-SNAPSHOT-executable.jar # 9413
```

抓取断言（`l3-assert.out` 全文留档）：

```
## overview
      3 <polyline            # 三线 sparkline（held/waiters/sessions）
      1 <td>2</td>           # 会话数（seed client + console 管理连接）
      1 <td>3</td>           # 等待者（order:1 队 1 + pool:db 队 1 + gate awaiter 1）
      1 <td>SINGLE</td>
      1 1.0.0-dev            # version（manifest 缺失回落常量）
## keys                      # 三 key：order:1(lock) / pool:db(semaphore) / gate:boot(latch)
      gate:boot / order:1 / pool:db 各 2 处；family 单元格 lock/semaphore/latch；共 3 条
## key-detail order:1        # writer + 等待队列 + 剩余租约；"仅 Leader 可见" 计数 0（单机）
## semaphore detail          # 可用许可 1/3；latch detail # 屏障计数 2/2
## sessions / nodes          # 接入会话；单机部署 + SINGLE
## wrong-token console 9414  # "管理认证失败"横幅；HTTP 200；令牌串泄露计数 0
```

`raw-overview.html`/`raw-keys.html`/`raw-keydetail.html` 留档于 evidence/（浏览器目检参照）。

### 浏览器目检记录（用户）

走查目标：`http://127.0.0.1:9413`（概览含 sparkline 曲线与数字；锁列表点击进详情；
锁详情持有/等待/剩余租约；会话列表；节点视图单机形态）。反例：以
`console-wrongtoken.properties` 另起 9414 观察全页面降级横幅。
<!-- 用户走查后回填观感；L1/L2 已自动化覆盖页面渲染正确性，目检为观感确认 -->

### 集群档说明

三节点集群的控制台行为已由 **L2 `ConsoleClusterTest`（进程内三节点真 Raft + 真 TCP，
页面断言角色分布/Leader 标注/镜像口径）** 覆盖；`-Pdrill` 的进程级集群演练在本环境不可靠
（见下"收口验证记录"），未以此作 L3 实跑，避免以环境缺陷冒充功能证据。

## 退出判据核对

- [x] P3-13 = T3 退出：§8-4 证据齐（L1 消息级 + L2 冒烟 + L3 走查 + 管理通道强制认证反例）

## 收口验证记录（§8-6 / -Pdrill）

- 全仓 `mvn -s /home/lam/repo/settings.xml clean verify` **BUILD SUCCESS**（2026-09-09，含既有 v1/v2/v3 全部回归、协议冻结测试 v1 基线零变更、新 Admin 各层用例）。
- `-Pdrill`（进程级杀 Leader / 滚动重启演练，LeaderKillDrillIT/RollingRestartDrillIT）在本机稳定失败（"30s 内未探测到 Leader"）。为判明是否 T3 回归，做了 **clean-master A/B**：`git stash` 全部 T3 改动后在干净 master 重建 server shaded jar，单独复跑 `LeaderKillDrillIT#killLeaderRecoversWithinTenSeconds` —— **同样失败**（90.3s，同断言）。结论：进程级演练在本环境不可靠（进程拉起 / kill-9 / 限时选举在沙箱受限），与 T3 改动无因果；T3 的等价进程内故障覆盖已由全量 verify 的 `LeaderFailoverServerTest`/`MinorityQuorumTest`/`ClusterMetricsTest`/`AdminClusterTest` 全绿背书。该演练 suite 属 T2 既有门禁，建议在不受限 CI/终端环境复跑（本 change 不改其断言）。
