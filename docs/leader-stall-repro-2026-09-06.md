# leader 复制停摆复现采样档案（phase2-leader-stall-followup 任务 1.1）

- 构造：design D1 in-JVM 3 节点（真实 RaftSubsystem+gRPC，election-timeout 800ms）——脏尾停当值 Leader→存活侧当选+健康写流→旧主带脏条目归群→采样窗。
- 判据：窗口内 Leader 身份（节点+任期）恒定 ∧ NOOP 探针零成功 ∧ commitIndex 首尾零推进 → STALL；身份切换/持续无主 → CHURN（不计命中）。
- 命令：`mvn -s <settings> -pl openlatch-server verify -Pdrill -Dit.test=LeaderStallReproDrillIT -Dtest=NoSuchUnitTest -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false`（K 轮：`-Ddrill.stall.rounds=K`，默认 8）。
- 环境：25.0.3 / Linux amd64 / 8 cpus

## 轮 1（09:20:29）— **RECOVERED**（归群后 3010ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=2 commit@kill=44 |
| 新 leader | node1 term=3 commit@归群前=82 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
10,1,3,true,89,OK
```

## 轮 2（09:20:36）— **RECOVERED**（归群后 3007ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=44 |
| 新 leader | node3 term=2 commit@归群前=80 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
7,3,2,true,89,OK
```

## 轮 3（09:20:45）— **RECOVERED**（归群后 3006ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=45 |
| 新 leader | node3 term=2 commit@归群前=83 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
5,3,2,true,90,OK
```

## 轮 4（09:20:53）— **RECOVERED**（归群后 3007ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=45 |
| 新 leader | node3 term=2 commit@归群前=81 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
7,3,2,true,90,OK
```

## 轮 5（09:21:01）— **RECOVERED**（归群后 3008ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=43 |
| 新 leader | node3 term=2 commit@归群前=81 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
8,3,2,true,90,OK
```

## 轮 6（09:21:11）— **RECOVERED**（归群后 3003ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=47 |
| 新 leader | node2 term=2 commit@归群前=83 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
3,2,2,true,92,OK
```

## 轮 7（09:21:21）— **RECOVERED**（归群后 3004ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=45 |
| 新 leader | node1 term=2 commit@归群前=83 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
4,1,2,true,92,OK
```

## 轮 8（09:21:29）— **RECOVERED**（归群后 3005ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=45 |
| 新 leader | node3 term=2 commit@归群前=83 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
5,3,2,true,92,OK
```

## 轮 9（09:21:36）— **RECOVERED**（归群后 3005ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=44 |
| 新 leader | node2 term=2 commit@归群前=82 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
5,2,2,true,92,OK
```

## 轮 10（09:21:44）— **RECOVERED**（归群后 3004ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=45 |
| 新 leader | node3 term=2 commit@归群前=83 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
4,3,2,true,92,OK
```

## 轮 11（09:21:52）— **RECOVERED**（归群后 3002ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=47 |
| 新 leader | node1 term=2 commit@归群前=83 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
2,1,2,true,92,OK
```

## 轮 12（09:22:00）— **RECOVERED**（归群后 3011ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=46 |
| 新 leader | node1 term=2 commit@归群前=82 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
11,1,2,true,92,OK
```

## 汇总

- 轮数 K=12：STALL=0 / RECOVERED=12 / CHURN=0 / ABORTED=0
- 命中率（STALL/有效轮，CHURN 计入分母不计入分子）：0.00 %
- 判读：STALL>0 即复现基座建立（任务 1.1 verify）；修复路径落地后复跑，2A 期望归零、2B 期望全轮 RECOVERED 且恢复耗时 ≲ T_stall+ε。

## Run 09:29:30（K=16，构造：稳态写流→脏尾停主→写载+竞速归群）

- 构造：design D1 in-JVM 3 节点（真实 RaftSubsystem+gRPC，election-timeout 800ms，探针恒开）——稳态写流建立水位→在途 NOOP 脏尾→SIGKILL 语义停当值 Leader→后台写载打流 + 400–1600ms 随机延迟归群（与选举窗重叠、不设就绪门）→宽限 3000ms 后采样窗。
- 判据：窗口内 Leader 身份（节点+任期）恒定 ∧ NOOP 探针零成功 ∧ commitIndex 首尾零推进 → STALL；身份切换/持续无主 → CHURN（不计命中）。
- 命令：`mvn -s <settings> -pl openlatch-server verify -Pdrill -Dit.test=LeaderStallReproDrillIT -Dtest=NoSuchUnitTest -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false -Ddrill.stall.rounds=16`。
- 环境：25.0.3 / Linux amd64 / 8 cpus

### 轮 1（09:29:40）— **RECOVERED**（归群后 3009ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=2 commit@kill=46 |
| 竞速归群 | 停主后 1136ms 归群；归群瞬刻 leader=node2 term=3 commit=75 |
| 写载 | 成功 42 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
7,2,3,true,145,OK
```

### 轮 2（09:29:49）— **RECOVERED**（归群后 3008ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=44 |
| 竞速归群 | 停主后 1517ms 归群；归群瞬刻 leader=node3 term=2 commit=80 |
| 写载 | 成功 46 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
8,3,2,true,152,OK
```

### 轮 3（09:29:58）— **RECOVERED**（归群后 3007ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=45 |
| 竞速归群 | 停主后 1238ms 归群；归群瞬刻 leader=node3 term=2 commit=77 |
| 写载 | 成功 34 / 失败 9 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
7,3,2,true,148,OK
```

### 轮 4（09:30:07）— **RECOVERED**（归群后 3003ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=44 |
| 竞速归群 | 停主后 560ms 归群；归群瞬刻 leader=node1 term=2 commit=58 |
| 写载 | 成功 36 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
3,1,2,true,129,OK
```

### 轮 5（09:30:15）— **RECOVERED**（归群后 3003ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=45 |
| 竞速归群 | 停主后 405ms 归群；归群瞬刻 leader=node3 term=2 commit=55 |
| 写载 | 成功 34 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
3,3,2,true,124,OK
```

### 轮 6（09:30:23）— **RECOVERED**（归群后 3003ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=45 |
| 竞速归群 | 停主后 483ms 归群；归群瞬刻 leader=node2 term=2 commit=59 |
| 写载 | 成功 35 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
3,2,2,true,129,OK
```

### 轮 7（09:30:32）— **RECOVERED**（归群后 3002ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=44 |
| 竞速归群 | 停主后 1263ms 归群；归群瞬刻 leader=node1 term=2 commit=74 |
| 写载 | 成功 43 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
2,1,2,true,144,OK
```

### 轮 8（09:30:40）— **RECOVERED**（归群后 3002ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=44 |
| 竞速归群 | 停主后 1250ms 归群；归群瞬刻 leader=node2 term=2 commit=76 |
| 写载 | 成功 43 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
2,2,2,true,146,OK
```

### 轮 9（09:30:48）— **RECOVERED**（归群后 3001ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=45 |
| 竞速归群 | 停主后 422ms 归群；归群瞬刻 leader=node2 term=2 commit=57 |
| 写载 | 成功 35 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
1,2,2,true,127,OK
```

### 轮 10（09:30:56）— **RECOVERED**（归群后 3004ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=44 |
| 竞速归群 | 停主后 770ms 归群；归群瞬刻 leader=node1 term=2 commit=66 |
| 写载 | 成功 38 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
4,1,2,true,136,OK
```

### 轮 11（09:31:04）— **RECOVERED**（归群后 3002ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=44 |
| 竞速归群 | 停主后 408ms 归群；归群瞬刻 leader=node3 term=2 commit=56 |
| 写载 | 成功 35 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
2,3,2,true,128,OK
```

### 轮 12（09:31:12）— **RECOVERED**（归群后 3002ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=45 |
| 竞速归群 | 停主后 934ms 归群；归群瞬刻 leader=node3 term=2 commit=71 |
| 写载 | 成功 32 / 失败 8 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
2,3,2,true,141,OK
```

### 轮 13（09:31:21）— **RECOVERED**（归群后 3002ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node1 term=1 commit@kill=44 |
| 竞速归群 | 停主后 1514ms 归群；归群瞬刻 leader=node2 term=2 commit=80 |
| 写载 | 成功 46 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
2,2,2,true,152,OK
```

### 轮 14（09:31:30）— **RECOVERED**（归群后 3002ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=44 |
| 竞速归群 | 停主后 1512ms 归群；归群瞬刻 leader=node3 term=2 commit=82 |
| 写载 | 成功 46 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
1,3,2,true,152,OK
```

### 轮 15（09:31:39）— **RECOVERED**（归群后 3002ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=46 |
| 竞速归群 | 停主后 967ms 归群；归群瞬刻 leader=node3 term=2 commit=70 |
| 写载 | 成功 40 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
2,3,2,true,141,OK
```

### 轮 16（09:31:46）— **RECOVERED**（归群后 3001ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=44 |
| 竞速归群 | 停主后 473ms 归群；归群瞬刻 leader=node2 term=2 commit=58 |
| 写载 | 成功 35 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
1,2,2,true,128,OK
```

### 汇总

- 轮数 K=16：STALL=0 / RECOVERED=16 / CHURN=0 / ABORTED=0
- 命中率（STALL/有效轮，CHURN 计入分母不计入分子）：0.00 %
- 判读：STALL>0 即复现基座建立（任务 1.1 verify）；修复路径落地后复跑，2A 期望归零、2B 期望全轮 RECOVERED 且恢复耗时 ≲ T_stall+ε。

## Run 12:07:43（K=8，构造：稳态写流→脏尾停主→写载+竞速归群）

- 构造：design D1 in-JVM 3 节点（真实 RaftSubsystem+gRPC，election-timeout 800ms，探针恒开）——稳态写流建立水位→在途 NOOP 脏尾→SIGKILL 语义停当值 Leader→后台写载打流 + 400–1600ms 随机延迟归群（与选举窗重叠、不设就绪门）→宽限 3000ms 后采样窗。
- 判据：窗口内 Leader 身份（节点+任期）恒定 ∧ NOOP 探针零成功 ∧ commitIndex 首尾零推进 → STALL；身份切换/持续无主 → CHURN（不计命中）。
- 命令：`mvn -s <settings> -pl openlatch-server verify -Pdrill -Dit.test=LeaderStallReproDrillIT -Dtest=NoSuchUnitTest -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false -Ddrill.stall.rounds=8`。
- 环境：25.0.3 / Linux amd64 / 8 cpus

### 轮 1（12:07:52）— **RECOVERED**（归群后 3008ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=44 |
| 竞速归群 | 停主后 861ms 归群；归群瞬刻 leader=node3 term=2 commit=66 |
| 写载 | 成功 39 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
8,3,2,true,137,OK
```

### 轮 2（12:08:00）— **RECOVERED**（归群后 3004ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=44 |
| 竞速归群 | 停主后 428ms 归群；归群瞬刻 leader=node3 term=2 commit=58 |
| 写载 | 成功 35 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
3,3,2,true,129,OK
```

### 轮 3（12:08:07）— **RECOVERED**（归群后 3005ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=44 |
| 竞速归群 | 停主后 501ms 归群；归群瞬刻 leader=node3 term=2 commit=58 |
| 写载 | 成功 36 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
5,3,2,true,128,OK
```

### 轮 4（12:08:16）— **RECOVERED**（归群后 3003ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=44 |
| 竞速归群 | 停主后 936ms 归群；归群瞬刻 leader=node2 term=2 commit=70 |
| 写载 | 成功 40 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
3,2,2,true,141,OK
```

### 轮 5（12:08:24）— **RECOVERED**（归群后 3004ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=45 |
| 竞速归群 | 停主后 1244ms 归群；归群瞬刻 leader=node1 term=2 commit=77 |
| 写载 | 成功 30 / 失败 13 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
3,1,2,true,146,OK
```

### 轮 6（12:08:32）— **RECOVERED**（归群后 3002ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=44 |
| 竞速归群 | 停主后 635ms 归群；归群瞬刻 leader=node1 term=2 commit=64 |
| 写载 | 成功 37 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
2,1,2,true,131,OK
```

### 轮 7（12:08:41）— **RECOVERED**（归群后 3004ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node2 term=1 commit@kill=45 |
| 竞速归群 | 停主后 1141ms 归群；归群瞬刻 leader=node1 term=2 commit=73 |
| 写载 | 成功 36 / 失败 6 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
4,1,2,true,144,OK
```

### 轮 8（12:08:52）— **RECOVERED**（归群后 3004ms 收敛）

| 前置 | 值 |
|---|---|
| 杀主（SIGKILL 语义停） | node3 term=1 commit@kill=44 |
| 竞速归群 | 停主后 712ms 归群；归群瞬刻 leader=node2 term=2 commit=64 |
| 写载 | 成功 38 / 失败 0 |
| 采样 | 样本 1，探针 ok/notReady/inFlight/other = 1/0/0/0 |

```
csv: tMs,leaderId,term,leaderReady,commitIndex,probe
4,2,2,true,134,OK
```

### 汇总

- 轮数 K=8：STALL=0 / RECOVERED=8 / CHURN=0 / ABORTED=0
- 命中率（STALL/有效轮，CHURN 计入分母不计入分子）：0.00 %
- 判读：STALL>0 即复现基座建立（任务 1.1 verify）；修复路径落地后复跑，2A 期望归零、2B 期望全轮 RECOVERED 且恢复耗时 ≲ T_stall+ε。

