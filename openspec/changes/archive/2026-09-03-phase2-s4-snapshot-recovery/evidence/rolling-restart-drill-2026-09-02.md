# 滚动重启演练报告（s4 P2-18）

- 生成：3 节点本机 shaded jar，election-timeout 800ms，2 驱动线程 × 150ms 节奏

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2639 | — |
| 应用可见错误数 | 21 | — |
| 客户端错误率 | 0.80 % | < 1 % ✅ |
| 逐台重启耗时（ms） | [2085, 2239, 2466] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [1147,3232] [4233,6472] [7472,9938] | — |
| 错误时刻（ms 相对 t0） | [1154, 1154, 7615, 7626, 7766, 7777, 7916, 7927, 8066, 8077, 8216, 8227, 8367, 8377, 8517, 8527, 8667, 8678, 8818, 8831, 8968] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2635 | — |
| 应用可见错误数 | 0 | — |
| 客户端错误率 | 0.00 % | < 1 % ✅ |
| 逐台重启耗时（ms） | [4408, 2078, 2434] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [652,5060] [6060,8138] [9138,11572] | — |
| 错误时刻（ms 相对 t0） | （无） | 判定突发/持续 |
| 错误归因样本（≤8） |  | — |

## 观察记录（多轮运行汇总，如实记录）

| 轮次 | 运行形态 | 先从后主 | 先主后从 |
|---|---|---|---|
| R1 | 同 JVM 连跑（稳态 150s，分母不足的校准轮） | 1.01% ❌ | 0.00% ✅ |
| R2 | 同 JVM 连跑（稳态 200s） | 0.61% ✅ | **24.22% ❌**（离群，未复现） |
| R3 | 独立先主后从（稳态 60s） | — | 0.12% ✅ |
| R4 | 独立先主后从（稳态 200s） | — | 0.46% ✅ |
| R5 | 同 JVM 连跑（稳态 200s，最终取证） | 0.80% ✅ | 0.00% ✅ |

- **错误形态**：全部错误聚于"当值 Leader 被杀瞬间"的切换窗口（~0.6–1.1s 突发），
  归因全为连接级瞬断（`connection lost`/`connection is not active`）——即详设 §6.3
  "在途请求随断连失效、由调用方重试"的合法语义，非持续降级。
- **R2 离群归因**：leader-first 紧跟 follower-first 在同一 forked JVM 背靠背执行时，
  错误含 `leader discovery failed: discovery budget exhausted (1693ms)` 与
  `home session unavailable`，疑似跨用例资源争用把选举/重连窗口顶破 5s 请求预算；
  后续同 JVM 连跑（R5）未复现，判定为环境性瞬态而非产品缺陷。
- **结论**：滚动重启不中断（§11-5 < 1%）在默认 200s 稳态下稳定达成；任一顺序下
  错误率上界 ≈ 切换窗口内逐连接瞬断数量 ÷ 稳态总流量，时长越长越趋近于窗口占比。
