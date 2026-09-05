# 滚动重启演练报告（s4 P2-18）

- 生成：3 节点本机 shaded jar，election-timeout 800ms，2 驱动线程 × 150ms 节奏

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2594 | — |
| 应用可见错误数 | 23 | — |
| 客户端错误率 | 0.89 % | < 1 % ✅ |
| 逐台重启耗时（ms） | [2405, 2185, 2405] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [258,2663] [3663,5848] [6848,9253] | — |
| 错误时刻（ms 相对 t0） | [267, 418, 568, 718, 868, 1019, 1170, 1753, 4311, 6853, 6853, 7004, 7004, 7154, 7154, 7304, 7305, 7455, 7455, 7605, 7605, 7756, 7756] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: request 2 superseded by re-registration; ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: release of 'roll-1-2' failed: NOT_HELD | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 638 | — |
| 应用可见错误数 | 166 | — |
| 客户端错误率 | 26.02 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [3055, 2256, 2840] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [286,3341] [4341,6597] [7597,10437] | — |
| 错误时刻（ms 相对 t0） | 166 个，首 2557 → 末 211556 ms（相邻采样中位间隔 101 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1696ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1697ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2608 | — |
| 应用可见错误数 | 50 | — |
| 客户端错误率 | 1.92 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [2052, 2180, 2296] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [554,2606] [3606,5786] [6787,9083] | — |
| 错误时刻（ms 相对 t0） | 50 个，首 561 → 末 7823 ms（相邻采样中位间隔 0 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 688 | — |
| 应用可见错误数 | 196 | — |
| 客户端错误率 | 28.49 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [2467, 2429, 2497] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [173,2640] [3640,6069] [7069,9566] | — |
| 错误时刻（ms 相对 t0） | 196 个，首 189 → 末 211531 ms（相邻采样中位间隔 2600 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2588 | — |
| 应用可见错误数 | 12 | — |
| 客户端错误率 | 0.46 % | < 1 % ✅ |
| 逐台重启耗时（ms） | [2041, 2946, 2313] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [570,2611] [3611,6557] [7557,9870] | — |
| 错误时刻（ms 相对 t0） | [583, 583, 733, 734, 883, 884, 1034, 1034, 1184, 1184, 5742, 5843] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1636 | — |
| 应用可见错误数 | 84 | — |
| 客户端错误率 | 5.13 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [4294, 2905, 2286] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [483,4777] [5778,8683] [9684,11970] | — |
| 错误时刻（ms 相对 t0） | 84 个，首 7949 → 末 212548 ms（相邻采样中位间隔 2700 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-0' timed out; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: release of 'roll-1-3' failed: NOT_LEADER; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

