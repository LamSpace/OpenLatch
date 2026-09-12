# 滚动重启演练报告（s4 P2-18）

- 生成：3 节点本机 shaded jar，election-timeout 800ms，2 驱动线程 × 150ms 节奏

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 927 | — |
| 应用可见错误数 | 198 | — |
| 客户端错误率 | 21.36 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [3146, 2767, 5674] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [475,3621] [4622,7389] [8390,14064] | — |
| 错误时刻（ms 相对 t0） | 198 个，首 482 → 末 15227 ms（相邻采样中位间隔 3 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 222 | — |
| 应用可见错误数 | 58 | — |
| 客户端错误率 | 26.13 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [3473, 2620, 3242] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [526,3999] [5000,7620] [8620,11862] | — |
| 错误时刻（ms 相对 t0） | 58 个，首 2898 → 末 72894 ms（相邻采样中位间隔 100 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1696ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: home session unavailable; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: home session unavailable; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 851 | — |
| 应用可见错误数 | 41 | — |
| 客户端错误率 | 4.82 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [2343, 2589, 2886] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [983,3326] [4326,6915] [7916,10802] | — |
| 错误时刻（ms 相对 t0） | 41 个，首 987 → 末 9925 ms（相邻采样中位间隔 31 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 804 | — |
| 应用可见错误数 | 8 | — |
| 客户端错误率 | 1.00 % | < 1 % ✅ |
| 逐台重启耗时（ms） | [3775, 2407, 2225] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [576,4351] [5353,7760] [8761,10986] | — |
| 错误时刻（ms 相对 t0） | [2873, 2873, 5372, 5377, 5522, 5527, 5672, 5677] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1689ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1690ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1698ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1697ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 852 | — |
| 应用可见错误数 | 32 | — |
| 客户端错误率 | 3.76 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [2425, 2184, 2691] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [554,2979] [3979,6163] [7163,9854] | — |
| 错误时刻（ms 相对 t0） | 32 个，首 563 → 末 8376 ms（相邻采样中位间隔 150 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 230 | — |
| 应用可见错误数 | 51 | — |
| 客户端错误率 | 22.17 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [3595, 2660, 2975] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [240,3835] [4835,7495] [8496,11471] | — |
| 错误时刻（ms 相对 t0） | 51 个，首 2615 → 末 73015 ms（相邻采样中位间隔 100 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1696ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted; ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: release of 'roll-0-2' failed: NOT_LEADER; ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: release of 'roll-1-2' failed: NOT_LEADER; ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: release of 'roll-0-2' failed: NOT_HELD; ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: release of 'roll-1-2' failed: NOT_HELD; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-2' timed out | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 236 | — |
| 应用可见错误数 | 55 | — |
| 客户端错误率 | 23.31 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [2171, 2910, 2385] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [845,3016] [4016,6926] [7927,10312] | — |
| 错误时刻（ms 相对 t0） | 55 个，首 848 → 末 73243 ms（相邻采样中位间隔 700 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-3' timed out; ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: release of 'roll-1-3' failed: NOT_LEADER; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: no active session; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-3' timed out | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 868 | — |
| 应用可见错误数 | 11 | — |
| 客户端错误率 | 1.27 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [4564, 2451, 2482] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [632,5196] [6197,8648] [9648,12130] | — |
| 错误时刻（ms 相对 t0） | [6255, 6311, 6405, 6461, 6555, 6612, 6705, 6762, 6856, 6913, 7008] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 792 | — |
| 应用可见错误数 | 8 | — |
| 客户端错误率 | 1.01 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [3201, 2696, 2494] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [751,3952] [4955,7651] [8651,11145] | — |
| 错误时刻（ms 相对 t0） | [757, 758, 1171, 1172, 3362, 3362, 5862, 5862] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-2' timed out; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-1-2' timed out; ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: no leader available within budget (election window); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: no leader available within budget (election window) | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 844 | — |
| 应用可见错误数 | 136 | — |
| 客户端错误率 | 16.11 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [4315, 3412, 3465] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [370,4685] [5689,9101] [10102,13567] | — |
| 错误时刻（ms 相对 t0） | 136 个，首 2629 → 末 17327 ms（相邻采样中位间隔 135 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1698ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1680ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1680ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 820 | — |
| 应用可见错误数 | 16 | — |
| 客户端错误率 | 1.95 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [2401, 2621, 2149] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [503,2904] [3904,6525] [7526,9675] | — |
| 错误时刻（ms 相对 t0） | [506, 506, 657, 657, 807, 807, 957, 957, 1107, 1107, 1257, 1257, 1408, 1408, 7213, 7213] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 870 | — |
| 应用可见错误数 | 12 | — |
| 客户端错误率 | 1.38 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [2359, 4524, 2497] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [527,2886] [3887,8411] [9411,11908] | — |
| 错误时刻（ms 相对 t0） | [3997, 4008, 4147, 4158, 4297, 4311, 4448, 4461, 4598, 4611, 4749, 4762] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 783 | — |
| 应用可见错误数 | 24 | — |
| 客户端错误率 | 3.07 % | < 1 % ❌（如实记录） |
| 逐台重启耗时（ms） | [2397, 3197, 2356] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [599,2996] [3996,7193] [8193,10549] | — |
| 错误时刻（ms 相对 t0） | [609, 610, 8197, 8197, 8347, 8347, 8497, 8497, 8647, 8647, 8797, 8797, 8948, 8948, 9098, 9098, 9248, 9250, 9398, 9400, 9549, 9550, 9701, 9703] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 849 | — |
| 应用可见错误数 | 0 | — |
| 客户端错误率 | 0.00 % | < 1 % ✅ |
| 逐台重启耗时（ms） | [4535, 2188, 2588] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [685,5220] [6220,8408] [9409,11997] | — |
| 错误时刻（ms 相对 t0） | （无） | 判定突发/持续 |
| 错误归因样本（≤8） |  | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1175 | — |
| 应用可见错误数 | 4 | — |
| 全程客户端错误率 | 0.34 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2351, 2904, 2430] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [632,2983] [3984,6888] [7888,10318] | — |
| 错误时刻（ms 相对 t0） | [635, 636, 6066, 6066] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-0' timed out; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-1-0' timed out | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1183 | — |
| 应用可见错误数 | 3 | — |
| 全程客户端错误率 | 0.25 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [3177, 2345, 2187] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [259,3436] [4437,6782] [7782,9969] | — |
| 错误时刻（ms 相对 t0） | [2631, 2631, 4693] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1696ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1182 | — |
| 应用可见错误数 | 30 | — |
| 全程客户端错误率 | 2.54 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2423, 2489, 2471] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [450,2873] [3874,6363] [7363,9834] | — |
| 错误时刻（ms 相对 t0） | 30 个，首 458 → 末 8272 ms（相邻采样中位间隔 1 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1224 | — |
| 应用可见错误数 | 46 | — |
| 全程客户端错误率 | 3.76 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2561, 2122, 1971] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [249,2810] [3810,5932] [6932,8903] | — |
| 错误时刻（ms 相对 t0） | 46 个，首 257 → 末 3563 ms（相邻采样中位间隔 0 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1240 | — |
| 应用可见错误数 | 36 | — |
| 全程客户端错误率 | 2.90 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2318, 2298, 4471] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [477,2795] [3795,6093] [7094,11565] | — |
| 错误时刻（ms 相对 t0） | 36 个，首 480 → 末 8632 ms（相邻采样中位间隔 147 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1218 | — |
| 应用可见错误数 | 45 | — |
| 全程客户端错误率 | 3.69 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2681, 2201, 2484] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [190,2871] [3872,6073] [7073,9557] | — |
| 错误时刻（ms 相对 t0） | 45 个，首 207 → 末 6334 ms（相邻采样中位间隔 0 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1200 | — |
| 应用可见错误数 | 16 | — |
| 全程客户端错误率 | 1.33 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2207, 2303, 2361] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [374,2581] [3581,5884] [6884,9245] | — |
| 错误时刻（ms 相对 t0） | [7002, 7012, 7152, 7162, 7303, 7312, 7453, 7462, 7603, 7612, 7753, 7762, 7903, 7913, 8053, 8065] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1254 | — |
| 应用可见错误数 | 16 | — |
| 全程客户端错误率 | 1.28 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2350, 2358, 4519] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [567,2917] [3917,6275] [7275,11794] | — |
| 错误时刻（ms 相对 t0） | [3981, 3989, 4132, 4139, 4286, 4290, 4436, 4440, 4586, 4591, 4736, 4741, 4886, 4891, 5038, 5042] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1224 | — |
| 应用可见错误数 | 20 | — |
| 全程客户端错误率 | 1.63 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2214, 2300, 2406] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [814,3028] [4028,6328] [7329,9735] | — |
| 错误时刻（ms 相对 t0） | [819, 820, 7343, 7344, 7494, 7494, 7644, 7644, 7794, 7794, 7944, 7944, 8094, 8095, 8245, 8245, 8395, 8395, 8547, 8547] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1232 | — |
| 应用可见错误数 | 14 | — |
| 全程客户端错误率 | 1.14 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [4296, 2076, 2423] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [603,4899] [5900,7976] [8976,11399] | — |
| 错误时刻（ms 相对 t0） | [5977, 5992, 6128, 6142, 6278, 6292, 6428, 6442, 6578, 6592, 6728, 6742, 6878, 6893] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1192 | — |
| 应用可见错误数 | 30 | — |
| 全程客户端错误率 | 2.52 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2111, 2300, 2564] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [552,2663] [3663,5963] [6964,9528] | — |
| 错误时刻（ms 相对 t0） | 30 个，首 555 → 末 8435 ms（相邻采样中位间隔 8 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 318 | — |
| 应用可见错误数 | 82 | — |
| 全程客户端错误率 | 25.79 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 36 | = 0 ❌（停摆未愈） |
| 逐台重启耗时（ms） | [3479, 2589, 2766] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [253,3732] [4736,7325] [8326,11092] | — |
| 错误时刻（ms 相对 t0） | 82 个，首 2503 → 末 103902 ms（相邻采样中位间隔 100 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1690ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1691ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1697ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1216 | — |
| 应用可见错误数 | 16 | — |
| 全程客户端错误率 | 1.32 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2105, 2375, 2351] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [611,2716] [3716,6091] [7091,9442] | — |
| 错误时刻（ms 相对 t0） | [7131, 7178, 7281, 7328, 7431, 7478, 7581, 7628, 7731, 7778, 7882, 7928, 8032, 8079, 8182, 8229] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1192 | — |
| 应用可见错误数 | 2 | — |
| 全程客户端错误率 | 0.17 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [3483, 2204, 2268] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [387,3870] [4870,7074] [8074,10342] | — |
| 错误时刻（ms 相对 t0） | [2742, 2744] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1694ms) | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1222 | — |
| 应用可见错误数 | 16 | — |
| 全程客户端错误率 | 1.31 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2537, 2347, 2380] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [1423,3960] [4961,7308] [8308,10688] | — |
| 错误时刻（ms 相对 t0） | [8418, 8427, 8569, 8578, 8719, 8728, 8869, 8878, 9019, 9028, 9170, 9178, 9320, 9328, 9470, 9481] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1235 | — |
| 应用可见错误数 | 16 | — |
| 全程客户端错误率 | 1.30 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2475, 4396, 2679] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [516,2991] [3991,8387] [9388,12067] | — |
| 错误时刻（ms 相对 t0） | [4114, 4130, 4264, 4280, 4414, 4430, 4564, 4580, 4715, 4731, 4865, 4881, 5015, 5031, 5167, 5181] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 310 | — |
| 应用可见错误数 | 100 | — |
| 全程客户端错误率 | 32.26 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 36 | = 0 ❌（停摆未愈） |
| 逐台重启耗时（ms） | [2622, 2403, 2403] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [509,3131] [4131,6534] [7534,9937] | — |
| 错误时刻（ms 相对 t0） | 100 个，首 516 → 末 101263 ms（相邻采样中位间隔 2601 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1169 | — |
| 应用可见错误数 | 10 | — |
| 全程客户端错误率 | 0.86 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [3671, 2248, 2062] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [238,3909] [4909,7157] [8157,10219] | — |
| 错误时刻（ms 相对 t0） | [2510, 2510, 4910, 5012, 5063, 5162, 5213, 5312, 5363, 10210] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1697ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1698ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1270 | — |
| 应用可见错误数 | 150 | — |
| 全程客户端错误率 | 11.81 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [3159, 3283, 1986] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [522,3681] [4681,7964] [8964,10950] | — |
| 错误时刻（ms 相对 t0） | 150 个，首 525 → 末 11661 ms（相邻采样中位间隔 5 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1206 | — |
| 应用可见错误数 | 2 | — |
| 全程客户端错误率 | 0.17 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [3258, 2448, 2047] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [319,3577] [4577,7025] [8026,10073] | — |
| 错误时刻（ms 相对 t0） | [2694, 2696] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1696ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1697ms) | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1205 | — |
| 应用可见错误数 | 20 | — |
| 全程客户端错误率 | 1.66 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2048, 2394, 2474] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [491,2539] [3540,5934] [6934,9408] | — |
| 错误时刻（ms 相对 t0） | [502, 503, 2711, 2711, 6967, 6987, 7117, 7137, 7267, 7287, 7417, 7437, 7567, 7588, 7718, 7738, 7868, 7888, 8018, 8039] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-1' timed out; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-1-1' timed out; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1244 | — |
| 应用可见错误数 | 1 | — |
| 全程客户端错误率 | 0.08 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [4443, 2072, 2522] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [681,5124] [6124,8196] [9196,11718] | — |
| 错误时刻（ms 相对 t0） | [1888] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: release of 'roll-0-0' failed: INVALID_TOKEN | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1237 | — |
| 应用可见错误数 | 12 | — |
| 全程客户端错误率 | 0.97 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2413, 2101, 2359] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [886,3299] [4299,6400] [7400,9759] | — |
| 错误时刻（ms 相对 t0） | [891, 891, 7477, 7491, 7627, 7642, 7777, 7792, 7927, 7942, 8080, 8094] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 1246 | — |
| 应用可见错误数 | 23 | — |
| 全程客户端错误率 | 1.85 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [4405, 3803, 2299] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [663,5068] [6068,9871] [10871,13170] | — |
| 错误时刻（ms 相对 t0） | [1997, 8231, 8231, 9531, 9531, 9681, 9684, 9831, 9834, 9984, 9984, 10134, 10134, 10284, 10285, 10434, 10435, 10586, 10588, 10736, 10738, 10887, 10890] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: release of 'roll-0-0' failed: INVALID_TOKEN; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-1' timed out; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-1-1' timed out; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2602 | — |
| 应用可见错误数 | 4 | — |
| 全程客户端错误率 | 0.15 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2064, 2927, 2036] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [793,2857] [3857,6784] [7784,9820] | — |
| 错误时刻（ms 相对 t0） | [796, 796, 5988, 5988] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-2' timed out; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-1-2' timed out | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2614 | — |
| 应用可见错误数 | 0 | — |
| 全程客户端错误率 | 0.00 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2391, 1991, 2504] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [395,2786] [3786,5777] [6777,9281] | — |
| 错误时刻（ms 相对 t0） | （无） | 判定突发/持续 |
| 错误归因样本（≤8） |  | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2685 | — |
| 应用可见错误数 | 162 | — |
| 全程客户端错误率 | 6.03 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2254, 3498, 2991] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [706,2960] [3962,7460] [8461,11452] | — |
| 错误时刻（ms 相对 t0） | 162 个，首 709 → 末 12750 ms（相邻采样中位间隔 4 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2618 | — |
| 应用可见错误数 | 6 | — |
| 全程客户端错误率 | 0.23 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [3268, 2696, 2876] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [293,3561] [4562,7258] [8258,11134] | — |
| 错误时刻（ms 相对 t0） | [2628, 2628, 4788, 4788, 4939, 4939] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1695ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: home session unavailable; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: home session unavailable; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2623 | — |
| 应用可见错误数 | 20 | — |
| 全程客户端错误率 | 0.76 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2452, 2515, 2392] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [492,2944] [3945,6460] [7460,9852] | — |
| 错误时刻（ms 相对 t0） | [502, 508, 1281, 1283, 7484, 7497, 7635, 7647, 7785, 7797, 7935, 7948, 8085, 8098, 8235, 8248, 8386, 8399, 8536, 8549] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2638 | — |
| 应用可见错误数 | 13 | — |
| 全程客户端错误率 | 0.49 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2455, 2570, 2571] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [739,3194] [4194,6764] [7764,10335] | — |
| 错误时刻（ms 相对 t0） | [4199, 4219, 4349, 4369, 4499, 4519, 4649, 4669, 4800, 4820, 4950, 4970, 5100] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先从后主

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 2624 | — |
| 应用可见错误数 | 11 | — |
| 全程客户端错误率 | 0.42 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 0 | = 0 ✅ |
| 逐台重启耗时（ms） | [2441, 2305, 2495] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [655,3096] [4096,6401] [7401,9896] | — |
| 错误时刻（ms 相对 t0） | [660, 7544, 7552, 7694, 7702, 7844, 7852, 7994, 8002, 8145, 8152] | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection lost; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active; ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: connection is not active | — |

## 顺序：先主后从

| 指标 | 值 | 判定 |
|---|---|---|
| 应用可见请求总数 | 639 | — |
| 应用可见错误数 | 153 | — |
| 全程客户端错误率 | 23.94 % | 仅报告（分段判据，2B.1） |
| 自愈事件（停摆判定/重启升级） | 0 / 0 | 计入观察记录 |
| 预算窗后残留错误（末窗+45000ms 起） | 116 | = 0 ❌（停摆未愈） |
| 逐台重启耗时（ms） | [3176, 2470, 3073] | 任意时刻 ≥2/3 存活 |
| 重启窗口（ms 相对 t0） | [392,3568] [4568,7038] [8038,11111] | — |
| 错误时刻（ms 相对 t0） | 153 个，首 2658 → 末 211957 ms（相邻采样中位间隔 2500 ms） | 判定突发/持续 |
| 错误归因样本（≤8） | ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1694ms); ExecutionException: io.github.lamspace.openlatch.client.OpenLatchException: leader discovery failed: discovery budget exhausted (1696ms); ExecutionException: io.github.lamspace.openlatch.client.ServerUnavailableException: leader migrated, wait budget exhausted; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-1-1' timed out; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-1' timed out; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-1' timed out; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-1-1' timed out; ExecutionException: io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException: acquire of 'roll-0-1' timed out | — |

