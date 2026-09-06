# T2 验收证据：监控指标（phase3-t2-metrics）

对应验收标准 §8-3："指标清单逐项有断言用例，Prometheus 抓取联调记录在案"。

## 1. §3.2 指标清单 ↔ 断言用例对照

| # | 指标（逻辑名） | 断言用例 | 覆盖点 |
|---|---|---|---|
| 1 | `openlatch.server.locks.held{type}` | `ServerMetricsVocabularyTest.gaugeLineNames`（线名）；`MetricsEndpointIT.fixedScriptMetricValuesMatchExpectations`（单机值：semaphore=1、锁归零、**LATCH 不计**）；`ClusterMetricsTest.leaderWritePathCountsGaugesAndFollowerNotLeader`（集群投影）；`CoreEngineStatsTest.steadyStateAggregatesByFamily`（数据源） | 单机+集群+core |
| 2 | `openlatch.server.waiters` | `MetricsEndpointIT`（=2 含 latch awaiter）；`ClusterMetricsTest`（Leader 队列=1）；`CoreEngineStatsTest` | 单机+集群+core |
| 3 | `openlatch.server.sessions` | `ServerMetricsDispatchTest.sessionsGaugeFollowsRegistry`；`MetricsEndpointIT`（=2） | 单机 |
| 4 | `openlatch.server.acquire.total{status}` | `ServerMetricsVocabularyTest.counterLineNamesCarryTotalSuffixOnce`；`ServerMetricsDispatchTest`（恰好一次/三档）；`MetricsEndpointIT`（OK=4/QUEUED=2/DENIED=1）；`ClusterMetricsTest`（含 `{status="NOT_LEADER"}`——集群路径非盲，§3.4 勘误回归锁定） | 双路径 |
| 5 | `openlatch.server.acquire.duration{result}` | `ServerMetricsVocabularyTest.timerLineNamesFoldSecondsAndResultLabel`（`_seconds_bucket/_count/_sum`）；`MetricsEndpointIT`（三档样本数=获取计数）；`ClusterMetricsTest`（集群档含提交等待） | 双路径 |
| 6 | `openlatch.server.release.total{status}` | `ServerMetricsDispatchTest`（latch countDown→release 家族口径）；`MetricsEndpointIT`（OK=3、NOT_HELD=1） | 双路径 |
| 7 | `openlatch.server.renew.total{status}` | `MetricsEndpointIT`（INVALID_TOKEN/NOT_HELD 各 1——失败续租告警输入）；`ServerMetricsVocabularyTest` | 双路径 |
| 8 | `openlatch.server.lease.expired.total` | `MetricsEndpointIT`（单机扫描=1）；`ClusterMetricsTest.leaseExpiredCountsOncePerReplica`（各副本恰 +1、重试/守卫零计）；`ServerMetricsVocabularyTest` | 双路径 |
| 9 | `openlatch.server.queue.depth.max` | `MetricsEndpointIT`（=1 采样）；`CoreEngineStatsTest`（队深聚合） | 单机+集群 |
| 10 | `openlatch.cluster.is_leader{node_id}` | `ClusterMetricsTest`（1/0 分布 + `isLeaderFlipsAfterLeadershipTransfer` 切换翻转）；`MetricsEndpointIT`（单机不注册：键集无该线） | 集群 |

## 2. 端点与行为不变

| 判据 | 用例 |
|---|---|
| `/metrics` 200 + `text/plain; version=0.0.4` | `MetricsHttpServerTest.metricsReturnsPrometheusText`、`OpenLatchServerMetricsTest` |
| `/healthz` 200 | `MetricsHttpServerTest.healthzAlwaysOk` |
| 其余路径/方法 404 且不泄露注册表 | `MetricsHttpServerTest.unknownPathsAndMethodsNotFound` |
| 管理端口随启停绑定/解除；冲突快速失败不半启动 | `OpenLatchServerMetricsTest` |
| 指标开/关应答逐字段一致（归一口径） | `MetricsEndpointIT.metricsToggleKeepsResponsesIdentical` |
| 并发抓取弱一致安全（压测中 ≥10 次抓取全 200） | `MetricsEndpointIT.concurrentScrapesStayHealthy` |
| 配置默认（启用+9412）/关闭/非法快速失败 | `MetricsConfigTest`、`OpenLatchServerMetricsTest` |

## 3. 客户端与 starter

| 判据 | 用例 |
|---|---|
| 默认关闭零记录（未注入即无观测路径） | `ClientMetricsTest.disabledFacadeIsNoop`、`legacyConstructorRegistersNoMeters`；`ClientMetricsIT.contextWorksWithoutMeterRegistry`（starter 态） |
| `requests.total{type,status}` 逐项吻合（OK/QUEUED/TIMEOUT/UNAVAILABLE/FAILED） | `ClientMetricsTest`（多路径）；`ClientMetricsIT.scriptedRequestsCountedByTypeAndStatus` |
| `request.duration` 每完成请求伴样本 | 同上 |
| `reconnect.total` / `locks.lost.total`（失锁至多一次） | `ClientMetricsIT.serverDownCountsReconnectAndLockLostExactlyOnce` |
| starter 存在 `MeterRegistry` bean 即注入 | `OpenLatchAutoConfigurationTest.meterRegistryBeanIsInjectedIntoClient`（真流量落宿主注册表） |
| micrometer 不强制传递（optional） | `openlatch-client/pom.xml`、`openlatch-spring-boot-starter/pom.xml`（optional 声明；starter 主类经 `@ConditionalOnClass` 守卫缺席不炸） |

## 4. core 观察面回归底线

`CoreEngineStatsTest`（5 用例）+ 既有 core 全量 72 用例绿；`KeyEntry` 新增 `waiterCount()`、`SessionRegistry.size()`、`CoreEngine.stats()` 均纯读——互斥/新家族既有语义测试一字未改（P3-01 同纪律）。

## 5. 真 Prometheus 抓取联调（§8-3"记录在案"）

**步骤**（docker 守护可用时执行，输出粘贴至本节"捕获"段）：

```bash
# 1) 启动服务器（管理端口默认 9412）
java -Dopenlatch.config=/dev/null \
  -jar openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar &
# 2) 拉一轮负载（任一客户端跑锁获取/释放）
# 3) 起 Prometheus（本目录配置）
docker run --rm -p 9090:9090 \
  --add-host=host.docker.internal:host-gateway \
  -v $PWD/openspec/changes/phase3-t2-metrics/evidence/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus &
# 4) 抓取断言：目标序列在 /api/v1/query 可见
curl -s 'http://127.0.0.1:9090/api/v1/query?query=openlatch_server_acquire_total' | head -c 800
```

**捕获**（2026-09-07 01:19 CST，Docker Desktop / prom/prometheus:v2.53.4）：

> 环境注记：本机 9412 被一遗留 `-Pdrill` 演练孤儿进程（PID 378278，集群模式
> 节点，测试残留）占用，故本次证据以 `openlatch.server.port=29410` +
> `openlatch.server.metrics.port=29412` 跑同构流程（见
> `evidence/prometheus-run.yml`；默认 9412 步骤见上方命令块，语义等价）。

```text
== /api/v1/targets ==
http://host.docker.internal:29412/metrics -> up lastScrape= 2026-09-06T17:19:52Z
（Prometheus 容器日志：Server is ready to receive web requests / Completed loading of configuration）

== /api/v1/query openlatch_server_acquire_total ==（客户端脚本：2 锁授予 + 1 排队 + 1 sem 授予 + 释放）
 {'job': 'openlatch', 'status': 'OK'}     = 3
 {'job': 'openlatch', 'status': 'QUEUED'} = 1
== /api/v1/query openlatch_server_release_total ==
 {'job': 'openlatch', 'status': 'OK'}     = 4   # 2 unlock + sem.release + latch countDown
== /api/v1/query openlatch_server_locks_held ==（脚本收尾全部归还，稳态归零；sem 线存在=家族标签口径生效）
 {'type': 'lock'}      = 0
 {'type': 'semaphore'} = 0
== /api/v1/query openlatch_server_acquire_duration_seconds_count ==
 granted = 3
 queued  = 1
== histogram_quantile(0.99, rate(..._bucket[1m])) ==
 p99_seconds = 0.0009948249007687285     # ← 真实 Prometheus 从 _bucket 线算出分位，格式合规最强佐证
```

`/metrics` 原始 exposition 同刻节选（值与上列查询逐项吻合）：

```text
openlatch_server_acquire_total{status="OK"} 3.0
openlatch_server_acquire_total{status="QUEUED"} 1.0
# TYPE openlatch_server_acquire_duration_seconds histogram
openlatch_server_release_total{status="OK"} 4.0
openlatch_server_locks_held{type="lock"} 0.0
openlatch_server_locks_held{type="semaphore"} 0.0
openlatch_server_queue_depth_max 0.0
openlatch_server_sessions 0.0
openlatch_server_waiters 0.0
```

结论：§8-3"指标清单逐项有断言用例，Prometheus 抓取联调记录在案"两项判据均闭环。
