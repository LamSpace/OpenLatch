/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.server;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LatchAwaitRequest;
import io.github.lamspace.openlatch.protocol.LatchCountDownRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.metrics.MetricsText;
import io.github.lamspace.openlatch.server.metrics.ServerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 T2 指标断言测试（L2 固定脚本档，spec"服务端指标清单与线路命名"/
 * "请求埋点覆盖单机与集群双路径"/"Gauge 取值语义与采样安全"）：真单机
 * 服务器（锁端口 0 + 管理端口 0）执行固定脚本后基线差值逐项核对
 * {@code /metrics}；附"指标开/关应答一致"与并发抓取冒烟两案。
 */
@Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
class MetricsEndpointIT {

    /** 抓取用 HTTP 客户端。 */
    private static final HttpClient HTTP =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /** 长租约（脚本内不被到期扫描波及）。 */
    private static final long LONG_LEASE = 60_000L;
    /** 短租约（到期计数用例）。 */
    private static final long SHORT_LEASE = 200L;

    /**
     * 抓取管理端口并解析为序列映射。
     *
     * @param port 管理端口
     * @return 序列键 → 值
     */
    private static Map<String, Double> scrape(int port) throws Exception {
        HttpResponse<String> resp = HTTP.send(
                HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + "/metrics")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        return MetricsText.samples(resp.body());
    }

    /**
     * 序列键：Prometheus 线名（逻辑名点转下划线，counter 的 {@code .total}
     * 尾翻译为 {@code _total}）+ 稳定化标签。与 {@link MetricsText} 的
     * 样本键同构（design D2 映射口径）。
     *
     * @param logicalName Micrometer 逻辑名（{@link ServerMetrics} 常量）
     * @param labelKv     标签键值对
     * @return 序列键
     */
    private static String series(String logicalName, String... labelKv) {
        Map<String, String> labels = new java.util.TreeMap<>();
        for (int i = 0; i < labelKv.length; i += 2) {
            labels.put(labelKv[i], labelKv[i + 1]);
        }
        return logicalName.replace('.', '_') + labels;
    }

    /** acquire 信封。 */
    private static Envelope acquire(String key, long threadId, long waitMs, long leaseMs) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_ACQUIRE)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key)
                        .setLockType(LockType.LOCK_TYPE_REENTRANT).setThreadId(threadId)
                        .setLeaseMs(leaseMs).setWaitMs(waitMs))
                .build();
    }

    /** semaphore acquire 信封。 */
    private static Envelope semAcquire(String key, long threadId, int permits, int total) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_ACQUIRE)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key)
                        .setLockType(LockType.LOCK_TYPE_SEMAPHORE).setThreadId(threadId)
                        .setLeaseMs(LONG_LEASE).setWaitMs(-1)
                        .setPermits(permits).setPermitsTotal(total))
                .build();
    }

    /** release 信封。 */
    private static Envelope release(String key, long token, long threadId) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_RELEASE)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey(key)
                        .setLeaseToken(token).setThreadId(threadId))
                .build();
    }

    /** renew 信封。 */
    private static Envelope renew(String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LEASE_RENEW)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder().setKey(key).setLeaseToken(token))
                .build();
    }

    /** latch await 信封。 */
    private static Envelope latchAwait(String key, long total) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LATCH_AWAIT)
                .setLatchAwaitRequest(LatchAwaitRequest.newBuilder().setKey(key).setTotal(total))
                .build();
    }

    /** latch countDown 信封。 */
    private static Envelope latchCountDown(String key, long count, long total) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LATCH_COUNT_DOWN)
                .setLatchCountDownRequest(LatchCountDownRequest.newBuilder()
                        .setKey(key).setCount(count).setTotal(total))
                .build();
    }

    /** 耗时样本线序列键（Prometheus Timer 的 {@code _seconds_count} 形态）。 */
    private static String timerSeries(String result) {
        return ServerMetrics.ACQUIRE_DURATION.replace('.', '_')
                + "_seconds_count" + new java.util.TreeMap<>(Map.of("result", result));
    }

    /**
     * 固定操作脚本（详设 §7 T2"执行固定脚本后逐项核对"）：两条 v3 连接，
     * 覆盖获取（授予/排队/立即拒绝）、释放（OK/NOT_HELD）、续租失败
     * （INVALID_TOKEN/NOT_HELD）、Semaphore 授予、Latch 初始化+挂起、
     * 短租约到期强制释放。脚本内不断言协议结果正确性（那是既有套件的
     * 职责），只保证形态可达，让指标差值可推。
     *
     * @param a 连接一（持有者剧本）
     * @param b 连接二（竞争者剧本）
     * @throws Exception 协议往返异常
     */
    private static void runScript(TestProtocolClient a, TestProtocolClient b) throws Exception {
        Envelope a1 = a.sendAndAwait(withId(a, acquire("m-a1", 1, -1, LONG_LEASE)));
        long tA1 = a1.getAcquireResponse().getLeaseToken();
        Envelope a2 = a.sendAndAwait(withId(a, acquire("m-a2", 1, -1, LONG_LEASE)));
        long tA2 = a2.getAcquireResponse().getLeaseToken();
        b.sendAndAwait(withId(b, acquire("m-a2", 2, -1, LONG_LEASE)));   // QUEUED
        b.sendAndAwait(withId(b, acquire("m-a2", 3, 0, LONG_LEASE)));    // DENIED
        a.sendAndAwait(withId(a, release("m-a1", tA1, 1)));              // OK
        a.sendAndAwait(withId(a, renew("m-a1", 999)));                   // 条目已回收 → NOT_HELD
        a.sendAndAwait(withId(a, renew("m-a2", 999)));                   // 持有中错凭证 → INVALID_TOKEN
        a.sendAndAwait(withId(a, release("m-a2", tA2, 1)));              // OK（b 等待项留守队列）
        a.sendAndAwait(withId(a, release("m-a2", tA2, 1)));              // NOT_HELD
        a.sendAndAwait(withId(a, semAcquire("m-sem", 1, 1, 2)));         // OK（持有中）
        a.sendAndAwait(withId(a, latchCountDown("m-lat", 0, 3)));        // 纯初始化 OK（release 家族）
        a.sendAndAwait(withId(a, latchAwait("m-lat", 3)));               // QUEUED（awaiter 留守）
        a.sendAndAwait(withId(a, acquire("m-exp", 4, 0, SHORT_LEASE)));  // OK，不释放 → 待到期
    }

    /** 补请求 id 的信封。 */
    private static Envelope withId(TestProtocolClient c, Envelope msg) {
        return msg.toBuilder().setRequestId(c.nextRequestId()).build();
    }

    @Test
    void fixedScriptMetricValuesMatchExpectations() throws Exception {
        OpenLatchServer server = new OpenLatchServer(TestServers.fastExpiryConfig(0),
                ClusterConfig.disabled(), new MetricsConfig(true, 0));
        server.start();
        TestProtocolClient a = new TestProtocolClient();
        TestProtocolClient b = new TestProtocolClient();
        try {
            Map<String, Double> base = scrape(server.metricsPort());
            a.connect("127.0.0.1", server.port());
            a.hello();
            b.connect("127.0.0.1", server.port());
            b.hello();

            runScript(a, b);

            // 短租约到期：轮询到计数落地（tick 100ms，上限 30s）。
            Map<String, Double> after = null;
            String expiredSeries = series(ServerMetrics.LEASE_EXPIRED_TOTAL);
            long deadline = System.currentTimeMillis() + 30_000;
            while (System.currentTimeMillis() < deadline) {
                after = scrape(server.metricsPort());
                if (after.getOrDefault(expiredSeries, 0d) - base.getOrDefault(expiredSeries, 0d) >= 1) {
                    break;
                }
                Thread.sleep(200);
            }
            assertThat(after).isNotNull();

            // 计数逐项（基线差值）。
            assertThat(delta(base, after, series(ServerMetrics.ACQUIRE_TOTAL, "status", "OK")))
                    .as("授予：a1/a2/sem/exp").isEqualTo(4);
            assertThat(delta(base, after, series(ServerMetrics.ACQUIRE_TOTAL, "status", "QUEUED")))
                    .as("排队：b 与 latch await").isEqualTo(2);
            assertThat(delta(base, after, series(ServerMetrics.ACQUIRE_TOTAL, "status", "DENIED")))
                    .isEqualTo(1);
            assertThat(delta(base, after, series(ServerMetrics.RELEASE_TOTAL, "status", "OK")))
                    .as("释放 a1/a2 + latch 初始化").isEqualTo(3);
            assertThat(delta(base, after, series(ServerMetrics.RELEASE_TOTAL, "status", "NOT_HELD")))
                    .isEqualTo(1);
            assertThat(delta(base, after, series(ServerMetrics.RENEW_TOTAL, "status", "NOT_HELD")))
                    .isEqualTo(1);
            assertThat(delta(base, after, series(ServerMetrics.RENEW_TOTAL, "status", "INVALID_TOKEN")))
                    .isEqualTo(1);
            assertThat(delta(base, after, expiredSeries)).isEqualTo(1);

            // 耗时三档样本数与获取计数吻合（Timer 翻译线：_seconds_count）。
            assertThat(delta(base, after, timerSeries("granted"))).isEqualTo(4);
            assertThat(delta(base, after, timerSeries("queued"))).isEqualTo(2);
            assertThat(delta(base, after, timerSeries("denied"))).isEqualTo(1);

            // gauge 终态：sem 持有 1；锁条目全部回收归零；latch 存续但不计 held；
            // 等待者 = b 的锁队列项 + latch awaiter；队深 1；会话 2。
            assertThat(after.get(series(ServerMetrics.LOCKS_HELD, "type", "semaphore")))
                    .isEqualTo(1d);
            assertThat(after.get(series(ServerMetrics.LOCKS_HELD, "type", "lock")))
                    .as("exp 已到期、a1/a2 已释放，锁家族归零").isEqualTo(0d);
            assertThat(after.get(series(ServerMetrics.WAITERS))).isEqualTo(2d);
            assertThat(after.get(series(ServerMetrics.QUEUE_DEPTH_MAX))).isEqualTo(1d);
            assertThat(after.get(series(ServerMetrics.SESSIONS))).isEqualTo(2d);

            // 单机不注册 is_leader；HELLO 不入计数家族；无双 _total 后缀。
            assertThat(after.keySet()).noneMatch(k -> k.startsWith("openlatch_cluster_is_leader"));
            assertThat(after.keySet()).noneMatch(k -> k.contains("_total_total"));
        } finally {
            a.close();
            b.close();
            server.stop();
        }
    }

    /** 差值：现值 − 基线（现值序列缺失即失败）。 */
    private static double delta(Map<String, Double> before, Map<String, Double> after, String s) {
        assertThat(after).as("序列 %s 未出现", s).containsKey(s);
        return after.get(s) - before.getOrDefault(s, 0d);
    }

    @Test
    void metricsToggleKeepsResponsesIdentical() throws Exception {
        List<Envelope> on = responsesOf(true);
        List<Envelope> off = responsesOf(false);
        assertThat(off).hasSameSizeAs(on);
        for (int i = 0; i < on.size(); i++) {
            assertThat(normalize(off.get(i))).as("第 %d 应答", i)
                    .isEqualTo(normalize(on.get(i)));
        }
    }

    /**
     * 在指标开/关的两台同配置服务器上跑同一脚本（不含到期与时间敏感步），
     * 收集全部应答。
     *
     * @param enabled 指标开关
     * @return 应答序列（归一比较用）
     * @throws Exception 装配或往返异常
     */
    private static List<Envelope> responsesOf(boolean enabled) throws Exception {
        OpenLatchServer server = new OpenLatchServer(TestServers.config(0),
                ClusterConfig.disabled(), new MetricsConfig(enabled, 0));
        server.start();
        TestProtocolClient a = new TestProtocolClient();
        TestProtocolClient b = new TestProtocolClient();
        List<Envelope> out = new ArrayList<>();
        try {
            a.connect("127.0.0.1", server.port());
            a.hello();
            b.connect("127.0.0.1", server.port());
            b.hello();
            Envelope g = a.sendAndAwait(withId(a, acquire("t-a", 1, -1, LONG_LEASE)));
            out.add(g);
            out.add(b.sendAndAwait(withId(b, acquire("t-a", 2, -1, LONG_LEASE))));
            out.add(b.sendAndAwait(withId(b, acquire("t-a", 3, 0, LONG_LEASE))));
            out.add(a.sendAndAwait(withId(a, release("t-a", g.getAcquireResponse().getLeaseToken(), 1))));
            out.add(a.sendAndAwait(withId(a, semAcquire("t-s", 1, 2, 2))));
            out.add(a.sendAndAwait(withId(a, latchCountDown("t-l", 0, 2))));
            out.add(a.sendAndAwait(withId(a, latchAwait("t-l", 2))));
        } finally {
            a.close();
            b.close();
            server.stop();
        }
        return out;
    }

    /**
     * 应答归一：抹除环境相关字段（请求 id 回显、租约凭证与到期时刻、
     * 授予租期毫秒数——两台独立服务器的发号与时钟必然不同），保留
     * 类型/状态/位次/fullyReleased 的裁决面。
     *
     * @param resp 原始应答
     * @return 归一文本
     */
    private static String normalize(Envelope resp) {
        Envelope.Builder b = resp.toBuilder().clearRequestId();
        switch (resp.getType()) {
            case LOCK_ACQUIRE -> {
                var ar = resp.getAcquireResponse();
                b.setAcquireResponse(ar.toBuilder()
                        .setLeaseToken(0).setLeaseExpiresAtMs(0).setGrantedLeaseMs(0));
            }
            case LEASE_RENEW -> {
                var rr = resp.getLeaseRenewResponse();
                b.setLeaseRenewResponse(rr.toBuilder().setLeaseExpiresAtMs(0));
            }
            default -> {
            }
        }
        return b.build().toString();
    }

    @Test
    void concurrentScrapesStayHealthy() throws Exception {
        OpenLatchServer server = new OpenLatchServer(TestServers.config(0),
                ClusterConfig.disabled(), new MetricsConfig(true, 0));
        server.start();
        TestProtocolClient h1 = new TestProtocolClient();
        TestProtocolClient h2 = new TestProtocolClient();
        h1.connect("127.0.0.1", server.port());
        h1.hello();
        h2.connect("127.0.0.1", server.port());
        h2.hello();
        boolean[] stop = {false};
        try {
            Runnable hammer = () -> {
                TestProtocolClient c = new TestProtocolClient();
                try {
                    c.connect("127.0.0.1", server.port());
                    c.hello();
                    long i = 0;
                    while (!stop[0]) {
                        String key = "hammer-" + (i % 4);
                        Envelope g = c.sendAndAwait(withId(c, acquire(key, 1, -1, LONG_LEASE)));
                        if (g.getAcquireResponse().getStatus() == StatusCode.OK) {
                            c.sendAndAwait(withId(c, release(key,
                                    g.getAcquireResponse().getLeaseToken(), 1)));
                        } else {
                            c.sendAndAwait(withId(c, acquire(key, 1, 0, LONG_LEASE)));
                        }
                        i++;
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    c.close();
                }
            };
            Thread t1 = new Thread(hammer);
            Thread t2 = new Thread(hammer);
            t1.start();
            t2.start();
            int scrapes = 0;
            long deadline = System.currentTimeMillis() + 3_000;
            while (System.currentTimeMillis() < deadline) {
                scrape(server.metricsPort()); // 格式违约/并发异常都会在此炸
                scrapes++;
                Thread.sleep(100);
            }
            stop[0] = true;
            t1.join(30_000);
            t2.join(30_000);
            assertThat(scrapes).as("持续抓取").isGreaterThanOrEqualTo(10);
            Map<String, Double> fin = scrape(server.metricsPort());
            assertThat(fin.keySet()).anyMatch(k ->
                    k.startsWith("openlatch_server_acquire_total{status="));
        } finally {
            stop[0] = true;
            h1.close();
            h2.close();
            server.stop();
        }
    }
}
