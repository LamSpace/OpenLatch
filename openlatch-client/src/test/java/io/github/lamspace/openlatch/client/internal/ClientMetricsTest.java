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

package io.github.lamspace.openlatch.client.internal;

import io.github.lamspace.openlatch.client.OpenLatchException;
import io.github.lamspace.openlatch.client.OpenLatchTimeoutException;
import io.github.lamspace.openlatch.client.ServerUnavailableException;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.HashedWheelTimer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * 客户端指标门面与多路复用器观测收口（T2，spec"客户端可选监控指标"）：
 * 默认关闭零记录；启用后按 {type,status} 逐项计数（成功应答取状态码名，
 * 失败归 TIMEOUT/UNAVAILABLE/FAILED）、耗时样本伴随、重连与失锁计数。
 */
class ClientMetricsTest {

    /** 共享定时器（请求超时用）。 */
    private HashedWheelTimer timer;
    /** 断言对象注册表。 */
    private io.micrometer.core.instrument.simple.SimpleMeterRegistry registry;
    /** 启用形态门面。 */
    private ClientMetrics metrics;

    @BeforeEach
    void setUp() {
        timer = new HashedWheelTimer(r -> {
            Thread t = new Thread(r, "test-timer");
            t.setDaemon(true);
            return t;
        }, 20, TimeUnit.MILLISECONDS);
        registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        metrics = new ClientMetrics(registry);
    }

    @AfterEach
    void tearDown() {
        timer.stop();
    }

    private static Envelope request(long rid) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid).build();
    }

    private static Envelope response(long rid, StatusCode status) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireResponse(AcquireResponse.newBuilder().setStatus(status))
                .build();
    }

    private double counter(String type, String status) {
        var c = registry.find(ClientMetrics.REQUESTS_TOTAL)
                .tag("type", type).tag("status", status).counter();
        return c == null ? 0d : c.count();
    }

    @Test
    void disabledFacadeIsNoop() {
        assertThat(ClientMetrics.DISABLED.enabled()).isFalse();
        ClientMetrics.DISABLED.recordRequest(MessageType.LOCK_ACQUIRE, null, null, 1);
        ClientMetrics.DISABLED.recordReconnect();
        ClientMetrics.DISABLED.recordLockLost();
        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void legacyConstructorRegistersNoMeters() throws Exception {
        RequestMultiplexer plain = new RequestMultiplexer(timer, () -> null, () -> null);
        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> plain.send(Envelope.newBuilder().setType(MessageType.LOCK_ACQUIRE)
                        .setRequestId(1), 1000).get());
        assertThat(registry.getMeters()).isEmpty();
    }

    @Test
    void unavailablePathCountsOnce() {
        RequestMultiplexer mux = new RequestMultiplexer(timer, () -> null, () -> null, metrics);
        CompletableFuture<Envelope> f = mux.send(
                Envelope.newBuilder().setType(MessageType.LOCK_ACQUIRE).setRequestId(1), 1000);
        assertThatExceptionOfType(ExecutionException.class)
                .isThrownBy(() -> f.get(5, TimeUnit.SECONDS))
                .withCauseInstanceOf(ServerUnavailableException.class);
        assertThat(counter("LOCK_ACQUIRE", "UNAVAILABLE")).isEqualTo(1);
        assertThat(registry.get(ClientMetrics.REQUEST_DURATION).timer().count()).isEqualTo(1);
        // 无会话快速路径不得双计数（send 不重复注册 sendWithId 的收口）。
        assertThat(registry.find(ClientMetrics.REQUESTS_TOTAL).counters().size()).isEqualTo(1);
    }

    @Test
    void responseStatusNameBecomesLabel() {
        Channel ch = new EmbeddedChannel();
        RequestMultiplexer mux = new RequestMultiplexer(timer, () -> ch, () -> null, metrics);
        try {
            mux.sendWithId(request(7), 5_000);
            mux.onResponse(response(7, StatusCode.OK));
            mux.sendWithId(request(8), 5_000);
            mux.onResponse(response(8, StatusCode.QUEUED));
            assertThat(counter("LOCK_ACQUIRE", "OK")).isEqualTo(1);
            assertThat(counter("LOCK_ACQUIRE", "QUEUED")).isEqualTo(1);
            assertThat(registry.get(ClientMetrics.REQUEST_DURATION).timer().count()).isEqualTo(2);
        } finally {
            ch.close();
        }
    }

    @Test
    void timeoutCountsTimeoutLabel() throws Exception {
        Channel ch = new EmbeddedChannel();
        RequestMultiplexer mux = new RequestMultiplexer(timer, () -> ch, () -> null, metrics);
        mux.setOutboundGate(e -> false); // 写黑洞：必超时
        try {
            CompletableFuture<Envelope> f = mux.sendWithId(request(9), 50);
            assertThatExceptionOfType(ExecutionException.class)
                    .isThrownBy(() -> f.get(5, TimeUnit.SECONDS))
                    .withCauseInstanceOf(OpenLatchTimeoutException.class);
            assertThat(counter("LOCK_ACQUIRE", "TIMEOUT")).isEqualTo(1);
        } finally {
            ch.close();
        }
    }

    @Test
    void supersededOldFutureCountsFailed() {
        Channel ch = new EmbeddedChannel();
        RequestMultiplexer mux = new RequestMultiplexer(timer, () -> ch, () -> null, metrics);
        try {
            CompletableFuture<Envelope> first = mux.sendWithId(request(11), 5_000);
            mux.sendWithId(request(11), 5_000); // 同 id 交叠：旧条目 superseded
            assertThatExceptionOfType(ExecutionException.class)
                    .isThrownBy(() -> first.get(5, TimeUnit.SECONDS))
                    .withCauseInstanceOf(OpenLatchException.class);
            assertThat(counter("LOCK_ACQUIRE", "FAILED")).isEqualTo(1);
        } finally {
            ch.close();
        }
    }

    @Test
    void disconnectFailAllCountsUnavailable() {
        Channel ch = new EmbeddedChannel();
        RequestMultiplexer mux = new RequestMultiplexer(timer, () -> ch, () -> null, metrics);
        try {
            CompletableFuture<Envelope> f = mux.sendWithId(request(13), 5_000);
            mux.failAll(new ServerUnavailableException("connection lost"));
            assertThatExceptionOfType(ExecutionException.class)
                    .isThrownBy(() -> f.get(5, TimeUnit.SECONDS))
                    .withCauseInstanceOf(ServerUnavailableException.class);
            assertThat(counter("LOCK_ACQUIRE", "UNAVAILABLE")).isEqualTo(1);
        } finally {
            ch.close();
        }
    }

    @Test
    void reconnectAndLostCounters() {
        metrics.recordReconnect();
        metrics.recordReconnect();
        metrics.recordLockLost();
        assertThat(registry.get(ClientMetrics.RECONNECT_TOTAL).counter().count()).isEqualTo(2);
        assertThat(registry.get(ClientMetrics.LOCKS_LOST_TOTAL).counter().count()).isEqualTo(1);
    }
}
