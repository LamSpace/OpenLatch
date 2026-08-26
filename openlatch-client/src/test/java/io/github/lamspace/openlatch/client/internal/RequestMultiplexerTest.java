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

import io.github.lamspace.openlatch.client.OpenLatchTimeoutException;
import io.github.lamspace.openlatch.client.ServerUnavailableException;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.HashedWheelTimer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 请求多路复用单测（tasks 3.1–3.3）：响应关联、每请求超时、未知 id 路由与
 * 连接不可用快速失败。
 */
class RequestMultiplexerTest {

    /** 细刻度定时器：缩短测试等待。 */
    private HashedWheelTimer timer;
    /** 内存通道：出站消息可断言，入站消息可注入。 */
    private EmbeddedChannel channel;
    /** 会话上下文：requestId 从 1 起。 */
    private SessionContext session;
    /** 被测对象。 */
    private RequestMultiplexer multiplexer;

    /**
     * 装配：通道与会话均指向内存夹具。
     */
    @BeforeEach
    void setUp() {
        timer = new HashedWheelTimer(r -> {
            Thread t = new Thread(r, "test-mux-timer");
            t.setDaemon(true);
            return t;
        }, 10, TimeUnit.MILLISECONDS);
        channel = new EmbeddedChannel();
        session = new SessionContext(42L);
        multiplexer = new RequestMultiplexer(timer, () -> channel, () -> session);
    }

    /**
     * 释放定时器与通道。
     */
    @AfterEach
    void tearDown() {
        timer.stop();
        channel.close();
    }

    /** 出站信封携带协议版本与递增的 requestId。 */
    @Test
    void sendAssignsRequestIdAndProtocolVersion() {
        multiplexer.send(acquireBuilder(), 1000);
        Envelope out = channel.readOutbound();
        assertThat(out).isNotNull();
        assertThat(out.getProtocolVersion()).isEqualTo(1);
        assertThat(out.getRequestId()).isEqualTo(1);

        multiplexer.send(acquireBuilder(), 1000);
        Envelope out2 = channel.readOutbound();
        assertThat(out2.getRequestId()).isEqualTo(2);
    }

    /** 同 requestId 的响应完成对应 future。 */
    @Test
    void responseCompletesMatchingFuture() throws Exception {
        CompletableFuture<Envelope> future = multiplexer.send(acquireBuilder(), 1000);
        Envelope out = channel.readOutbound();

        Envelope response = Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(out.getRequestId())
                .setAcquireResponse(AcquireResponse.newBuilder().setStatus(StatusCode.OK))
                .build();
        multiplexer.onResponse(response);

        assertThat(future.get(1, TimeUnit.SECONDS)).isSameAs(response);
    }

    /** 请求超时无响应时 future 以超时异常失败。 */
    @Test
    void timeoutFailsFuture() {
        CompletableFuture<Envelope> future = multiplexer.send(acquireBuilder(), 50);
        channel.readOutbound();

        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(OpenLatchTimeoutException.class);
    }

    /** 响应到达后超时任务不再生效（重复完成不影响结果）。 */
    @Test
    void responseBeforeTimeoutWins() throws Exception {
        CompletableFuture<Envelope> future = multiplexer.send(acquireBuilder(), 50);
        Envelope out = channel.readOutbound();
        Envelope response = Envelope.newBuilder()
                .setRequestId(out.getRequestId())
                .setAcquireResponse(AcquireResponse.newBuilder().setStatus(StatusCode.OK))
                .build();
        multiplexer.onResponse(response);

        assertThat(future.get(1, TimeUnit.SECONDS)).isSameAs(response);
        // 等待超过超时窗口，future 保持已完成状态
        Thread.sleep(120);
        assertThat(future).isCompleted();
    }

    /** 未知 requestId 的响应路由给孤儿下沉点。 */
    @Test
    void unknownResponseRoutedToOrphanSink() {
        AtomicReference<Envelope> orphan = new AtomicReference<>();
        multiplexer.setOrphanSink(orphan::set);

        Envelope stray = Envelope.newBuilder()
                .setRequestId(999)
                .setAcquireResponse(AcquireResponse.newBuilder().setStatus(StatusCode.OK))
                .build();
        multiplexer.onResponse(stray);

        assertThat(orphan.get()).isSameAs(stray);
    }

    /** failAll 使全部挂起请求以给定原因失败。 */
    @Test
    void failAllFailsPendingRequests() {
        CompletableFuture<Envelope> f1 = multiplexer.send(acquireBuilder(), 10_000);
        CompletableFuture<Envelope> f2 = multiplexer.send(acquireBuilder(), 10_000);

        multiplexer.failAll(new ServerUnavailableException("disconnected"));

        assertThatThrownBy(() -> f1.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ServerUnavailableException.class);
        assertThatThrownBy(() -> f2.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ServerUnavailableException.class);
    }

    /** 通道不可用时发送立即失败。 */
    @Test
    void sendWithoutActiveChannelFails() {
        channel.close();
        CompletableFuture<Envelope> future = multiplexer.send(acquireBuilder(), 1000);
        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ServerUnavailableException.class);
    }

    /**
     * 构造一个获取请求构建器（内容由多路复用层无关紧要）。
     *
     * @return 请求构建器
     */
    private static Envelope.Builder acquireBuilder() {
        return Envelope.newBuilder().setType(MessageType.LOCK_ACQUIRE);
    }
}
