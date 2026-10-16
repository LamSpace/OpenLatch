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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.client.internal.ConnectionManager;
import io.github.lamspace.openlatch.client.internal.RequestMultiplexer;
import io.github.lamspace.openlatch.client.internal.ScriptedServer;
import io.github.lamspace.openlatch.client.internal.SessionContext;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.HashedWheelTimer;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 业务出站取口的**会话/通道同代**判据（确定性夹具）。
 *
 * <p>锁定的两条形态：
 * <ol>
 *   <li><b>握手未完成期</b>——TCP 已连通（{@code HELLO} 已发出）而会话尚未建立时，
 *       业务请求 MUST NOT 写到线上，MUST 以瞬态"服务不可用"收场；握手请求自身
 *       不受该门约束（同一窗口内仍须正常发出）。</li>
 *   <li><b>取口期间换代</b>——取会话与取通道之间会话已更新时，请求 MUST NOT 以
 *       旧代编号落在新一代通道上，MUST 以瞬态"服务不可用"收场。</li>
 * </ol>
 *
 * <p>两条判据都以"服务端（或内存通道）侧零出现该请求"为观测点：既能红在
 * "写了不该写的东西"，也不会被"本端先超时"这类时序噪声误判成绿。
 * 第三个用例是反向锁：会话稳定且通道可用时请求**必须**照常写出——挡住
 * "把闸门焊死后连正常路径一起掐死"的过修复。
 */
class OutboundPickupCoherenceTest {

    /** 等待夹具窗口的轮询上限（毫秒）。 */
    private static final long WINDOW_WAIT_MS = 5_000;

    /** 业务请求的发送预算（毫秒）：足够观察"写出后等应答"的形态，也不拖长用例。 */
    private static final long SEND_BUDGET_MS = 300L;

    /**
     * 握手未完成期的业务请求不得写出，且握手自身仍须发出。
     *
     * @throws Exception 等待或断言失败
     */
    @Test
    void businessRequestIsNotWrittenBeforeHandshakeCompletes() throws Exception {
        try (ScriptedServer server = new ScriptedServer(env -> null)) {
            OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .reconnectInitialBackoff(Duration.ofMillis(50))
                    .reconnectMaxBackoff(Duration.ofMillis(200))
                    .build();
            try {
                // 窗口前提：TCP 已连通、HELLO 已达桩、会话尚未建立。
                assertThat(awaitTrue(() -> server.countType(MessageType.HELLO) >= 1, WINDOW_WAIT_MS))
                        .as("握手请求须已在未建立会话的窗口内发出").isTrue();
                assertThat(client.connectionManager().state())
                        .isEqualTo(ConnectionManager.State.HELLO_SENT);
                assertThat(client.connectionManager().session()).as("会话尚未建立").isNull();

                server.clearReceived();
                Envelope business = Envelope.newBuilder()
                        .setType(MessageType.LOCK_ACQUIRE)
                        .setRequestId(7L)
                        .build();
                Throwable failure = failureOf(
                        client.requestMultiplexer().sendWithId(business, SEND_BUDGET_MS));

                assertThat(server.countType(MessageType.LOCK_ACQUIRE))
                        .as("业务请求不得写出到线路").isZero();
                assertThat(failure).as("无会话窗口的业务请求须以瞬态不可用收场")
                        .isInstanceOf(ServerUnavailableException.class);
            } finally {
                client.shutdown();
            }
        }
    }

    /**
     * 取口期间换代：请求不得以旧代编号落在新一代通道上。
     *
     * @throws Exception 断言失败
     */
    @Test
    void businessRequestIsNotWrittenWhenSessionSwitchesMidPickup() throws Exception {
        HashedWheelTimer timer = newTimer("pickup-mid-switch-timer");
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            SessionContext stale = new SessionContext(11L);
            SessionContext fresh = new SessionContext(12L);
            // 首次取到旧代会话、取通道后的回读给出新一代会话：模拟取口期间的换代。
            AtomicInteger reads = new AtomicInteger();
            RequestMultiplexer mux = new RequestMultiplexer(timer, () -> channel,
                    () -> reads.getAndIncrement() == 0 ? stale : fresh);

            Envelope business = Envelope.newBuilder()
                    .setType(MessageType.LOCK_ACQUIRE)
                    .setRequestId(stale.nextRequestId())
                    .build();
            Throwable failure = failureOf(mux.sendWithId(business, SEND_BUDGET_MS));

            assertThat(channel.<Object>readOutbound()).as("不得写出到换代后的通道").isNull();
            assertThat(failure).as("取口期间换代须判负").isInstanceOf(ServerUnavailableException.class);
        } finally {
            timer.stop();
            channel.close();
        }
    }

    /**
     * 反向锁：会话稳定且通道可用时请求照常写出（过修复防线）。
     *
     * @throws Exception 断言失败
     */
    @Test
    void businessRequestIsWrittenWhenSessionIsStable() throws Exception {
        HashedWheelTimer timer = newTimer("pickup-stable-timer");
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            SessionContext session = new SessionContext(13L);
            RequestMultiplexer mux = new RequestMultiplexer(timer, () -> channel, () -> session);

            Envelope business = Envelope.newBuilder()
                    .setType(MessageType.LOCK_ACQUIRE)
                    .setRequestId(session.nextRequestId())
                    .build();
            mux.sendWithId(business, SEND_BUDGET_MS);

            assertThat(channel.<Object>readOutbound()).as("同代取口下请求须照常写出")
                    .isEqualTo(business);
        } finally {
            timer.stop();
            channel.close();
        }
    }

    /**
     * 取 future 的失败原因（无失败即断言失败）。
     *
     * @param future 待观察的 future
     * @return 失败原因
     * @throws Exception 等待中断
     */
    private static Throwable failureOf(CompletableFuture<Envelope> future) throws Exception {
        try {
            Envelope answered = future.get(WINDOW_WAIT_MS, TimeUnit.MILLISECONDS);
            throw new AssertionError("expected failure, got answer: " + answered.getType());
        } catch (ExecutionException e) {
            return e.getCause();
        }
    }

    /**
     * 条件轮询等待。
     *
     * @param condition 条件
     * @param timeoutMs 等待上限（毫秒）
     * @return 条件成立返回 {@code true}；到点仍不成立返回 {@code false}
     * @throws InterruptedException 等待被中断
     */
    private static boolean awaitTrue(BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return condition.getAsBoolean();
    }

    /**
     * 细刻度守护定时器（用例收尾须 stop）。
     *
     * @param name 线程名前缀
     * @return 定时器
     */
    private static HashedWheelTimer newTimer(String name) {
        return new HashedWheelTimer(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        }, 10, TimeUnit.MILLISECONDS);
    }
}
