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

import io.github.lamspace.openlatch.client.internal.ScriptedServer;
import io.github.lamspace.openlatch.protocol.BarrierAwaitResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RemoteBarrier} 车道的应答**缺码形**裁决（{@link ScriptedServer} 直驱）。
 *
 * <p>缺码形（{@code Envelope.type} 回显而载荷 oneof 缺 {@code barrier_await_response}/
 * {@code barrier_action_done_response}/{@code barrier_leave_response}）MUST NOT 被读作
 * protobuf 默认实例——其 {@code status} 为枚举零值 {@code OK}。等待族后果：{@code await}
 * **假绿返回"世代已合拢"**、{@code breakBarrier} 静默空成功、{@code generation}/{@code parties}
 * 读作 0，破坏"返回即已破"次序契约。
 *
 * <p><b>分流</b>：等待路径（{@code BARRIER_AWAIT}）同会话以同 {@code request_id} 重发
 * （跨会话走既有"到场不跨会话重放"裁决）；执行者收尾（{@code BARRIER_ACTION_DONE}）按
 * {@code (会话, 世代)} 幂等重发；{@code breakBarrier} 抛显式异常（离场确认以"不确定"收场）。
 */
@Timeout(value = 40, unit = TimeUnit.SECONDS)
class RemoteBarrierScriptedTest {

    /** HELLO 应答（v7 服务端形态）。 */
    private static Envelope hello(Envelope req) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(9001L)
                .setServerProtocolVersion(7).setDefaultLeaseMs(30_000))
                .build();
    }

    /** 异型应答：type 回显 TYPE，但载荷 oneof 缺对应成员。 */
    private static Envelope malformed(Envelope req, MessageType type) {
        return req.toBuilder().setType(type).build();
    }

    /**
     * 缺码形 {@code BARRIER_AWAIT} 应答：await MUST NOT 被读作默认实例
     * （{@code status=OK}）而假绿返回 true；同会话同 id 重发至预算耗尽以
     * {@code false} 收场。
     */
    @Test
    void malformedAwaitReplyIsNotReadAsTripped() throws Exception {
        try (ScriptedServer server = new ScriptedServer(req -> req.getType() == MessageType.HELLO
                ? hello(req) : malformed(req, MessageType.BARRIER_AWAIT))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(1))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OBarrier barrier = client.newBarrier("b", 2);
                long t0 = System.currentTimeMillis();
                assertThat(barrier.await(2, TimeUnit.SECONDS))
                        .as("缺码形 MUST NOT 被读作默认实例（status=OK）而假绿返回已合拢")
                        .isFalse();
                assertThat(System.currentTimeMillis() - t0)
                        .as("失败应在等待预算内显式收口").isLessThan(5_000L);
            }
            assertThat(server.countType(MessageType.BARRIER_AWAIT))
                    .as("缺码形应在等待车道以同 id 重发").isGreaterThan(1);
        }
    }

    /**
     * 缺码形 {@code BARRIER_ACTION_DONE} 应答：执行者收尾 MUST NOT 被读作
     * 默认实例（{@code status=OK}）静默了结；按 {@code (会话, 世代)} 幂等重发，
     * 预算内不可判定则以显式超时收场。
     */
    @Test
    void malformedActionDoneReplyIsNotReadAsCompleted() throws Exception {
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            if (req.getType() == MessageType.BARRIER_AWAIT) {
                return req.toBuilder().setType(MessageType.BARRIER_AWAIT)
                        .setBarrierAwaitResponse(BarrierAwaitResponse.newBuilder()
                                .setStatus(StatusCode.QUEUED).setExecutor(true)
                                .setGeneration(1L).setParties(1L))
                        .build();
            }
            return malformed(req, MessageType.BARRIER_ACTION_DONE);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(1))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OBarrier barrier = client.newBarrier("b", 1, () -> { });
                assertThatThrownBy(() -> barrier.await(2, TimeUnit.SECONDS))
                        .as("action-done 缺码形 MUST NOT 被读作 OK 静默了结")
                        .isInstanceOf(OpenLatchTimeoutException.class);
            }
            assertThat(server.countType(MessageType.BARRIER_ACTION_DONE))
                    .as("action-done 缺码形应按世代幂等重发").isGreaterThan(1);
        }
    }

    /**
     * 缺码形 {@code BARRIER_LEAVE} 应答（{@code breakBarrier} 路径）：
     * MUST NOT 被读作默认实例（{@code status=OK}）而静默空成功；抛显式异常。
     */
    @Test
    void malformedBreakBarrierReplyIsNotReadAsConfirmed() throws Exception {
        try (ScriptedServer server = new ScriptedServer(req -> req.getType() == MessageType.HELLO
                ? hello(req) : malformed(req, MessageType.BARRIER_LEAVE))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(1))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OBarrier barrier = client.newBarrier("b", 2);
                assertThatThrownBy(barrier::breakBarrier)
                        .as("缺码形 MUST NOT 被读作默认实例（status=OK）而静默空成功")
                        .isInstanceOf(OpenLatchException.class);
            }
        }
    }
}
