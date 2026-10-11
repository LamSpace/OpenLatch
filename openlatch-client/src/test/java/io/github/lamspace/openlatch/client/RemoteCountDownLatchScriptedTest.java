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
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.LatchAwaitResponse;
import io.github.lamspace.openlatch.protocol.LatchCountDownResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RemoteCountDownLatch} 车道对同型 {@code NOT_LEADER} 拒绝的分支归属
 * 锁定（换主窗拒绝码形修复的 v3 面回归判据）：
 *
 * <p><b>判据背景</b>：集群非权威拒绝此前对 LATCH 消息落 default 异型码形
 * （acquire 载荷），客户端按 oneof 取自身应答读到 protobuf 默认实例——
 * {@code status} 零值 {@code OK}，{@code await} 假绿"已破障"、
 * {@code countDown} 假绿 remaining=0（静默数据腐化形态）。服务端码形修复
 * 后拒绝携同型 {@code latch_*_response{NOT_LEADER}}，本用例锁定车道分派
 * 落既有异常口径：<b>显式 {@code OpenLatchException}、不返回 true、不返回
 * 0、不悬挂</b>——修复只消灭假绿，不引入新失败形态（design D3 兼容注记的
 * 可执行化）。
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class RemoteCountDownLatchScriptedTest {

    /** HELLO 应答（v7 服务端形态）。 */
    private static Envelope hello(Envelope req) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(9001L)
                .setServerProtocolVersion(7).setDefaultLeaseMs(30_000))
                .build();
    }

    /**
     * 同型 {@code LatchAwaitResponse{NOT_LEADER}}：await 显式抛
     * {@code OpenLatchException}——既不假绿返回 true，也不悬挂至预算耗尽。
     */
    @Test
    void sameShapeNotLeaderRejectOnAwaitThrowsNotFalseGreen() throws Exception {
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            return req.toBuilder().setType(MessageType.LATCH_AWAIT)
                    .setLatchAwaitResponse(LatchAwaitResponse.newBuilder()
                            .setStatus(StatusCode.NOT_LEADER))
                    .build();
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OCountDownLatch latch = client.newCountDownLatch("l", 2);
                long t0 = System.currentTimeMillis();
                assertThatThrownBy(() -> latch.await(10, TimeUnit.SECONDS))
                        .as("同型 NOT_LEADER 拒绝须显式异常，MUST NOT 假绿返回 true")
                        .isInstanceOf(OpenLatchException.class);
                assertThat(System.currentTimeMillis() - t0)
                        .as("显式失败不得悬挂至 await 预算")
                        .isLessThan(5_000L);
            }
        }
    }

    /**
     * 同型 {@code LatchCountDownResponse{NOT_LEADER}}：countDown 显式抛
     * {@code OpenLatchException}——既有异常口径（非 OK 即抛）覆盖 NOT_LEADER，
     * 修复前后异常类型一致、仅拒绝码从不可见变为可见。
     */
    @Test
    void sameShapeNotLeaderRejectOnCountDownThrowsNotZero() throws Exception {
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            return req.toBuilder().setType(MessageType.LATCH_COUNT_DOWN)
                    .setLatchCountDownResponse(LatchCountDownResponse.newBuilder()
                            .setStatus(StatusCode.NOT_LEADER))
                    .build();
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OCountDownLatch latch = client.newCountDownLatch("l", 2);
                assertThatThrownBy(latch::countDown)
                        .as("同型 NOT_LEADER 拒绝须显式异常，MUST NOT 假绿回 remaining")
                        .isInstanceOf(OpenLatchException.class);
            }
        }
    }
}
