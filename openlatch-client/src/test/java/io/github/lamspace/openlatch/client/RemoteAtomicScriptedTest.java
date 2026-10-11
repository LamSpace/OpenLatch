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
import io.github.lamspace.openlatch.protocol.AtomicOpResponse;
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
 * 原子族车道的应答**缺码形**裁决（{@link ScriptedServer} 直驱）。
 *
 * <p>缺码形（`Envelope.type` 回显 `ATOMIC_OP` 而载荷 oneof 缺
 * `atomic_op_response`）MUST NOT 被读作 protobuf 默认实例——默认实例
 * `status` 是枚举零值 `OK`、`value` 是 0，据此裁决会让 `addAndGet` 之类
 * **静默返回错值**（判例同 {@code OBlockingQueue API} 的缺码形条款，那边
 * 的表现是 `take` 交付空串、`size` 读回 0）。
 */
@Timeout(value = 40, unit = TimeUnit.SECONDS)
class RemoteAtomicScriptedTest {

    /** 单机 HELLO 应答（不填 Leader 提示字段 → proto3 默认 0）。 */
    private static Envelope hello(Envelope req) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(9001L)
                .setServerProtocolVersion(2).setDefaultLeaseMs(30_000))
                .build();
    }

    /** 正常原子应答（OK + 值）。 */
    private static Envelope atomicOk(Envelope req, long value) {
        return req.toBuilder().setType(MessageType.ATOMIC_OP)
                .setAtomicOpResponse(AtomicOpResponse.newBuilder()
                        .setStatus(StatusCode.OK).setApplied(true)
                        .setValue(value).setVersion(1))
                .build();
    }

    /** 异型应答：type 回显 ATOMIC_OP，但载荷 oneof 缺 `atomic_op_response`。 */
    private static Envelope atomicMalformed(Envelope req) {
        return req.toBuilder().setType(MessageType.ATOMIC_OP).build();
    }

    @Test
    void malformedAtomicReplyIsNotReadAsOkZero() throws Exception {
        try (ScriptedServer server = new ScriptedServer(req -> req.getType() == MessageType.HELLO
                ? hello(req) : atomicMalformed(req))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .defaultWaitTimeout(Duration.ofSeconds(10))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OAtomicLong a = client.newAtomicLong("ak");
                long t0 = System.currentTimeMillis();
                assertThatThrownBy(() -> a.addAndGet(5))
                        .as("缺码形应答 MUST NOT 被读作默认实例（status=OK/value=0）而静默返回 0")
                        .isInstanceOf(OpenLatchTimeoutException.class);
                assertThat(System.currentTimeMillis() - t0)
                        .as("失败应在请求预算内显式收口").isLessThan(6_000L);
            }
            assertThat(server.countType(MessageType.ATOMIC_OP))
                    .as("缺码形应按可重试失败重发，而非一次即弃").isGreaterThan(1);
        }
    }

    @Test
    void malformedAtomicReferenceReplyIsNotReadAsDefault() throws Exception {
        try (ScriptedServer server = new ScriptedServer(req -> req.getType() == MessageType.HELLO
                ? hello(req) : atomicMalformed(req))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .defaultWaitTimeout(Duration.ofSeconds(10))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OAtomicReference r = client.newAtomicReference("rk", "v0");
                assertThatThrownBy(() -> r.setString("v1"))
                        .as("有值引用族同款：缺码形 MUST NOT 被读作默认实例而静默成功")
                        .isInstanceOf(OpenLatchTimeoutException.class);
            }
            assertThat(server.countType(MessageType.ATOMIC_OP))
                    .as("缺码形应按可重试失败重发").isGreaterThan(1);
        }
    }

    @Test
    void wellFormedReplyStillSucceeds() throws Exception {
        // 护栏不得扩大化：正常应答（载荷在场）照旧成功。
        try (ScriptedServer server = new ScriptedServer(req -> req.getType() == MessageType.HELLO
                ? hello(req) : atomicOk(req, 5L))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .defaultWaitTimeout(Duration.ofSeconds(10))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                assertThat(client.newAtomicLong("ak").addAndGet(5))
                        .as("载荷在场的应答不受本条影响").isEqualTo(5L);
            }
        }
    }
}
