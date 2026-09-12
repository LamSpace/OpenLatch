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

import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HELLO 业务令牌认证门控：开启命中放行/失败同形拒绝并断连零会话副作用、多令牌双活、
 * 摘令牌后被拒、默认关闭维持"非空即拒"兼容守卫。
 */
class AuthHandshakeTest {

    /** 启动一个认证开启（配置令牌表）的服务器，用例负责 {@code stop()}。 */
    private static OpenLatchServer startAuthServer(String... tokens) {
        AuthConfig auth = new AuthConfig(true, List.of(tokens));
        OpenLatchServer server = new OpenLatchServer(TestServers.config(0),
                ClusterConfig.disabled(), MetricsConfig.disabled(), AdminConfig.unconfigured(),
                auth, TlsConfig.disabled());
        server.start();
        return server;
    }

    /** 启动一个认证关闭（默认）的服务器。 */
    private static OpenLatchServer startDefaultServer() {
        OpenLatchServer server = new OpenLatchServer(TestServers.config(0),
                ClusterConfig.disabled(), MetricsConfig.disabled(), AdminConfig.unconfigured(),
                AuthConfig.unconfigured(), TlsConfig.disabled());
        server.start();
        return server;
    }

    /** 携带给定令牌的 HELLO 信封。 */
    private static Envelope helloEnvelope(long requestId, String token) {
        HelloRequest.Builder hello = HelloRequest.newBuilder()
                .setClientProtocolVersion(OpenLatchServer.PROTOCOL_VERSION);
        if (token != null && !token.isEmpty()) {
            hello.setAuthToken(token);
        }
        return Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.HELLO)
                .setRequestId(requestId)
                .setHelloRequest(hello)
                .build();
    }

    /** 轮询等待连接被服务端关闭。 */
    private static void awaitClosed(TestProtocolClient client, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && client.isConnected()) {
            Thread.sleep(20);
        }
        assertThat(client.isConnected()).as("连接应在 %dms 内被服务端断开", timeoutMs).isFalse();
    }

    @Test
    void authOnCorrectTokenSucceeds() throws Exception {
        OpenLatchServer server = startAuthServer("tok-a", "tok-b");
        try (TestProtocolClient c1 = new TestProtocolClient()) {
            c1.connect("127.0.0.1", server.port());
            Envelope ok = c1.sendAndAwait(helloEnvelope(c1.nextRequestId(), "tok-a"));
            assertThat(ok.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(ok.getHelloResponse().getSessionId()).isGreaterThan(0);
            assertThat(server.sessions().size()).isEqualTo(1);
        }
        try (TestProtocolClient c2 = new TestProtocolClient()) {
            c2.connect("127.0.0.1", server.port());
            Envelope ok = c2.sendAndAwait(helloEnvelope(c2.nextRequestId(), "tok-b"));
            // 多令牌双活：轮换期任一配置令牌放行。
            assertThat(ok.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
        }
        server.stop();
    }

    @Test
    void authOnEmptyAndWrongRejectedIdenticallyNoLeak() throws Exception {
        OpenLatchServer server = startAuthServer("tok-a");
        try {
            StatusCode emptyStatus;
            StatusCode wrongStatus;
            try (TestProtocolClient empty = new TestProtocolClient()) {
                empty.connect("127.0.0.1", server.port());
                Envelope resp = empty.sendAndAwait(helloEnvelope(empty.nextRequestId(), ""));
                emptyStatus = resp.getHelloResponse().getStatus();
                awaitClosed(empty, 3000);
            }
            try (TestProtocolClient wrong = new TestProtocolClient()) {
                wrong.connect("127.0.0.1", server.port());
                Envelope resp = wrong.sendAndAwait(helloEnvelope(wrong.nextRequestId(), "not-the-token"));
                wrongStatus = resp.getHelloResponse().getStatus();
                awaitClosed(wrong, 3000);
            }
            // 空与错同形（不泄露原因），且零会话副作用。
            assertThat(emptyStatus).isEqualTo(StatusCode.INVALID_REQUEST);
            assertThat(wrongStatus).isEqualTo(StatusCode.INVALID_REQUEST);
            assertThat(server.sessions().size()).isZero();
        } finally {
            server.stop();
        }
    }

    @Test
    void removedTokenRejectedAfterRotation() throws Exception {
        OpenLatchServer server = startAuthServer("tok-a");
        try {
            try (TestProtocolClient old = new TestProtocolClient()) {
                old.connect("127.0.0.1", server.port());
                // 旧令牌已摘：HELLO 被拒（轮换收敛）。
                Envelope resp = old.sendAndAwait(helloEnvelope(old.nextRequestId(), "tok-b"));
                assertThat(resp.getHelloResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
                awaitClosed(old, 3000);
            }
            try (TestProtocolClient current = new TestProtocolClient()) {
                current.connect("127.0.0.1", server.port());
                Envelope ok = current.sendAndAwait(helloEnvelope(current.nextRequestId(), "tok-a"));
                assertThat(ok.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
            }
        } finally {
            server.stop();
        }
    }

    @Test
    void authOffKeepsCompatGuard() throws Exception {
        OpenLatchServer server = startDefaultServer();
        try (TestProtocolClient plain = new TestProtocolClient()) {
            plain.connect("127.0.0.1", server.port());
            Envelope ok = plain.sendAndAwait(helloEnvelope(plain.nextRequestId(), ""));
            assertThat(ok.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(ok.getHelloResponse().getSessionId()).isGreaterThan(0);
        }
        try (TestProtocolClient token = new TestProtocolClient()) {
            token.connect("127.0.0.1", server.port());
            Envelope resp = token.sendAndAwait(helloEnvelope(token.nextRequestId(), "unexpected"));
            // 默认关闭 = v1 兼容守卫：非空令牌仍被拒（兼容回归）。
            assertThat(resp.getHelloResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
            awaitClosed(token, 3000);
        }
        server.stop();
    }
}
