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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URISyntaxException;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 服务端 TLS 门控（服务端 TLS 传输层）：
 * 明文连接被拒、正确证书通过、mTLS 客户端证书强制、握手超时断开、坏配置
 * 启动快速失败。证书夹具为 {@code src/test/resources/tls/} 的预生成 PEM。
 */
class TlsGatingTest {

    /** 夹具目录下的文件路径（PEM，绝对路径）。 */
    private static Path fixture(String name) {
        try {
            return Path.of(TlsGatingTest.class.getResource("/tls/" + name).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("夹具路径非法: " + name, e);
        }
    }

    /** 启动一个开启 TLS 的服务器（临时端口），用例负责 {@code stop()}。 */
    private static OpenLatchServer startTlsServer(boolean requireClientCert) {
        TlsConfig tls = new TlsConfig(true,
                fixture("server-cert.pem").toString(),
                fixture("server-key.pem").toString(),
                fixture("ca.pem").toString(),
                requireClientCert);
        OpenLatchServer server = new OpenLatchServer(TestServers.config(0),
                ClusterConfig.disabled(), MetricsConfig.disabled(), AdminConfig.unconfigured(), tls);
        server.start();
        return server;
    }

    /** 客户端 TLS 上下文：信任夹具 CA（可选附客户端证书，mTLS 用）。 */
    private static SslContext clientSsl(boolean withClientCert) {
        try {
            SslContextBuilder b = SslContextBuilder.forClient()
                    .trustManager(fixture("ca.pem").toFile());
            if (withClientCert) {
                b.keyManager(fixture("client-cert.pem").toFile(), fixture("client-key.pem").toFile());
            }
            return b.build();
        } catch (IOException e) {
            throw new IllegalStateException("客户端 TLS 上下文构造失败", e);
        }
    }

    /** 轮询等待连接被服务端关闭（握手/解码失败路径），超时抛出断言失败。 */
    private static void awaitClosed(TestProtocolClient client, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && client.isConnected()) {
            Thread.sleep(20);
        }
        assertThat(client.isConnected())
                .as("连接应在 %dms 内被服务端断开", timeoutMs)
                .isFalse();
    }

    private static Envelope helloEnvelope(long requestId) {
        return Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.HELLO)
                .setRequestId(requestId)
                .setHelloRequest(HelloRequest.newBuilder()
                        .setClientProtocolVersion(OpenLatchServer.PROTOCOL_VERSION))
                .build();
    }

    @Test
    void plaintextClientRejectedWhenTlsEnabled() throws Exception {
        OpenLatchServer server = startTlsServer(false);
        try (TestProtocolClient plain = new TestProtocolClient()) {
            plain.connect("127.0.0.1", server.port());
            assertThat(plain.isConnected()).isTrue();
            // 明文协议字节 → SslHandler 解码失败 → 服务端断连（无会话副作用）。
            plain.send(helloEnvelope(plain.nextRequestId()));
            awaitClosed(plain, 3000);
            assertThat(server.sessions().size()).isZero();
        } finally {
            server.stop();
        }
    }

    @Test
    void tlsClientHandshakeSucceeds() throws Exception {
        OpenLatchServer server = startTlsServer(false);
        try (TestProtocolClient tls = new TestProtocolClient()) {
            tls.connect("127.0.0.1", server.port(), clientSsl(false));
            long sessionId = tls.hello();
            assertThat(sessionId).isGreaterThan(0);
            assertThat(server.sessions().size()).isEqualTo(1);
        } finally {
            server.stop();
        }
    }

    @Test
    void mtlsRejectsClientWithoutCert() throws Exception {
        OpenLatchServer server = startTlsServer(true);
        try (TestProtocolClient tls = new TestProtocolClient()) {
            tls.connect("127.0.0.1", server.port(), clientSsl(false));
            awaitClosed(tls, 3000);
            assertThat(server.sessions().size()).isZero();
        } finally {
            server.stop();
        }
    }

    @Test
    void mtlsAcceptsClientWithCert() throws Exception {
        OpenLatchServer server = startTlsServer(true);
        try (TestProtocolClient tls = new TestProtocolClient()) {
            tls.connect("127.0.0.1", server.port(), clientSsl(true));
            long sessionId = tls.hello();
            assertThat(sessionId).isGreaterThan(0);
            assertThat(server.sessions().size()).isEqualTo(1);
        } finally {
            server.stop();
        }
    }

    @Test
    void silentConnectionClosedByHandshakeTimeout() throws Exception {
        OpenLatchServer server = startTlsServer(false);
        try {
            // 裸 TCP 连接后不发任何 TLS 握手字节：握手超时（5s）断开，而非悬挂
            // 至读空闲（60s）。read() 返回 -1（EOF）即服务端已断开。
            try (Socket raw = new Socket()) {
                raw.connect(new InetSocketAddress("127.0.0.1", server.port()), 2000);
                raw.setSoTimeout(9000);
                long start = System.currentTimeMillis();
                int read = raw.getInputStream().read();
                assertThat(read).isEqualTo(-1);
                assertThat(System.currentTimeMillis() - start)
                        .as("握手超时应在 5s 内断开，而非等到 60s 读空闲")
                        .isLessThan(8000);
            }
            assertThat(server.sessions().size()).isZero();
        } finally {
            server.stop();
        }
    }

    @Test
    void badCertConfigFailsStartFast() {
        TlsConfig bad = new TlsConfig(true, "/no/such/server-cert.pem", "/no/such/server-key.pem", null, false);
        OpenLatchServer server = new OpenLatchServer(TestServers.config(0),
                ClusterConfig.disabled(), MetricsConfig.disabled(), AdminConfig.unconfigured(), bad);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("TLS 配置加载失败");
    }
}
