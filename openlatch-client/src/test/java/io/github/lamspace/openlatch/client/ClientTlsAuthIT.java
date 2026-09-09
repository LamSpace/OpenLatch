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

import io.github.lamspace.openlatch.server.AdminConfig;
import io.github.lamspace.openlatch.server.AuthConfig;
import io.github.lamspace.openlatch.server.ClusterConfig;
import io.github.lamspace.openlatch.server.MetricsConfig;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.TlsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 客户端 TLS 与业务令牌消费端到端（Phase 3 详设 §5.1/§5.2，spec"客户端 TLS 与
 * 认证消费"）：正确 trust-store + 业务令牌通过并走通业务；错误 trust-store /
 * mTLS 缺客户端证书 / 令牌不符 → 连接不进入已建会话（挂起操作快速失败、退避
 * 重连不忙转）；默认关闭（明文无令牌）行为不受安全项影响。
 *
 * <p>证书夹具为 {@code src/test/resources/tls/} 的预生成 PEM（与 openlatch-server
 * 夹具同源）。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ClientTlsAuthIT {

    /** 夹具目录下的文件路径（PEM，绝对路径）。 */
    private static Path fixture(String name) {
        try {
            return Path.of(ClientTlsAuthIT.class.getResource("/tls/" + name).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("夹具路径非法: " + name, e);
        }
    }

    /** 启动开启 TLS 的服务器（临时端口）。 */
    private static OpenLatchServer startTlsServer(boolean requireClientCert) {
        TlsConfig tls = new TlsConfig(true,
                fixture("server-cert.pem").toString(),
                fixture("server-key.pem").toString(),
                fixture("ca.pem").toString(),
                requireClientCert);
        return startServer(AuthConfig.unconfigured(), tls);
    }

    /** 启动认证开启（配置令牌）的服务器（明文）。 */
    private static OpenLatchServer startAuthServer(String token) {
        return startServer(new AuthConfig(true, List.of(token)), TlsConfig.disabled());
    }

    private static OpenLatchServer startServer(AuthConfig auth, TlsConfig tls) {
        OpenLatchServer server = new OpenLatchServer(ClientTestServers.config(0),
                ClusterConfig.disabled(), MetricsConfig.disabled(), AdminConfig.unconfigured(),
                auth, tls);
        server.start();
        return server;
    }

    /** 开启 TLS 的客户端构建器（信任给定 CA，mTLS 时附客户端证书）。 */
    private static OpenLatchClient.Builder tlsClient(String trustStore,
                                                     boolean withClientCert) {
        OpenLatchClient.Builder b = OpenLatchClient.builder()
                .tlsEnabled(true)
                .tlsTrustStore(trustStore);
        if (withClientCert) {
            b.tlsClientCert(fixture("client-cert.pem").toString())
                    .tlsClientKey(fixture("client-key.pem").toString());
        }
        return b;
    }

    /** 轮询等待客户端进入或离开已建会话。 */
    private static boolean awaitActive(OpenLatchClient client, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (client.isActive()) {
                return true;
            }
            Thread.sleep(20);
        }
        return client.isActive();
    }

    @Test
    void tlsCorrectTrustServesBusiness() throws Exception {
        OpenLatchServer server = startTlsServer(false);
        try (OpenLatchClient client = tlsClient(fixture("ca.pem").toString(), false)
                .address("127.0.0.1:" + server.port())
                .build()) {
            assertThat(awaitActive(client, 5000)).as("TLS 握手应成功并进入已建会话").isTrue();
            OSemaphore sem = client.newSemaphore("tls-e2e", 1);
            assertThat(sem.tryAcquire()).isTrue();
            sem.release();
        } finally {
            server.stop();
        }
    }

    @Test
    void tlsUntrustedServerNeverBecomesActive() throws Exception {
        OpenLatchServer server = startTlsServer(false);
        try (OpenLatchClient client = tlsClient(fixture("other-ca.pem").toString(), false)
                .address("127.0.0.1:" + server.port())
                .build()) {
            // trust-store 不含服务端签发 CA：TLS 握手失败，退避重连、不进入会话。
            assertThat(awaitActive(client, 1500)).as("不可信服务端不得建立会话").isFalse();
        } finally {
            server.stop();
        }
    }

    @Test
    void mtlsMissingClientCertNeverBecomesActive() throws Exception {
        OpenLatchServer server = startTlsServer(true);
        try (OpenLatchClient client = tlsClient(fixture("ca.pem").toString(), false)
                .address("127.0.0.1:" + server.port())
                .build()) {
            // mTLS REQUIRE：无客户端证书握手被拒，不进入会话。
            assertThat(awaitActive(client, 1500)).as("缺客户端证书不得建立会话").isFalse();
        } finally {
            server.stop();
        }
    }

    @Test
    void mtlsWithClientCertServesBusiness() throws Exception {
        OpenLatchServer server = startTlsServer(true);
        try (OpenLatchClient client = tlsClient(fixture("ca.pem").toString(), true)
                .address("127.0.0.1:" + server.port())
                .build()) {
            assertThat(awaitActive(client, 5000)).as("mTLS 携客户端证书应进入已建会话").isTrue();
            OSemaphore sem = client.newSemaphore("mtls-e2e", 1);
            assertThat(sem.tryAcquire()).isTrue();
            sem.release();
        } finally {
            server.stop();
        }
    }

    @Test
    void authWrongTokenNeverBecomesActive() throws Exception {
        OpenLatchServer server = startAuthServer("the-token");
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port())
                .authToken("not-the-token")
                .build()) {
            // HELLO 被服务端以 INVALID_REQUEST 拒绝并断连：认证被拒不当作已建会话。
            assertThat(awaitActive(client, 1500)).as("令牌不符不得建立会话").isFalse();
        } finally {
            server.stop();
        }
    }

    @Test
    void authCorrectTokenServesBusiness() throws Exception {
        OpenLatchServer server = startAuthServer("the-token");
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port())
                .authToken("the-token")
                .build()) {
            assertThat(awaitActive(client, 5000)).as("业务令牌命中应进入已建会话").isTrue();
            OSemaphore sem = client.newSemaphore("auth-e2e", 1);
            assertThat(sem.tryAcquire()).isTrue();
            sem.release();
        } finally {
            server.stop();
        }
    }
}
