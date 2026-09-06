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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 管理端口随服务器生命周期（spec"指标配置与管理端点生命周期"）：启用时随
 * {@code start} 绑定、随 {@code stop} 解除；兼容构造重载不监听（既有装配
 * 不受扰）；管理端口冲突整体启动失败、不进入半启动。
 */
class OpenLatchServerMetricsTest {

    private final HttpClient client =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private HttpResponse<String> get(int port, String path) throws Exception {
        return client.send(HttpRequest.newBuilder(new URI("http://127.0.0.1:" + port + path))
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void enabledEndpointFollowsServerLifecycle() throws Exception {
        OpenLatchServer server = new OpenLatchServer(TestServers.config(0),
                ClusterConfig.disabled(), new MetricsConfig(true, 0));
        int managementPort;
        server.start();
        try {
            managementPort = server.metricsPort();
            assertThat(managementPort).isPositive();
            assertThat(get(managementPort, "/healthz").statusCode()).isEqualTo(200);
            assertThat(get(managementPort, "/metrics").statusCode()).isEqualTo(200);
        } finally {
            server.stop();
        }
        assertThat(server.metricsPort()).isEqualTo(-1);
        // 关停即解除监听：原端口拒连。
        assertThatThrownBy(() -> get(managementPort, "/healthz")).isInstanceOf(IOException.class);
    }

    @Test
    void legacyConstructorsDoNotBindMetrics() throws Exception {
        OpenLatchServer server = TestServers.start(TestServers.config(0));
        try {
            assertThat(server.metricsPort()).isEqualTo(-1);
        } finally {
            server.stop();
        }
    }

    @Test
    void metricsBindConflictFailsWholeStartup() throws Exception {
        try (ServerSocket busy = new ServerSocket(0)) {
            OpenLatchServer server = new OpenLatchServer(TestServers.config(0),
                    ClusterConfig.disabled(), new MetricsConfig(true, busy.getLocalPort()));
            assertThatThrownBy(server::start)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("管理端口启动失败");
            // 不进入半启动：锁端口监听已回收（重复 stop 幂等确认状态干净）。
            server.stop();
            assertThat(server.metricsPort()).isEqualTo(-1);
        }
    }
}
