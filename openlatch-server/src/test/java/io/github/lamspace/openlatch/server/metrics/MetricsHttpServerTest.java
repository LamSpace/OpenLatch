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

package io.github.lamspace.openlatch.server.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
 * 管理端口 HTTP 服务（管理端点仅两路径）：临时端口绑定、两路径响应
 * 与 Content-Type、未知路径/方法 404、关停解除绑定、端口冲突快速失败。
 */
class MetricsHttpServerTest {

    private PrometheusMeterRegistry registry;
    private MetricsHttpServer server;
    private HttpClient client;

    @BeforeEach
    void setUp() {
        registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        server = new MetricsHttpServer(registry, 0);
        server.start();
        client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
    }

    private String url(String path) {
        return "http://127.0.0.1:" + server.port() + path;
    }

    @Test
    void metricsReturnsPrometheusText() throws Exception {
        Counter.builder("openlatch.server.acquire.total").tag("status", "OK")
                .register(registry).increment(3);
        HttpResponse<String> resp = client.send(
                HttpRequest.newBuilder(URI.create(url("/metrics"))).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.headers().firstValue("Content-Type").orElse(""))
                .startsWith("text/plain; version=0.0.4");
        assertThat(resp.body()).contains("openlatch_server_acquire_total{status=\"OK\"} 3");
    }

    @Test
    void healthzAlwaysOk() throws Exception {
        HttpResponse<String> resp = client.send(
                HttpRequest.newBuilder(URI.create(url("/healthz"))).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
        assertThat(resp.body()).isEqualTo("ok");
    }

    @Test
    void unknownPathsAndMethodsNotFound() throws Exception {
        for (String path : new String[]{"/", "/admin", "/metrics/", "/metricsx"}) {
            HttpResponse<String> resp = client.send(
                    HttpRequest.newBuilder(URI.create(url(path))).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(resp.statusCode()).as(path).isEqualTo(404);
            assertThat(resp.body()).doesNotContain("openlatch");
        }
        HttpResponse<String> post = client.send(
                HttpRequest.newBuilder(URI.create(url("/metrics")))
                        .POST(HttpRequest.BodyPublishers.ofString("")).build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(post.statusCode()).isEqualTo(404);
    }

    @Test
    void querySuffixStillMatchesMetrics() throws Exception {
        HttpResponse<String> resp = client.send(
                HttpRequest.newBuilder(URI.create(url("/metrics?extra=1"))).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(200);
    }

    @Test
    void closeUnbindsPort() throws Exception {
        int port = server.port();
        server.close();
        server = null;
        assertThatThrownBy(() -> client.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/healthz"))
                        .timeout(Duration.ofSeconds(2)).GET().build(),
                HttpResponse.BodyHandlers.ofString()))
                .isInstanceOf(IOException.class);
    }

    @Test
    void bindConflictFailsFast() throws Exception {
        try (ServerSocket busy = new ServerSocket(0)) {
            busy.setReuseAddress(true);
            MetricsHttpServer other = new MetricsHttpServer(registry, busy.getLocalPort());
            assertThatThrownBy(other::start)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("管理端口启动失败");
        }
    }

}
