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

package io.github.lamspace.openlatch.console;

import io.github.lamspace.openlatch.server.AdminConfig;
import io.github.lamspace.openlatch.server.ClusterConfig;
import io.github.lamspace.openlatch.server.MetricsConfig;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * L2 冒烟基座工具：以临时端口拉起"锁端口 0 + 管理指标
 * 端口 0 + 已配置 admin-token"的真实服务器——控制台端到端用例与协议
 * 拒绝用例共用的唯一启动口径（metrics 常开使概览曲线区可断言）。
 */
final class ConsoleTestSupport {

    /** L2 冒烟统一使用的管理令牌。 */
    static final String ADMIN_TOKEN = "console-it-token";

    /** 工具类不可实例化。 */
    private ConsoleTestSupport() {
    }

    /**
     * 启动端口全临时、管理通道已配置的服务器。
     *
     * @return 已启动服务器（调用方负责 stop）
     */
    static OpenLatchServer startServer() {
        ServerConfig d = ServerConfig.defaults();
        ServerConfig config = new ServerConfig(0,
                d.workerThreads(), d.idleTimeoutMs(), d.defaultLeaseMs(), d.minLeaseMs(),
                d.maxLeaseMs(), d.leaseTickIntervalMs(), d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
        OpenLatchServer server = new OpenLatchServer(config, ClusterConfig.disabled(),
                new MetricsConfig(true, 0), new AdminConfig(ADMIN_TOKEN));
        server.start();
        return server;
    }

    /**
     * 节点展示名字符串（与服务端业务临时端口的 host:port 拼接，控制台查询参数用）。
     *
     * @param server 已启动服务器
     * @return {@code 127.0.0.1:<port>}
     */
    static String nodeAddress(OpenLatchServer server) {
        return "127.0.0.1:" + server.port();
    }

    /**
     * GET 控制台页面正文（Boot 4 起 starter-test 不再随附 TestRestTemplate，
     * 冒烟断言直接以 JDK HTTP 客户端取渲染结果）。
     *
     * @param port 控制台实际端口
     * @param path 页面路径
     * @return 响应正文（非 200 即断言失败）
     */
    static String get(int port, String path) {
        try {
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(5)).build();
            HttpResponse<String> resp = client.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                            .timeout(Duration.ofSeconds(20)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new AssertionError("页面 " + path + " 返回 HTTP " + resp.statusCode());
            }
            return resp.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("页面请求被中断: " + path, e);
        } catch (Exception e) {
            throw new AssertionError("页面请求失败: " + path, e);
        }
    }
}
