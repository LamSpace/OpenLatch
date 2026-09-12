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

import io.github.lamspace.openlatch.console.admin.AdminClient;
import io.github.lamspace.openlatch.console.admin.AdminClientPool;
import io.github.lamspace.openlatch.console.admin.AdminUnavailableException;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.AdminConfig;
import io.github.lamspace.openlatch.server.AuthConfig;
import io.github.lamspace.openlatch.server.ClusterConfig;
import io.github.lamspace.openlatch.server.MetricsConfig;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.TlsConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 控制台对 TLS/认证节点的连接：TLS/认证节点可只读观察 / 服务端开认证而
 * 控制台缺业务令牌 → 节点认证失败
 * 降级，不以无令牌请求反复冲击。经 {@link AdminClientPool}/{@link AdminClient}
 * 直连真服务器（走同一业务端口的 HELLO + ADMIN 通道），证书夹具为
 * {@code src/test/resources/tls/} 的预生成 PEM。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ConsoleTlsAuthTest {

    /** 夹具目录下的文件路径（PEM，绝对路径）。 */
    private static Path fixture(String name) {
        try {
            return Path.of(ConsoleTlsAuthTest.class.getResource("/tls/" + name).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("夹具路径非法: " + name, e);
        }
    }

    /** 启动 TLS + 业务认证开启、管理令牌已配置的服务器（临时端口）。 */
    private static OpenLatchServer startSecuredServer() {
        ServerConfig d = ServerConfig.defaults();
        ServerConfig config = new ServerConfig(0, d.workerThreads(), d.idleTimeoutMs(),
                d.defaultLeaseMs(), d.minLeaseMs(), d.maxLeaseMs(), d.leaseTickIntervalMs(),
                d.headReplyTimeoutMs(), d.maxKeyLength(), d.maxQueueDepthPerKey(),
                d.maxInflightPerConnection());
        TlsConfig tls = new TlsConfig(true,
                fixture("server-cert.pem").toString(),
                fixture("server-key.pem").toString(),
                fixture("ca.pem").toString(), false);
        OpenLatchServer server = new OpenLatchServer(config, ClusterConfig.disabled(),
                MetricsConfig.disabled(), new AdminConfig("console-admin-token"),
                new AuthConfig(true, List.of("console-biz-token")), tls);
        server.start();
        return server;
    }

    /** 控制台配置（指向给定节点；可选业务令牌 + TLS）。 */
    private static ConsoleConfig consoleConfig(String address, boolean tls,
                                               String authToken) {
        java.util.Map<String, String> map = new java.util.LinkedHashMap<>();
        map.put(ConsoleConfig.KEY_PREFIX + "server-addresses", address);
        map.put(ConsoleConfig.KEY_PREFIX + "admin-token", "console-admin-token");
        if (tls) {
            map.put(ConsoleConfig.KEY_PREFIX + "tls-enabled", "true");
            map.put(ConsoleConfig.KEY_PREFIX + "tls-trust-store", fixture("ca.pem").toString());
        }
        if (authToken != null) {
            map.put(ConsoleConfig.KEY_PREFIX + "auth-token", authToken);
        }
        return ConsoleConfig.from(map::get);
    }

    @Test
    void consoleQueriesTlsAndAuthNode() throws Exception {
        OpenLatchServer server = startSecuredServer();
        try {
            ConsoleConfig config = consoleConfig("127.0.0.1:" + server.port(), true, "console-biz-token");
            AdminClientPool pool = new AdminClientPool(config);
            try {
                AdminClient client = pool.nodes().values().iterator().next();
                // TLS 握手 + 业务令牌 HELLO + 逐消息 admin-token 全链路。
                assertThat(client.summary().getStatus()).isEqualTo(StatusCode.OK);
                assertThat(client.summary().getNodeRole()).isEqualTo("SINGLE");
            } finally {
                pool.destroy();
            }
        } finally {
            server.stop();
        }
    }

    @Test
    void consoleMissingBusinessTokenDegradesNode() throws Exception {
        OpenLatchServer server = startSecuredServer();
        try {
            // 服务端业务认证开启而控制台缺业务令牌：HELLO 被 INVALID_REQUEST 拒并断连
            // → 节点按认证失败/不可达降级（管理查询抛 AdminUnavailableException），
            // 不把被拒当作可读节点。
            ConsoleConfig config = consoleConfig("127.0.0.1:" + server.port(), true, null);
            AdminClientPool pool = new AdminClientPool(config);
            try {
                AdminClient client = pool.nodes().values().iterator().next();
                assertThatThrownBy(client::summary)
                        .isInstanceOf(AdminUnavailableException.class);
            } finally {
                pool.destroy();
            }
        } finally {
            server.stop();
        }
    }
}
