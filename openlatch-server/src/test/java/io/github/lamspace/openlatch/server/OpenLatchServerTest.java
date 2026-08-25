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
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 服务器生命周期（设计说明书 §5.2/§5.6）：监听与端口释放、端口冲突快速失败与资源回收、
 * 关停幂等与未启动即关停的安全性。
 */
class OpenLatchServerTest {

    private static ServerConfig configOnPort(int port) {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(port, d.workerThreads(), d.idleTimeoutMs(), d.defaultLeaseMs(),
                d.minLeaseMs(), d.maxLeaseMs(), d.leaseTickIntervalMs(), d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    @Test
    @Timeout(30)
    void start_listens_on_assigned_port_and_stop_releases_it() throws IOException {
        OpenLatchServer server = new OpenLatchServer(configOnPort(0));
        server.start();
        int port = server.port();
        assertThat(port).isPositive();

        try (Socket probe = new Socket()) {
            probe.connect(new InetSocketAddress("127.0.0.1", port), 2000);
            assertThat(probe.isConnected()).isTrue();
        }

        server.stop();

        // 端口已释放：可立即重新绑定。
        try (ServerSocket rebind = new ServerSocket(port)) {
            assertThat(rebind.isBound()).isTrue();
        }
    }

    @Test
    @Timeout(30)
    void start_on_occupied_port_fails_fast_and_cleans_up() throws IOException {
        int occupied;
        try (ServerSocket squatter = new ServerSocket(0)) {
            occupied = squatter.getLocalPort();
            OpenLatchServer server = new OpenLatchServer(configOnPort(occupied));
            assertThatThrownBy(server::start)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining(String.valueOf(occupied));
        }
        // 启动失败后资源已回收：同一端口可被新实例占用。
        OpenLatchServer retry = new OpenLatchServer(configOnPort(occupied));
        retry.start();
        assertThat(retry.port()).isEqualTo(occupied);
        retry.stop();
    }

    @Test
    @Timeout(30)
    void stop_is_idempotent_and_bounded() {
        OpenLatchServer server = new OpenLatchServer(configOnPort(0));
        server.start();
        server.stop();
        server.stop(); // 第二次调用安全空转
    }

    @Test
    @Timeout(30)
    void stop_before_start_is_safe() {
        OpenLatchServer server = new OpenLatchServer(configOnPort(0));
        server.stop();
    }
}
