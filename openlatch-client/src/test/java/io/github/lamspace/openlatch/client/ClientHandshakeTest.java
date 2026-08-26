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

import io.github.lamspace.openlatch.server.OpenLatchServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 建连与握手集成用例（tasks 2.1–2.3）：对真实服务器握手成功；
 * 服务不可达时客户端不崩溃、持续重试直至关停。
 */
class ClientHandshakeTest {

    /** 对真实服务器：建连 + 握手成功，进入可用状态。 */
    @Test
    void connectAndHandshakeAgainstRealServer() throws Exception {
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.config(0));
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port())
                .build()) {
            client.connectAsync().get(5, TimeUnit.SECONDS);
            assertThat(client.isActive()).isTrue();
        } finally {
            server.stop();
        }
    }

    /** 单客户端只建立一条连接：构造器自动连接与显式 connectAsync 不得重复调度连接。 */
    @Test
    void exactlyOneConnectionPerClient() throws Exception {
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.config(0));
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port())
                .build()) {
            client.connectAsync().get(5, TimeUnit.SECONDS);
            Thread.sleep(300); // 允许潜在的第二条连接建立（若有）
            assertThat(server.sessions().size()).isEqualTo(1);
        } finally {
            server.stop();
        }
    }

    /** 服务不可达：连接尝试失败但客户端持续重试，不抛未捕获异常；关停后连接尝试失败收尾。 */
    @Test
    void unreachableServerKeepsRetryingUntilShutdown() throws Exception {
        int deadPort = freePort();
        OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + deadPort)
                .reconnectInitialBackoff(Duration.ofMillis(50))
                .reconnectMaxBackoff(Duration.ofMillis(200))
                .build();
        try {
            CompletableFuture<Void> connecting = client.connectAsync();
            Thread.sleep(400);
            assertThat(connecting).isNotDone();
            assertThat(client.isActive()).isFalse();

            client.shutdown();
            assertThatThrownBy(() -> connecting.get(3, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(IllegalStateException.class);
        } finally {
            client.shutdown();
        }
    }

    /**
     * 取一个当前空闲的端口（取后立即释放，随后连接该端口将被拒绝）。
     *
     * @return 空闲端口
     * @throws IOException 套接字操作失败
     */
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
