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
