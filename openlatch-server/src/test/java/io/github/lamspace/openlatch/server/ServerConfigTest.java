package io.github.lamspace.openlatch.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ServerConfigTest {

    @TempDir
    Path tempDir;

    @Test
    void defaults_match_design_doc() {
        ServerConfig cfg = ServerConfig.load(null);

        assertThat(cfg.port()).isEqualTo(9410);
        assertThat(cfg.workerThreads()).isEqualTo(Runtime.getRuntime().availableProcessors() * 2);
        assertThat(cfg.idleTimeoutMs()).isEqualTo(60_000L);
        assertThat(cfg.defaultLeaseMs()).isEqualTo(30_000L);
        assertThat(cfg.minLeaseMs()).isEqualTo(1_000L);
        assertThat(cfg.maxLeaseMs()).isEqualTo(3_600_000L);
        assertThat(cfg.leaseTickIntervalMs()).isEqualTo(500L);
        assertThat(cfg.headReplyTimeoutMs()).isEqualTo(5_000L);
        assertThat(cfg.maxKeyLength()).isEqualTo(512);
        assertThat(cfg.maxQueueDepthPerKey()).isEqualTo(4096);
        assertThat(cfg.maxInflightPerConnection()).isEqualTo(1024);
    }

    @Test
    void blank_path_falls_back_to_defaults() {
        assertThat(ServerConfig.load("   ")).isEqualTo(ServerConfig.defaults());
    }

    @Test
    void properties_file_overrides_defaults() throws IOException {
        Path file = tempDir.resolve("server.properties");
        Files.writeString(file, """
                openlatch.server.port = 19410
                openlatch.server.worker-threads = 3
                openlatch.server.session.idle-timeout-ms = 5000
                openlatch.server.lease.default-ms = 2000
                openlatch.server.lease.min-ms = 500
                openlatch.server.lease.max-ms = 60000
                openlatch.server.lease.tick-interval-ms = 100
                openlatch.server.queue.head-reply-timeout-ms = 800
                openlatch.server.limit.max-key-length = 64
                openlatch.server.limit.max-queue-depth-per-key = 16
                openlatch.server.limit.max-inflight-per-connection = 8
                """);

        ServerConfig cfg = ServerConfig.load(file.toString());

        assertThat(cfg.port()).isEqualTo(19410);
        assertThat(cfg.workerThreads()).isEqualTo(3);
        assertThat(cfg.idleTimeoutMs()).isEqualTo(5_000L);
        assertThat(cfg.defaultLeaseMs()).isEqualTo(2_000L);
        assertThat(cfg.minLeaseMs()).isEqualTo(500L);
        assertThat(cfg.maxLeaseMs()).isEqualTo(60_000L);
        assertThat(cfg.leaseTickIntervalMs()).isEqualTo(100L);
        assertThat(cfg.headReplyTimeoutMs()).isEqualTo(800L);
        assertThat(cfg.maxKeyLength()).isEqualTo(64);
        assertThat(cfg.maxQueueDepthPerKey()).isEqualTo(16);
        assertThat(cfg.maxInflightPerConnection()).isEqualTo(8);
    }

    @Test
    void missing_keys_fall_back_to_defaults() throws IOException {
        Path file = tempDir.resolve("partial.properties");
        Files.writeString(file, "openlatch.server.port = 19411\n");

        ServerConfig cfg = ServerConfig.load(file.toString());

        assertThat(cfg.port()).isEqualTo(19411);
        assertThat(cfg.defaultLeaseMs()).isEqualTo(ServerConfig.DEFAULT_LEASE_MS);
        assertThat(cfg.maxQueueDepthPerKey()).isEqualTo(ServerConfig.DEFAULT_MAX_QUEUE_DEPTH_PER_KEY);
    }

    @Test
    void missing_file_fails_fast() {
        assertThatThrownBy(() -> ServerConfig.load(tempDir.resolve("absent.properties").toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("无法读取配置文件");
    }

    @Test
    void invalid_number_fails_fast_with_key_name() throws IOException {
        Path file = tempDir.resolve("bad-number.properties");
        Files.writeString(file, "openlatch.server.port = abc\n");

        assertThatThrownBy(() -> ServerConfig.load(file.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.server.port");
    }

    @Test
    void out_of_range_port_rejected() throws IOException {
        Path file = tempDir.resolve("bad-port.properties");
        Files.writeString(file, "openlatch.server.port = 70000\n");

        assertThatThrownBy(() -> ServerConfig.load(file.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port");
    }

    @Test
    void lease_bounds_must_be_ordered() throws IOException {
        Path file = tempDir.resolve("bad-lease.properties");
        Files.writeString(file, """
                openlatch.server.lease.min-ms = 5000
                openlatch.server.lease.default-ms = 2000
                """);

        assertThatThrownBy(() -> ServerConfig.load(file.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("min <= default <= max");
    }

    @Test
    void non_positive_limits_rejected() throws IOException {
        Path file = tempDir.resolve("bad-limit.properties");
        Files.writeString(file, "openlatch.server.limit.max-queue-depth-per-key = 0\n");

        assertThatThrownBy(() -> ServerConfig.load(file.toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-queue-depth-per-key");
    }

    @Test
    void to_core_config_maps_fields() {
        ServerConfig cfg = ServerConfig.load(null);
        var core = cfg.toCoreConfig();

        assertThat(core.defaultLeaseMs()).isEqualTo(cfg.defaultLeaseMs());
        assertThat(core.minLeaseMs()).isEqualTo(cfg.minLeaseMs());
        assertThat(core.maxLeaseMs()).isEqualTo(cfg.maxLeaseMs());
        assertThat(core.headReplyTimeoutMs()).isEqualTo(cfg.headReplyTimeoutMs());
        assertThat(core.maxKeyLength()).isEqualTo(cfg.maxKeyLength());
        assertThat(core.maxQueueDepthPerKey()).isEqualTo(cfg.maxQueueDepthPerKey());
    }
}
