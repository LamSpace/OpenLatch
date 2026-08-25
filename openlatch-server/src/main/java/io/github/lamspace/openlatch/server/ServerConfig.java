package io.github.lamspace.openlatch.server;

import io.github.lamspace.openlatch.core.CoreConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * 服务器配置。配置键与默认值对齐设计说明书 §5.7。
 * <p>
 * 加载入口为 {@link #load(String)}：参数为 {@code null} 或空白时用内置默认值；
 * 否则从指定路径读取 Java Properties，缺省键回落默认值，非法值快速失败。
 */
public record ServerConfig(
        int port,
        int workerThreads,
        long idleTimeoutMs,
        long defaultLeaseMs,
        long minLeaseMs,
        long maxLeaseMs,
        long leaseTickIntervalMs,
        long headReplyTimeoutMs,
        int maxKeyLength,
        int maxQueueDepthPerKey,
        int maxInflightPerConnection) {

    public static final String CONFIG_PATH_PROPERTY = "openlatch.config";

    public static final int DEFAULT_PORT = 9410;
    public static final long DEFAULT_IDLE_TIMEOUT_MS = 60_000L;
    public static final long DEFAULT_LEASE_MS = 30_000L;
    public static final long MIN_LEASE_MS = 1_000L;
    public static final long MAX_LEASE_MS = 3_600_000L;
    public static final long DEFAULT_LEASE_TICK_INTERVAL_MS = 500L;
    public static final long DEFAULT_HEAD_REPLY_TIMEOUT_MS = 5_000L;
    public static final int DEFAULT_MAX_KEY_LENGTH = 512;
    public static final int DEFAULT_MAX_QUEUE_DEPTH_PER_KEY = 4096;
    public static final int DEFAULT_MAX_INFLIGHT_PER_CONNECTION = 1024;

    /** 全默认配置（worker 线程数取 2 × CPU）。 */
    public static ServerConfig defaults() {
        return new ServerConfig(
                DEFAULT_PORT,
                defaultWorkerThreads(),
                DEFAULT_IDLE_TIMEOUT_MS,
                DEFAULT_LEASE_MS,
                MIN_LEASE_MS,
                MAX_LEASE_MS,
                DEFAULT_LEASE_TICK_INTERVAL_MS,
                DEFAULT_HEAD_REPLY_TIMEOUT_MS,
                DEFAULT_MAX_KEY_LENGTH,
                DEFAULT_MAX_QUEUE_DEPTH_PER_KEY,
                DEFAULT_MAX_INFLIGHT_PER_CONNECTION);
    }

    /**
     * 从 {@code path} 指向的 Properties 文件加载配置；{@code path} 为 {@code null}
     * 或空白时返回 {@link #defaults()}。文件缺省的键回落默认值；非法值抛出
     * {@link IllegalArgumentException} 并指明配置键。
     */
    public static ServerConfig load(String path) {
        ServerConfig base = defaults();
        if (path == null || path.isBlank()) {
            return base;
        }
        Properties props = new Properties();
        Path file = Path.of(path);
        try (InputStream in = Files.newInputStream(file)) {
            props.load(in);
        } catch (IOException e) {
            throw new IllegalArgumentException("无法读取配置文件: " + file + " (" + e.getMessage() + ")");
        }
        ServerConfig cfg = new ServerConfig(
                intOf(props, "openlatch.server.port", base.port()),
                intOf(props, "openlatch.server.worker-threads", base.workerThreads()),
                longOf(props, "openlatch.server.session.idle-timeout-ms", base.idleTimeoutMs()),
                longOf(props, "openlatch.server.lease.default-ms", base.defaultLeaseMs()),
                longOf(props, "openlatch.server.lease.min-ms", base.minLeaseMs()),
                longOf(props, "openlatch.server.lease.max-ms", base.maxLeaseMs()),
                longOf(props, "openlatch.server.lease.tick-interval-ms", base.leaseTickIntervalMs()),
                longOf(props, "openlatch.server.queue.head-reply-timeout-ms", base.headReplyTimeoutMs()),
                intOf(props, "openlatch.server.limit.max-key-length", base.maxKeyLength()),
                intOf(props, "openlatch.server.limit.max-queue-depth-per-key", base.maxQueueDepthPerKey()),
                intOf(props, "openlatch.server.limit.max-inflight-per-connection", base.maxInflightPerConnection()));
        cfg.validate();
        return cfg;
    }

    /** 映射到 core 层配置。 */
    public CoreConfig toCoreConfig() {
        return new CoreConfig(
                defaultLeaseMs, minLeaseMs, maxLeaseMs,
                headReplyTimeoutMs, maxKeyLength, maxQueueDepthPerKey);
    }

    private static int defaultWorkerThreads() {
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    private static int intOf(Properties props, String key, int fallback) {
        String v = props.getProperty(key);
        if (v == null || v.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 " + key + " 非法（应为整数）: " + v);
        }
    }

    private static long longOf(Properties props, String key, long fallback) {
        String v = props.getProperty(key);
        if (v == null || v.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 " + key + " 非法（应为长整数）: " + v);
        }
    }

    private void validate() {
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("配置项 openlatch.server.port 非法（应为 1-65535）: " + port);
        }
        if (workerThreads < 1) {
            throw new IllegalArgumentException(
                    "配置项 openlatch.server.worker-threads 非法（应 >= 1）: " + workerThreads);
        }
        if (idleTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "配置项 openlatch.server.session.idle-timeout-ms 非法（应 > 0）: " + idleTimeoutMs);
        }
        if (minLeaseMs <= 0 || defaultLeaseMs <= 0 || maxLeaseMs <= 0) {
            throw new IllegalArgumentException("租约配置必须为正数: min=" + minLeaseMs
                    + ", default=" + defaultLeaseMs + ", max=" + maxLeaseMs);
        }
        if (!(minLeaseMs <= defaultLeaseMs && defaultLeaseMs <= maxLeaseMs)) {
            throw new IllegalArgumentException("租约配置必须满足 min <= default <= max: min=" + minLeaseMs
                    + ", default=" + defaultLeaseMs + ", max=" + maxLeaseMs);
        }
        if (leaseTickIntervalMs <= 0) {
            throw new IllegalArgumentException(
                    "配置项 openlatch.server.lease.tick-interval-ms 非法（应 > 0）: " + leaseTickIntervalMs);
        }
        if (headReplyTimeoutMs <= 0) {
            throw new IllegalArgumentException(
                    "配置项 openlatch.server.queue.head-reply-timeout-ms 非法（应 > 0）: " + headReplyTimeoutMs);
        }
        if (maxKeyLength < 1) {
            throw new IllegalArgumentException(
                    "配置项 openlatch.server.limit.max-key-length 非法（应 >= 1）: " + maxKeyLength);
        }
        if (maxQueueDepthPerKey < 1) {
            throw new IllegalArgumentException(
                    "配置项 openlatch.server.limit.max-queue-depth-per-key 非法（应 >= 1）: " + maxQueueDepthPerKey);
        }
        if (maxInflightPerConnection < 1) {
            throw new IllegalArgumentException(
                    "配置项 openlatch.server.limit.max-inflight-per-connection 非法（应 >= 1）: "
                            + maxInflightPerConnection);
        }
    }
}
