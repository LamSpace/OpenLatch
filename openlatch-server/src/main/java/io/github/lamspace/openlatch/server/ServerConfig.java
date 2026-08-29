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
 *
 * @param port                     监听端口，{@code 0} 表示由操作系统分配
 * @param workerThreads            Netty worker 线程数
 * @param idleTimeoutMs            连接读空闲超时，超时断连（毫秒）
 * @param defaultLeaseMs           请求未指定租约时的默认租约时长（毫秒）
 * @param minLeaseMs               租约时长下限（毫秒）
 * @param maxLeaseMs               租约时长上限（毫秒）
 * @param leaseTickIntervalMs      租约扫描调度周期（毫秒）
 * @param headReplyTimeoutMs       已通知队首的响应超时（毫秒）
 * @param maxKeyLength             锁键长度上限（UTF-8 字节）
 * @param maxQueueDepthPerKey      单 key 等待队列深度上限
 * @param maxInflightPerConnection 单连接在途请求上限
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

    /** 指定配置文件路径的系统属性键。 */
    public static final String CONFIG_PATH_PROPERTY = "openlatch.config";

    /** 默认监听端口。 */
    public static final int DEFAULT_PORT = 9410;
    /** 默认连接读空闲超时（毫秒）。 */
    public static final long DEFAULT_IDLE_TIMEOUT_MS = 60_000L;
    /** 默认租约时长（毫秒）。 */
    public static final long DEFAULT_LEASE_MS = 30_000L;
    /** 租约时长下限（毫秒）。 */
    public static final long MIN_LEASE_MS = 1_000L;
    /** 租约时长上限（毫秒）。 */
    public static final long MAX_LEASE_MS = 3_600_000L;
    /** 默认租约扫描调度周期（毫秒）。 */
    public static final long DEFAULT_LEASE_TICK_INTERVAL_MS = 500L;
    /** 默认队首响应超时（毫秒）。 */
    public static final long DEFAULT_HEAD_REPLY_TIMEOUT_MS = 5_000L;
    /** 默认锁键长度上限（UTF-8 字节）。 */
    public static final int DEFAULT_MAX_KEY_LENGTH = 512;
    /** 默认单 key 等待队列深度上限。 */
    public static final int DEFAULT_MAX_QUEUE_DEPTH_PER_KEY = 4096;
    /** 默认单连接在途请求上限。 */
    public static final int DEFAULT_MAX_INFLIGHT_PER_CONNECTION = 1024;

    /**
     * 全默认配置（worker 线程数取 2 × CPU）。
     *
     * @return 全默认配置
     */
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
     *
     * @param path 配置文件路径，{@code null} 或空白表示全默认
     * @return 加载后的配置
     * @throws IllegalArgumentException 文件不可读或任一配置项非法
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

    /**
     * 映射到 core 层配置。
     *
     * @return core 层配置
     */
    public CoreConfig toCoreConfig() {
        return new CoreConfig(
                defaultLeaseMs, minLeaseMs, maxLeaseMs,
                headReplyTimeoutMs, maxKeyLength, maxQueueDepthPerKey);
    }

    /**
     * worker 线程数默认值：2 × CPU 核数。
     *
     * @return 默认线程数
     */
    private static int defaultWorkerThreads() {
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    /**
     * 读取整数配置项：键缺省或值为空白时取回落值；值非整数时抛出
     * {@link IllegalArgumentException} 并在消息中指明配置键与非法值。
     *
     * @param props    配置属性表
     * @param key      配置键
     * @param fallback 缺省回落值
     * @return 配置值或回落值
     * @throws IllegalArgumentException 值非整数
     */
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

    /**
     * 读取长整数配置项：语义与 {@link #intOf} 相同，值非长整数时抛出
     * {@link IllegalArgumentException} 并指明配置键与非法值。
     *
     * @param props    配置属性表
     * @param key      配置键
     * @param fallback 缺省回落值
     * @return 配置值或回落值
     * @throws IllegalArgumentException 值非长整数
     */
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

    /**
     * 全量校验，任一非法即抛出 {@link IllegalArgumentException} 并在消息中
     * 指明配置键：端口 0–65535（0 表示由操作系统分配临时端口，与记录级
     * javadoc 及 {@code OpenLatchServer} 的实际端口日志输出一致）；
     * {@code workerThreads >= 1}；
     * {@code idleTimeoutMs > 0}；租约三项均为正数且满足
     * {@code min <= default <= max}；{@code leaseTickIntervalMs > 0}；
     * {@code headReplyTimeoutMs > 0}；{@code maxKeyLength >= 1}；
     * {@code maxQueueDepthPerKey >= 1}；{@code maxInflightPerConnection >= 1}。
     * 仅由 {@link #load} 在构造后调用（{@link #defaults()} 的取值天然合法，
     * 不经此校验）。
     *
     * @throws IllegalArgumentException 任一配置项非法
     */
    private void validate() {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("配置项 openlatch.server.port 非法（应为 0-65535，0 取临时端口）: " + port);
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
