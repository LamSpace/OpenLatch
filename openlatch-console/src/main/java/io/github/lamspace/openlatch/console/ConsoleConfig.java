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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 控制台配置（{@code openlatch.console.*} 键族，Properties 文件与 Spring Environment
 * 双通道同源——main 经文件桥接为系统属性，测试/直配经 Environment 绑定，
 * 两路取值口径逐键一致）。
 *
 * <p><b>快速失败契约</b>：地址列表为空、令牌空白、端口越界、间隔/超时
 * 非正——全部在构造即抛 {@link IllegalArgumentException} 并指明配置键，
 * MUST NOT 静默回落。
 *
 * @param addresses        目标节点地址列表（≥1 项，指向服务端业务端口）
 * @param adminToken       管理令牌（与服务端 {@code openlatch.server.admin.token} 一致）
 * @param port             HTTP 监听端口（默认 9413；0 取临时端口，测试用）
 * @param refreshSeconds   页面轮询刷新间隔（秒）
 * @param metricsPort      节点指标端口（概览页 {@code /metrics} 拉取目标）
 * @param requestTimeoutMs 管理请求超时（毫秒）
 * @param security         节点连接安全配置（可选业务令牌与 TLS；缺省
 *                         {@link Security#NONE} 明文无令牌）
 */
public record ConsoleConfig(
        List<Address> addresses,
        String adminToken,
        int port,
        int refreshSeconds,
        int metricsPort,
        long requestTimeoutMs,
        Security security) {

    /** 配置键前缀。 */
    public static final String KEY_PREFIX = "openlatch.console.";

    /**
     * 节点连接安全配置：
     * 控制台与节点间亦过服务端同一 TLS/认证门闩——{@code tlsEnabled} 开启时
     * AdminClient 以 PEM 执行 TLS 握手，{@code authToken} 配置时 HELLO 携带
     * 该业务令牌（与逐消息 {@code admin-token} 独立、不互替借道）。
     *
     * @param tlsEnabled     是否启用节点 TLS（默认 false）
     * @param tlsTrustStore  可信任 CA 的 PEM 证书集路径；未配置取系统默认信任
     * @param tlsClientCert  mTLS 客户端 PEM 证书路径；与 {@code tlsClientKey} 成对
     * @param tlsClientKey   mTLS 客户端 PEM 私钥路径
     * @param authToken      业务令牌（HELLO 携带；服务端业务认证开启时必需）
     */
    public record Security(boolean tlsEnabled, String tlsTrustStore,
                           String tlsClientCert, String tlsClientKey, String authToken) {

        /** 缺省：明文、无令牌（TLS/认证关闭的服务端默认匹配）。 */
        public static final Security NONE = new Security(false, null, null, null, null);

        /**
         * 紧凑构造：空白串归一为 null；mTLS cert/key 须成对（结构性校验）。
         *
         * @throws IllegalArgumentException cert/key 仅配置其一
         */
        public Security {
            tlsTrustStore = norm(tlsTrustStore);
            tlsClientCert = norm(tlsClientCert);
            tlsClientKey = norm(tlsClientKey);
            authToken = norm(authToken);
            if ((tlsClientCert == null) != (tlsClientKey == null)) {
                throw new IllegalArgumentException(
                        "配置项 " + KEY_PREFIX + "tls-client-cert/tls-client-key 必须成对配置（mTLS）");
            }
        }

        /**
         * 从键查找解析（Properties 与 Spring Environment 双通道共用口径）。
         *
         * @param lookup 键查找（返回 {@code null} 表缺省）
         * @return 安全配置（缺省 {@link #NONE} 语义）
         */
        public static Security from(java.util.function.Function<String, String> lookup) {
            return new Security(
                    boolOf(lookup, KEY_PREFIX + "tls-enabled", false),
                    lookup.apply(KEY_PREFIX + "tls-trust-store"),
                    lookup.apply(KEY_PREFIX + "tls-client-cert"),
                    lookup.apply(KEY_PREFIX + "tls-client-key"),
                    lookup.apply(KEY_PREFIX + "auth-token"));
        }

        /**
         * 归一空白串。
         *
         * @param value 原值
         * @return 归一值
         */
        private static String norm(String value) {
            return value == null || value.isBlank() ? null : value;
        }

        /**
         * 解析布尔（仅 {@code true} 当真）。
         *
         * @param lookup   键查找
         * @param key      键
         * @param fallback 缺省值
         * @return 取值
         */
        private static boolean boolOf(java.util.function.Function<String, String> lookup,
                                      String key, boolean fallback) {
            String v = lookup.apply(key);
            return v == null || v.isBlank() ? fallback : Boolean.parseBoolean(v.trim());
        }
    }

    /** 默认 HTTP 监听端口。 */
    public static final int DEFAULT_PORT = 9413;
    /** 默认轮询刷新间隔（秒）。 */
    public static final int DEFAULT_REFRESH_SECONDS = 5;
    /** 默认指标端口（与服务端 {@code MetricsConfig.DEFAULT_PORT} 对齐）。 */
    public static final int DEFAULT_METRICS_PORT = 9412;
    /** 默认管理请求超时（毫秒）。 */
    public static final long DEFAULT_REQUEST_TIMEOUT_MS = 5_000L;

    /**
     * 单个目标节点地址。
     *
     * @param host 主机
     * @param port 业务端口（非指标端口）
     */
    public record Address(String host, int port) {

        /**
         * 解析 {@code host:port} 形态。
         *
         * @param spec 配置项
         * @return 地址值对象
         * @throws IllegalArgumentException 形态非法（缺端口、端口非整数或越界）
         */
        public static Address parse(String spec) {
            int at = spec.lastIndexOf(':');
            if (at <= 0 || at == spec.length() - 1) {
                throw new IllegalArgumentException(
                        "配置项 " + KEY_PREFIX + "server-addresses 非法（应为 host:port）: " + spec);
            }
            int p;
            try {
                p = Integer.parseInt(spec.substring(at + 1).trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "配置项 " + KEY_PREFIX + "server-addresses 端口非整数: " + spec);
            }
            if (p < 1 || p > 65535) {
                throw new IllegalArgumentException(
                        "配置项 " + KEY_PREFIX + "server-addresses 端口越界（应为 1-65535）: " + spec);
            }
            return new Address(spec.substring(0, at).trim(), p);
        }
    }

    /**
     * 紧凑构造：全量校验（快速失败）。
     *
     * @throws IllegalArgumentException 任一字段非法
     */
    public ConsoleConfig {
        security = security == null ? Security.NONE : security;
        if (addresses == null || addresses.isEmpty()) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "server-addresses 不能为空（至少一个 host:port）");
        }
        addresses = List.copyOf(addresses);
        if (adminToken == null || adminToken.isBlank()) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "admin-token 不能为空白（管理通道强制认证）");
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "port 非法（应为 0-65535，0 取临时端口）: " + port);
        }
        if (metricsPort < 1 || metricsPort > 65535) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "metrics-port 非法（应为 1-65535）: " + metricsPort);
        }
        if (refreshSeconds < 1) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "refresh-interval-seconds 应为正整数: " + refreshSeconds);
        }
        if (requestTimeoutMs < 1) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "request-timeout-ms 应为正整数: " + requestTimeoutMs);
        }
    }

    /**
     * 地址列表的 {@code host:port} 字符串形态（逗号分隔）。
     *
     * @param csv 逗号分隔地址串
     * @return 解析结果（空串视为空列表，交由构造校验拒绝）
     */
    public static List<Address> parseAddresses(String csv) {
        List<Address> out = new ArrayList<>();
        if (csv == null || csv.isBlank()) {
            return out;
        }
        for (String part : csv.split(",")) {
            if (!part.isBlank()) {
                out.add(Address.parse(part.trim()));
            }
        }
        return out;
    }

    /**
     * 从 Properties 解析（文件与 Environment 共用口径）。
     *
     * @param props 属性表
     * @return 解析后的配置（构造即校验）
     * @throws IllegalArgumentException 任一键值非法
     */
    public static ConsoleConfig fromProperties(Properties props) {
        return from(props::getProperty);
    }

    /**
     * 从"键 → 值"查找函数解析——Properties 与 Spring Environment 双通道
     * 共用的唯一取值口径（文件桥接与直配语义一致）。
     *
     * @param lookup 键查找（返回 {@code null} 表缺省）
     * @return 解析后的配置（构造即校验）
     * @throws IllegalArgumentException 任一键值非法
     */
    public static ConsoleConfig from(java.util.function.Function<String, String> lookup) {
        return new ConsoleConfig(
                parseAddresses(lookup.apply(KEY_PREFIX + "server-addresses")),
                lookup.apply(KEY_PREFIX + "admin-token"),
                intOf(lookup, KEY_PREFIX + "port", DEFAULT_PORT),
                intOf(lookup, KEY_PREFIX + "refresh-interval-seconds", DEFAULT_REFRESH_SECONDS),
                intOf(lookup, KEY_PREFIX + "metrics-port", DEFAULT_METRICS_PORT),
                longOf(lookup, KEY_PREFIX + "request-timeout-ms", DEFAULT_REQUEST_TIMEOUT_MS),
                Security.from(lookup));
    }

    /**
     * 从 {@code path} 指向的 Properties 文件加载；{@code path} 空白抛快速失败
     * （控制台无默认地址可回落——缺配置即无法工作）。
     *
     * @param path 配置文件路径（系统属性 {@code openlatch.console.config}）
     * @return 加载后的配置
     * @throws IllegalArgumentException 文件缺失、不可读或路径为空
     */
    public static ConsoleConfig load(String path) {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("缺少控制台配置文件："
                    + "请以 -Dopenlatch.console.config=<path> 指定（需含 "
                    + KEY_PREFIX + "server-addresses 与 " + KEY_PREFIX + "admin-token）");
        }
        Properties props = new Properties();
        Path file = Path.of(path);
        try (InputStream in = Files.newInputStream(file)) {
            props.load(in);
        } catch (IOException e) {
            throw new IllegalArgumentException("无法读取配置文件: " + file + " (" + e.getMessage() + ")");
        }
        return fromProperties(props);
    }

    /**
     * 整数配置项解析（缺省回落，非整数快速失败）。
     *
     * @param lookup   键查找
     * @param key      键
     * @param fallback 缺省值
     * @return 取值
     */
    private static int intOf(java.util.function.Function<String, String> lookup,
                             String key, int fallback) {
        String v = lookup.apply(key);
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
     * 长整数配置项解析（缺省回落，非整数快速失败）。
     *
     * @param lookup   键查找
     * @param key      键
     * @param fallback 缺省值
     * @return 取值
     */
    private static long longOf(java.util.function.Function<String, String> lookup,
                               String key, long fallback) {
        String v = lookup.apply(key);
        if (v == null || v.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("配置项 " + key + " 非法（应为整数）: " + v);
        }
    }
}
