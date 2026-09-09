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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * 传输层 TLS 配置（Phase 3 详设 §5.1/§10.4 T4，spec"服务端 TLS 传输层"；
 * {@code openlatch.server.tls.*} 键族）。
 *
 * <p><b>与 {@link ServerConfig}/{@link MetricsConfig}/{@link AdminConfig} 的
 * 关系</b>：同一 Properties 文件的 TLS 子集，独立记录而非扩展既有记录——
 * "同文件独立加载"先例（{@link ClusterConfig} 起），{@code ServerConfig} 构造
 * 签名保持稳定，既有装配与测试不受扰动。
 *
 * <p><b>证书形态</b>：服务端证书与私钥以 PEM 文件路径提供（{@code cert}/
 * {@code key}），经 {@code SslContextBuilder.forServer} 直供——不做
 * PKCS12/keystore 转换，生产与测试走同一 PEM 加载路径；{@code trust-store}
 * 为可信任 CA 的 PEM 证书集，仅在 {@code require-client-cert=true}（mTLS）
 * 时必需。本配置只校验路径的<em>形态</em>（非空/配对），文件本身可读性在
 * {@link OpenLatchServer#start()} 构造 {@code SslContext} 时校验（快速失败，
 * 对齐"端口占用启动失败"语义，design D1）。
 *
 * <p><b>默认值口径</b>：未配置（{@code path} 空白、键缺省）即
 * {@link #disabled()}——明文协议栈，服务端行为与现状逐字节一致（spec"默认
 * 关闭明文照常"）。证书更新以重启生效，无热加载。
 *
 * @param enabled          是否启用 TLS（{@code false} 即明文栈，与现状一致）
 * @param cert             服务端 PEM 证书（链）文件路径；{@code enabled} 时必填
 * @param key              服务端 PEM 私钥文件路径；{@code enabled} 时必填
 * @param trustStore       可信任 CA 的 PEM 证书集路径；{@code require-client-cert} 时必填
 * @param requireClientCert 是否要求客户端证书（mTLS，双向认证）
 */
public record TlsConfig(boolean enabled, String cert, String key, String trustStore,
                        boolean requireClientCert) {

    /** 配置键前缀（详设 §5.1）。 */
    public static final String KEY_PREFIX = "openlatch.server.tls.";

    /** 默认 TLS 开关（关闭——明文栈，§6 兼容性策略"默认关闭"）。 */
    public static final boolean DEFAULT_ENABLED = false;
    /** 默认 mTLS 开关（关闭——单向 TLS）。 */
    public static final boolean DEFAULT_REQUIRE_CLIENT_CERT = false;

    /**
     * 紧凑构造：空白路径归一为 {@code null}，随后执行结构性校验——启用
     * 时必须配对提供 cert/key；要求客户端证书时必须提供 trust-store。
     *
     * @throws IllegalArgumentException 结构性缺失（{@code enabled} 缺 cert/key、
     *                                 或 {@code require-client-cert} 缺 trust-store）
     */
    public TlsConfig {
        cert = cert == null || cert.isBlank() ? null : cert;
        key = key == null || key.isBlank() ? null : key;
        trustStore = trustStore == null || trustStore.isBlank() ? null : trustStore;
        if (enabled) {
            // TLS 关闭时其余键无效果（行为与明文一致）；开启才做配对校验。
            if (cert == null || key == null) {
                throw new IllegalArgumentException(
                        "配置项 " + KEY_PREFIX + "enabled=true 时 MUST 同时配置 "
                                + KEY_PREFIX + "cert 与 " + KEY_PREFIX + "key（PEM 路径）");
            }
            if (requireClientCert && trustStore == null) {
                throw new IllegalArgumentException(
                        "配置项 " + KEY_PREFIX + "require-client-cert=true 时 MUST 配置 "
                                + KEY_PREFIX + "trust-store（可信任 CA PEM）");
            }
        }
    }

    /**
     * 关闭形态（明文栈）——兼容构造重载与库内嵌缺省；与不引入本特性逐字节
     * 一致（spec"默认关闭明文照常"）。
     *
     * @return 关闭 TLS 配置
     */
    public static TlsConfig disabled() {
        return new TlsConfig(DEFAULT_ENABLED, null, null, null, DEFAULT_REQUIRE_CLIENT_CERT);
    }

    /**
     * 从 Properties 解析 TLS 子集；键缺省回落默认值（关闭）。
     *
     * @param props 已加载的属性表
     * @return 解析后的配置（构造即结构性校验）
     * @throws IllegalArgumentException 结构性缺失
     */
    public static TlsConfig fromProperties(Properties props) {
        boolean enabled = boolOf(props, KEY_PREFIX + "enabled", DEFAULT_ENABLED);
        String cert = props.getProperty(KEY_PREFIX + "cert");
        String key = props.getProperty(KEY_PREFIX + "key");
        String trustStore = props.getProperty(KEY_PREFIX + "trust-store");
        boolean requireClientCert =
                boolOf(props, KEY_PREFIX + "require-client-cert", DEFAULT_REQUIRE_CLIENT_CERT);
        return new TlsConfig(enabled, cert, key, trustStore, requireClientCert);
    }

    /**
     * 从 {@code path} 指向的 Properties 文件加载 TLS 配置；{@code path} 为
     * {@code null} 或空白返回 {@link #disabled()}（明文栈）。
     *
     * @param path 配置文件路径（与 {@link ServerConfig#load(String)} 同源）
     * @return 加载后的配置
     * @throws IllegalArgumentException 文件不可读或结构性缺失
     */
    public static TlsConfig load(String path) {
        if (path == null || path.isBlank()) {
            return disabled();
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
     * 解析布尔配置项（仅 {@code true} 当真，大小写不敏感；与
     * {@link ClusterConfig} 同口径）。
     *
     * @param props    属性表
     * @param key      配置键
     * @param fallback 缺省回落值
     * @return 配置值或回落值
     */
    private static boolean boolOf(Properties props, String key, boolean fallback) {
        String v = props.getProperty(key);
        return v == null || v.isBlank() ? fallback : Boolean.parseBoolean(v.trim());
    }
}
