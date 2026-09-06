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
 * 监控指标配置（Phase 3 详设 §3.1/§10.2 T2，spec"指标配置与管理端点生命周期"；
 * {@code openlatch.server.metrics.*} 键族）。
 *
 * <p><b>与 {@link ServerConfig}/{@link ClusterConfig} 的关系</b>：同一 Properties
 * 文件的指标子集，独立记录而非扩展既有记录——保持 {@code ServerConfig} 构造
 * 签名稳定（与 {@link ClusterConfig} 的"同文件独立加载"先例同纪律，既有装配
 * 与测试不受扰动）。
 *
 * <p><b>默认值口径</b>：文件加载路径（{@link #load(String)}，含 path 为空的
 * 全默认启动）下指标 <em>默认启用</em>、管理端口 9412——生产部署开箱即可被抓取；
 * 而 {@link OpenLatchServer} 的兼容构造重载取 {@link #disabled()}，库内嵌
 * （测试/嵌入场景）不因构造对象而抢占固定端口，启用需显式传入本配置。
 *
 * <p><b>校验契约</b>：端口 MUST 落在 0–65535（0 表示操作系统分配临时端口，
 * 测试用）；非法值在构造即快速失败并指明配置键，MUST NOT 静默回落。
 *
 * @param enabled 是否启用管理 HTTP 端点（{@code false} 时不监听；埋点仍
 *                累积进内存注册表，"关闭即不抓取"而非"关闭即无数据"）
 * @param port    管理端口（默认 9412；0 表示临时端口）
 */
public record MetricsConfig(boolean enabled, int port) {

    /** 配置键前缀（详设 §3.1）。 */
    public static final String KEY_PREFIX = "openlatch.server.metrics.";

    /** 默认启用（文件加载路径）。 */
    public static final boolean DEFAULT_ENABLED = true;
    /** 默认管理端口。 */
    public static final int DEFAULT_PORT = 9412;

    /**
     * 紧凑构造：即刻校验端口区间。
     *
     * @throws IllegalArgumentException 端口不在 0–65535
     */
    public MetricsConfig {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "port 非法（应为 0-65535，0 取临时端口）: " + port);
        }
    }

    /**
     * 全默认（启用，9412）——配置文件加载路径的缺省形态。
     *
     * @return 默认配置
     */
    public static MetricsConfig defaults() {
        return new MetricsConfig(DEFAULT_ENABLED, DEFAULT_PORT);
    }

    /**
     * 关闭形态（不监听管理端口）——服务器兼容构造重载的缺省，
     * 保证既有测试与库内嵌装配不因本特性改变端口占用行为。
     *
     * @return 关闭配置
     */
    public static MetricsConfig disabled() {
        return new MetricsConfig(false, DEFAULT_PORT);
    }

    /**
     * 从 Properties 解析指标子集；键缺省回落默认值（启用 + 9412）。
     *
     * @param props 已加载的属性表
     * @return 解析后的配置（构造即校验）
     * @throws IllegalArgumentException 任一键值非法（非布尔按 false 解析同
     *                                  {@link ClusterConfig} 口径；端口非整数或越界快速失败）
     */
    public static MetricsConfig fromProperties(Properties props) {
        boolean enabled = boolOf(props, KEY_PREFIX + "enabled", DEFAULT_ENABLED);
        int port = intOf(props, KEY_PREFIX + "port", DEFAULT_PORT);
        return new MetricsConfig(enabled, port);
    }

    /**
     * 从 {@code path} 指向的 Properties 文件加载指标配置；{@code path} 为
     * {@code null} 或空白返回 {@link #defaults()}（全默认启动指标开启）。
     *
     * @param path 配置文件路径（与 {@link ServerConfig#load(String)} 同源）
     * @return 加载后的配置
     * @throws IllegalArgumentException 文件不可读或配置项非法
     */
    public static MetricsConfig load(String path) {
        if (path == null || path.isBlank()) {
            return defaults();
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

    /**
     * 解析整数配置项；非整数抛 {@link IllegalArgumentException} 指明配置键。
     *
     * @param props    属性表
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
}
