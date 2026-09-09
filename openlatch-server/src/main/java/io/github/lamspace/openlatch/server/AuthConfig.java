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

import io.github.lamspace.openlatch.server.security.ConstantTime;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 业务令牌认证配置（Phase 3 详设 §5.2/§10.4 P3-16，spec"业务令牌认证与默认
 * 兼容守卫"；{@code openlatch.server.auth.*} 键族）。
 *
 * <p><b>与 {@link ServerConfig}/{@link TlsConfig}/{@link AdminConfig} 的关系</b>：
 * 同一 Properties 文件的认证子集，独立记录而非扩展既有记录（"同文件独立加载"
 * 先例）。业务令牌与 {@code AdminConfig} 的管理令牌 MUST 相互独立（spec
 * "管理令牌独立性"），两者不互替、不借道。
 *
 * <p><b>令牌承载通道</b>：业务令牌经 HELLO 的 {@code HelloRequest.auth_token}
 * 携带（Phase 1 预留字段，P3-16 起启用），一次完成于握手；会话生命周期内不做
 * 逐请求鉴权（连接即身份，详设 §5.3）。
 *
 * <p><b>默认值口径</b>：{@code enabled=false}（默认）= Phase 1 兼容守卫（HELLO
 * 携带非空 {@code auth_token} → 拒绝并断连，行为与现状逐字节一致）；
 * {@code enabled=true} 时 MUST 配置 ≥1 个令牌（启动快速失败），校验命中任一
 * 即放行——多令牌支持轮换期双活（spec"轮换期多令牌双活"）。
 *
 * @param enabled 是否启用业务认证（默认 false = 兼容守卫）
 * @param tokens  业务令牌列表（逗号分隔配置；{@code enabled=true} 时 ≥1；
 *                令牌不得含逗号）
 */
public record AuthConfig(boolean enabled, List<String> tokens) {

    /** 配置键前缀（详设 §5.2）。 */
    public static final String KEY_PREFIX = "openlatch.server.auth.";

    /** 默认认证开关（关闭——Phase 1 兼容守卫）。 */
    public static final boolean DEFAULT_ENABLED = false;

    /**
     * 紧凑构造：令牌空白条目剔除、逐项去空白，随后结构性校验——开启时必须
     * 持 ≥1 个令牌（防"开了认证却无令牌可验"的启动即全拒形态）。
     *
     * @throws IllegalArgumentException {@code enabled=true} 但令牌列表为空
     */
    public AuthConfig {
        List<String> normalized = new ArrayList<>(tokens.size());
        for (String t : tokens) {
            if (t != null && !t.isBlank()) {
                normalized.add(t.trim());
            }
        }
        tokens = List.copyOf(normalized);
        if (enabled && tokens.isEmpty()) {
            throw new IllegalArgumentException(
                    "配置项 " + KEY_PREFIX + "enabled=true 时 MUST 配置至少一个 "
                            + KEY_PREFIX + "tokens 令牌（逗号分隔，令牌不得含逗号）");
        }
    }

    /**
     * 关闭形态（Phase 1 兼容守卫）——兼容构造重载与库内嵌缺省。
     *
     * @return 关闭认证配置
     */
    public static AuthConfig unconfigured() {
        return new AuthConfig(DEFAULT_ENABLED, List.of());
    }

    /**
     * 认证是否开启且已配置令牌（{@code true} 才做命中校验）。
     *
     * @return 开启返回 {@code true}
     */
    public boolean isEnabled() {
        return enabled && !tokens.isEmpty();
    }

    /**
     * 呈现令牌是否命中任一配置令牌。逐项以常量时间比较（{@link ConstantTime}），
     * 遍历不提前退出（防命中位置侧信道）；未开启/无令牌一律返回 {@code false}
     * （纵深防御，正常由握手门闩按开关分叉）。
     *
     * @param presented HELLO 携带的 {@code auth_token}
     * @return 命中任一配置令牌返回 {@code true}
     */
    public boolean accepts(String presented) {
        if (!isEnabled() || presented == null) {
            return false;
        }
        boolean hit = false;
        for (String candidate : tokens) {
            // 恒遍历全部，MUST NOT 提前返回。
            hit |= ConstantTime.matches(presented, candidate);
        }
        return hit;
    }

    /**
     * 从 Properties 解析认证子集；键缺省回落默认（关闭）。
     *
     * @param props 已加载的属性表
     * @return 解析后的配置（构造即结构性校验）
     * @throws IllegalArgumentException 开启而无令牌
     */
    public static AuthConfig fromProperties(Properties props) {
        boolean enabled = boolOf(props, KEY_PREFIX + "enabled", DEFAULT_ENABLED);
        String csv = props.getProperty(KEY_PREFIX + "tokens");
        return new AuthConfig(enabled, splitTokens(csv));
    }

    /**
     * 从 {@code path} 指向的 Properties 文件加载认证配置；{@code path} 为
     * {@code null} 或空白返回 {@link #unconfigured()}（兼容守卫）。
     *
     * @param path 配置文件路径（与 {@link ServerConfig#load(String)} 同源）
     * @return 加载后的配置
     * @throws IllegalArgumentException 文件不可读或开启而无令牌
     */
    public static AuthConfig load(String path) {
        if (path == null || path.isBlank()) {
            return unconfigured();
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
     * 逗号分隔令牌串 → 令牌列表（空白串 → 空表）。
     *
     * @param csv 配置值（可为 {@code null}/空白）
     * @return 令牌列表（未去空白——由紧凑构造归一）
     */
    private static List<String> splitTokens(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (String part : csv.split(",")) {
            out.add(part);
        }
        return out;
    }

    /**
     * 解析布尔配置项（仅 {@code true} 当真；与 {@link TlsConfig} 同口径）。
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
