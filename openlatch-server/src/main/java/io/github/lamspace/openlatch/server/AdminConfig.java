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
 * 管理观察配置（{@code openlatch.server.admin.*} 键族）。
 *
 * <p><b>与 {@link ServerConfig}/{@link MetricsConfig} 的关系</b>：同一
 * Properties 文件的管理子集，独立记录而非扩展既有记录（"同文件独立加载"
 * 先例，构造面不受扰动）。
 *
 * <p><b>令牌承载通道</b>：管理令牌经每条 ADMIN 请求
 * 的 {@code token} 字段承载、逐请求校验，MUST NOT 占用 HELLO 的
 * {@code auth_token}（其"非空即断连"规则保持，与业务令牌通道
 * 互不纠缠）。
 *
 * <p><b>默认值口径</b>：未配置（{@code path} 空白、键缺省或值为空白串）
 * 即 {@link #unconfigured()}——拒绝一切 ADMIN 请求（安全默认：宁拒绝
 * 不裸奔）；该形态下服务端其余行为与不启用本特性逐字节一致。
 *
 * @param token 管理令牌；{@code null} 表示未配置（一律拒绝 ADMIN）
 */
public record AdminConfig(String token) {

    /** 配置键前缀。 */
    public static final String KEY_PREFIX = "openlatch.server.admin.";

    /**
     * 紧凑构造：空白令牌归一为"未配置"（{@code null}），杜绝
     * "配了个空串还以为配了"的静默裸奔形态。
     */
    public AdminConfig {
        token = token == null || token.isBlank() ? null : token;
    }

    /**
     * 未配置形态（拒绝一切 ADMIN）——兼容构造重载与库内嵌缺省。
     *
     * @return 未配置管理令牌
     */
    public static AdminConfig unconfigured() {
        return new AdminConfig(null);
    }

    /**
     * 是否已配置可用令牌（配置了 ADMIN 才有受理可能；未配置 = 一律拒绝）。
     *
     * @return 已配置返回 true
     */
    public boolean isConfigured() {
        return token != null;
    }

    /**
     * 从 Properties 解析管理子集；键缺省或未配置形态回落
     * {@link #unconfigured()}。
     *
     * @param props 已加载的属性表
     * @return 解析后的配置
     */
    public static AdminConfig fromProperties(Properties props) {
        return new AdminConfig(props.getProperty(KEY_PREFIX + "token"));
    }

    /**
     * 从 {@code path} 指向的 Properties 文件加载管理配置；{@code path} 为
     * {@code null} 或空白返回 {@link #unconfigured()}（全默认启动 ADMIN
     * 关闭）。
     *
     * @param path 配置文件路径（与 {@link ServerConfig#load(String)} 同源）
     * @return 加载后的配置
     * @throws IllegalArgumentException 文件不可读
     */
    public static AdminConfig load(String path) {
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
}
