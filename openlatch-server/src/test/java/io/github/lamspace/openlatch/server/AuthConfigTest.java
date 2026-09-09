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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link AuthConfig} 加载与校验（spec"业务令牌认证与默认兼容守卫"）：缺省回落
 * 兼容守卫、键覆盖、开启缺令牌快速失败、多令牌命中语义。
 */
class AuthConfigTest {

    @TempDir
    Path tmp;

    private AuthConfig load(String... lines) throws IOException {
        Path file = tmp.resolve("auth.properties");
        Files.writeString(file, String.join("\n", lines) + "\n");
        return AuthConfig.load(file.toString());
    }

    @Test
    void nullPathUnconfigured() {
        AuthConfig cfg = AuthConfig.load(null);
        assertThat(cfg.enabled()).isFalse();
        assertThat(cfg.isEnabled()).isFalse();
        assertThat(cfg.accepts("anything")).isFalse();
    }

    @Test
    void blankFileUnconfigured() throws IOException {
        assertThat(load()).isEqualTo(AuthConfig.unconfigured());
    }

    @Test
    void fileOverridesTokens() throws IOException {
        AuthConfig cfg = load("openlatch.server.auth.enabled=true",
                "openlatch.server.auth.tokens=  tok-a ,tok-b ");
        assertThat(cfg.enabled()).isTrue();
        assertThat(cfg.isEnabled()).isTrue();
        assertThat(cfg.tokens()).containsExactly("tok-a", "tok-b");
        assertThat(cfg.accepts("tok-a")).isTrue();
        assertThat(cfg.accepts("tok-b")).isTrue();
        assertThat(cfg.accepts("tok-c")).isFalse();
        assertThat(cfg.accepts(null)).isFalse();
    }

    @Test
    void enabledWithoutTokensFailsFast() {
        assertThatThrownBy(() -> new AuthConfig(true, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.server.auth.tokens");
        assertThatThrownBy(() -> new AuthConfig(true, List.of("  ")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void disabledAcceptsNothing() {
        // 关闭 = 兼容守卫：不进入命中判定（accepts 纵深防御恒 false）。
        AuthConfig cfg = new AuthConfig(false, List.of("a"));
        assertThat(cfg.isEnabled()).isFalse();
        assertThat(cfg.accepts("a")).isFalse();
    }

    @Test
    void unreadableFileFailsFast() {
        assertThatThrownBy(() -> AuthConfig.load(tmp.resolve("missing.properties").toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("无法读取配置文件");
    }
}
