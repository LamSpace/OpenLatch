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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link MetricsConfig} 加载与校验：
 * 缺省回落（启用 + 9412）、文件覆盖、非法值快速失败。
 */
class MetricsConfigTest {

    @TempDir
    Path tmp;

    private MetricsConfig load(String... lines) throws IOException {
        Path file = tmp.resolve("metrics.properties");
        Files.writeString(file, String.join("\n", lines) + "\n");
        return MetricsConfig.load(file.toString());
    }

    @Test
    void nullPathDefaultsEnabled() {
        MetricsConfig cfg = MetricsConfig.load(null);
        assertThat(cfg.enabled()).isTrue();
        assertThat(cfg.port()).isEqualTo(9412);
    }

    @Test
    void blankFileFallsBackToDefaults() throws IOException {
        assertThat(load()).isEqualTo(MetricsConfig.defaults());
    }

    @Test
    void fileOverridesKeys() throws IOException {
        MetricsConfig cfg = load("openlatch.server.metrics.enabled=false",
                "openlatch.server.metrics.port=19999");
        assertThat(cfg.enabled()).isFalse();
        assertThat(cfg.port()).isEqualTo(19999);
    }

    @Test
    void portZeroAllowed() throws IOException {
        MetricsConfig cfg = load("openlatch.server.metrics.port=0");
        assertThat(cfg.port()).isZero();
    }

    @Test
    void invalidPortFailsFast() {
        assertThatThrownBy(() -> new MetricsConfig(true, 70000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.server.metrics.port");
        assertThatThrownBy(() -> new MetricsConfig(true, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nonNumericPortFailsFast() {
        assertThatThrownBy(() -> load("openlatch.server.metrics.port=abc"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.server.metrics.port");
    }

    @Test
    void unreadableFileFailsFast() {
        assertThatThrownBy(() -> MetricsConfig.load(tmp.resolve("missing.properties").toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("无法读取配置文件");
    }
}
