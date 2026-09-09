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
 * {@link TlsConfig} 加载与结构性校验（spec"服务端 TLS 传输层"）：缺省回落
 * 明文栈、键覆盖、启用必填 cert/key、mTLS 必填 trust-store 的快速失败。
 */
class TlsConfigTest {

    @TempDir
    Path tmp;

    private TlsConfig load(String... lines) throws IOException {
        Path file = tmp.resolve("tls.properties");
        Files.writeString(file, String.join("\n", lines) + "\n");
        return TlsConfig.load(file.toString());
    }

    @Test
    void nullPathDisabled() {
        assertThat(TlsConfig.load(null)).isEqualTo(TlsConfig.disabled());
        assertThat(TlsConfig.disabled().enabled()).isFalse();
    }

    @Test
    void blankFileDisabled() throws IOException {
        assertThat(load()).isEqualTo(TlsConfig.disabled());
    }

    @Test
    void fileOverridesKeys() throws IOException {
        TlsConfig cfg = load("openlatch.server.tls.enabled=true",
                "openlatch.server.tls.cert=/etc/latch/server-cert.pem",
                "openlatch.server.tls.key=/etc/latch/server-key.pem",
                "openlatch.server.tls.require-client-cert=true",
                "openlatch.server.tls.trust-store=/etc/latch/ca.pem");
        assertThat(cfg.enabled()).isTrue();
        assertThat(cfg.cert()).isEqualTo("/etc/latch/server-cert.pem");
        assertThat(cfg.requireClientCert()).isTrue();
        assertThat(cfg.trustStore()).isEqualTo("/etc/latch/ca.pem");
    }

    @Test
    void blankPathValuesNormalizedToNull() {
        // 空白即 null → 结构性校验在解析期快速失败（缺 cert/key）。
        assertThatThrownBy(() -> load("openlatch.server.tls.enabled=true",
                "openlatch.server.tls.cert=   ",
                "openlatch.server.tls.key="))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.server.tls.cert");
    }

    @Test
    void enabledWithoutCertAndKeyFailsFast() {
        assertThatThrownBy(() -> new TlsConfig(true, null, null, null, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.server.tls.cert");
        assertThatThrownBy(() -> new TlsConfig(true, "/c", null, null, false))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.server.tls.cert");
    }

    @Test
    void mtlsWithoutTrustStoreFailsFast() {
        assertThatThrownBy(() -> new TlsConfig(true, "/c", "/k", null, true))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.server.tls.trust-store");
    }

    @Test
    void requireClientCertIgnoredWhenDisabled() {
        // mTLS 开关在 TLS 关闭时无效果（不快速失败，行为与明文一致）。
        TlsConfig cfg = new TlsConfig(false, null, null, null, true);
        assertThat(cfg.enabled()).isFalse();
    }

    @Test
    void unreadableFileFailsFast() {
        assertThatThrownBy(() -> TlsConfig.load(tmp.resolve("missing.properties").toString()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("无法读取配置文件");
    }
}
