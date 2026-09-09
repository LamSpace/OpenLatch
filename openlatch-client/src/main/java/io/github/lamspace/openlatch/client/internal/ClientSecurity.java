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

package io.github.lamspace.openlatch.client.internal;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import java.io.IOException;
import java.nio.file.Path;

/**
 * 客户端安全装配工具（Phase 3 详设 §5.1/§5.2，spec"客户端 TLS 与认证消费"）：
 * 由 {@link ClientConfig} 构造客户端 {@link SslContext}——PEM 直供、与
 * 服务端/测试同一条加载路径。连接状态机与种子发现探针共用一个装配源，保证
 * "主连接 / 探针 / 重连"的每一次连接尝试以同一 TLS 配置执行（无绕过路径）。
 */
final class ClientSecurity {

    /** 工具类禁止实例化。 */
    private ClientSecurity() {
    }

    /**
     * 按配置构造客户端 {@link SslContext}；TLS 未启用返回 {@code null}（明文）。
     * 启用时以 {@code trust-store} 为可信任 CA（缺省取系统默认信任），mTLS
     * （客户端 cert/key 均配置）附客户端证书。
     *
     * @param config 客户端配置
     * @return TLS 上下文；{@code tlsEnabled=false} 返回 {@code null}
     * @throws IllegalStateException PEM 文件不可读/不可解析或 cert/key 未成对
     */
    static SslContext clientSslContext(ClientConfig config) {
        if (!config.tlsEnabled()) {
            return null;
        }
        try {
            SslContextBuilder builder = SslContextBuilder.forClient();
            if (config.tlsTrustStore() != null && !config.tlsTrustStore().isBlank()) {
                builder.trustManager(Path.of(config.tlsTrustStore()).toFile());
            }
            boolean hasCert = config.tlsClientCert() != null && !config.tlsClientCert().isBlank();
            boolean hasKey = config.tlsClientKey() != null && !config.tlsClientKey().isBlank();
            if (hasCert != hasKey) {
                throw new IllegalStateException("客户端 TLS 配置无效：tlsClientCert 与 tlsClientKey 必须成对配置");
            }
            if (hasCert) {
                builder.keyManager(Path.of(config.tlsClientCert()).toFile(),
                        Path.of(config.tlsClientKey()).toFile());
            }
            return builder.build();
        } catch (IOException e) {
            throw new IllegalStateException("客户端 TLS 配置加载失败（PEM 文件不可读或不可解析）: "
                    + e.getMessage(), e);
        }
    }
}
