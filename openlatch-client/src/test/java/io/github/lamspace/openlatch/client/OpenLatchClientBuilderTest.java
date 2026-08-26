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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.client.internal.ClientConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * builder 构建与 §6.7 默认值表断言（tasks 1.2/1.4）。
 */
class OpenLatchClientBuilderTest {

    /** 未提供地址时构建必须失败：地址是唯一必填项。 */
    @Test
    void buildWithoutAddressFails() {
        assertThatThrownBy(() -> OpenLatchClient.builder().build())
                .isInstanceOf(IllegalStateException.class);
    }

    /** 地址缺少端口时构建必须失败。 */
    @Test
    void buildWithMalformedAddressFails() {
        assertThatThrownBy(() -> OpenLatchClient.builder().address("no-port").build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** 仅指定地址时，其余参数取 §6.7 默认值。 */
    @Test
    void defaultsMatchSpec() {
        try (OpenLatchClient client = OpenLatchClient.builder().address("127.0.0.1:9410").build()) {
            ClientConfig config = client.config();
            assertThat(config.host()).isEqualTo("127.0.0.1");
            assertThat(config.port()).isEqualTo(9410);
            assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(5));
            assertThat(config.defaultWaitTimeout()).isEqualTo(Duration.ofSeconds(30));
            assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(3));
            assertThat(config.reconnectInitialBackoff()).isEqualTo(Duration.ofMillis(200));
            assertThat(config.reconnectMaxBackoff()).isEqualTo(Duration.ofSeconds(10));
            assertThat(config.workerThreads()).isEqualTo(1);
        }
    }

    /** 覆盖值逐项生效。 */
    @Test
    void overridesTakeEffect() {
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address("localhost:1234")
                .requestTimeout(Duration.ofSeconds(2))
                .defaultWaitTimeout(Duration.ofSeconds(7))
                .connectTimeout(Duration.ofSeconds(1))
                .reconnectInitialBackoff(Duration.ofMillis(50))
                .reconnectMaxBackoff(Duration.ofSeconds(1))
                .workerThreads(2)
                .build()) {
            ClientConfig config = client.config();
            assertThat(config.host()).isEqualTo("localhost");
            assertThat(config.port()).isEqualTo(1234);
            assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(2));
            assertThat(config.defaultWaitTimeout()).isEqualTo(Duration.ofSeconds(7));
            assertThat(config.connectTimeout()).isEqualTo(Duration.ofSeconds(1));
            assertThat(config.reconnectInitialBackoff()).isEqualTo(Duration.ofMillis(50));
            assertThat(config.reconnectMaxBackoff()).isEqualTo(Duration.ofSeconds(1));
            assertThat(config.workerThreads()).isEqualTo(2);
        }
    }

    /** shutdown 幂等：重复调用不抛异常。 */
    @Test
    void shutdownIsIdempotent() {
        OpenLatchClient client = OpenLatchClient.builder().address("127.0.0.1:9410").build();
        client.shutdown();
        client.shutdown();
    }
}
