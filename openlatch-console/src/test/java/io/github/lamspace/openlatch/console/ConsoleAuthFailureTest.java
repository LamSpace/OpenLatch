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

import io.github.lamspace.openlatch.server.OpenLatchServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 T3 认证反例（spec"错误令牌明确降级"）：控制台令牌与服务端不符时
 * 全部页面降级为"管理认证失败"横幅、HTTP 仍 200、不泄露比对细节，
 * 且退避窗内重复刷新不产生重连风暴（页面连续可得）。
 */
@SpringBootTest(classes = OpenLatchConsoleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsoleAuthFailureTest {

    /** 令牌正确配置的真服务器（控制台侧故意配错）。 */
    private static final OpenLatchServer SERVER = ConsoleTestSupport.startServer();

    /** 控制台实际监听端口（随机）。 */
    @Value("${local.server.port}")
    private int port;

    /**
     * 注入错误令牌的控制台配置。
     *
     * @param registry 动态属性注册器
     */
    @DynamicPropertySource
    static void consoleProperties(DynamicPropertyRegistry registry) {
        registry.add("openlatch.console.server-addresses",
                () -> ConsoleTestSupport.nodeAddress(SERVER));
        registry.add("openlatch.console.admin-token", () -> "totally-wrong-token");
        registry.add("openlatch.console.metrics-port", SERVER::metricsPort);
        registry.add("openlatch.console.refresh-interval-seconds", () -> "2");
    }

    /** 关停服务器。 */
    @AfterAll
    void tearDown() {
        SERVER.stop();
    }

    @Test
    void wrongTokenPagesDegradeToAuthBannerWithoutLeaks() {
        String overview = ConsoleTestSupport.get(port, "/");
        assertThat(overview).contains("管理认证失败")
                .doesNotContain("totally-wrong-token"); // 不向浏览器泄露令牌
        // 退避窗内再刷新：仍是清晰降级页（不 5xx、不泄露服务端比对细节）。
        String again = ConsoleTestSupport.get(port, "/");
        assertThat(again).contains("管理认证失败");
        assertThat(ConsoleTestSupport.get(port, "/keys"))
                .contains("管理认证失败");
        assertThat(ConsoleTestSupport.get(port, "/sessions"))
                .contains("管理认证失败");
        assertThat(ConsoleTestSupport.get(port, "/nodes"))
                .contains("管理认证失败");
    }
}
