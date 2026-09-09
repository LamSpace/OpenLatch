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
 * §7 T3 指标区独立降级用例（spec"指标区独立降级"）：锁通道正常而
 * {@code /metrics} 不可达（指标端口指向未监听端口，等价节点
 * {@code metrics.enabled=false}）时，概览数字照常呈现、曲线区单独标注
 * 不可用、MUST NOT 整页错误或以零值冒充。
 */
@SpringBootTest(classes = OpenLatchConsoleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsoleMetricsDownTest {

    /** 真服务器（管理指标端口同样开启，但控制台配置指向无人监听的端口）。 */
    private static final OpenLatchServer SERVER = ConsoleTestSupport.startServer();

    /** 控制台实际监听端口（随机）。 */
    @Value("${local.server.port}")
    private int port;

    /**
     * 注入正确地址与 token，但指标端口置 1（无服务监听）。
     *
     * @param registry 动态属性注册器
     */
    @DynamicPropertySource
    static void consoleProperties(DynamicPropertyRegistry registry) {
        registry.add("openlatch.console.server-addresses",
                () -> ConsoleTestSupport.nodeAddress(SERVER));
        registry.add("openlatch.console.admin-token", () -> ConsoleTestSupport.ADMIN_TOKEN);
        registry.add("openlatch.console.metrics-port", () -> "1");
        registry.add("openlatch.console.refresh-interval-seconds", () -> "2");
    }

    /** 关停服务器。 */
    @AfterAll
    void tearDown() {
        SERVER.stop();
    }

    @Test
    void metricsDownKeepsNumbersAndDegradesSparklineOnly() {
        String body = ConsoleTestSupport.get(port, "/");
        assertThat(body).contains("SINGLE").contains(OpenLatchServer.serverVersion())
                .contains("该节点指标不可用")
                .doesNotContain("不可用：")  // 锁通道未故障：不出现节点级错误
                .doesNotContain("<polyline");
    }
}
