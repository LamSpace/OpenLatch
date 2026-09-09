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

import java.io.IOException;
import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 T3 故障降级用例（spec"任一节点故障 MUST NOT 阻断对其余节点的呈现"）：
 * 地址列表含一个真服务器与一个"占而未听"端口（宕机形态），概览须逐块
 * 如实降级——死节点标注不可达、活节点数字照常呈现、恢复后自动回连由
 * 懒重连语义保证（下一次查询即重握手）。
 */
@SpringBootTest(classes = OpenLatchConsoleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsolePartialFailureTest {

    /** 正常节点。 */
    private static final OpenLatchServer HEALTHY = ConsoleTestSupport.startServer();
    /** 占而未听端口：连接即拒（模拟宕机节点）。 */
    private static final String DEAD_NODE = "127.0.0.1:" + deadPort();

    /** 控制台实际监听端口（随机）。 */
    @Value("${local.server.port}")
    private int port;

    /**
     * 申请一个临时端口并立即释放（保持"无人监听"形态）。
     *
     * @return 端口号
     */
    private static int deadPort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("无法分配死端口", e);
        }
    }

    /**
     * 注入两节点地址列表（一活一死）与正确令牌。
     *
     * @param registry 动态属性注册器
     */
    @DynamicPropertySource
    static void consoleProperties(DynamicPropertyRegistry registry) {
        registry.add("openlatch.console.server-addresses",
                () -> ConsoleTestSupport.nodeAddress(HEALTHY) + "," + DEAD_NODE);
        registry.add("openlatch.console.admin-token", () -> ConsoleTestSupport.ADMIN_TOKEN);
        registry.add("openlatch.console.metrics-port", HEALTHY::metricsPort);
        registry.add("openlatch.console.refresh-interval-seconds", () -> "2");
    }

    /** 关停服务器。 */
    @AfterAll
    void tearDown() {
        HEALTHY.stop();
    }

    @Test
    void deadNodeDegradesWhileHealthyNodesRender() {
        String body = ConsoleTestSupport.get(port, "/");
        assertThat(body).contains(DEAD_NODE).contains("不可用");
        // 活节点块照常呈现（数字/角色/曲线不受死节点拖累）。
        assertThat(body).contains("SINGLE");
        assertThat(body).doesNotContain("管理认证失败");
        // 再次查询另一页面：活节点持续可用（失败分类不粘滞）。
        assertThat(ConsoleTestSupport.get(port, "/keys"))
                .contains(ConsoleTestSupport.nodeAddress(HEALTHY))
                .contains(DEAD_NODE);
    }
}
