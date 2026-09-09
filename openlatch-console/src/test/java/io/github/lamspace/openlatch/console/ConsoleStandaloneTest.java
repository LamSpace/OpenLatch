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

import io.github.lamspace.openlatch.client.OCountDownLatch;
import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OSemaphore;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 T3 控制台端到端冒烟（单机档，spec"五页面只读呈现"）：同 JVM 拉起真
 * 服务器（锁端口 0 + 指标端口 0 + admin-token 已配置）与真 Boot 控制台，
 * 以业务 client SDK 预置"重入锁持有+排队、Semaphore 部分许可+大请求排队、
 * Latch 等待者"，对五页面 HTML 逐项断言并覆盖轮询刷新后的曲线区。
 */
@SpringBootTest(classes = OpenLatchConsoleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ConsoleStandaloneTest {

    /** 上下文创建前就绪的临时端口服务器（starter IT 同法）。 */
    private static final OpenLatchServer SERVER = ConsoleTestSupport.startServer();
    /** 预置负载的业务客户端。 */
    private static OpenLatchClient seedClient;
    /** 阻塞负载投递池（daemon：用例结束不强求解锁）。 */
    private static final ExecutorService BLOCKED = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "console-seed-blocked");
        t.setDaemon(true);
        return t;
    });
    /** 贯穿用例的持有引用（防 GC 与意外释放）。 */
    private static OLock lockHolder;
    /** 信号量持有者。 */
    private static OSemaphore semHolder;
    /** 屏障句柄。 */
    private static OCountDownLatch gate;

    /** 控制台实际监听端口（随机）。 */
    @Value("${local.server.port}")
    private int port;

    /**
     * 把服务器实际端口注入控制台配置（地址/token/指标端口/快刷 2s）。
     *
     * @param registry 动态属性注册器
     */
    @DynamicPropertySource
    static void consoleProperties(DynamicPropertyRegistry registry) {
        registry.add("openlatch.console.server-addresses",
                () -> ConsoleTestSupport.nodeAddress(SERVER));
        registry.add("openlatch.console.admin-token", () -> ConsoleTestSupport.ADMIN_TOKEN);
        registry.add("openlatch.console.metrics-port", SERVER::metricsPort);
        registry.add("openlatch.console.refresh-interval-seconds", () -> "2");
    }

    /**
     * 预置负载：order:1 持有+排队、pool:db 2/3 许可+3 许可排队、gate:boot
     * 计数 2 含一个 awaiter；等待三把 key 在锁列表页可见后再进用例。
     *
     * @throws Exception 连接/预置失败
     */
    @BeforeAll
    void seed() throws Exception {
        seedClient = OpenLatchClient.builder()
                .address(ConsoleTestSupport.nodeAddress(SERVER)).build();
        seedClient.connectAsync().get(10, TimeUnit.SECONDS);
        lockHolder = seedClient.newReentrantLock("order:1");
        lockHolder.lock();
        BLOCKED.submit(() -> {
            try {
                OLock w = seedClient.newReentrantLock("order:1");
                w.lock();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        semHolder = seedClient.newSemaphore("pool:db", 3);
        semHolder.acquire(2);
        BLOCKED.submit(() -> {
            try {
                OSemaphore w = seedClient.newSemaphore("pool:db");
                w.acquire(3);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        gate = seedClient.newCountDownLatch("gate:boot", 2);
        BLOCKED.submit(() -> {
            try {
                gate.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitVisible("/keys", "gate:boot");
    }

    /** 关停服务器与客户端（daemon 阻塞线程随连接关闭自然脱队）。 */
    @AfterAll
    void tearDown() {
        if (seedClient != null) {
            seedClient.close();
        }
        SERVER.stop();
        BLOCKED.shutdownNow();
    }

    /**
     * 轮询页面直到出现目标串（异步排队登记的就绪门闩，不裸 sleep）。
     *
     * @param path   页面路径
     * @param needle 目标串
     */
    private void awaitVisible(String path, String needle) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (get(path).contains(needle)) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("等待页面可见被中断", e);
            }
        }
        throw new AssertionError("页面 " + path + " 超时未见 " + needle);
    }

    /** GET 页面正文。 */
    private String get(String path) {
        return ConsoleTestSupport.get(port, path);
    }

    @Test
    void overviewShowsAggregatesRoleVersionAndSparklines() {
        String body = get("/");
        assertThat(body).contains("SINGLE")
                .contains(OpenLatchServer.serverVersion())
                .doesNotContain("管理认证失败");
        // 持有 lock=1 / semaphore=1 / latch 条目=1 / 等待者=3（三队列各一）。
        assertThat(body).contains("<td>1</td>");
        assertThat(body).contains("<td>3</td>");
        // 会话数：业务客户端 + 控制台自身管理连接。
        assertThat(body).contains("<td>2</td>");
        // sparkline 三线已渲染（指标区未降级）。
        assertThat(body).contains("<polyline");
        assertThat(body).doesNotContain("该节点指标不可用");
    }

    @Test
    void keysPageListsAllFamiliesWithPagingInfo() {
        String body = get("/keys");
        assertThat(body).contains("order:1").contains("pool:db").contains("gate:boot")
                .contains("lock").contains("semaphore").contains("latch");
        // 三行 key（链接计数），总条数读数为 3（pager 的 <span>3</span>）。
        assertThat(org.springframework.util.StringUtils.countOccurrencesOf(
                body, "/key?node=")).isEqualTo(3);
        assertThat(body).contains("<span>3</span>");
        // 锁名可点进详情（只读边界：无解锁按钮/表单）。
        assertThat(body).contains("/key?node=");
        assertThat(body).doesNotContain("<form method=\"post\"");
    }

    @Test
    void keyDetailShowsHoldersAndQueueForStandalone() {
        String body = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER) + "&key=order:1");
        assertThat(body).contains("等待队列").contains("writer")
                .doesNotContain("仅 Leader 可见")
                .doesNotContain("无持有者");
        // 信号量明细带池读数、屏障明细带计数读数。
        String sem = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER) + "&key=pool:db");
        assertThat(sem).contains("可用许可 1/3").contains("holder");
        String latch = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER) + "&key=gate:boot");
        assertThat(latch).contains("屏障计数 2/2").contains("latch");
    }

    @Test
    void keyDetailMissingKeyRendersNotHeldBanner() {
        String body = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER) + "&key=nope:x");
        assertThat(body).contains("NOT_HELD").contains("无条目状态");
    }

    @Test
    void sessionsPageListsBothConnections() {
        String body = get("/sessions");
        assertThat(body).contains("接入会话").doesNotContain("无接入会话");
    }

    @Test
    void nodesPageReportsSingleModeHonestly() {
        String body = get("/nodes");
        assertThat(body).contains("单机部署").contains("SINGLE");
    }
}
