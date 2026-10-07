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

import io.github.lamspace.openlatch.client.OBarrier;
import io.github.lamspace.openlatch.client.OCountDownLatch;
import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OSemaphore;
import io.github.lamspace.openlatch.client.OpenLatchException;
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
 * 控制台端到端冒烟（单机档，五页面只读呈现）：同 JVM 拉起真
 * 服务器（锁端口 0 + 指标端口 0 + admin-token 已配置）与真 Boot 控制台，
 * 以业务 client SDK 预置"重入锁持有+排队、Semaphore 部分许可+大请求排队、
 * Latch 等待者"，并以裸协议 v9 折叠 ACQUIRE 预置条件等待（v9 条件区段与
 * "持有已随 await 释放"形态的数据源，不依赖并行交付中的 SDK 门面），
 * 对五页面 HTML 逐项断言并覆盖轮询刷新后的曲线区。
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
    /** v8 订阅句柄（贯穿用例，防提前退订）。 */
    private static io.github.lamspace.openlatch.client.OTopicSubscription topicSub;
    /**
     * v9 条件等待预置连接（裸协议 v9 客户端：折叠 await 属协议面而非既有
     * SDK 门面——条件区段的数据源须独立于并行交付的 OCondition）。
     */
    private static ConsoleRawClient condClient;

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
     * v8 追加 alerts:topic 订阅与一条哨兵载荷发布（页面零外发断言）。
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
        // 循环屏障：一方到场挂起（parties=2 未集齐），观察面须呈现世代读数。
        OBarrier stage = seedClient.newBarrier("stage:sync", 2);
        BLOCKED.submit(() -> {
            try {
                stage.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (OpenLatchException e) {
                // 兜底超时破障（teardown 前）：观察面已断言过，脱队可接受。
            }
        });
        awaitVisible("/keys", "stage:sync");
        // v7 队列：延时形态灌满容量（delay=0 即可见元素），再挂一个等容量的 put。
        io.github.lamspace.openlatch.client.ODelayQueue tasks =
                seedClient.newDelayQueue("tasks:queue", 2);
        tasks.offerDelayed(new byte[] {1}, 0, TimeUnit.MILLISECONDS);
        tasks.offerDelayed(new byte[] {2}, 0, TimeUnit.MILLISECONDS);
        BLOCKED.submit(() -> {
            try {
                tasks.put(new byte[] {3});
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitVisible("/keys", "tasks:queue");
        // v8 topic：订阅 alerts:topic 并发布一条带哨兵载荷的消息——
        // 订阅维须可见，载荷在一切管理页面零出现。
        io.github.lamspace.openlatch.client.OTopic alerts =
                seedClient.newTopic("alerts:topic");
        topicSub = alerts.subscribe(message -> {
            // 观察哨：交付到本地即可，页面断言不依赖它。
        });
        alerts.publish("console-payload-secret".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        awaitVisible("/keys", "alerts:topic");
        // v9 条件等待（裸协议折叠 ACQUIRE 预置，独立于并行交付的 SDK OCondition）：
        // 先持有再折叠 await——持有随 await 释放、条件等待者使条目在管理面持续
        // 可见（详情页条件区段与"已随 await 释放"形态注记依赖此状态）。
        condClient = new ConsoleRawClient();
        condClient.connect("127.0.0.1", SERVER.port(), 9);
        assertThat(condClient.acquire("cond:job", 7L, 0L, null))
                .isEqualTo(io.github.lamspace.openlatch.protocol.StatusCode.OK);
        assertThat(condClient.acquire("cond:job", 7L, -1L, "gate"))
                .isEqualTo(io.github.lamspace.openlatch.protocol.StatusCode.QUEUED);
        awaitVisible("/keys", "cond:job");
        // v10 phaser：构造注册 2 方、一方到场即返，另一方线程挂 arriveAndAwaitAdvance
        //（相位未合拢持续挂起——页签三计数、等待区段与"等待者含相位等待"口径的
        // 断言依赖此状态；teardown 关会话即隐式摘除收敛）。
        io.github.lamspace.openlatch.client.OPhaser phaser =
                seedClient.newPhaser("stage:phaser", 3);
        phaser.arrive(); // 1/3；BLOCKED 的 A_A 再计一发（2/3 挂起）
        BLOCKED.submit(() -> {
            try {
                phaser.arriveAndAwaitAdvance();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                // 会话关闭/超时形态：观察面已断言过，脱队可接受。
            }
        });
        awaitVisible("/keys", "stage:phaser");
        // v11 timer：装载远钟 + 一线程挂 await（未到期持续挂起——keys 页三元组、
        // 详情等待区段与"等待者含 timer 挂起"口径断言依赖此状态）。
        io.github.lamspace.openlatch.client.OTimer timer =
                seedClient.newTimer("stage:timer");
        timer.schedule(600_000, java.util.concurrent.TimeUnit.MILLISECONDS);
        BLOCKED.submit(() -> {
            try {
                timer.await(30, java.util.concurrent.TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                // 会话关闭/超时形态：观察面已断言过，脱队可接受。
            }
        });
        awaitVisible("/keys", "stage:timer");
    }

    /** 关停服务器与客户端（daemon 阻塞线程随连接关闭自然脱队）。 */
    @AfterAll
    void tearDown() {
        if (condClient != null) {
            condClient.close();
        }
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
        // 持有 lock=1 / semaphore=1 / latch 条目=1 / barrier 条目=1 / queue 条目=1 /
        // phaser 条目=1 / timer 条目=1 /
        // 等待者=8（锁/信号量/Latch/屏障各一 + v7 队列等容量挂起者一 + v9 条件
        // 等待者一 + v10 phaser 到场等待者一 + v11 timer 旁观等待者一——v9/v10/v11
        // 口径：等待者数字含条件、相位与到期等待者，表头随行注记）。
        assertThat(body).contains("<td>1</td>");
        assertThat(body).contains("<td>8</td>");
        assertThat(body).contains("等待者（含条件/相位等待）");
        // v10：PHASER 条目数列（概览表头与读数）；v11：TIMER 条目数列。
        assertThat(body).contains("PHASER 条目");
        assertThat(body).contains("TIMER 条目");
        // 会话数：业务客户端 + 控制台自身管理连接 + v9 条件预置裸协议连接。
        assertThat(body).contains("<td>3</td>");
        // sparkline 三线已渲染（指标区未降级）。
        assertThat(body).contains("<polyline");
        assertThat(body).doesNotContain("该节点指标不可用");
    }

    @Test
    void keysPageListsAllFamiliesWithPagingInfo() {
        String body = get("/keys");
        assertThat(body).contains("order:1").contains("pool:db").contains("gate:boot")
                .contains("stage:sync").contains("tasks:queue").contains("alerts:topic")
                .contains("cond:job").contains("stage:phaser")
                .contains("lock").contains("semaphore").contains("latch").contains("barrier")
                .contains("queue").contains("topic").contains("phaser");
        // 九行 key（链接计数），总条数读数为 9（pager 的 <span>9</span>）——
        // 含 v9 条件等待键（持有已随 await 释放、条件集使条目持续可见）与
        // v11 timer 旁观键（装载存续使条目持续可见）。
        assertThat(org.springframework.util.StringUtils.countOccurrencesOf(
                body, "/key?node=")).isEqualTo(9);
        assertThat(body).contains("<span>9</span>");
        // v9 LOCK 行条件等待数随行呈现（Leader/单机来源非零如实）。
        assertThat(body).contains("1 条件等待");
        // topic 行读数（订阅数；Leader 本地登记注记）。
        assertThat(body).contains("1 订阅（Leader 本地登记）");
        // 屏障行读数（parties · 世代 · 到场）。
        assertThat(body).contains("2 方 · 世代 1 · 到场 1");
        // 队列行读数（容量 · 深度 · 队首预览——全量元素不外发）。
        assertThat(body).contains("2 容量 · 深度 2 · 队首 1B");
        // v10 phaser 行读数（registered · 相位 · 到场——三计数复制态口径）。
        assertThat(body).contains("3 registered · 相位 0 · 到场 2");
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
        // 循环屏障明细：parties/世代/到场/最近了结读数 + 无持有语义占位。
        String barrier = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER) + "&key=stage:sync");
        assertThat(barrier).contains("parties 2").contains("当前世代 1")
                .contains("到场 1").contains("最近了结 none")
                .contains("无持有者（循环屏障按到场合拢裁决，无持有语义）");
        // v7 队列明细：容量/深度/驻留/队首到期读数 + 等容量挂起者轨道列。
        String queue = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER)
                + "&key=tasks:queue");
        assertThat(queue).contains("容量 2").contains("深度 2").contains("驻留 2B")
                .contains("等容量");
        assertThat(queue).contains("无持有者（队列元素绑定 key 不绑定会话，无持有语义）");
        // v8 topic 明细：订阅读数 + 订阅者区段 + 无持有/无等待语义占位；
        // 哨兵载荷在所有页面零出现（观察面零放大）。
        String topic = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER)
                + "&key=alerts:topic");
        assertThat(topic).contains("订阅 1").contains("<h3>订阅者</h3>")
                .contains("无持有者（广播订阅不持有 key，无持有语义）")
                .contains("无人等待（广播交付经服务端推送通道，无等待队列）")
                .doesNotContain("console-payload-secret");
        assertThat(get("/keys")).doesNotContain("console-payload-secret");
        assertThat(get("/")).doesNotContain("console-payload-secret");
        // v10 phaser 明细：账簿三计数横注 + 注册配额表 + 相位等待区段
        //（登记到达序、已见相位读数；无持有语义如实标注）。
        String ph = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER)
                + "&key=stage:phaser");
        assertThat(ph).contains("相位 0 · registered 3 · 当前到场 2")
                .contains("<h3>注册配额</h3>").contains("<h3>相位等待</h3>")
                .contains("无持有者（相位器按注册/到场合拢裁决，无持有语义）");
        // v11 timer 明细：三元组横注（装载态与绝对到期时刻原始读数、无 marked
        // 折算）+ 装载账簿表 + 到期等待区段（登记到达序）+ 无持有语义如实标注。
        String tm = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER)
                + "&key=stage:timer");
        assertThat(tm).contains("代次 1").contains("<h3>装载账簿</h3>")
                .contains("<h3>到期等待</h3>").contains("在装")
                .contains("无持有者（延时触发绑定 key 不绑定会话，装载者死亡钟照响，无持有语义）")
                .doesNotContain("已到期");
        // v9 条件等待区段（单机口径）：区段标题、条件名寻址明细、
        // "持有已随 await 释放"形态注记（不以空壳掩盖）；等待队列区段
        // 与之并列且搬运项不重复（当前无搬运，队列为"无人等待"）。
        String cond = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER)
                + "&key=cond:job");
        assertThat(cond).contains("<h3>条件等待</h3>").contains("<td>gate</td>")
                .contains("持有已随 await 释放").doesNotContain("仅 Leader 可见")
                .doesNotContain("无条件等待者");
        // 无条件等待的 LOCK 键如实零呈现：区段在、明细空（不藏区段、不造假）。
        String order = get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER)
                + "&key=order:1");
        assertThat(order).contains("<h3>条件等待</h3>").contains("无条件等待者");
        // 非条件 LOCK 键同区段呈现但如实零（order:1 无搬运入集）。
        assertThat(get("/key?node=" + ConsoleTestSupport.nodeAddress(SERVER)
                + "&key=order:1")).contains("<h3>条件等待</h3>")
                .contains("无条件等待者");
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
