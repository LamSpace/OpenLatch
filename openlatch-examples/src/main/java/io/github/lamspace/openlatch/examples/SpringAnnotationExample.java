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

package io.github.lamspace.openlatch.examples;

import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.spring.OpenLatch;

/**
 * 示例 5：Spring Boot 应用 + {@code @OpenLatch}（SpEL key，详设 §9），
 * 兼作验收标准 4 的活证据——除 starter 依赖与注解外零接入代码。
 *
 * <p>演示三件事：异 key 并发（{@code #orderId}）、同 key 串行排队、
 * 立即式获取被拒抛 {@link LockAcquisitionTimeoutException}。
 * 服务器以进程内内嵌方式启动（design D6，演示夹具，生产请独立部署）。
 *
 * <p>运行：{@code mvn -pl openlatch-examples exec:java
 * -Dexec.mainClass=io.github.lamspace.openlatch.examples.SpringAnnotationExample}
 */
@SpringBootConfiguration
@EnableAutoConfiguration
public class SpringAnnotationExample {

    /**
     * 公开无参构造：Boot 实例化配置类所需。
     */
    public SpringAnnotationExample() {
    }

    /**
     * 被注解业务服务。
     */
    static class OrderService {

        /**
         * 包内构造：经 {@link #orderService()} Bean 工厂创建。
         */
        OrderService() {
        }

        /**
         * 按订单号建锁的创建方法（SpEL key：{@code #orderId}）。
         *
         * @param orderId 订单号（即锁键）
         * @return 处理结果描述
         * @throws InterruptedException 模拟工作时被打断
         */
        @OpenLatch(key = "#orderId")
        public String createOrder(String orderId) throws InterruptedException {
            log("create(" + orderId + ") ENTER");
            TimeUnit.MILLISECONDS.sleep(300);
            log("create(" + orderId + ") EXIT ");
            return "created:" + orderId;
        }

        /**
         * 立即式批处理（锁被占直接失败，不排队）。
         *
         * @return 处理结果描述
         * @throws InterruptedException 模拟工作时被打断
         */
        @OpenLatch(key = "'batch'", waitTime = 0)
        public String batchJob() throws InterruptedException {
            log("batch ENTER");
            TimeUnit.MILLISECONDS.sleep(300);
            log("batch EXIT ");
            return "batch";
        }
    }

    /**
     * 业务服务 Bean。
     *
     * @return 服务实例
     */
    @Bean
    OrderService orderService() {
        return new OrderService();
    }

    /**
     * 示例时序基准点（main 启动上下文前赋值）。
     */
    static volatile long t0;

    /**
     * 打相对时间戳的日志行。
     *
     * @param message 内容
     */
    private static void log(String message) {
        System.out.printf("[spring +%4dms] %s (%s)%n",
                (System.nanoTime() - t0) / 1_000_000, message,
                Thread.currentThread().getName());
    }

    /**
     * 入口：先起内嵌服务器，端口经默认属性传给 starter；再跑演示。
     *
     * @param args 未使用
     * @throws Exception 连接或线程等待异常
     */
    public static void main(String[] args) throws Exception {
        OpenLatchServer server = ExampleServers.startDefault();
        t0 = System.nanoTime();
        try (ConfigurableApplicationContext ctx = new SpringApplicationBuilder(
                SpringAnnotationExample.class)
                .web(WebApplicationType.NONE)
                .properties("openlatch.server-host=127.0.0.1",
                        "openlatch.server-port=" + server.port())
                .run(args)) {
            OrderService orders = ctx.getBean(OrderService.class);
            io.github.lamspace.openlatch.client.OpenLatchClient client =
                    ctx.getBean(io.github.lamspace.openlatch.client.OpenLatchClient.class);
            client.connectAsync().get(5, TimeUnit.SECONDS);

            log("--- 1) different SpEL keys run concurrently ---");
            Thread a = new Thread(() -> call(orders, "A"), "worker-A");
            Thread b = new Thread(() -> call(orders, "B"), "worker-B");
            a.start();
            b.start();
            a.join();
            b.join();

            log("--- 2) same key serializes (second waits) ---");
            Thread c = new Thread(() -> call(orders, "X"), "worker-C");
            c.start();
            TimeUnit.MILLISECONDS.sleep(50);
            Thread d = new Thread(() -> call(orders, "X"), "worker-D");
            d.start();
            c.join();
            d.join();

            log("--- 3) waitTime=0 rejects while lock held ---");
            Thread e = new Thread(() -> call(orders, null), "worker-E");
            e.start();
            TimeUnit.MILLISECONDS.sleep(50);
            call(orders, null);
            e.join();
        } finally {
            server.stop();
        }
        System.exit(0);
    }

    /**
     * 调用辅助：null 走批处理立即式路径，否则走订单创建路径。
     *
     * @param orders  服务
     * @param orderId 订单号；null 表示立即式批处理
     */
    private static void call(OrderService orders, String orderId) {
        try {
            if (orderId == null) {
                orders.batchJob();
            } else {
                orders.createOrder(orderId);
            }
        } catch (LockAcquisitionTimeoutException e) {
            log("REJECTED fast-fail: " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
