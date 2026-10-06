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

import io.github.lamspace.openlatch.server.OpenLatchServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 队列 SDK 端到端（v7，单机服务端）：put/take 闭环与 String 便利族、offer/poll
 * 超时本地计时、阻塞挂起的线程中断、超限元素显式异常零生效、容量与形态断言、
 * drainTo 批量与 DELAY 到期可见性（真实墙钟短延时）。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClientQueueIT {

    /** 被测服务器（单机）。 */
    private OpenLatchServer server;
    /** 生产端客户端。 */
    private OpenLatchClient producer;
    /** 消费端客户端。 */
    private OpenLatchClient consumer;

    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        producer = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        consumer = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        producer.connectAsync().get(5, TimeUnit.SECONDS);
        consumer.connectAsync().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        producer.close();
        consumer.close();
        server.stop();
    }

    @Test
    void putTakeRoundTripWithConvenienceFamilies() throws Exception {
        OBlockingQueue queue = producer.newBlockingQueue("it:q", 4);
        queue.put("任务一");
        queue.put("byte".getBytes(StandardCharsets.UTF_8));
        assertThat(queue.size()).isEqualTo(2);
        assertThat(queue.remainingCapacity()).isEqualTo(2);
        OBlockingQueue reader = consumer.newBlockingQueue("it:q", 4);
        assertThat(reader.takeAsString()).isEqualTo("任务一");
        assertThat(new String(reader.take(), StandardCharsets.UTF_8)).isEqualTo("byte");
        assertThat(reader.poll()).isNull(); // 空队立即式
        assertThat(reader.peek()).isNull();
    }

    @Test
    void offerPollTimeoutsAreLocalTimed() throws Exception {
        OBlockingQueue full = producer.newBlockingQueue("it:cap", 1);
        assertThat(full.offer("one")).isTrue();
        assertThat(full.offer("two")).isFalse();            // 队满立即拒
        OBlockingQueue empty = consumer.newBlockingQueue("it:cap", 1);
        assertThat(empty.poll()).isNotNull();               // 取走"one"
        long startNs = System.nanoTime();
        assertThat(empty.poll(200, TimeUnit.MILLISECONDS)).isNull(); // 空队预算计时
        assertThat((System.nanoTime() - startNs) / 1_000_000).isGreaterThanOrEqualTo(180);
        startNs = System.nanoTime();
        // 注意：上面被放弃的 200ms 挂起者占据 take 轨队首直至超时清扫（契约行为），
        // 此处仅验证 put 轨（空闲）的带超时兑现即时成功。
        assertThat(full.offer("three", 2, TimeUnit.SECONDS)).isTrue();
        assertThat((System.nanoTime() - startNs) / 1_000_000).isLessThan(1_500);
        assertThat(full.size()).isEqualTo(1);
    }

    @Test
    void blockedTakeInterruptsLocally() throws Exception {
        OBlockingQueue queue = producer.newBlockingQueue("it:int", 2);
        Thread taker = new Thread(() -> {
            try {
                queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (OpenLatchException e) {
                // 中断竞态下的显式失败同样可接受
            }
        });
        taker.start();
        Thread.sleep(300); // 确保已进入挂起
        taker.interrupt();
        taker.join(5_000);
        assertThat(taker.isAlive()).isFalse(); // 中断即时返回（不被服务端挂起卡死）
        // 投递者死亡不吞元素断言的中服务端侧由 ClusterQueueTest 覆盖；此处验证
        // 中断后队列仍可正常生产消费。
        queue.put("after");
        assertThat(queue.poll()).isEqualTo("after".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void oversizedElementFailsExplicitlyWithZeroEffect() throws Exception {
        OBlockingQueue queue = producer.newBlockingQueue("it:big", 2);
        byte[] big = new byte[5 * 1024]; // 服务端默认 4096 上限
        assertThatThrownBy(() -> queue.offer(big))
                .isInstanceOf(OpenLatchException.class);
        assertThat(queue.size()).isZero(); // 零生效
        // 恰限放行。
        assertThat(queue.offer(new byte[4096])).isTrue();
    }

    @Test
    void capacityAndFormAssertionsRejectMismatchedHandles() throws Exception {
        OBlockingQueue formed = producer.newBlockingQueue("it:assert", 3);
        formed.offer("x");
        OBlockingQueue other = consumer.newBlockingQueue("it:assert", 9); // 主张不符
        assertThatThrownBy(() -> other.offer("y")).isInstanceOf(OpenLatchException.class);
        ODelayQueue wrongForm = consumer.newDelayQueue("it:assert", 3); // 跨形态互拒
        assertThatThrownBy(() -> wrongForm.offerDelayed("z", 1, TimeUnit.MILLISECONDS))
                .isInstanceOf(OpenLatchException.class);
        assertThat(formed.size()).isEqualTo(1); // 双双零扰动
    }

    @Test
    void drainToBatchesAndDelayFormHidesThenDelivers() throws Exception {
        ODelayQueue queue = producer.newDelayQueue("it:drain", 8);
        for (int i = 0; i < 3; i++) {
            queue.offerDelayed("d" + i, 0, TimeUnit.MILLISECONDS);
        }
        List<byte[]> sink = new ArrayList<>();
        assertThat(queue.drainTo(sink, 2)).isEqualTo(2);
        assertThat(sink).hasSize(2);
        assertThat(queue.size()).isEqualTo(1);
        // 注入一个未到期元素：驻留计数 +1，但到期序队首仍是已到期的 d2——
        // peek/take 先兑现 d2，随后 take 等待 late 到期后交付。
        queue.offerDelayed("late", 600, TimeUnit.MILLISECONDS);
        assertThat(queue.size()).isEqualTo(2);
        assertThat(new String(queue.peek(), StandardCharsets.UTF_8)).isEqualTo("d2");
        assertThat(queue.takeAsString()).isEqualTo("d2");
        long startNs = System.nanoTime();
        assertThat(queue.takeAsString()).isEqualTo("late");
        assertThat((System.nanoTime() - startNs) / 1_000_000)
                .as("late 未到期不得先出")
                .isGreaterThanOrEqualTo(100);
        assertThat(queue.size()).isZero();
    }

    @Test
    void delayQueueGrantsInExpiryOrder() throws Exception {
        ODelayQueue queue = producer.newDelayQueue("it:order", 8);
        queue.offerDelayed("soon", 300, TimeUnit.MILLISECONDS);
        queue.offerDelayed("later", 1_500, TimeUnit.MILLISECONDS);
        assertThat(new String(queue.take(), StandardCharsets.UTF_8)).isEqualTo("soon");
        long startNs = System.nanoTime();
        assertThat(new String(queue.take(), StandardCharsets.UTF_8)).isEqualTo("later");
        assertThat((System.nanoTime() - startNs) / 1_000_000)
                .as("未到期不得先出：等待覆盖剩余延迟")
                .isGreaterThanOrEqualTo(100);
    }
}
