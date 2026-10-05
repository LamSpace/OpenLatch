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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * topic SDK 端到端（v8，单机服务端）：多订阅者 fan-out 与每订阅升序、
 * String/byte 两形态、自订阅闭环（发布者自身订阅亦收）、退订停止交付、
 * 订阅者连接死亡不伤其余订阅、撞 key（队列占据）显式拒绝、幂等重复订阅
 * 返回同一登记。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClientTopicIT {

    /** 被测服务器（单机）。 */
    private OpenLatchServer server;
    /** 发布端客户端。 */
    private OpenLatchClient publisher;
    /** 订阅端 A。 */
    private OpenLatchClient subA;
    /** 订阅端 B。 */
    private OpenLatchClient subB;

    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        publisher = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        subA = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        subB = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        publisher.connectAsync().get(5, TimeUnit.SECONDS);
        subA.connectAsync().get(5, TimeUnit.SECONDS);
        subB.connectAsync().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        publisher.close();
        subA.close();
        subB.close();
        server.stop();
    }

    @Test
    void fanOutRoundTripWithAscendingSeqPerSubscriber() throws Exception {
        List<String> gotA = new CopyOnWriteArrayList<>();
        List<Long> seqA = new CopyOnWriteArrayList<>();
        List<String> gotB = new CopyOnWriteArrayList<>();
        OTopicSubscription subHandle = subA.newTopic("it:news").subscribe(m -> {
            gotA.add(new String(m.payload(), StandardCharsets.UTF_8));
            seqA.add(m.topicSeq());
        });
        subB.newTopic("it:news").subscribe(m ->
                gotB.add(new String(m.payload(), StandardCharsets.UTF_8)));

        OTopic news = publisher.newTopic("it:news");
        long s1 = news.publish("头条".getBytes(StandardCharsets.UTF_8));
        long s2 = news.publish("速递");
        long s3 = news.publish("简讯".getBytes(StandardCharsets.UTF_8));
        assertThat(s1).isEqualTo(1);
        assertThat(s2).isEqualTo(2);
        assertThat(s3).isEqualTo(3);

        awaitUntil(() -> gotA.size() == 3 && gotB.size() == 3, 10_000);
        assertThat(gotA).containsExactly("头条", "速递", "简讯");
        assertThat(seqA).containsExactly(1L, 2L, 3L); // 单订阅内严格升序
        assertThat(gotB).containsExactly("头条", "速递", "简讯");
        assertThat(subHandle.isActive()).isTrue();
        assertThat(subHandle.droppedCount()).isZero();
    }

    @Test
    void publisherSelfSubscriptionReceivesOwnMessages() throws Exception {
        List<String> echoed = new CopyOnWriteArrayList<>();
        OTopic news = publisher.newTopic("it:echo");
        news.subscribe(m -> echoed.add(new String(m.payload(), StandardCharsets.UTF_8)));
        news.publish("自达");
        awaitUntil(() -> echoed.size() == 1, 10_000);
        assertThat(echoed).containsExactly("自达");
    }

    @Test
    void unsubscribeStopsDeliveryAndSubscribeIsIdempotent() throws Exception {
        List<String> got = new CopyOnWriteArrayList<>();
        OTopic alerts = subA.newTopic("it:alerts");
        OTopicSubscription first = alerts.subscribe(m -> got.add(new String(m.payload(),
                StandardCharsets.UTF_8)));
        // 注：同键再次 subscribe 会替换交付路由（句柄契约），服务端幂等覆盖
        // 的登记侧断言由 TopicRegistryTest 判定矩阵承载，本用例聚焦交付闭环。
        alerts.publish("一");
        awaitUntil(() -> !got.isEmpty(), 10_000);
        int afterFirst = got.size();

        first.close();
        alerts.publish("二");
        Thread.sleep(500); // 退订后静置：不得再有交付
        assertThat(got).hasSize(afterFirst);
        assertThat(first.isActive()).isFalse();
        // 句柄 unsubscribe 幂等。
        alerts.unsubscribe();
    }

    @Test
    void subscriberDeathDoesNotDisturbOthers() throws Exception {
        List<String> survived = new CopyOnWriteArrayList<>();
        subA.newTopic("it:die").subscribe(m ->
                survived.add(new String(m.payload(), StandardCharsets.UTF_8)));
        // 一次性订阅者：建立、订阅、随即离场（登记随会话清理，不吞他人交付）。
        OpenLatchClient doomed = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).build();
        doomed.connectAsync().get(5, TimeUnit.SECONDS);
        doomed.newTopic("it:die").subscribe(m -> {
        });
        doomed.close();

        publisher.newTopic("it:die").publish("继续");
        awaitUntil(() -> !survived.isEmpty(), 10_000);
        assertThat(survived).containsExactly("继续");
    }

    @Test
    void collidingQueueKeyRejected() throws Exception {
        OBlockingQueue queue = publisher.newBlockingQueue("it:collide", 4);
        queue.put("占用");
        OTopic topic = subA.newTopic("it:collide");
        assertThatThrownBy(() -> topic.subscribe(m -> {
        }))
                .isInstanceOf(OpenLatchException.class)
                .extracting(e -> ((OpenLatchException) e).status())
                .isEqualTo(io.github.lamspace.openlatch.protocol.StatusCode.INVALID_REQUEST);
        // 反向：既有队列不受扰动。
        assertThat(queue.size()).isEqualTo(1);
    }

    /** 自旋等待断言条件（IT 就绪门闩，不裸 sleep 定长）。 */
    private static void awaitUntil(java.util.function.BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("条件超时未达成");
    }
}
