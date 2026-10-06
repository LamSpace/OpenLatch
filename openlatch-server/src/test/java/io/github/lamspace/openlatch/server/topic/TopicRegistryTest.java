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

package io.github.lamspace.openlatch.server.topic;

import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link TopicRegistry} 判定矩阵：幂等订阅、退订回收与键消解、订阅上限
 * 拒绝、drop-newest 计数与排空顺序、term seq 单调、去重槽重放不双扇出、
 * 会话摘除（死亡即退订）、换主清零与观察读数。投递经包内注入的假
 * {@code DeliverySink} 确定性驱动"可写/反压"两态。
 */
class TopicRegistryTest {

    /** 登记表 + 会话注册表 + 假投递汇夹具。 */
    private static final class Fixture {
        /** 捕获的投递信封（全部订阅合并，写出序）。 */
        final List<Envelope> delivered = new ArrayList<>();
        /** 可写性开关（false=反压，缓冲堆积、触发 drop-newest）。 */
        boolean writable = true;
        /** 丢弃监听回调计数（条数）。 */
        final AtomicLong dropEvents = new AtomicLong();
        /** 会话注册表。 */
        final ServerSessionRegistry sessions = new ServerSessionRegistry();
        /** 被测登记表。 */
        final TopicRegistry registry;

        /**
         * 构造夹具。
         *
         * @param cap 单 key 订阅数上限
         * @param buf 每订阅缓冲条数
         */
        Fixture(int cap, int buf) {
            registry = new TopicRegistry(sessions, cap, buf, new TopicRegistry.DeliverySink() {
                @Override
                public boolean isWritable(ServerSession session) {
                    return writable;
                }

                @Override
                public void deliver(ServerSession session, Envelope envelope) {
                    synchronized (delivered) {
                        delivered.add(envelope);
                    }
                }
            });
            registry.setDropListener(dropEvents::addAndGet);
        }

        /**
         * 登记一个已握手（v8）会话。
         *
         * @param sessionId 逻辑会话 id
         */
        void hello(long sessionId) {
            ServerSession session = new ServerSession(new EmbeddedChannel());
            session.activate(sessionId, 8);
            sessions.register(session);
        }

        /** 便捷订阅（断言放行后返回路由键）。 */
        long subscribe(long sessionId, String key) {
            var r = registry.subscribe(sessionId, key, System.currentTimeMillis());
            assertThat(r.status()).isEqualTo(StatusCode.OK);
            return r.subscriptionId();
        }

        /** 便捷发布（断言放行后返回受理结果）。 */
        TopicRegistry.PublishResult publish(long sessionId, String key, long opSeq, String msg) {
            return registry.publish(sessionId, key, opSeq,
                    msg.getBytes(StandardCharsets.UTF_8), System.currentTimeMillis());
        }

        /** 已捕获信封的 topic_seq 升序列表（全订阅合并）。 */
        List<Long> deliveredSeqs() {
            synchronized (delivered) {
                return delivered.stream().map(e -> e.getTopicMessage().getTopicSeq()).toList();
            }
        }
    }

    @Test
    void subscribeIsIdempotentPerSessionKey() {
        Fixture f = new Fixture(4, 8);
        f.hello(1L);
        long first = f.subscribe(1L, "t");
        long again = f.subscribe(1L, "t");

        assertThat(again).isEqualTo(first);
        assertThat(f.registry.subscriberCount("t")).isEqualTo(1);
        assertThat(f.registry.topicKeyCount()).isEqualTo(1);
    }

    @Test
    void subscriberCapRejectsWithoutDisturbingExisting() {
        Fixture f = new Fixture(2, 8);
        f.hello(1L);
        f.hello(2L);
        f.hello(3L);
        long s1 = f.subscribe(1L, "t");
        long s2 = f.subscribe(2L, "t");

        var third = f.registry.subscribe(3L, "t", System.currentTimeMillis());
        assertThat(third.status()).isEqualTo(StatusCode.REJECT_SUBSCRIBERS);
        assertThat(third.subscriptionId()).isZero();
        assertThat(f.registry.subscriberCount("t")).isEqualTo(2);

        // 腾位后立即可订阅（上限为存续态判定非终身额度）。
        f.registry.unsubscribe(1L, "t");
        var took = f.registry.subscribe(3L, "t", System.currentTimeMillis());
        assertThat(took.status()).isEqualTo(StatusCode.OK);
        assertThat(took.subscriptionId()).isNotEqualTo(s1).isNotEqualTo(s2);
    }

    @Test
    void unsubscribeReclaimsAndDissolvesKey() {
        Fixture f = new Fixture(4, 8);
        f.hello(1L);
        f.subscribe(1L, "t");

        f.registry.unsubscribe(1L, "t");
        assertThat(f.registry.topicKeyCount()).isZero();
        assertThat(f.registry.topicKeys()).isEmpty();
        // 幂等：重复退订与未登记退订均无副作用。
        f.registry.unsubscribe(1L, "t");
        f.registry.unsubscribe(99L, "nope");
        assertThat(f.registry.topicKeyCount()).isZero();
    }

    @Test
    void publishFansOutInAscendingSeqWithFullEnvelopeFields() {
        Fixture f = new Fixture(4, 8);
        f.hello(1L);
        f.hello(2L);
        long sub1 = f.subscribe(1L, "t");
        long sub2 = f.subscribe(2L, "t");

        var r1 = f.publish(9L, "t", 1L, "a");
        var r2 = f.publish(9L, "t", 2L, "b");
        assertThat(r1.replay()).isFalse();
        assertThat(r1.topicSeq()).isEqualTo(1);
        assertThat(r2.topicSeq()).isEqualTo(2);

        assertThat(f.deliveredSeqs()).containsExactly(1L, 1L, 2L, 2L);
        Envelope first = f.delivered.get(0);
        assertThat(first.getType()).isEqualTo(MessageType.TOPIC_MESSAGE);
        assertThat(first.getRequestId()).isZero();
        assertThat(first.getProtocolVersion()).isEqualTo(8);
        assertThat(first.getTopicMessage().getKey()).isEqualTo("t");
        assertThat(first.getTopicMessage().getPublisherSid()).isEqualTo(9L);
        assertThat(first.getTopicMessage().getPayloadBytes().toStringUtf8()).isEqualTo("a");
        assertThat(first.getTopicMessage().getSubscriptionId()).isIn(sub1, sub2);
        assertThat(f.delivered).hasSize(4);
    }

    @Test
    void dedupSlotReplaysWithoutDoubleFanout() {
        Fixture f = new Fixture(4, 8);
        f.hello(1L);
        f.subscribe(1L, "t");

        var first = f.publish(1L, "t", 7L, "m");
        var replay = f.publish(1L, "t", 7L, "m");
        assertThat(replay.replay()).isTrue();
        assertThat(replay.topicSeq()).isEqualTo(first.topicSeq());
        assertThat(f.deliveredSeqs()).containsExactly(first.topicSeq());

        // 新序号 → 新受理。
        var next = f.publish(1L, "t", 8L, "m2");
        assertThat(next.replay()).isFalse();
        assertThat(f.deliveredSeqs()).containsExactly(first.topicSeq(), next.topicSeq());
    }

    @Test
    void slowSubscriptionDropsNewestThenDrainsHeldInOrder() {
        Fixture f = new Fixture(2, 2);
        f.hello(1L);
        f.subscribe(1L, "t");
        f.writable = false;

        assertThat(f.publish(9L, "t", 1L, "m1").topicSeq()).isEqualTo(1);
        assertThat(f.publish(9L, "t", 2L, "m2").topicSeq()).isEqualTo(2);
        // 缓冲（2 条）已满：seq 3、4 被丢最新（不挤占已入队的 1、2）。
        assertThat(f.publish(9L, "t", 3L, "m3").topicSeq()).isEqualTo(3);
        assertThat(f.publish(9L, "t", 4L, "m4").topicSeq()).isEqualTo(4);
        assertThat(f.registry.droppedTotal()).isEqualTo(2);
        assertThat(f.dropEvents.get()).isEqualTo(2);
        assertThat(f.deliveredSeqs()).isEmpty();

        // 恢复可写但缓冲仍满：入队先判、泵送在后——第 5 条（最新）亦被丢，
        // Publisher 回执照常 OK（drop-newest 对发布侧透明）。
        f.writable = true;
        assertThat(f.publish(9L, "t", 5L, "m5").topicSeq()).isEqualTo(5);
        assertThat(f.deliveredSeqs()).containsExactly(1L, 2L);
        assertThat(f.registry.droppedTotal()).isEqualTo(3);

        // 存量排空后新消息正常入队交付（升序恢复，gap 即丢弃推断面）。
        assertThat(f.publish(9L, "t", 6L, "m6").topicSeq()).isEqualTo(6);
        assertThat(f.deliveredSeqs()).containsExactly(1L, 2L, 6L);
        assertThat(f.registry.droppedTotal()).isEqualTo(3);
    }

    @Test
    void deadChannelVoidsBufferWithoutCounting() {
        Fixture f = new Fixture(4, 8);
        // 会话从未登记到连接注册表：publish 正常受理但无处投递，缓冲作废。
        f.subscribe(7L, "t");

        var r = f.publish(9L, "t", 1L, "x");
        assertThat(r.topicSeq()).isEqualTo(1);
        assertThat(f.deliveredSeqs()).isEmpty();
        assertThat(f.registry.droppedTotal()).isZero();
        // 登记仍在（等待会话清理路径摘除，非投递失败即摘）。
        assertThat(f.registry.subscriberCount("t")).isEqualTo(1);
    }

    @Test
    void removeSessionPurgesSubscriptionsAndSlotsAcrossKeys() {
        Fixture f = new Fixture(4, 8);
        f.hello(1L);
        f.hello(2L);
        f.subscribe(1L, "ta");
        f.subscribe(1L, "tb");
        f.subscribe(2L, "ta");
        f.publish(1L, "ta", 3L, "z"); // 建立会话 1 的去重槽

        f.registry.removeSession(1L);
        assertThat(f.registry.subscriberCount("ta")).isEqualTo(1);
        assertThat(f.registry.subscriberCount("tb")).isZero();
        assertThat(f.registry.topicKeys()).containsExactly("ta");

        // 摘除后再登记同 (session,key) 得新路由键（槽亦已清空：重发不重放）。
        long fresh = f.subscribe(1L, "ta");
        var re = f.publish(1L, "ta", 3L, "z-again");
        assertThat(re.replay()).isFalse();
        assertThat(f.registry.subscribers("ta")).anyMatch(sv -> sv.subscriptionId() == fresh);
    }

    @Test
    void clearDropsAllStateOnLeadershipGain() {
        Fixture f = new Fixture(4, 8);
        f.hello(1L);
        f.subscribe(1L, "t");
        f.publish(1L, "t", 1L, "x");

        f.registry.clear();
        assertThat(f.registry.topicKeyCount()).isZero();
        // 换主重订阅 = 新登记：seq 基线重新起算、旧去重槽不作废式重放。
        f.subscribe(1L, "t");
        var after = f.publish(1L, "t", 1L, "y");
        assertThat(after.replay()).isFalse();
        assertThat(after.topicSeq()).isEqualTo(1);
    }

    @Test
    void observationReadingsAreInsertionOrderedAndSorted() {
        Fixture f = new Fixture(4, 8);
        f.hello(1L);
        f.hello(2L);
        f.subscribe(1L, "tb");
        long s2 = f.subscribe(2L, "tb");
        f.subscribe(1L, "ta");

        assertThat(f.registry.topicKeys()).containsExactly("ta", "tb");
        assertThat(f.registry.maxSubscribersCurrent()).isEqualTo(2);
        var subs = f.registry.subscribers("tb");
        assertThat(subs).hasSize(2);
        assertThat(subs.get(0).sessionId()).isEqualTo(1L);
        assertThat(subs.get(1).sessionId()).isEqualTo(2L);
        assertThat(subs.get(1).subscriptionId()).isEqualTo(s2);
        assertThat(subs.get(0).subscribedAtMs()).isPositive();
    }
}
