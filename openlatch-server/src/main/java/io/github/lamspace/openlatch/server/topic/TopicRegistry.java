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

import com.google.protobuf.ByteString;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TopicMessage;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * topic 订阅登记表——v8 广播原语唯一的存续态载体，Leader 本地易失态。
 *
 * <p><b>职责</b>：承载 {@code (session, key)} 订阅登记、subscription_id 与
 * term 内 {@code topic_seq} 分配、PUBLISH 每 (key, 会话) 去重槽、每订阅
 * 有界缓冲（drop-newest 弱背压）与丢弃计数，并在 publish 受理点完成
 * fan-out 入队与本地连接投递。本登记表是 topic <b>零复制日志</b>
 * 裁决的落点：topic 没有任何复制状态，全部字段随进程/任期
 * 消失，MUST NOT 被序列化进日志或快照。
 *
 * <p><b>状态机</b>：key 级条目在首个 SUBSCRIBE 时建立、最后一个订阅被
 * 摘除时删除（防泄漏：UNSUBSCRIBE/会话死亡/失联探针三路回收后 key 不
 * 残留）；subscription_id 全局单调（登记表实例内，换主后重新起算——
 * 路由键仅在 term 内有意义）；topic_seq 按 key 单调、<b>仅 term 内有序</b>
 * （条目重建即重置，订阅者跨 term 不比较序号）。会话死亡摘除的是
 * 订阅登记、缓冲与去重槽——与队列"死亡不吞元素"刻意相反：<b>topic
 * 的存续态就是订阅本身</b>，会话没了订阅即终结，无消息可"幸存"。
 *
 * <p><b>线程模型</b>：全部公开方法在登记表监视器内完成（判例
 * {@code WaitQueue}）：调用点跨连接 EventLoop（SUBSCRIBE/UNSUBSCRIBE/
 * PUBLISH 受理）、Raft 应用线程（{@code SESSION_CLOSE} 摘除）与角色
 * 事件线程（{@link #clear()}），synchronized 是唯一的互斥来源。
 * 投递路径只做 {@code writeAndFlush} 异步入队（Netty 契约线程安全），
 * 绝不在锁内等待写完成——单订阅的慢连接因此无法阻塞其他订阅或
 * Publisher 应答（drop-newest 的"不扩散"由此保证）。
 *
 * <p><b>投递语义</b>：至多一次。入队即受理（{@code OK} 不承诺交付）；
 * 缓冲满丢最新一条并计数；死通道的既入队消息静默丢弃（连接没了
 * 就没有"交付"可言）；无定时器、无重试、无重放——换主窗陈旧消息
 * MUST NOT 被补投。{@code topic_seq} 的 gap 是订阅侧唯一的丢弃推断
 * 依据（SDK 层实现，服务端不通知单条丢失）。
 *
 * <p><b>调用者义务</b>：接入层 MUST 在调用本类前完成 v8 门、形状校验、
 * {@code maxValueBytes} 钳制与撞 key 探测（判定唯一在接入层，判例 v6
 * 钳制唯一/v7 元素纪律）；本类 MUST NOT 重复校验载荷尺寸或读取本地
 * 配置做二次裁决。订阅会话在场性由握手门闩保证（未握手连接不进入
 * 分发层），本类不设 REJECT_SESSION 分支。
 */
public final class TopicRegistry {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(TopicRegistry.class);

    /** 会话注册表：subscription 投递按逻辑会话 id 反查连接（判例 NotifyEventBridge）。 */
    private final ServerSessionRegistry sessions;
    /** 单 key 订阅数上限（入口裁决值，构造定形）。 */
    private final int maxSubscribersPerKey;
    /** 每订阅在途缓冲条数上限（drop-newest 触发线，构造定形）。 */
    private final int maxSubscriptionBuffer;

    /** key → 键级状态；TreeMap 保证管理面导出的确定性字典序。 */
    private final Map<String, KeyState> keys = new TreeMap<>();
    /** subscription_id 分配器（登记表实例内全局单调；0 保留为"无"）。 */
    private long subscriptionIdGen;
    /** 累计丢弃计数（服务端侧 drop 总量，指标与测试观察口）。 */
    private final AtomicLong droppedTotal = new AtomicLong();
    /** 丢弃事件监听器（指标埋点回挂；可为 {@code null}，volatile 装配后期绑定）。 */
    private volatile LongConsumer dropListener;

    /**
     * 构造登记表（生产形态：投递走 Netty 通道）。
     *
     * @param sessions              本节点会话注册表（投递反查连接）
     * @param maxSubscribersPerKey  单 key 订阅数上限（{@code >= 1}，
     *                              由 {@code ServerConfig} 校验后传入）
     * @param maxSubscriptionBuffer 每订阅在途缓冲条数上限（{@code >= 1}）
     */
    public TopicRegistry(ServerSessionRegistry sessions, int maxSubscribersPerKey,
                         int maxSubscriptionBuffer) {
        this(sessions, maxSubscribersPerKey, maxSubscriptionBuffer, NettySink.INSTANCE);
    }

    /**
     * 构造登记表（注入投递汇形态，单测用假通道驱动缓冲堆积/排空路径；
     * 生产装配 MUST NOT 使用本形态）。
     *
     * @param sessions              本节点会话注册表
     * @param maxSubscribersPerKey  订阅数上限
     * @param maxSubscriptionBuffer 缓冲条数上限
     * @param sink                  投递汇
     */
    TopicRegistry(ServerSessionRegistry sessions, int maxSubscribersPerKey,
                  int maxSubscriptionBuffer, DeliverySink sink) {
        this.sessions = sessions;
        this.maxSubscribersPerKey = maxSubscribersPerKey;
        this.maxSubscriptionBuffer = maxSubscriptionBuffer;
        this.sink = sink;
    }

    /** 投递汇（构造定形，包内可注入假通道，见 {@link #TopicRegistry(ServerSessionRegistry, int, int, DeliverySink)}）。 */
    private final DeliverySink sink;

    /**
     * 投递汇接缝：把"连接可写性判定 + 信封写出"从登记表裁决中分离，
     * 使缓冲堆积/排空路径可确定性单测。生产形态为 {@link NettySink}。
     */
    interface DeliverySink {

        /**
         * 该会话连接当前可否继续投递（可写性判据）。
         *
         * @param session 目标会话（已确认在场且通道活跃）
         * @return 可投递返回 true；反压中返回 false（余量留缓冲）
         */
        boolean isWritable(ServerSession session);

        /**
         * 写出一条 {@code TOPIC_MESSAGE} 推送信封（异步投递语义，
         * MUST NOT 在调用线程等待写完成）。
         *
         * @param session 目标会话
         * @param envelope 推送信封
         */
        void deliver(ServerSession session, Envelope envelope);
    }

    /** 生产投递汇：Netty {@code isWritable} 反压判据 + {@code writeAndFlush} 写出。 */
    private enum NettySink implements DeliverySink {

        /** 单例。 */
        INSTANCE;

        /**
         * 通道可写性判据（写出高水位即 false，触发缓冲堆积）。
         *
         * @param session 目标会话
         * @return 可写返回 true
         */
        @Override
        public boolean isWritable(ServerSession session) {
            return session.channel().isWritable();
        }

        /**
         * 异步写出；失败仅 debug 记录（至多一次契约：不重试、不报错，
         * 登记摘除由会话清理路径收口，判例 {@code pushAwaitNotify}）。
         *
         * @param session 目标会话
         * @param envelope 推送信封
         */
        @Override
        public void deliver(ServerSession session, Envelope envelope) {
            session.channel().writeAndFlush(envelope).addListener(f -> {
                if (!f.isSuccess() && log.isDebugEnabled()) {
                    log.debug("topic push failed for session {}: {}",
                            session.sessionId(),
                            f.cause() == null ? "channel closed" : f.cause().toString());
                }
            });
        }
    }

    /**
     * 回挂丢弃事件监听器（装配后期绑定，判例 {@code setQueueReadyDriver}）。
     * 每次服务端侧 drop 触发 {@code accept(1)}；{@code null} 摘挂。
     *
     * @param listener 监听器，可为 {@code null}
     */
    public void setDropListener(LongConsumer listener) {
        this.dropListener = listener;
    }

    /**
     * 订阅登记结果：拒绝时 {@code status} 为 {@code REJECT_SUBSCRIBERS}，
     * 放行时携带订阅路由键。
     *
     * @param status         协议状态码（OK/REJECT_SUBSCRIBERS）
     * @param subscriptionId 订阅路由键（拒绝路径为 0）
     */
    public record SubscribeResult(StatusCode status, long subscriptionId) {
    }

    /**
     * PUBLISH 受理结果。
     *
     * @param replay   是否命中去重槽的重放（未二次 fan-out）
     * @param topicSeq term 内受理序号（重放时为首次分配值）
     */
    public record PublishResult(boolean replay, long topicSeq) {
    }

    /**
     * 管理面订阅者视图（会话 id、路由键、建立时刻）。
     *
     * @param sessionId      订阅会话的逻辑会话 id
     * @param subscriptionId 订阅路由键
     * @param subscribedAtMs 订阅建立时刻（epoch 毫秒）
     */
    public record SubscriberView(long sessionId, long subscriptionId, long subscribedAtMs) {
    }

    /**
     * 订阅（幂等覆盖）。(session, key) 唯一登记：既有登记时返回原路由键、
     * 缓冲与丢弃计数存续（SDK 换主重订阅路径的复用基础）；新登记受
     * {@code maxSubscribersPerKey} 入口裁决。
     *
     * <p>判定顺序：既有登记命中 → 幂等返回；容量未满 → 建立登记；
     * 否则 {@code REJECT_SUBSCRIBERS}（该 key 既有订阅零扰动——不挤占、
     * 不断开，判例双"满"分轨的拒绝面纪律）。
     *
     * @param sessionId 订阅会话
     * @param key       topic 键（接入层已校验形状/键长）
     * @param nowMs     受理时刻（epoch 毫秒，登记观察值）
     * @return 订阅结果
     */
    public synchronized SubscribeResult subscribe(long sessionId, String key, long nowMs) {
        KeyState ks = keys.computeIfAbsent(key, k -> new KeyState());
        Subscription existing = ks.bySession.get(sessionId);
        if (existing != null) {
            return new SubscribeResult(StatusCode.OK, existing.subscriptionId);
        }
        if (ks.bySession.size() >= maxSubscribersPerKey) {
            return new SubscribeResult(StatusCode.REJECT_SUBSCRIBERS, 0);
        }
        Subscription created = new Subscription(sessionId, ++subscriptionIdGen, key, nowMs);
        ks.bySession.put(sessionId, created);
        return new SubscribeResult(StatusCode.OK, created.subscriptionId);
    }

    /**
     * 退订（幂等）：摘除 (session, key) 登记并释放其缓冲；未存在的登记
     * 静默成功（无错误态，判例 UNSUBSCRIBE 词表纪律）。key 的订阅数归零时
     * 键级条目随之删除（去重槽与 seq 一并消失——防泄漏验收点的机制侧）。
     *
     * @param sessionId 退订会话
     * @param key       topic 键
     */
    public synchronized void unsubscribe(long sessionId, String key) {
        KeyState ks = keys.get(key);
        if (ks == null) {
            return;
        }
        ks.bySession.remove(sessionId);
        ks.slots.remove(sessionId);
        if (ks.bySession.isEmpty()) {
            keys.remove(key);
        }
    }

    /**
     * 发布受理：先查 (key, session) 去重槽——命中即重放（返回首次分配的
     * {@code topic_seq}，MUST NOT 二次 fan-out，判例 v7 每会话去重槽）；
     * 未命中则分配 term 内新 seq、覆写槽，并对该 key 全部登记订阅逐一
     * 入队 + 泵送投递。
     *
     * <p>受理语义：返回即"已入 fan-out"；单订阅缓冲满时该订阅丢弃本条
     * 并计数（drop-newest），其余订阅不受影响。PUBLISH 不隐式建立订阅
     * 登记：对无登记的 key 发布经 {@code computeIfAbsent} 建键态仅为此刻
     * 的扇出与 seq 账——若该键此刻无订阅者，随后即空，键态随
     * {@code unsubscribe}/{@code removeSession} 的收口路径消解。
     *
     * @param sessionId 发布会话
     * @param key       topic 键（接入层已校验形状/载荷钳制）
     * @param opSeq     会话内单调写序号（去重槽比对基准）
     * @param payload   消息体（非 null，字节级原样交付）
     * @param nowMs     受理时刻（epoch 毫秒，写入 {@code publish_ts_ms}）
     * @return 受理结果（replay 标记 + topic_seq）
     */
    public synchronized PublishResult publish(long sessionId, String key, long opSeq,
                                              byte[] payload, long nowMs) {
        KeyState ks = keys.computeIfAbsent(key, k -> new KeyState());
        Slot hit = ks.slots.get(sessionId);
        if (hit != null && hit.opSeq == opSeq) {
            return new PublishResult(true, hit.topicSeq);
        }
        long seq = ++ks.seqGen;
        ks.slots.put(sessionId, new Slot(opSeq, seq));
        for (Subscription sub : ks.bySession.values()) {
            enqueue(sub, seq, sessionId, nowMs, payload);
            pump(sub);
        }
        // 无订阅者时键态可能仅剩去重槽：若此后无人订阅亦无人退订，
        // 槽随 removeSession/换主 clear 收口，不构成登记泄漏。
        if (ks.bySession.isEmpty() && keys.get(key) == ks) {
            keys.remove(key);
        }
        return new PublishResult(false, seq);
    }

    /**
     * 会话摘除：移除该会话在全部 key 上的订阅登记、缓冲与去重槽
     * （死亡即退订）。由两条路径触发：本节点断连清理（单机分发器
     * 收口）与 {@code SESSION_CLOSE} 应用点（集群 Leader 侧，含失联
     * 探针补发）。摘除后 MUST NOT 再向该会话投递任何消息。
     *
     * @param sessionId 要摘除的会话
     */
    public synchronized void removeSession(long sessionId) {
        for (String key : new ArrayList<>(keys.keySet())) {
            KeyState ks = keys.get(key);
            if (ks == null) {
                continue;
            }
            ks.bySession.remove(sessionId);
            ks.slots.remove(sessionId);
            if (ks.bySession.isEmpty() && ks.slots.isEmpty()) {
                keys.remove(key);
            }
        }
    }

    /**
     * 任期清零：换主（当选）时丢弃全部登记——判例 {@code WaitQueue.clear()}
     * 的"挂起者随换主清零重挂"，topic 的对应物是订阅随换主清零重订阅
     * （客户端 SDK 自动重挂，见 client-sdk OTopic 契约）。
     */
    public synchronized void clear() {
        keys.clear();
    }

    // ===== 观察面（管理/指标；只读，MUST NOT 改变任何状态） =====

    /**
     * 订阅登记键数（SUMMARY {@code topic_entries} 口径）。
     *
     * @return 当前键态数
     */
    public synchronized int topicKeyCount() {
        return keys.size();
    }

    /**
     * 登记键字典序快照（LIST_KEYS 行来源；仅 Leader/单机调用侧呈现）。
     *
     * @return key 列表（不可变快照）
     */
    public synchronized List<String> topicKeys() {
        return List.copyOf(keys.keySet());
    }

    /**
     * 单键订阅数（LIST_KEYS/KEY_DETAIL 计数口径；未登记键为 0）。
     *
     * @param key topic 键
     * @return 订阅数
     */
    public synchronized int subscriberCount(String key) {
        KeyState ks = keys.get(key);
        return ks == null ? 0 : ks.bySession.size();
    }

    /**
     * 单键订阅者明细快照（KEY_DETAIL 列表口径；插入序=订阅建立序）。
     *
     * @param key topic 键
     * @return 订阅者视图列表（未登记键为空列表）
     */
    public synchronized List<SubscriberView> subscribers(String key) {
        KeyState ks = keys.get(key);
        if (ks == null) {
            return List.of();
        }
        List<SubscriberView> out = new ArrayList<>(ks.bySession.size());
        for (Subscription sub : ks.bySession.values()) {
            out.add(new SubscriberView(sub.sessionId, sub.subscriptionId, sub.subscribedAtMs));
        }
        return out;
    }

    /**
     * 抓取时刻单键最大订阅数（{@code topic.subscribers.max} gauge 口径，
     * 与等待队深/元素深度两 gauge 同"抓取时刻单键峰值"语义）。
     *
     * @return 当前各键订阅数最大值；无登记为 0
     */
    public synchronized int maxSubscribersCurrent() {
        int max = 0;
        for (KeyState ks : keys.values()) {
            max = Math.max(max, ks.bySession.size());
        }
        return max;
    }

    /**
     * 服务端侧累计丢弃条数（drop-newest 事件总数；指标与测试观察口）。
     *
     * @return 累计丢弃数
     */
    public long droppedTotal() {
        return droppedTotal.get();
    }

    /**
     * 单订阅入队（drop-newest）：缓冲未满则追加尾部；已满时丢弃<b>本条
     * （最新）</b>并计数——既有缓冲照常交付，不挤占队头。
     *
     * @param sub     目标订阅
     * @param seq     topic_seq
     * @param publisherSid 发布会话
     * @param nowMs   受理时刻
     * @param payload 消息体
     */
    private void enqueue(Subscription sub, long seq, long publisherSid, long nowMs, byte[] payload) {
        if (sub.buffer.size() >= maxSubscriptionBuffer) {
            sub.dropped++;
            droppedTotal.incrementAndGet();
            LongConsumer listener = dropListener;
            if (listener != null) {
                listener.accept(1L);
            }
            return;
        }
        sub.buffer.addLast(new Pending(seq, publisherSid, nowMs, payload));
    }

    /**
     * 泵送投递：通道在场且活跃、投递汇判可写时按入队序逐条写出；
     * 通道不可用即作废缓冲（死通道静默丢弃不计数——"交付"以连接存续
     * 为前提，登记摘除由会话清理路径收口）；反压中停泵，余量留缓冲，
     * 下一次 publish 再泵。
     *
     * @param sub 目标订阅
     */
    private void pump(Subscription sub) {
        ServerSession session = sessions.get(sub.sessionId);
        if (session == null || !session.channel().isActive()) {
            sub.buffer.clear(); // 连接已不存在：缓冲随之作废（至多一次，无重投）
            return;
        }
        while (!sub.buffer.isEmpty() && sink.isWritable(session)) {
            Pending p = sub.buffer.pollFirst();
            sink.deliver(session, Envelope.newBuilder()
                    .setProtocolVersion(session.protocolVersion())
                    .setType(MessageType.TOPIC_MESSAGE)
                    .setRequestId(0)
                    .setTopicMessage(TopicMessage.newBuilder()
                            .setKey(sub.key)
                            .setSubscriptionId(sub.subscriptionId)
                            .setTopicSeq(p.seq)
                            .setPublisherSid(p.publisherSid)
                            .setPublishTsMs(p.publishTsMs)
                            .setPayloadBytes(ByteString.copyFrom(p.payload)))
                    .build());
        }
    }

    /** 键级状态：订阅表（LinkedHashMap=建立序，管理面导出确定性序）、seq 账、去重槽。 */
    private static final class KeyState {
        /** 会话 → 订阅登记（建立序）。 */
        final LinkedHashMap<Long, Subscription> bySession = new LinkedHashMap<>();
        /** 发布会话 → 去重槽（恒每会话最近一次）。 */
        final Map<Long, Slot> slots = new LinkedHashMap<>();
        /** term 内 seq 分配账。 */
        long seqGen;

        /** 构造空键级状态（订阅表、槽表与 seq 账均为零值起步）。 */
        KeyState() {
        }
    }

    /** 订阅登记：路由键、缓冲、丢弃计数与所属键。 */
    private static final class Subscription {
        /** 订阅会话。 */
        final long sessionId;
        /** 订阅路由键（TOPIC_MESSAGE 关联用）。 */
        final long subscriptionId;
        /** 所属 topic 键。 */
        final String key;
        /** 建立时刻（epoch 毫秒）。 */
        final long subscribedAtMs;
        /** 在途缓冲（入队序；泵送侧按序取走）。 */
        final ArrayDeque<Pending> buffer = new ArrayDeque<>();
        /** 本订阅累计丢弃数。 */
        long dropped;

        /**
         * 构造订阅登记。
         *
         * @param sessionId      订阅会话
         * @param subscriptionId 路由键
         * @param key            所属 topic 键（泵送信封的 key 字段来源）
         * @param subscribedAtMs 建立时刻
         */
        Subscription(long sessionId, long subscriptionId, String key, long subscribedAtMs) {
            this.sessionId = sessionId;
            this.subscriptionId = subscriptionId;
            this.key = key;
            this.subscribedAtMs = subscribedAtMs;
        }
    }

    /**
     * 缓冲中的待投递消息。
     *
     * @param seq         term 内 topic_seq
     * @param publisherSid 发布会话
     * @param publishTsMs 受理时刻（epoch 毫秒）
     * @param payload     消息体字节
     */
    private record Pending(long seq, long publisherSid, long publishTsMs, byte[] payload) {
    }

    /**
     * PUBLISH 去重槽：最近受理的 (op_seq → topic_seq)。
     *
     * @param opSeq    最近受理的写序号
     * @param topicSeq 对应分配的受理序号
     */
    private record Slot(long opSeq, long topicSeq) {
    }
}
