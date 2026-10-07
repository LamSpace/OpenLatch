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

import com.google.protobuf.ByteString;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TopicMessage;
import io.github.lamspace.openlatch.protocol.TopicOp;
import io.github.lamspace.openlatch.protocol.TopicOpRequest;
import io.github.lamspace.openlatch.protocol.TopicOpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Objects;

/**
 * {@link OTopic} 的远端实现（v8）：SUBSCRIBE/UNSUBSCRIBE 走直发请求-应答
 * 车道（恒立即回执），PUBLISH 继承原子/队列写车道纪律——同 key 在途互斥
 * （复用 {@code atomicWriteMonitor}）+ 同 {@code op_seq} 自动重发，服务端
 * (key, 会话) 去重槽保证同任期不双扇出。
 *
 * <p><b>订阅存续与换主重挂</b>：订阅绑定受理时的路由会话（获取车道优先、
 * 回落 home，判例 {@code latchRoute}）。路由会话更替（重连/改道/车道
 * 生灭）时由连接激活事件与 30s 保活周期双通道自动重发 SUBSCRIBE——
 * 服务端 (session,key) 幂等覆盖使重复登记无副作用；重挂后的交付经新路由键
 * 恢复，断档期消息不追回（至多一次）。保活周期同时兜底"同节点换主
 * （连接未断）服务端登记清零"的静默断供（≤一个周期恢复）。
 *
 * <p><b>交付与本地两级缓冲</b>：{@code TOPIC_MESSAGE} 推送经
 * {@code OpenLatchClient} 的 (会话, subscription_id) 路由表命中本句柄后，
 * 先按同任期 {@code topic_seq} 缺口累计丢弃计数（换 term 基线重置），再入
 * 本地有界队列（256 条）；队列满 drop-newest 并计数。每订阅一条虚拟派发线程，
 * 单订阅串行回调，异常吞并记录不断链——全程不占网络 EventLoop，平台线程数
 * 不随订阅数增长。
 *
 * <p><b>发布放弃语义</b>：写路径与原子/队列同纪律——在途总界限内瞬态失败
 * 同信封重发；会话中途更替则放弃并抛（新会话下去重槽失效，盲目重发会
 * 二次扇出）。放弃/超时后效果不确定，应用层重试可能双投（契约声明的
 * 至多一次面）。
 */
final class RemoteTopic implements OTopic {

    /** 日志器（回调异常吞并记录）。 */
    private static final Logger log = LoggerFactory.getLogger(RemoteTopic.class);
    /** 应答读界在请求超时之外的余量（毫秒），与原子/队列车道一致。 */
    private static final long SLACK_MS = 1000L;
    /** 断线重连窗口的会话轮询间隔（毫秒）。 */
    private static final long SESSION_POLL_MS = 50L;

    /** 所属客户端。 */
    private final OpenLatchClient client;
    /** topic key。 */
    private final String key;
    /**
     * 当前活跃订阅（{@code null}=未订阅）。subscribe 替换、重挂与
     * unsubscribe 的引用点——volatile 单次读写，登记换绑的原子性由
     * 订阅句柄自身的 {@code rebinding} 门闩与路由表覆盖语义兜底。
     */
    private volatile Subscription subscription;

    /**
     * 构造 topic 句柄（{@link OpenLatchClient#newTopic(String)} 唯一入口）。
     *
     * @param client 所属客户端
     * @param key    topic 键（非空）
     */
    RemoteTopic(OpenLatchClient client, String key) {
        this.client = client;
        this.key = key;
    }

    /** {@inheritDoc} */
    @Override
    public String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override
    public long publish(byte[] payload) throws InterruptedException {
        Objects.requireNonNull(payload, "payload");
        Object monitor = client.atomicWriteMonitor(key);
        synchronized (monitor) {
            return executePublish(client.nextAtomicOpSeq(), payload);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long publish(String text) throws InterruptedException {
        Objects.requireNonNull(text, "text");
        return publish(text.getBytes(StandardCharsets.UTF_8));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Long> publishAsync(byte[] payload) {
        Objects.requireNonNull(payload, "payload");
        CompletableFuture<Long> future = new CompletableFuture<>();
        OpenLatchClient.LatchRoute route = client.latchRoute();
        if (route == null) {
            future.completeExceptionally(
                    new ServerUnavailableException("topic publish on '" + key + "': no active session"));
            return future;
        }
        long rid = route.session().nextRequestId();
        Envelope env = topicEnvelope(rid, TopicOpRequest.newBuilder()
                .setKey(key).setOp(TopicOp.TOPIC_OP_PUBLISH)
                .setPayloadBytes(ByteString.copyFrom(payload))
                .setOpSeq(client.nextAtomicOpSeq()));
        long budgetMs = client.config().requestTimeout().toMillis();
        route.mux().sendWithId(env, budgetMs + SLACK_MS).whenComplete((answer, err) -> {
            if (err != null || answer == null) {
                future.completeExceptionally(unwrap(err == null
                        ? new ServerUnavailableException("topic publish on '" + key + "' lost") : err));
                return;
            }
            TopicOpResponse resp = answer.getTopicOpResponse();
            if (resp.getStatus() == StatusCode.OK) {
                future.complete(resp.getTopicSeq());
            } else {
                future.completeExceptionally(new OpenLatchException(resp.getStatus(),
                        "topic publish on '" + key + "' rejected: " + resp.getStatus()));
            }
        });
        return future;
    }

    /** {@inheritDoc} */
    @Override
    public OTopicSubscription subscribe(OTopicMessageHandler handler) throws InterruptedException {
        Objects.requireNonNull(handler, "handler");
        Subscription prev = subscription;
        Subscription next = new Subscription(this, handler);
        long[] bound = executeSubscribe();
        next.bind(bound[0], bound[1]);
        client.attachTopicSubscription(bound[0], bound[1], next);
        subscription = next;
        client.trackLiveTopic(this);
        if (prev != null) {
            // 被替换订阅仅退役本地：(session,key) 登记已由新订阅幂等接管。
            retire(prev, false);
        }
        return next;
    }

    /** {@inheritDoc} */
    @Override
    public void unsubscribe() {
        Subscription s = subscription;
        if (s == null) {
            return;
        }
        subscription = null;
        client.untrackLiveTopic(this);
        retire(s, true);
    }

    /**
     * 旧订阅句柄 {@code close()} 的归一入口：仅当它仍是当前活跃订阅时执行
     * 完整退订；已被替换的旧句柄只退役本地态——线上 UNSUBSCRIBE 按
     * (session, key) 识别，误发会摘掉替换后的新登记（服务端键粒度粗于
     * 句柄，此处必须甄别）。
     *
     * @param s 调用 close() 的订阅
     */
    private void unsubscribeIfCurrent(Subscription s) {
        if (subscription == s) {
            unsubscribe();
            return;
        }
        s.deactivate();
        client.detachTopicSubscription(s.boundSid, s.boundSubId, s);
    }

    /**
     * 摘除一个订阅：停派发、路由表除名（值甄别，防误摘后到者）、
     * 可选的尽力线上退订。连接已换/已断时跳过线上退订（服务端随会话
     * 死亡三路回收兜底，至多一次语义下无交付残留风险）。
     *
     * @param s        待退役订阅
     * @param wire     是否发送线上 UNSUBSCRIBE（句柄级 unsubscribe=true，
     *                 被替换的旧订阅=false——其 (session,key) 已由新订阅接管）
     */
    private void retire(Subscription s, boolean wire) {
        s.deactivate();
        client.detachTopicSubscription(s.boundSid, s.boundSubId, s);
        if (!wire) {
            return;
        }
        OpenLatchClient.LatchRoute route = client.latchRoute();
        if (route == null || route.session().sessionId() != s.boundSid) {
            return;
        }
        long rid = route.session().nextRequestId();
        Envelope env = topicEnvelope(rid, TopicOpRequest.newBuilder()
                .setKey(key).setOp(TopicOp.TOPIC_OP_UNSUBSCRIBE));
        try {
            route.mux().sendWithId(env,
                    client.config().requestTimeout().toMillis() + SLACK_MS);
            // 尽力而为：不等待回执（断连窗内回执必然丢失，服务端清理不依赖它）。
        } catch (RuntimeException e) {
            log.debug("best-effort unsubscribe send failed for '{}'", key, e);
        }
    }

    /**
     * 同步登记循环（SUBSCRIBE 幂等，会话更替时按新会话重建信封续行）：
     * 总界限内瞬态失败同形态重发；{@code REJECT_SUBSCRIBERS}/形状/版本门
     * 拒绝即时抛出。
     *
     * @return {@code {受理会话 id, subscription_id}}
     * @throws InterruptedException      中断
     * @throws OpenLatchException        不可重试拒绝
     * @throws OpenLatchTimeoutException 登记未在总界限内确认（重试安全——幂等）
     */
    private long[] executeSubscribe() throws InterruptedException {
        long budgetMs = client.config().requestTimeout().toMillis();
        long deadline = System.currentTimeMillis() + budgetMs;
        Envelope env = null;
        long envSid = -1L;
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new OpenLatchTimeoutException("topic subscribe on '" + key
                        + "' not confirmed within request timeout; registration is idempotent — retry is safe");
            }
            OpenLatchClient.LatchRoute route = client.latchRoute();
            if (route == null) {
                Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
                continue;
            }
            if (env == null || envSid != route.session().sessionId()) {
                long rid = route.session().nextRequestId();
                env = topicEnvelope(rid, TopicOpRequest.newBuilder()
                        .setKey(key).setOp(TopicOp.TOPIC_OP_SUBSCRIBE));
                envSid = route.session().sessionId();
            }
            TopicOpResponse resp;
            try {
                Envelope answer = route.mux().sendWithId(env,
                        Math.min(remaining, budgetMs + SLACK_MS))
                        .get(Math.min(remaining, budgetMs + SLACK_MS), TimeUnit.MILLISECONDS);
                resp = answer.getTopicOpResponse();
            } catch (ExecutionException e) {
                Throwable cause = unwrap(e);
                if (isTransientRetryable(cause)) {
                    continue;
                }
                if (cause instanceof InterruptedException ie) {
                    throw ie;
                }
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                throw new OpenLatchException("topic subscribe on '" + key + "' failed", cause);
            } catch (TimeoutException e) {
                continue; // 读界兜底：幂等重投
            }
            StatusCode st = resp.getStatus();
            if (st == StatusCode.OK) {
                return new long[]{envSid, resp.getSubscriptionId()};
            }
            if (st == StatusCode.NOT_LEADER || st == StatusCode.OVERLOADED) {
                continue;
            }
            throw new OpenLatchException(st, "topic subscribe on '" + key + "' rejected: " + st);
        }
    }

    /**
     * 发布写循环（在 {@link #publish} 的在途互斥临界区内调用）：信封与会话
     * 绑定；会话中途更替即放弃（去重槽随旧会话失效，续发会二次扇出）。
     *
     * @param opSeq   会话内单调写序号
     * @param payload 消息体
     * @return 受理序号
     * @throws InterruptedException 中断
     */
    private long executePublish(long opSeq, byte[] payload) throws InterruptedException {
        long budgetMs = client.config().requestTimeout().toMillis();
        long deadline = System.currentTimeMillis() + budgetMs;
        Long startSession = null;
        Envelope env = null;
        long envSid = -1L;
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new OpenLatchTimeoutException("topic publish on '" + key
                        + "' exceeded request timeout; the message may have been accepted and"
                        + " fanned out — a retry may double-deliver");
            }
            OpenLatchClient.LatchRoute route = client.latchRoute();
            if (route == null) {
                Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
                continue;
            }
            if (startSession == null) {
                startSession = route.session().sessionId();
            }
            if (env == null || envSid != route.session().sessionId()) {
                long rid = route.session().nextRequestId();
                env = topicEnvelope(rid, TopicOpRequest.newBuilder()
                        .setKey(key).setOp(TopicOp.TOPIC_OP_PUBLISH)
                        .setPayloadBytes(ByteString.copyFrom(payload))
                        .setOpSeq(opSeq));
                envSid = route.session().sessionId();
            }
            if (route.session().sessionId() != startSession) {
                throw new OpenLatchException(StatusCode.SESSION_EXPIRED,
                        "session switched mid-flight for publish on '" + key
                                + "' — dedup slot no longer applies; retry may double-deliver");
            }
            TopicOpResponse resp;
            try {
                Envelope answer = route.mux().sendWithId(env,
                        Math.min(remaining, budgetMs + SLACK_MS))
                        .get(Math.min(remaining, budgetMs + SLACK_MS), TimeUnit.MILLISECONDS);
                resp = answer.getTopicOpResponse();
            } catch (ExecutionException e) {
                Throwable cause = unwrap(e);
                if (isTransientRetryable(cause)) {
                    continue; // 同信封（同 op_seq）重发，服务端去重槽裁决
                }
                if (cause instanceof InterruptedException ie) {
                    throw ie;
                }
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                throw new OpenLatchException("topic publish on '" + key + "' failed", cause);
            } catch (TimeoutException e) {
                continue; // 读界兜底：同序号重发
            }
            StatusCode st = resp.getStatus();
            if (st == StatusCode.OK) {
                return resp.getTopicSeq();
            }
            if (st == StatusCode.NOT_LEADER || st == StatusCode.OVERLOADED) {
                continue;
            }
            throw new OpenLatchException(st, "topic publish on '" + key + "' rejected: " + st);
        }
    }

    /**
     * 重挂/保活触发的异步重登记（fire-and-forget）：幂等 SUBSCRIBE 经当前
     * 路由发出；成功换绑（旧路由表项除名、基线重置），失败留待下一周期/
     * 事件。同会话同键的幂等覆盖使本方法可无条件周期调用。
     */
    void rebindAsync() {
        Subscription s = subscription;
        if (s == null || !s.active) {
            return;
        }
        if (!s.rebinding.compareAndSet(false, true)) {
            return; // 已有重登记在途
        }
        OpenLatchClient.LatchRoute route = client.latchRoute();
        if (route == null) {
            s.rebinding.set(false);
            return;
        }
        long rid = route.session().nextRequestId();
        long sid = route.session().sessionId();
        Envelope env = topicEnvelope(rid, TopicOpRequest.newBuilder()
                .setKey(key).setOp(TopicOp.TOPIC_OP_SUBSCRIBE));
        long budgetMs = client.config().requestTimeout().toMillis();
        route.mux().sendWithId(env, budgetMs + SLACK_MS).whenComplete((answer, err) -> {
            s.rebinding.set(false);
            if (err != null || answer == null || s != subscription) {
                return; // 失败/被替换：旧绑定保留，下一周期或事件再挂
            }
            TopicOpResponse resp = answer.getTopicOpResponse();
            if (resp.getStatus() != StatusCode.OK) {
                return;
            }
            long newSubId = resp.getSubscriptionId();
            if (sid == s.boundSid && newSubId == s.boundSubId) {
                return; // 登记本已生效（幂等覆盖回执同键）
            }
            client.detachTopicSubscription(s.boundSid, s.boundSubId, s);
            s.bind(sid, newSubId);
            s.resetSeqBaseline();
            client.attachTopicSubscription(sid, newSubId, s);
        });
    }

    /**
     * 构造 TOPIC_OP 请求信封（版本字段由多路复用器发送时定形，判例
     * {@code RemoteAtomicBase}）。
     *
     * @param rid  请求 id
     * @param body 请求载荷构造器
     * @return 信封
     */
    private static Envelope topicEnvelope(long rid, TopicOpRequest.Builder body) {
        return Envelope.newBuilder()
                .setType(MessageType.TOPIC_OP)
                .setRequestId(rid)
                .setTopicOpRequest(body)
                .build();
    }

    /**
     * 瞬态可重发判定（与原子/队列车道同表）。
     *
     * @param cause 解包原因
     * @return 可重发为 {@code true}
     */
    private static boolean isTransientRetryable(Throwable cause) {
        return cause instanceof ServerUnavailableException
                || cause instanceof OpenLatchTimeoutException
                || (cause instanceof OpenLatchException ole
                        && ole.status() == StatusCode.NOT_LEADER);
    }

    /**
     * 解包 {@link ExecutionException}。
     *
     * @param t 待解包异常
     * @return 真实原因
     */
    private static Throwable unwrap(Throwable t) {
        return t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
    }

    /**
     * 活跃订阅句柄（{@link OTopicSubscription} 实现）：绑定路由、本地二级
     * 缓冲与虚拟派发线程（单订阅串行回调）。
     */
    static final class Subscription implements OTopicSubscription {

        /** SDK 本地二级缓冲条数上限（drop-newest，与服务端侧同策略）。 */
        private static final int LOCAL_BUFFER_LIMIT = 256;

        /**
         * 派发线程工厂：每订阅一条虚拟线程，顺序号命名服务 thread dump 可读性。
         * 虚拟线程形态使平台线程数不随订阅数增长；单订阅串行回调承诺由
         * "每订阅恰一条派发线程"的形态保有，与线程种类无关。
         */
        private static final ThreadFactory DELIVERY_THREADS =
                Thread.ofVirtual().name("openlatch-topic-delivery-", 0).factory();

        /** 应用处理器。 */
        private final OTopicMessageHandler handler;
        /** 本地缓冲（到达序）。 */
        private final LinkedBlockingQueue<TopicMessage> queue =
                new LinkedBlockingQueue<>(LOCAL_BUFFER_LIMIT);
        /** 丢弃观察计数（同任期缺口推断 + 本地溢出）。 */
        private final AtomicLong dropped = new AtomicLong();
        /** 重登记在途门闩（单订阅至多一个重挂请求在途；周期与事件共用）。 */
        private final AtomicBoolean rebinding = new AtomicBoolean();
        /** 派发线程停止信号。 */
        private volatile boolean active = true;
        /** 受理会话 id（绑定路由表键）。 */
        volatile long boundSid;
        /** 服务端路由键（绑定路由表值；0=未定）。 */
        volatile long boundSubId;
        /** 同任期最近交付序号（gap 推断基线；0=未置基线）。 */
        private volatile long lastSeq;
        /** 派发线程。 */
        private final Thread dispatcher;
        /** 所属句柄（close 归一与替换甄别入口）。 */
        private final RemoteTopic owner;

        /**
         * 构造订阅并启动虚拟派发线程（形态承诺：不消耗平台线程；停止经
         * {@link #deactivate()} 标志与派发循环超时自然收敛，无 interrupt/join）。
         *
         * @param owner   所属句柄
         * @param handler 应用处理器
         */
        Subscription(RemoteTopic owner, OTopicMessageHandler handler) {
            this.owner = owner;
            this.handler = handler;
            this.dispatcher = DELIVERY_THREADS.newThread(this::deliveryLoop);
            this.dispatcher.start();
        }

        /**
         * 绑定路由（受理/重挂成功时由 {@code RemoteTopic} 调用）。
         *
         * @param sid   受理会话
         * @param subId 服务端路由键
         */
        void bind(long sid, long subId) {
            this.boundSid = sid;
            this.boundSubId = subId;
        }

        /**
         * 入站交付（EventLoop 线程，MUST 非阻塞）：同任期 gap 推断计数、
         * 基线更新、本地缓冲入队（满则丢最新并计数）。
         *
         * @param message 交付消息（protobuf 读数，只读消费）
         */
        void onPush(TopicMessage message) {
            if (!active) {
                return;
            }
            long seq = message.getTopicSeq();
            long prev = lastSeq;
            if (prev != 0 && seq > prev + 1) {
                dropped.addAndGet(seq - prev - 1);
            }
            lastSeq = seq;
            if (!queue.offer(message)) {
                dropped.incrementAndGet(); // 本地二级 drop-newest
            }
        }

        /** 换 term 后重置序号基线（不跨任期计 gap）。 */
        void resetSeqBaseline() {
            lastSeq = 0;
        }

        /** 停止派发并清空本地缓冲（至多一次：未派发即丢弃，无害）。 */
        void deactivate() {
            active = false;
            queue.clear();
        }

        /** 派发循环：单订阅内串行回调，异常吞并记录不断链。 */
        private void deliveryLoop() {
            while (active) {
                TopicMessage m;
                try {
                    m = queue.poll(200L, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
                if (m == null) {
                    continue;
                }
                try {
                    handler.onMessage(new MessageView(m));
                } catch (RuntimeException e) {
                    log.warn("topic handler threw on '{}' seq {} — swallowed, delivery continues",
                            m.getKey(), m.getTopicSeq(), e);
                }
            }
        }

        /** {@inheritDoc} */
        @Override
        public long droppedCount() {
            return dropped.get();
        }

        /** {@inheritDoc} */
        @Override
        public boolean isActive() {
            return active;
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
            owner.unsubscribeIfCurrent(this);
        }
    }

    /**
     * 交付消息只读视图（载荷独立副本）。
     *
     * @param wire 线路推送载荷
     */
    private record MessageView(TopicMessage wire) implements OTopicMessage {

        /** {@inheritDoc} */
        @Override
        public String key() {
            return wire.getKey();
        }

        /** {@inheritDoc} */
        @Override
        public long topicSeq() {
            return wire.getTopicSeq();
        }

        /** {@inheritDoc} */
        @Override
        public long publisherSessionId() {
            return wire.getPublisherSid();
        }

        /** {@inheritDoc} */
        @Override
        public long publishedAtMs() {
            return wire.getPublishTsMs();
        }

        /** {@inheritDoc} */
        @Override
        public byte[] payload() {
            return wire.getPayloadBytes().toByteArray();
        }
    }
}
