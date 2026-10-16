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

package io.github.lamspace.openlatch.client.internal;

import io.github.lamspace.openlatch.client.OpenLatchException;
import io.github.lamspace.openlatch.client.OpenLatchTimeoutException;
import io.github.lamspace.openlatch.client.ServerUnavailableException;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.netty.channel.Channel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 单连接请求多路复用器：全部出站请求的唯一收口。
 *
 * <p><b>职责与不变量</b>：
 * <ul>
 *   <li>每个出站请求以当前会话的 {@code requestId} 登记
 *       {@code requestId → (future, deadline)}，并在共享定时器上挂超时任务；</li>
 *   <li>入站响应按 {@code request_id} 摘除并完成对应 future；</li>
 *   <li><b>每个请求必有超时</b>：超时任务触发时以
 *       {@link OpenLatchTimeoutException} 失败对应 future；超时摘除以条目身份
 *       CAS（{@code remove(id, entry)}）执行，同 id 重复登记时旧条目的超时
 *       任务不会误杀新条目；</li>
 *   <li>同 id 重复登记（重发交叠）时，旧条目以 {@code superseded} 异常完成后
 *       让位于新条目——任何已挂起调用方的 future 都不会被静默覆盖丢失；</li>
 *   <li><b>出站取口同代</b>：业务出站（{@link #send}、{@link #sendWithId} 与
 *       {@link #sendPinned}）取口时要求存在活动会话，且会话与通道<b>同代</b>
 *       （取会话 → 取通道 → 回读会话，三次一致方得写出）；无活动会话（握手未
 *       完成、断连/重连窗）、取口期间换代、或**信封的同代基准已非当前会话**时
 *       以瞬态"服务不可用"快速失败，<b>不写出</b>。持有会话快照（路由）的调用方
 *       MUST 用 {@link #sendPinned}（以快照为基准判负）。握手请求先于会话存在，
 *       故经专用出口 {@link #sendHandshake} 发出（不查会话）；该豁免是路径级的，
 *       业务请求无入口可达；</li>
 *   <li>无匹配挂起项的入站信封（孤儿响应）路由给孤儿下沉点，由等待跟踪
 *       组件处理补偿归还。</li>
 * </ul>
 *
 * <p><b>线程模型</b>：future 在事件发生处完成——响应在 EventLoop 线程、
 * 超时在定时器线程、断连失败在断连检测线程。链接在这些 future 上的回调
 * 不得阻塞（公开 API 文档义务）。
 */
public final class RequestMultiplexer {

    /** v10 协议版本，业务出站信封固定携带（服务端应答回显此版本）。 */
    private static final int PROTOCOL_VERSION = 11;

    /** 挂起请求表：requestId → (future, 超时任务)。 */
    private final ConcurrentMap<Long, PendingRequest> inflight = new ConcurrentHashMap<>();
    /** 共享定时器：每请求超时任务挂于此。 */
    private final HashedWheelTimer timer;
    /** 当前活动通道供应者；无活动连接时返回 {@code null}。 */
    private final Supplier<Channel> channelSupplier;
    /** 当前会话供应者；无活动会话时返回 {@code null}。 */
    private final Supplier<SessionContext> sessionSupplier;
    /** 客户端指标门面；禁用形态下观测回调整体不注册（零额外路径）。 */
    private final ClientMetrics metrics;
    /** 孤儿响应下沉点；未设置时静默丢弃。 */
    private volatile Consumer<Envelope> orphanSink = envelope -> {
        // 默认丢弃：等待跟踪组件装配前的窗口期不应有孤儿响应
    };
    /**
     * 出站门（测试注入口）：谓词返回 {@code false} 时请求
     * 仍登记挂起但不实际写出，模拟半开连接的"写黑洞"。生产代码不设置。
     */
    private volatile java.util.function.Predicate<Envelope> outboundGate = envelope -> true;

    /**
     * 挂起请求条目。
     *
     * @param future      请求的响应 future
     * @param timeoutTask 该请求的超时任务句柄，响应到达时取消
     */
    private record PendingRequest(CompletableFuture<Envelope> future, Timeout timeoutTask) {
    }

    /**
     * 创建多路复用器（不埋点，既有测试夹具形态）。通道与会话经供应者延迟
     * 获取，避免与连接管理的构造循环。
     *
     * @param timer           共享定时器
     * @param channelSupplier 活动通道供应者
     * @param sessionSupplier 活动会话供应者
     */
    public RequestMultiplexer(HashedWheelTimer timer, Supplier<Channel> channelSupplier,
            Supplier<SessionContext> sessionSupplier) {
        this(timer, channelSupplier, sessionSupplier, null);
    }

    /**
     * 创建多路复用器（生产形态：请求终局经 {@code metrics} 旁路观测，
     * 不影响任何完成语义与返回值）。
     *
     * @param timer           共享定时器
     * @param channelSupplier 活动通道供应者
     * @param sessionSupplier 活动会话供应者
     * @param metrics         指标门面，{@code null} 或不启用即零回调
     */
    public RequestMultiplexer(HashedWheelTimer timer, Supplier<Channel> channelSupplier,
            Supplier<SessionContext> sessionSupplier, ClientMetrics metrics) {
        this.timer = timer;
        this.channelSupplier = channelSupplier;
        this.sessionSupplier = sessionSupplier;
        this.metrics = metrics;
    }

    /**
     * 发送请求：从当前会话分配新 {@code requestId}，填充协议版本与请求 id
     * 后写出，并登记挂起项与超时任务。
     *
     * @param builder   请求信封构建器（无需设置协议版本与请求 id）
     * @param timeoutMs 该请求的超时（毫秒）
     * @return 响应 future；通道或会话不可用时以 {@link ServerUnavailableException} 失败
     */
    public CompletableFuture<Envelope> send(Envelope.Builder builder, long timeoutMs) {
        long startNanos = System.nanoTime();
        SessionContext session = sessionSupplier.get();
        if (session == null) {
            return instrumented(failedFuture(new ServerUnavailableException("no active session")),
                    builder.getType(), startNanos);
        }
        long requestId = session.nextRequestId();
        Envelope envelope = builder.setProtocolVersion(PROTOCOL_VERSION).setRequestId(requestId).build();
        // 观测收口在 sendWithId（避免双注册双计数）；本方法的 startNanos 仅
        // 服务"无会话即失败"快速路径。分配 requestId 的会话即本信封的"同代"
        // 判据基准：取口若已换代，取口判负（不得让旧代编号落在新代通道上）。
        return sendPinned(envelope, session, timeoutMs);
    }

    /**
     * 以信封自带的 {@code requestId} 发送（{@code AWAIT_NOTIFY} 后的同 id 重发
     * 等业务重发路径使用），登记挂起项与超时任务。同 id 存在旧挂起项时不静默覆盖：
     * 新条目替换登记，旧条目取消其超时任务并以 {@code superseded} 的
     * {@link io.github.lamspace.openlatch.client.OpenLatchException} 完成，
     * 保证两个调用方的 future 均有界完成。
     *
     * <p><b>同代基准取调用当下的活动会话</b>——本形态的语义是"信封由当前会话
     * <b>现取现发</b>"（调用方刚在活动会话上分配了 {@code requestId}）。**持有会话
     * 快照**（路由、重发登记等）的调用方 MUST NOT 用本形态：快照已非当前会话时
     * 本形态无从据信封识别，须改用 {@link #sendPinned}，以快照为基准判负。
     *
     * @param envelope  完整信封（已含请求 id）
     * @param timeoutMs 该请求的超时（毫秒）
     * @return 响应 future；通道或会话不可用、或取口期间换代时以
     *         {@link ServerUnavailableException} 失败（且不写出）
     */
    public CompletableFuture<Envelope> sendWithId(Envelope envelope, long timeoutMs) {
        return sendPinned(envelope, sessionSupplier.get(), timeoutMs);
    }

    /**
     * 业务出站收口（**带同代基准**）：以 {@code pinned}（分配本信封
     * {@code requestId} 的会话）为基准执行取口，通过后登记并写出。
     *
     * <p>取口判据（判负一律瞬态失败且 <b>不写出</b>）：
     * <ol>
     *   <li>{@code pinned} 非空——无活动会话（握手未完成、断连/重连窗）时
     *       业务请求注定被服务端拒（会话取自连接），故本端先判负，不再白写一帧；</li>
     *   <li>活跃通道可从供应者取得——通道不可用即"连接不可用"；</li>
     *   <li><b>回读会话与 {@code pinned} 同对象</b>——会话对象每连接新建（对象
     *       身份即代次），且会话与通道在连接状态机内成对更换。换代则判负，
     *       杜绝"旧代编号落在新代通道上"：服务端按<b>连接</b>判会话，跨代的
     *       去重槽（{@code (session, op_seq)}）不互相覆盖 ⇒ 跨代落点会被当
     *       新操作受理，是<b>静默重复生效</b>而非被拒。本方法不做"换会话重发"
     *       （那正是该静默窗本身）——换会话只能是调用方的决定，且须由其
     *       换代守卫/幂等判据背书。</li>
     * </ol>
     * 判负用既有瞬态失败语义（{@link ServerUnavailableException}）：调用方按
     * 可重试处理（取新路由 / 由其自身逻辑尝试续发）。
     *
     * @param envelope  已带 {@code requestId} 的信封
     * @param pinned    该信封的同代基准会话；{@code null} 表示当前无活动会话
     * @param timeoutMs 该请求的超时（毫秒）
     * @return 响应 future
     */
    public CompletableFuture<Envelope> sendPinned(Envelope envelope, SessionContext pinned,
            long timeoutMs) {
        long startNanos = System.nanoTime();
        if (pinned == null) {
            return instrumented(failedFuture(new ServerUnavailableException("no active session")),
                    envelope.getType(), startNanos);
        }
        Channel channel = channelSupplier.get();
        if (channel == null || !channel.isActive()) {
            return instrumented(failedFuture(new ServerUnavailableException("connection is not active")),
                    envelope.getType(), startNanos);
        }
        if (sessionSupplier.get() != pinned) {
            return instrumented(failedFuture(new ServerUnavailableException(
                    "session changed after the request was stamped by '" + pinned.sessionId() + "'")),
                    envelope.getType(), startNanos);
        }
        return dispatch(envelope, channel, timeoutMs, startNanos);
    }

    /**
     * 握手出站：以调用方给定的通道为出口，<b>不查会话</b>（握手先于会话存在，
     * 其应答才带来 {@code sessionId}）。登记、同 id 交叠与超时口径与
     * {@link #sendWithId} 同表。
     *
     * <p>本方法是握手豁免的唯一入口，且要求显式传入通道——业务请求没有可达
     * 路径借道此处绕过会话门（豁免为路径级，不是旗标）。
     *
     * @param envelope  握手信封（自带 {@code requestId}）
     * @param channel   本次连接刚建立的通道（未握手，故不走通道供应者）
     * @param timeoutMs 握手超时（毫秒）
     * @return 响应 future；通道不可用时以 {@link ServerUnavailableException} 失败
     */
    public CompletableFuture<Envelope> sendHandshake(Envelope envelope, Channel channel,
            long timeoutMs) {
        long startNanos = System.nanoTime();
        if (channel == null || !channel.isActive()) {
            return instrumented(failedFuture(new ServerUnavailableException("connection is not active")),
                    envelope.getType(), startNanos);
        }
        return dispatch(envelope, channel, timeoutMs, startNanos);
    }

    /**
     * 出站登记与写出（两条发送路径的唯一收口）。
     *
     * @param envelope   待写信封（自带 {@code requestId}）
     * @param channel    出口通道
     * @param timeoutMs  该请求的超时（毫秒）
     * @param startNanos 发出起始时刻（观测用）
     * @return 响应 future
     */
    private CompletableFuture<Envelope> dispatch(Envelope envelope, Channel channel,
            long timeoutMs, long startNanos) {
        CompletableFuture<Envelope> future = new CompletableFuture<>();
        long requestId = envelope.getRequestId();
        // 超时回调持有本条目引用，摘除以 remove(id, entry) 身份 CAS 执行，
        // 同 id 交叠时先到的超时不会误杀后登记的条目。
        PendingRequest[] holder = new PendingRequest[1];
        Timeout timeoutTask = timer.newTimeout(t -> onTimeout(requestId, holder[0]), timeoutMs,
                java.util.concurrent.TimeUnit.MILLISECONDS);
        PendingRequest entry = new PendingRequest(future, timeoutTask);
        holder[0] = entry;
        PendingRequest previous = inflight.put(requestId, entry);
        if (previous != null && previous != entry) {
            // 同 id 重复登记（重发交叠）：旧条目以 superseded 完成后让位于新条目，
            // 杜绝"覆盖后旧 future 永不完成"的悬挂。
            previous.timeoutTask().cancel();
            previous.future().completeExceptionally(new OpenLatchException(
                    "request " + requestId + " superseded by re-registration"));
        }
        if (outboundGate.test(envelope)) {
            channel.writeAndFlush(envelope);
        }
        return instrumented(future, envelope.getType(), startNanos);
    }

    /**
     * 观测挂载：在返回的 future 上注册旁路完成回调记录请求终局。
     * 禁用形态（{@code metrics == null} 或未启用）原样返回——不注册回调、
     * 零额外对象分配；启用时回调仅做记录，MUST NOT 改变 future 的完成值、
     * 完成时序或异常传播（返回值即入参，链在观测回调之后）。
     *
     * @param future     请求 future
     * @param type       请求消息类型
     * @param startNanos 发出起始时刻
     * @return 同一 future
     */
    private CompletableFuture<Envelope> instrumented(CompletableFuture<Envelope> future,
            io.github.lamspace.openlatch.protocol.MessageType type, long startNanos) {
        ClientMetrics m = metrics;
        if (m == null || !m.enabled()) {
            return future;
        }
        future.whenComplete((resp, err) ->
                m.recordRequest(type, err == null ? resp : null, err, System.nanoTime() - startNanos));
        return future;
    }

    /**
     * 入站响应分发：按 {@code request_id} 摘除挂起项并完成其 future；
     * 无匹配挂起项时交给孤儿下沉点。
     *
     * @param envelope 入站信封（调用方保证非 {@code AWAIT_NOTIFY}）
     */
    public void onResponse(Envelope envelope) {
        PendingRequest pending = inflight.remove(envelope.getRequestId());
        if (pending == null) {
            orphanSink.accept(envelope);
            return;
        }
        pending.timeoutTask().cancel();
        pending.future().complete(envelope);
    }

    /**
     * 以给定原因使全部挂起请求失败（断连快速失败路径）。
     * 摘除全部挂起项并取消其超时任务。
     *
     * @param cause 失败原因
     */
    public void failAll(Throwable cause) {
        for (Long requestId : inflight.keySet()) {
            PendingRequest pending = inflight.remove(requestId);
            if (pending != null) {
                pending.timeoutTask().cancel();
                pending.future().completeExceptionally(cause);
            }
        }
    }

    /**
     * 设置孤儿响应下沉点。
     *
     * @param sink 孤儿信封处理器
     */
    public void setOrphanSink(Consumer<Envelope> sink) {
        this.orphanSink = sink;
    }

    /**
     * 设置出站门（测试注入口）。谓词返回 {@code false} 的信封
     * 不实际写出但仍走超时登记，用于模拟半开连接。
     *
     * @param gate 出站谓词
     */
    public void setOutboundGate(java.util.function.Predicate<Envelope> gate) {
        this.outboundGate = gate;
    }

    /**
     * 当前挂起请求数，供测试与诊断。
     *
     * @return 挂起请求数
     */
    public int inflightCount() {
        return inflight.size();
    }

    /**
     * 超时任务回调：以条目身份 CAS 摘除挂起项并失败其 future。
     * 同 id 已被重发条目替换时 CAS 落空，本回调无副作用（不误杀新条目）；
     * {@code entry} 为 {@code null} 仅在定时器早于登记完成的病态窗口出现，
     * 此时放弃摘除（条目的响应/failAll 路径仍会兜底完成）。
     *
     * @param requestId 超时的请求 id
     * @param entry     登记时的挂起条目（身份比对基准）
     */
    private void onTimeout(long requestId, PendingRequest entry) {
        if (entry == null) {
            return;
        }
        if (inflight.remove(requestId, entry)) {
            entry.future().completeExceptionally(
                    new OpenLatchTimeoutException("request " + requestId + " timed out"));
        }
    }

    /**
     * 构造已失败的 future。
     *
     * @param cause 失败原因
     * @return 失败的 future
     */
    private static CompletableFuture<Envelope> failedFuture(Throwable cause) {
        CompletableFuture<Envelope> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }
}
