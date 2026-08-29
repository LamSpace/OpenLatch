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
 * 单连接请求多路复用器（详设 §6.4）：全部出站请求的唯一收口。
 *
 * <p><b>职责与不变量</b>：
 * <ul>
 *   <li>每个出站请求以当前会话的 {@code requestId} 登记
 *       {@code requestId → (future, deadline)}，并在共享定时器上挂超时任务；</li>
 *   <li>入站响应按 {@code request_id} 摘除并完成对应 future；</li>
 *   <li><b>每个请求必有超时</b>（概要设计 §4.3 标准 3）：超时任务触发时以
 *       {@link OpenLatchTimeoutException} 失败对应 future；超时摘除以条目身份
 *       CAS（{@code remove(id, entry)}）执行，同 id 重复登记时旧条目的超时
 *       任务不会误杀新条目；</li>
 *   <li>同 id 重复登记（重发交叠）时，旧条目以 {@code superseded} 异常完成后
 *       让位于新条目——任何已挂起调用方的 future 都不会被静默覆盖丢失
 *       （变更 phase1-audit-remediation design D3）；</li>
 *   <li>无匹配挂起项的入站信封（孤儿响应）路由给孤儿下沉点，由等待跟踪
 *       组件处理补偿归还（详设 §6.5、design.md D3）。</li>
 * </ul>
 *
 * <p><b>线程模型</b>：future 在事件发生处完成——响应在 EventLoop 线程、
 * 超时在定时器线程、断连失败在断连检测线程。链接在这些 future 上的回调
 * 不得阻塞（公开 API 文档义务）。
 */
public final class RequestMultiplexer {

    /** Phase 1 协议版本，出站信封固定携带。 */
    private static final int PROTOCOL_VERSION = 1;

    /** 挂起请求表：requestId → (future, 超时任务)。 */
    private final ConcurrentMap<Long, PendingRequest> inflight = new ConcurrentHashMap<>();
    /** 共享定时器：每请求超时任务挂于此。 */
    private final HashedWheelTimer timer;
    /** 当前活动通道供应者；无活动连接时返回 {@code null}。 */
    private final Supplier<Channel> channelSupplier;
    /** 当前会话供应者；无活动会话时返回 {@code null}。 */
    private final Supplier<SessionContext> sessionSupplier;
    /** 孤儿响应下沉点；未设置时静默丢弃。 */
    private volatile Consumer<Envelope> orphanSink = envelope -> {
        // 默认丢弃：等待跟踪组件装配前的窗口期不应有孤儿响应
    };
    /**
     * 出站门（测试注入口，design.md D7）：谓词返回 {@code false} 时请求
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
     * 创建多路复用器。通道与会话经供应者延迟获取，避免与连接管理的构造循环。
     *
     * @param timer           共享定时器
     * @param channelSupplier 活动通道供应者
     * @param sessionSupplier 活动会话供应者
     */
    public RequestMultiplexer(HashedWheelTimer timer, Supplier<Channel> channelSupplier,
            Supplier<SessionContext> sessionSupplier) {
        this.timer = timer;
        this.channelSupplier = channelSupplier;
        this.sessionSupplier = sessionSupplier;
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
        SessionContext session = sessionSupplier.get();
        if (session == null) {
            return failedFuture(new ServerUnavailableException("no active session"));
        }
        long requestId = session.nextRequestId();
        Envelope envelope = builder.setProtocolVersion(PROTOCOL_VERSION).setRequestId(requestId).build();
        return sendWithId(envelope, timeoutMs);
    }

    /**
     * 以信封自带的 {@code requestId} 发送（{@code AWAIT_NOTIFY} 后的同 id 重发
     * 与握手请求使用），登记挂起项与超时任务。同 id 存在旧挂起项时不静默覆盖：
     * 新条目替换登记，旧条目取消其超时任务并以 {@code superseded} 的
     * {@link io.github.lamspace.openlatch.client.OpenLatchException} 完成，
     * 保证两个调用方的 future 均有界完成（design D3）。
     *
     * @param envelope  完整信封（已含请求 id）
     * @param timeoutMs 该请求的超时（毫秒）
     * @return 响应 future；通道不可用时以 {@link ServerUnavailableException} 失败
     */
    public CompletableFuture<Envelope> sendWithId(Envelope envelope, long timeoutMs) {
        Channel channel = channelSupplier.get();
        if (channel == null || !channel.isActive()) {
            return failedFuture(new ServerUnavailableException("connection is not active"));
        }
        CompletableFuture<Envelope> future = new CompletableFuture<>();
        long requestId = envelope.getRequestId();
        // 超时回调持有本条目引用，摘除以 remove(id, entry) 身份 CAS 执行，
        // 同 id 交叠时先到的超时不会误杀后登记的条目（design D3）。
        PendingRequest[] holder = new PendingRequest[1];
        Timeout timeoutTask = timer.newTimeout(t -> onTimeout(requestId, holder[0]), timeoutMs,
                java.util.concurrent.TimeUnit.MILLISECONDS);
        PendingRequest entry = new PendingRequest(future, timeoutTask);
        holder[0] = entry;
        PendingRequest previous = inflight.put(requestId, entry);
        if (previous != null && previous != entry) {
            // 同 id 重复登记（重发交叠）：旧条目以 superseded 完成后让位于新条目，
            // 杜绝"覆盖后旧 future 永不完成"的悬挂（design D3）。
            previous.timeoutTask().cancel();
            previous.future().completeExceptionally(new OpenLatchException(
                    "request " + requestId + " superseded by re-registration"));
        }
        if (outboundGate.test(envelope)) {
            channel.writeAndFlush(envelope);
        }
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
     * 以给定原因使全部挂起请求失败（断连快速失败路径，详设 §6.2）。
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
     * 设置出站门（测试注入口，design.md D7）。谓词返回 {@code false} 的信封
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
