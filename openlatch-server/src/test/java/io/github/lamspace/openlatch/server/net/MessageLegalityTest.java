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

package io.github.lamspace.openlatch.server.net;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.SystemClock;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §10.2 消息合法性（pipeline 层）+ 变更 phase1-audit-remediation design D1：
 * 未知 MessageType 数值回 INVALID_REQUEST 语义的承载信封（type 以
 * {@code MESSAGE_TYPE_UNKNOWN} 占位、回显 request_id、无 payload）且不断连；
 * type/payload 不匹配在连接层直接断言"回包后连接仍存活"；
 * 分发路径未预期异常兜底为 INTERNAL_ERROR 响应而非静默悬挂。
 */
class MessageLegalityTest {

    /**
     * 构造装配了会话处理器（含核心、注册表与分发器）的单条嵌入通道。
     * 通知监听按 notifyThrows 分支：null 或 false 时静默；true 时抛出
     * {@code IllegalStateException("simulated bridge failure")}，
     * 用于模拟通知桥故障以驱动类注释第三维度（分发兜底 INTERNAL_ERROR）。
     *
     * @param notifyThrows 通知抛错开关（null/false 静默，true 抛错）
     * @return 已装配会话处理器的嵌入通道
     */
    private static EmbeddedChannel channel(AtomicBoolean notifyThrows) {
        CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
                (sessionId, requestId, key) -> {
                    if (notifyThrows != null && notifyThrows.get()) {
                        throw new IllegalStateException("simulated bridge failure");
                    }
                });
        ServerConfig config = ServerConfig.defaults();
        return new EmbeddedChannel(new ServerSessionHandler(
                core, config, new ServerSessionRegistry(), new RequestDispatcher(core)));
    }

    /**
     * 构造合法 HELLO 信封（版本 1、无认证令牌）。
     *
     * @param requestId 请求 id
     * @return 信封
     */
    private static Envelope hello(long requestId) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.HELLO)
                .setRequestId(requestId)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(1))
                .build();
    }

    /**
     * 构造排队式获取请求信封（wait_ms 取值见行内注释）。
     *
     * @param requestId 请求 id
     * @param key       锁键
     * @return 信封
     */
    private static Envelope acquire(long requestId, String key) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                // wait_ms=-1：排队式（0 为立即式，竞争时会直接 DENIED）。
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key).setWaitMs(-1))
                .build();
    }

    /**
     * 构造释放请求信封。
     *
     * @param requestId 请求 id
     * @param key       锁键
     * @param token     租约凭证
     * @return 信封
     */
    private static Envelope release(long requestId, String key, long token) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(requestId)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey(key).setLeaseToken(token))
                .build();
    }

    /**
     * 读出一条出站信封并断言其类型。
     *
     * @param ch 嵌入通道
     * @return 出站信封
     */
    private static Envelope readOut(EmbeddedChannel ch) {
        Object out = ch.readOutbound();
        assertThat(out).isInstanceOf(Envelope.class);
        return (Envelope) out;
    }

    @Test
    void unknown_message_type_number_rejected_without_disconnect() {
        EmbeddedChannel ch = channel(null);
        ch.pipeline().fireChannelActive();
        ch.writeInbound(hello(1));
        readOut(ch); // 握手 OK

        // type=99（协议未定义数值，Protobuf 解析为 UNRECOGNIZED），payload 合法。
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(1)
                .setTypeValue(99)
                .setRequestId(2)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey("k"))
                .build());

        Envelope resp = readOut(ch);
        assertThat(resp.getType()).isEqualTo(MessageType.MESSAGE_TYPE_UNKNOWN);
        assertThat(resp.getRequestId()).isEqualTo(2);
        assertThat(ch.isOpen()).isTrue();

        // 后续合法请求仍被正常处理。
        ch.writeInbound(acquire(3, "k"));
        Envelope after = readOut(ch);
        assertThat(after.getType()).isEqualTo(MessageType.LOCK_ACQUIRE);
        assertThat(after.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void unknown_message_type_before_handshake_rejected_without_disconnect() {
        EmbeddedChannel ch = channel(null);
        ch.pipeline().fireChannelActive();

        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(1)
                .setTypeValue(99)
                .setRequestId(5)
                .build());

        Envelope resp = readOut(ch);
        assertThat(resp.getType()).isEqualTo(MessageType.MESSAGE_TYPE_UNKNOWN);
        assertThat(resp.getRequestId()).isEqualTo(5);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void type_payload_mismatch_rejected_and_connection_survives_at_pipeline_level() {
        EmbeddedChannel ch = channel(null);
        ch.pipeline().fireChannelActive();
        ch.writeInbound(hello(1));
        readOut(ch);

        // LOCK_ACQUIRE 携带 ReleaseRequest payload：回 INVALID_REQUEST 且不断连（§10.2 连接层直接断言）。
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(2)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey("k"))
                .build());

        Envelope resp = readOut(ch);
        assertThat(resp.getType()).isEqualTo(MessageType.LOCK_ACQUIRE);
        assertThat(resp.getRequestId()).isEqualTo(2);
        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();

        // 同一连接继续正常服务。
        ch.writeInbound(acquire(3, "k"));
        assertThat(readOut(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void dispatch_failure_becomes_internal_error_response_not_silent_hang() {
        AtomicBoolean notifyThrows = new AtomicBoolean(false);
        CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
                (sessionId, requestId, key) -> {
                    if (notifyThrows.get()) {
                        throw new IllegalStateException("simulated bridge failure");
                    }
                });
        RequestDispatcher dispatcher = new RequestDispatcher(core);
        ServerSessionRegistry registry = new ServerSessionRegistry();
        EmbeddedChannel holder = new EmbeddedChannel(new ServerSessionHandler(
                core, ServerConfig.defaults(), registry, dispatcher));
        EmbeddedChannel waiter = new EmbeddedChannel(new ServerSessionHandler(
                core, ServerConfig.defaults(), registry, dispatcher));
        holder.pipeline().fireChannelActive();
        waiter.pipeline().fireChannelActive();

        holder.writeInbound(hello(1));
        readOut(holder);
        waiter.writeInbound(hello(1));
        readOut(waiter);
        holder.writeInbound(acquire(2, "k"));
        long token = readOut(holder).getAcquireResponse().getLeaseToken();
        waiter.writeInbound(acquire(2, "k"));
        assertThat(readOut(waiter).getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);

        // 释放触发队首通知；通知回调抛错（模拟 NotifyEventBridge 故障）→ 分发兜底 INTERNAL_ERROR。
        notifyThrows.set(true);
        holder.writeInbound(release(3, "k", token));

        Envelope resp = readOut(holder);
        assertThat(resp.getType()).isEqualTo(MessageType.LOCK_RELEASE);
        assertThat(resp.getRequestId()).isEqualTo(3);
        assertThat(resp.getReleaseResponse().getStatus()).isEqualTo(StatusCode.INTERNAL_ERROR);
        assertThat(holder.isOpen()).isTrue();
    }
}
