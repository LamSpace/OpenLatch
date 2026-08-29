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
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §5.4 单连接在途限额的 handler 级端到端断言（变更 phase1-audit-remediation）：
 * 写完成前请求计入在途（endRequest 挂在写完成回调上），超过
 * {@code maxInflightPerConnection} 的请求回 {@code OVERLOADED} 且不计入在途、
 * 不断连。出站写由 {@link HangingWriter} 滞留以模拟"响应尚未写完"。
 */
class InflightOverloadTest {

    /**
     * 出站滞留器：收下写请求但不完成 promise，模拟写队列背压——
     * 服务端因此视这些请求为持续在途。
     */
    private static final class HangingWriter extends ChannelOutboundHandlerAdapter {

        /** 已滞留的出站消息。 */
        final Deque<Object> messages = new ArrayDeque<>();

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            messages.add(msg); // 故意不 ctx.write 透传：promise 永不完成
        }
    }

    @Test
    void excess_inflight_requests_rejected_with_overloaded() {
        CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
                (sessionId, requestId, key) -> { });
        ServerConfig config = new ServerConfig(0, 1, 60_000L, 30_000L, 1_000L, 3_600_000L,
                500L, 5_000L, 512, 4096, 2); // maxInflightPerConnection = 2
        HangingWriter hanging = new HangingWriter();
        EmbeddedChannel ch = new EmbeddedChannel(hanging,
                new ServerSessionHandler(core, config, new ServerSessionRegistry(),
                        new RequestDispatcher(core)));
        ch.pipeline().fireChannelActive();

        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.HELLO)
                .setRequestId(1)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(1))
                .build());
        assertThat(hanging.messages).hasSize(1); // 握手响应（不计在途）

        // 两个请求占满在途限额：写未完成 → endRequest 不触发。
        ch.writeInbound(acquire(2));
        ch.writeInbound(acquire(3));
        assertThat(hanging.messages).hasSize(3);

        // 第三个请求超限：OVERLOADED，连接保持。
        ch.writeInbound(acquire(4));
        Envelope overflow = (Envelope) hanging.messages.peekLast();
        assertThat(overflow.getType()).isEqualTo(MessageType.LOCK_ACQUIRE);
        assertThat(overflow.getRequestId()).isEqualTo(4);
        assertThat(overflow.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OVERLOADED);
        assertThat(ch.isOpen()).isTrue();
    }

    /**
     * 构造排队式获取请求。
     *
     * @param requestId 请求 id
     * @return 信封
     */
    private static Envelope acquire(long requestId) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey("k").setWaitMs(-1))
                .build();
    }
}
