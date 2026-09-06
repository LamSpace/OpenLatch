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
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 T1 v3 门控与 FAIR 别名（协议层）：{@code LOCK_TYPE_FAIR} 仅对
 * 握手版本 ≥3 的会话开放，v1/v2 会话以 {@code INVALID_REQUEST} 消息级
 * 拒绝且不断连（非安全事件，区别于认证失败断连）；v3 会话上 FAIR 与
 * REENTRANT 按同族重入互认。
 */
class FairLockGatingTest {

    /**
     * 装配带分发器的嵌入通道。
     *
     * @return 已接入会话处理器的通道
     */
    private static EmbeddedChannel channel() {
        CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(), (s, r, k) -> { });
        return new EmbeddedChannel(new ServerSessionHandler(
                core, ServerConfig.defaults(), new ServerSessionRegistry(), new RequestDispatcher(core)));
    }

    /** 完成指定版本握手并返回会话通道。 */
    private static EmbeddedChannel handshake(int version) {
        EmbeddedChannel ch = channel();
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(version)
                .setType(MessageType.HELLO)
                .setRequestId(1)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(version))
                .build());
        Envelope resp = (Envelope) ch.readOutbound();
        assertThat(resp.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
        return ch;
    }

    /** 构造获取请求信封（排队式，线程 1；信封版本与会话握手版本一致）。 */
    private static Envelope acquire(long requestId, LockType type, String key, int version) {
        return Envelope.newBuilder()
                .setProtocolVersion(version)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(key).setLockType(type).setThreadId(1).setWaitMs(-1))
                .build();
    }

    /** 读出下一条出站信封。 */
    private static Envelope out(EmbeddedChannel ch) {
        return (Envelope) ch.readOutbound();
    }

    @Test
    void v3_session_can_acquire_fair_lock() {
        EmbeddedChannel ch = handshake(3);
        ch.writeInbound(acquire(2, LockType.LOCK_TYPE_FAIR, "k", 3));
        Envelope resp = out(ch);
        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(resp.getAcquireResponse().getLeaseToken()).isPositive();
    }

    @Test
    void v2_session_fair_request_rejected_without_disconnect() {
        EmbeddedChannel ch = handshake(2);
        ch.writeInbound(acquire(2, LockType.LOCK_TYPE_FAIR, "k", 2));
        Envelope resp = out(ch);
        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 消息级拒绝：连接存活，既有类型照常服务。
        ch.writeInbound(acquire(3, LockType.LOCK_TYPE_REENTRANT, "k", 2));
        assertThat(out(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void v1_session_fair_request_rejected_without_disconnect() {
        EmbeddedChannel ch = handshake(1);
        ch.writeInbound(acquire(2, LockType.LOCK_TYPE_FAIR, "k", 1));
        assertThat(out(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void fair_reenters_entry_established_as_reentrant() {
        EmbeddedChannel ch = handshake(3);
        ch.writeInbound(acquire(2, LockType.LOCK_TYPE_REENTRANT, "k", 3));
        Envelope g1 = out(ch);
        assertThat(g1.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

        ch.writeInbound(acquire(3, LockType.LOCK_TYPE_FAIR, "k", 3));
        Envelope g2 = out(ch);
        assertThat(g2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 同族别名：重入复用凭证。
        assertThat(g2.getAcquireResponse().getLeaseToken())
                .isEqualTo(g1.getAcquireResponse().getLeaseToken());
    }
}
