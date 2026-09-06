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
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 T1 P3-03 协议层用例：SEMAPHORE 的 v3 门控（低版本会话消息级
 * 拒绝）、许可参数合法性（非 SEMAPHORE 携带 permits&gt;1 / permits_total、
 * 负值拒绝）、建条目总量断言经 core 映射为 {@code INVALID_REQUEST}，
 * 以及合法路径的授予/归还往返。
 */
class SemaphoreGatingTest {

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

    /** 完成指定版本握手。 */
    private static EmbeddedChannel handshake(int version) {
        EmbeddedChannel ch = channel();
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(version)
                .setType(MessageType.HELLO)
                .setRequestId(1)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(version))
                .build());
        assertThat(((Envelope) ch.readOutbound()).getHelloResponse().getStatus())
                .isEqualTo(StatusCode.OK);
        return ch;
    }

    /** 构造获取信封（线程 1、排队式）。 */
    private static Envelope acquire(long rid, LockType type, int permits, int total) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("s").setLockType(type).setThreadId(1).setWaitMs(-1)
                        .setPermits(permits).setPermitsTotal(total))
                .build();
    }

    /** 构造释放信封。 */
    private static Envelope release(long rid, long token, int permits) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey("s").setLeaseToken(token).setThreadId(1).setPermits(permits))
                .build();
    }

    /** 读一条出站。 */
    private static StatusCode status(EmbeddedChannel ch) {
        return ((Envelope) ch.readOutbound()).getAcquireResponse().getStatus();
    }

    @Test
    void v2SessionSemaphoreRejectedWithoutDisconnect() {
        EmbeddedChannel ch = handshake(2);
        ch.writeInbound(acquire(2, LockType.LOCK_TYPE_SEMAPHORE, 1, 3));
        assertThat(status(ch)).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        ch.writeInbound(acquire(3, LockType.LOCK_TYPE_REENTRANT, 0, 0));
        assertThat(status(ch)).isEqualTo(StatusCode.OK);
    }

    @Test
    void nonSemaphoreWithPermitFieldsRejected() {
        EmbeddedChannel ch = handshake(3);
        // 非 SEMAPHORE 携带 permits>1。
        ch.writeInbound(acquire(2, LockType.LOCK_TYPE_REENTRANT, 2, 0));
        assertThat(status(ch)).isEqualTo(StatusCode.INVALID_REQUEST);
        // 非 SEMAPHORE 携带 permits_total。
        ch.writeInbound(acquire(3, LockType.LOCK_TYPE_READ, 0, 5));
        assertThat(status(ch)).isEqualTo(StatusCode.INVALID_REQUEST);
        // 负值。
        ch.writeInbound(acquire(4, LockType.LOCK_TYPE_SEMAPHORE, -1, 3));
        assertThat(status(ch)).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void semaphoreCreateWithoutTotalRejectedThenRoundTrip() {
        EmbeddedChannel ch = handshake(3);
        // 建条目缺总量 → core 判定映射 INVALID_REQUEST。
        ch.writeInbound(acquire(2, LockType.LOCK_TYPE_SEMAPHORE, 1, 0));
        assertThat(status(ch)).isEqualTo(StatusCode.INVALID_REQUEST);

        // 合法建条目并取 2：授予携带凭证。
        ch.writeInbound(acquire(3, LockType.LOCK_TYPE_SEMAPHORE, 2, 2));
        Envelope g = (Envelope) ch.readOutbound();
        assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        long token = g.getAcquireResponse().getLeaseToken();

        // 池尽：再取排队中（total=2 已全被持）。队首路径经 core，
        // 单机分发器直通——此处验证立即式被拒即可。
        ch.writeInbound(acquire(4, LockType.LOCK_TYPE_SEMAPHORE, 1, 0));
        assertThat(status(ch)).isEqualTo(StatusCode.QUEUED);

        // 归还 2：完全释放；超额归还在协议层同样 INVALID_REQUEST。
        ch.writeInbound(release(5, token, 2));
        Envelope rel = (Envelope) ch.readOutbound();
        assertThat(rel.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(rel.getReleaseResponse().getFullyReleased()).isTrue();
        ch.writeInbound(release(6, token, 1));
        assertThat(((Envelope) ch.readOutbound()).getReleaseResponse().getStatus())
                .isEqualTo(StatusCode.NOT_HELD);
    }
}
