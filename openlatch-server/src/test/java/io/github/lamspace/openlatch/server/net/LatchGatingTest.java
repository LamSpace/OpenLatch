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
import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LatchAwaitRequest;
import io.github.lamspace.openlatch.protocol.LatchCountDownRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 协议层用例：LATCH_COUNT_DOWN / LATCH_AWAIT 的 v3 门控
 * （低版本会话消息级拒绝、不断连）、负参数与无断言拒绝、纯初始化 →
 * 等待挂起 → 倒计数归零 → {@code AWAIT_NOTIFY} 推送 → 同 id 重发放行的
 * 单机双会话线路往返。
 */
class LatchGatingTest {

    /** 共享会话注册表（通知路由依据）。 */
    private final ServerSessionRegistry registry = new ServerSessionRegistry();
    /** 共享核心：通知事件按 sessionId 路由回其连接的通道。 */
    private final CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
            (sessionId, requestId, key) -> {
                ServerSession s = registry.get(sessionId);
                if (s != null) {
                    s.channel().writeAndFlush(Envelope.newBuilder()
                            .setProtocolVersion(3)
                            .setType(MessageType.AWAIT_NOTIFY)
                            .setRequestId(0)
                            .setAwaitNotify(AwaitNotify.newBuilder()
                                    .setKey(key).setRequestIdRef(requestId))
                            .build());
                }
            });
    /** 共享分发器。 */
    private final RequestDispatcher dispatcher = new RequestDispatcher(core);

    /** 建立已握手的嵌入通道。 */
    private EmbeddedChannel handshake(int version) {
        EmbeddedChannel ch = new EmbeddedChannel(new ServerSessionHandler(
                core, ServerConfig.defaults(), registry, dispatcher));
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

    /** 构造 countDown 信封。 */
    private static Envelope countDown(long rid, String key, long count, long total) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LATCH_COUNT_DOWN)
                .setRequestId(rid)
                .setLatchCountDownRequest(LatchCountDownRequest.newBuilder()
                        .setKey(key).setCount(count).setTotal(total))
                .build();
    }

    /** 构造 await 信封。 */
    private static Envelope await(long rid, String key, long total) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LATCH_AWAIT)
                .setRequestId(rid)
                .setLatchAwaitRequest(LatchAwaitRequest.newBuilder().setKey(key).setTotal(total))
                .build();
    }

    /** 读一条出站。 */
    private static Envelope out(EmbeddedChannel ch) {
        return (Envelope) ch.readOutbound();
    }

    @Test
    void latchMessagesRejectedForV2SessionWithoutDisconnect() {
        EmbeddedChannel ch = handshake(2);
        ch.writeInbound(countDown(2, "l", 1, 2));
        assertThat(out(ch).getLatchCountDownResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(await(3, "l", 2));
        assertThat(out(ch).getLatchAwaitResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        // v3-only 判别不伤既有类型：锁获取照常。
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(2)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(4)
                .setAcquireRequest(io.github.lamspace.openlatch.protocol.AcquireRequest.newBuilder()
                        .setKey("l2").setWaitMs(-1))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
    }

    @Test
    void negativeAndUnassertedParametersRejected() {
        EmbeddedChannel ch = handshake(3);
        ch.writeInbound(countDown(2, "l", -1, 2));
        assertThat(out(ch).getLatchCountDownResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(await(3, "l", -5));
        assertThat(out(ch).getLatchAwaitResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        // 不存在屏障的无断言操作被拒（core REJECT_LATCH_TOTAL → INVALID_REQUEST）。
        ch.writeInbound(countDown(4, "absent", 1, 0));
        assertThat(out(ch).getLatchCountDownResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(await(5, "absent", 0));
        assertThat(out(ch).getLatchAwaitResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void initAwaitQueueCountDownNotifyGrantRoundTrip() {
        EmbeddedChannel creator = handshake(3);
        EmbeddedChannel awaiter = handshake(3);

        // 创建者纯初始化 countDown(0, total=2)。
        creator.writeInbound(countDown(2, "rt", 0, 2));
        Envelope init = out(creator);
        assertThat(init.getType()).isEqualTo(MessageType.LATCH_COUNT_DOWN);
        assertThat(init.getLatchCountDownResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(init.getLatchCountDownResponse().getRemaining()).isEqualTo(2);

        // 等待者 await（不主张）挂起。
        awaiter.writeInbound(await(3, "rt", 0));
        Envelope q = out(awaiter);
        assertThat(q.getLatchAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(q.getLatchAwaitResponse().getQueuePosition()).isEqualTo(1);

        // 归零：countDown 应答 OK+remaining=0，等待者通道收到 AWAIT_NOTIFY。
        creator.writeInbound(countDown(4, "rt", 2, 0));
        Envelope cd = out(creator);
        assertThat(cd.getLatchCountDownResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(cd.getLatchCountDownResponse().getRemaining()).isZero();
        Envelope push = out(awaiter);
        assertThat(push.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
        assertThat(push.getAwaitNotify().getRequestIdRef()).isEqualTo(3);
        assertThat(push.getAwaitNotify().getKey()).isEqualTo("rt");

        // 同 id 重发 await：归零立即通过（幂等离队）。
        awaiter.writeInbound(await(3, "rt", 0));
        assertThat(out(awaiter).getLatchAwaitResponse().getStatus()).isEqualTo(StatusCode.OK);
    }
}
