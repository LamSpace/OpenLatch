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
import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.BarrierActionDoneRequest;
import io.github.lamspace.openlatch.protocol.BarrierAwaitRequest;
import io.github.lamspace.openlatch.protocol.BarrierLeaveRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LockType;
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
 * 协议层用例：三组 BARRIER 消息的 v5 门控（低版本会话消息级拒绝、
 * 不断连）、ACQUIRE 携带 {@code LOCK_TYPE_BARRIER} 被拒、parties 断言与
 * 形状拒绝、单机多会话全闭环——到场挂起 → 合拢推送 → 同 id 重发放行 →
 * 相位复用 → 动作两阶段 → 离场破障经在带裁决 {@code BARRIER_BROKEN} 了结。
 */
class BarrierGatingTest {

    /** 共享会话注册表（通知路由依据）。 */
    private final ServerSessionRegistry registry = new ServerSessionRegistry();
    /** 共享核心：通知事件按 sessionId 路由回其连接的通道。 */
    private final CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
            (sessionId, requestId, key) -> {
                ServerSession s = registry.get(sessionId);
                if (s != null) {
                    s.channel().writeAndFlush(Envelope.newBuilder()
                            .setProtocolVersion(5)
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

    /** 构造到场信封。 */
    private static Envelope await(long rid, String key, long parties, boolean action) {
        return Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_AWAIT)
                .setRequestId(rid)
                .setBarrierAwaitRequest(BarrierAwaitRequest.newBuilder()
                        .setKey(key).setParties(parties).setCarriesAction(action))
                .build();
    }

    /** 构造离场信封。 */
    private static Envelope leave(long rid, String key, long awaitRid) {
        return Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_LEAVE)
                .setRequestId(rid)
                .setBarrierLeaveRequest(BarrierLeaveRequest.newBuilder()
                        .setKey(key).setAwaitRequestId(awaitRid))
                .build();
    }

    /** 构造动作了结信封。 */
    private static Envelope actionDone(long rid, String key, long generation) {
        return Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_ACTION_DONE)
                .setRequestId(rid)
                .setBarrierActionDoneRequest(BarrierActionDoneRequest.newBuilder()
                        .setKey(key).setGeneration(generation))
                .build();
    }

    /** 读一条出站。 */
    private static Envelope out(EmbeddedChannel ch) {
        return (Envelope) ch.readOutbound();
    }

    @Test
    void barrierMessagesRejectedForV4SessionWithoutDisconnect() {
        EmbeddedChannel ch = handshake(4);
        ch.writeInbound(await(2, "b", 2, false));
        assertThat(out(ch).getBarrierAwaitResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(leave(3, "b", 0));
        assertThat(out(ch).getBarrierLeaveResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(actionDone(4, "b", 1));
        assertThat(out(ch).getBarrierActionDoneResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        // v4 既有能力照常：原子操作受理。
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(4)
                .setType(MessageType.ATOMIC_OP)
                .setRequestId(5)
                .setAtomicOpRequest(io.github.lamspace.openlatch.protocol.AtomicOpRequest
                        .newBuilder().setKey("a").setOp(io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET)
                        .setLockType(LockType.LOCK_TYPE_ATOMIC_LONG).setOperand(1).setOpSeq(1))
                .build());
        assertThat(out(ch).getAtomicOpResponse().getStatus()).isEqualTo(StatusCode.OK);
    }

    @Test
    void acquireCarryingBarrierTypeRejectedMessageLevel() {
        EmbeddedChannel ch = handshake(5);
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(2)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("bk").setLockType(LockType.LOCK_TYPE_BARRIER).setThreadId(1))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void partiesAssertionAndShapeRejections() {
        EmbeddedChannel a = handshake(5);
        // 不存在的屏障上纯加入（parties=0）被拒。
        a.writeInbound(await(2, "b0", 0, false));
        assertThat(out(a).getBarrierAwaitResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // 定型 parties=2。
        a.writeInbound(await(3, "b0", 2, false));
        assertThat(out(a).getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        // 主张不符拒绝。
        a.writeInbound(await(4, "b0", 3, false));
        Envelope mismatch = out(a);
        assertThat(mismatch.getBarrierAwaitResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(mismatch.getBarrierAwaitResponse().getParties()).isEqualTo(2);
        // 形状非法（负 parties / 负 await_request_id / 世代 0）消息级拒绝。
        a.writeInbound(await(5, "b0", -1, false));
        assertThat(out(a).getBarrierAwaitResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        a.writeInbound(leave(6, "b0", -1));
        assertThat(out(a).getBarrierLeaveResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        a.writeInbound(actionDone(7, "b0", 0));
        assertThat(out(a).getBarrierActionDoneResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(a.isOpen()).isTrue();
    }

    @Test
    void tripPushResendWrapAndReuseAcrossTwoSessions() {
        EmbeddedChannel a = handshake(5);
        EmbeddedChannel b = handshake(5);
        a.writeInbound(await(2, "ph", 2, false));
        Envelope qa = out(a);
        assertThat(qa.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(qa.getBarrierAwaitResponse().getGeneration()).isEqualTo(1);
        assertThat(qa.getBarrierAwaitResponse().getQueuePosition()).isEqualTo(1);
        b.writeInbound(await(2, "ph", 0, false));
        Envelope qb = out(b);
        // 当回合拢：B 直答 OK，A 收推送。
        assertThat(qb.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(qb.getBarrierAwaitResponse().getGeneration()).isEqualTo(1);
        Envelope pushA = out(a);
        assertThat(pushA.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
        assertThat(pushA.getAwaitNotify().getRequestIdRef()).isEqualTo(2);
        // A 同 id 重发放行（幂等了结），了结回显世代 1。
        a.writeInbound(await(2, "ph", 0, false));
        Envelope settled = out(a);
        assertThat(settled.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(settled.getBarrierAwaitResponse().getGeneration()).isEqualTo(1);
        // 相位复用后重组：新一批到场进世代 2。
        a.writeInbound(await(9, "ph", 0, false));
        Envelope gen2 = out(a);
        assertThat(gen2.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(gen2.getBarrierAwaitResponse().getGeneration()).isEqualTo(2);
    }

    @Test
    void actionTwoPhaseAndBreakOnLeaveOverWire() {
        EmbeddedChannel a = handshake(5);
        EmbeddedChannel x = handshake(5);
        a.writeInbound(await(2, "ap", 2, false));
        assertThat(out(a).getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        // 执行者到场：挂起+执行者标记，A 无推送（动作完成前不放行）。
        x.writeInbound(await(2, "ap", 0, true));
        Envelope exec = out(x);
        assertThat(exec.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(exec.getBarrierAwaitResponse().getExecutor()).isTrue();
        assertThat(exec.getBarrierAwaitResponse().getGeneration()).isEqualTo(1);
        assertThat((Object) a.readOutbound()).isNull();
        // 回报生效：A 收放行推送，双方结了。
        x.writeInbound(actionDone(3, "ap", 1));
        assertThat(out(x).getBarrierActionDoneResponse().getStatus()).isEqualTo(StatusCode.OK);
        Envelope push = out(a);
        assertThat(push.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
        a.writeInbound(await(2, "ap", 0, false));
        assertThat(out(a).getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 第二次世代的普通到场再被超时离场打破：同 id 重发得在带裁决。
        a.writeInbound(await(11, "ap", 2, false));
        Envelope q11 = out(a);
        assertThat(q11.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(q11.getBarrierAwaitResponse().getGeneration()).isEqualTo(2);
        a.writeInbound(leave(12, "ap", 11));
        assertThat(out(a).getBarrierLeaveResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 破障广播以"离场者本人除外"为契约（其本方 await 已本地终结）：
        // 此刻 a 的出站应为空——若多推即缺陷。
        assertThat((Object) a.readOutbound()).isNull();
        a.writeInbound(await(11, "ap", 0, false));
        Envelope broken = out(a);
        assertThat(broken.getBarrierAwaitResponse().getStatus())
                .isEqualTo(StatusCode.BARRIER_BROKEN);
        assertThat(a.isOpen()).isTrue();
    }
}
