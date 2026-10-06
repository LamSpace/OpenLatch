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
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.PhaserOp;
import io.github.lamspace.openlatch.protocol.PhaserOpRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 协议层用例：PHASER_OP 消息的 v10 门控（v≤9 会话消息级拒绝、不断连）、
 * ACQUIRE 携带 {@code LOCK_TYPE_PHASER} 被拒且不建条目、形状互斥矩阵全违例形
 * 拒绝零扰动、parties 配额与合并等待深度两护栏分轨、单机全闭环——注册 →
 * 到场挂起 → 合拢推送 → 同 id 重发以换代窗口终态了结 → 跨会话旁观等待 →
 * CANCEL 幂等且不收唤醒 → QUERY 纯读零推进、无条目拒绝家族互拒。
 */
class PhaserGatingTest {

    /** 共享会话注册表（通知路由依据）。 */
    private final ServerSessionRegistry registry = new ServerSessionRegistry();
    /** 共享核心：唤醒事件按 sessionId 路由回其连接通道。 */
    private final CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
            (sessionId, requestId, key) -> {
                ServerSession s = registry.get(sessionId);
                if (s != null) {
                    s.channel().writeAndFlush(Envelope.newBuilder()
                            .setProtocolVersion(10)
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

    /** 构造 PHASER 信封。 */
    private static Envelope phaser(long rid, PhaserOp op, String key, int parties,
            Long expected, long awaitRid) {
        PhaserOpRequest.Builder rb = PhaserOpRequest.newBuilder()
                .setKey(key).setOp(op).setParties(parties).setAwaitRequestId(awaitRid);
        if (expected != null) {
            rb.setExpectedPhase(expected);
        }
        return Envelope.newBuilder()
                .setProtocolVersion(10)
                .setType(MessageType.PHASER_OP)
                .setRequestId(rid)
                .setPhaserOpRequest(rb)
                .build();
    }

    /** 读一条出站。 */
    private static Envelope out(EmbeddedChannel ch) {
        return (Envelope) ch.readOutbound();
    }

    @Test
    void phaserOpRejectedForV9SessionWithoutDisconnect() {
        EmbeddedChannel ch = handshake(9);
        ch.writeInbound(phaser(2, PhaserOp.PHASER_OP_REGISTER, "p", 1, null, 0));
        assertThat(out(ch).getPhaserOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(phaser(3, PhaserOp.PHASER_OP_AWAIT_ADVANCE, "p", 0, 0L, 0));
        assertThat(out(ch).getPhaserOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(phaser(4, PhaserOp.PHASER_OP_QUERY, "p", 0, null, 0));
        assertThat(out(ch).getPhaserOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        // 无条目残留：v10 会话随后 REGISTER 照常建条目（门控拒绝零副作用）。
        EmbeddedChannel v10 = handshake(10);
        v10.writeInbound(phaser(5, PhaserOp.PHASER_OP_REGISTER, "p", 1, null, 0));
        assertThat(out(v10).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // v9 既有能力照常：条件读锁通道可用（获取锁 OK）。
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(9)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(6)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("lk").setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1).setLeaseMs(30_000).setWaitMs(0))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
    }

    @Test
    void acquireCarryingPhaserTypeRejectedAndNoEntry() {
        EmbeddedChannel ch = handshake(10);
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(10)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(2)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("x").setLockType(LockType.LOCK_TYPE_PHASER)
                        .setThreadId(1).setLeaseMs(30_000).setWaitMs(0))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // 未隐式建条目：QUERY 走无条目拒绝。
        ch.writeInbound(phaser(3, PhaserOp.PHASER_OP_QUERY, "x", 0, null, 0));
        assertThat(out(ch).getPhaserOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
    }

    @Test
    void shapeMatrixViolationsRejectedWithoutDisturbance() {
        EmbeddedChannel ch = handshake(10);
        ch.writeInbound(phaser(1, PhaserOp.PHASER_OP_REGISTER, "p", 2, null, 0));
        assertThat(out(ch).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 违例形全集（注册携 expected_phase / 到场携 parties / await 缺相位 /
        // await 携 parties / cancel 携相位 / cancel 目标非正 / 非 cancel 携目标）。
        assertThat(request(ch, phaser(2, PhaserOp.PHASER_OP_REGISTER, "p", 1, 3L, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, phaser(3, PhaserOp.PHASER_OP_ARRIVE, "p", 1, null, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, phaser(4, PhaserOp.PHASER_OP_AWAIT_ADVANCE, "p", 0, null, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, phaser(5, PhaserOp.PHASER_OP_AWAIT_ADVANCE, "p", 2, 0L, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, phaser(6, PhaserOp.PHASER_OP_CANCEL, "p", 0, 1L, 4)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, phaser(7, PhaserOp.PHASER_OP_CANCEL, "p", 0, null, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, phaser(8, PhaserOp.PHASER_OP_QUERY, "p", 0, null, 9)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // 零扰动：账簿仍为 registered=2、arrived=0、phase=0。
        ch.writeInbound(phaser(9, PhaserOp.PHASER_OP_QUERY, "p", 0, null, 0));
        Envelope q = out(ch);
        assertThat(q.getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(q.getPhaserOpResponse().getRegistered()).isEqualTo(2);
        assertThat(q.getPhaserOpResponse().getArrived()).isZero();
        assertThat(q.getPhaserOpResponse().getPhase()).isZero();
    }

    @Test
    void quotaCapAndDepthGuardrailsSeparateLines() {
        // 小护栏引擎：cap=2、深度=1（两超限分轨判读）。
        ServerSessionRegistry reg2 = new ServerSessionRegistry();
        CoreEngine small = new CoreEngine(
                new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                        CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                        CoreConfig.MAX_KEY_LENGTH, 1, 2),
                new SystemClock(), (s, r, k) -> {
                });
        RequestDispatcher d2 = new RequestDispatcher(small);
        EmbeddedChannel a = new EmbeddedChannel(new ServerSessionHandler(
                small, ServerConfig.defaults(), reg2, d2));
        hello(a, 10);
        a.writeInbound(phaser(2, PhaserOp.PHASER_OP_REGISTER, "p", 2, null, 0));
        assertThat(out(a).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // REGISTER 超限 → OVERLOADED（配额护栏）。
        a.writeInbound(phaser(3, PhaserOp.PHASER_OP_REGISTER, "p", 1, null, 0));
        assertThat(out(a).getPhaserOpResponse().getStatus())
                .isEqualTo(StatusCode.OVERLOADED);
        // 深度=1：首个旁观等待挂起，第二个 → OVERLOADED（深度护栏）。
        a.writeInbound(phaser(4, PhaserOp.PHASER_OP_AWAIT_ADVANCE, "p", 0, 0L, 0));
        assertThat(out(a).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        EmbeddedChannel b = new EmbeddedChannel(new ServerSessionHandler(
                small, ServerConfig.defaults(), reg2, d2));
        hello(b, 10);
        b.writeInbound(phaser(5, PhaserOp.PHASER_OP_AWAIT_ADVANCE, "p", 0, 0L, 0));
        assertThat(out(b).getPhaserOpResponse().getStatus())
                .isEqualTo(StatusCode.OVERLOADED);
        // 到场族宽容：第 3 方到场+等待在深度已满时仍登记（到场事实不绞杀）。
        b.writeInbound(phaser(6, PhaserOp.PHASER_OP_REGISTER, "p", 1, null, 0));
        assertThat(out(b).getPhaserOpResponse().getStatus())
                .isEqualTo(StatusCode.OVERLOADED); // 配额 cap=2 已占满
    }

    @Test
    void standaloneAwaitTripReissueAndCancelQuietNoWake() {
        EmbeddedChannel a = handshake(10);
        EmbeddedChannel b = handshake(10);
        a.writeInbound(phaser(1, PhaserOp.PHASER_OP_REGISTER, "p", 2, null, 0));
        assertThat(out(a).getPhaserOpResponse().getPhase()).isZero();
        b.writeInbound(phaser(2, PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0));
        Envelope q = out(b);
        assertThat(q.getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(q.getPhaserOpResponse().getPhase()).isZero();
        // 合拢恰由 A 的到场触达：A 直答 OK（到场相位 0），B 收唤醒推送。
        a.writeInbound(phaser(3, PhaserOp.PHASER_OP_ARRIVE, "p", 0, null, 0));
        assertThat(out(a).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        Envelope notify = out(b);
        assertThat(notify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
        assertThat(notify.getAwaitNotify().getRequestIdRef()).isEqualTo(2);
        // 了结重发：同 rid 命中换代窗口——终态回显到场相位 0，不双计新相位。
        b.writeInbound(phaser(2, PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT, "p", 0, null, 0));
        Envelope done = out(b);
        assertThat(done.getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(done.getPhaserOpResponse().getPhase()).isZero();
        EmbeddedChannel c = handshake(10);
        c.writeInbound(phaser(10, PhaserOp.PHASER_OP_QUERY, "p", 0, null, 0));
        Envelope cq = out(c);
        assertThat(cq.getPhaserOpResponse().getPhase()).isEqualTo(1);
        assertThat(cq.getPhaserOpResponse().getArrived()).isZero();
        // 已越相位旁观：即刻 OK；未越挂起后 CANCEL：后续推进不收唤醒。
        c.writeInbound(phaser(11, PhaserOp.PHASER_OP_AWAIT_ADVANCE, "p", 0, 0L, 0));
        assertThat(out(c).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        c.writeInbound(phaser(12, PhaserOp.PHASER_OP_AWAIT_ADVANCE, "p", 0, 1L, 0));
        assertThat(out(c).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        c.writeInbound(phaser(13, PhaserOp.PHASER_OP_CANCEL, "p", 0, null, 12));
        assertThat(out(c).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 重复 CANCEL 幂等。
        c.writeInbound(phaser(14, PhaserOp.PHASER_OP_CANCEL, "p", 0, null, 12));
        assertThat(out(c).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 第三次到场推进相位 2——c 已撤销，无推送。
        a.writeInbound(phaser(20, PhaserOp.PHASER_OP_ARRIVE, "p", 0, null, 0));
        assertThat(out(a).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        b.writeInbound(phaser(21, PhaserOp.PHASER_OP_ARRIVE, "p", 0, null, 0));
        assertThat(out(b).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        a.writeInbound(phaser(22, PhaserOp.PHASER_OP_ARRIVE, "p", 0, null, 0));
        assertThat(out(a).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        c.writeInbound(phaser(23, PhaserOp.PHASER_OP_QUERY, "p", 0, null, 0));
        assertThat(out(c).getPhaserOpResponse().getPhase()).isEqualTo(2);
    }

    @Test
    void crossFamilyMutualRejection() {
        EmbeddedChannel ch = handshake(10);
        // 锁 key 上 REGISTER 互拒。
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(10).setType(MessageType.LOCK_ACQUIRE).setRequestId(1)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("lk").setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1).setLeaseMs(30_000).setWaitMs(0))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        ch.writeInbound(phaser(2, PhaserOp.PHASER_OP_REGISTER, "lk", 1, null, 0));
        assertThat(out(ch).getPhaserOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // phaser key 上获取互拒且不扰动。
        ch.writeInbound(phaser(3, PhaserOp.PHASER_OP_REGISTER, "pk", 1, null, 0));
        assertThat(out(ch).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(10).setType(MessageType.LOCK_ACQUIRE).setRequestId(4)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("pk").setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1).setLeaseMs(30_000).setWaitMs(0))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(phaser(5, PhaserOp.PHASER_OP_QUERY, "pk", 0, null, 0));
        assertThat(out(ch).getPhaserOpResponse().getRegistered()).isEqualTo(1);
    }

    private static StatusCode request(EmbeddedChannel ch, Envelope env) {
        ch.writeInbound(env);
        return out(ch).getPhaserOpResponse().getStatus();
    }

    private static void hello(EmbeddedChannel ch, int version) {
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(version)
                .setType(MessageType.HELLO)
                .setRequestId(1)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(version))
                .build());
        assertThat(((Envelope) ch.readOutbound()).getHelloResponse().getStatus())
                .isEqualTo(StatusCode.OK);
    }
}
