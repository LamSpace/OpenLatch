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
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TimerOp;
import io.github.lamspace.openlatch.protocol.TimerOpRequest;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 协议层用例：TIMER_OP 消息的 v11 门控（v≤10 会话消息级拒绝、不断连）、
 * ACQUIRE 携带 {@code LOCK_TYPE_TIMER} 被拒且不建条目、形状互斥矩阵全违例形
 * 拒绝零扰动、horizon 越界走 {@code INVALID_REQUEST} 参数线与合并深度
 * {@code OVERLOADED} 资源线分轨、单机全闭环——装载 → 等待挂起 → 时钟推进
 * {@code wakeTimerReady()} 推送 → 同 id 重发以 {@code OK/marked} 了结 → 粘滞
 * 共见即刻通过 → 重装载换代清钟 → DISARM 唤醒以 {@code DENIED} 收束 →
 * CANCEL 幂等、QUERY 纯读零推进、无条目拒绝与家族互拒（含 timer key 上获取
 * 互拒）。全部用注入时钟，到期判定无壁钟依赖。
 */
class TimerGatingTest {

    /** 共享会话注册表（通知路由依据）。 */
    private final ServerSessionRegistry registry = new ServerSessionRegistry();
    /** 注入时钟（毫秒）——单机到期判定全可控。 */
    private final AtomicLong clockMs = new AtomicLong(1_000_000L);
    /** 共享核心：唤醒事件按 sessionId 路由回其连接通道。 */
    private final CoreEngine core = new CoreEngine(new CoreConfig(), clockMs::get,
            (sessionId, requestId, key) -> {
                ServerSession s = registry.get(sessionId);
                if (s != null) {
                    s.channel().writeAndFlush(Envelope.newBuilder()
                            .setProtocolVersion(11)
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

    /** 构造 TIMER 信封（delay=null 即不主张）。 */
    private static Envelope timer(long rid, TimerOp op, String key, Long delay,
            long awaitRid) {
        TimerOpRequest.Builder rb = TimerOpRequest.newBuilder()
                .setKey(key).setOp(op).setAwaitRequestId(awaitRid);
        if (delay != null) {
            rb.setDelayMs(delay);
        }
        return Envelope.newBuilder()
                .setProtocolVersion(11)
                .setType(MessageType.TIMER_OP)
                .setRequestId(rid)
                .setTimerOpRequest(rb)
                .build();
    }

    /** 读一条出站。 */
    private static Envelope out(EmbeddedChannel ch) {
        return (Envelope) ch.readOutbound();
    }

    private static StatusCode request(EmbeddedChannel ch, Envelope env) {
        ch.writeInbound(env);
        return out(ch).getTimerOpResponse().getStatus();
    }

    @Test
    void timerOpRejectedForV10SessionWithoutDisconnect() {
        EmbeddedChannel ch = handshake(10);
        ch.writeInbound(timer(2, TimerOp.TIMER_OP_SCHEDULE, "t", 1_000L, 0));
        assertThat(out(ch).getTimerOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(timer(3, TimerOp.TIMER_OP_AWAIT, "t", null, 0));
        assertThat(out(ch).getTimerOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(timer(4, TimerOp.TIMER_OP_QUERY, "t", null, 0));
        assertThat(out(ch).getTimerOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        // 门控拒绝零副作用：无条目残留，v11 会话随后 SCHEDULE 照常建条目。
        EmbeddedChannel v11 = handshake(11);
        v11.writeInbound(timer(5, TimerOp.TIMER_OP_SCHEDULE, "t", 1_000L, 0));
        Envelope resp = out(v11);
        assertThat(resp.getTimerOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(resp.getTimerOpResponse().getGeneration()).isEqualTo(1);
        // v10 既有能力照常：phaser 车道可用（REGISTER OK）。
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(10).setType(MessageType.PHASER_OP).setRequestId(6)
                .setPhaserOpRequest(io.github.lamspace.openlatch.protocol.PhaserOpRequest
                        .newBuilder().setKey("p")
                        .setOp(io.github.lamspace.openlatch.protocol.PhaserOp
                                .PHASER_OP_REGISTER).setParties(1))
                .build());
        assertThat(out(ch).getPhaserOpResponse().getStatus()).isEqualTo(StatusCode.OK);
    }

    @Test
    void acquireCarryingTimerTypeRejectedAndNoEntry() {
        EmbeddedChannel ch = handshake(11);
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(11)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(2)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("x").setLockType(LockType.LOCK_TYPE_TIMER)
                        .setThreadId(1).setLeaseMs(30_000).setWaitMs(0))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // 未隐式建条目：AWAIT/QUERY/DISARM 走无条目拒绝（INVALID_REQUEST）。
        assertThat(request(ch, timer(3, TimerOp.TIMER_OP_AWAIT, "x", null, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, timer(4, TimerOp.TIMER_OP_QUERY, "x", null, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, timer(5, TimerOp.TIMER_OP_DISARM, "x", null, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
    }

    @Test
    void shapeMatrixViolationsRejectedWithoutDisturbance() {
        EmbeddedChannel ch = handshake(11);
        assertThat(request(ch, timer(1, TimerOp.TIMER_OP_SCHEDULE, "t", 5_000L, 0)))
                .isEqualTo(StatusCode.OK);
        // 违例形全集：SCHEDULE 缺 delay / delay 负值 / 携 await_request_id；
        // AWAIT/DISARM/QUERY 携 delay；CANCEL 目标非正/携 delay；界外 op。
        assertThat(request(ch, timer(2, TimerOp.TIMER_OP_SCHEDULE, "t", null, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, timer(3, TimerOp.TIMER_OP_SCHEDULE, "t", -1L, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, timer(4, TimerOp.TIMER_OP_SCHEDULE, "t", 5_000L, 9)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, timer(5, TimerOp.TIMER_OP_AWAIT, "t", 1L, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, timer(6, TimerOp.TIMER_OP_DISARM, "t", 1L, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, timer(7, TimerOp.TIMER_OP_QUERY, "t", null, 1)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(ch, timer(8, TimerOp.TIMER_OP_CANCEL, "t", null, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // 零扰动：账簿仍为代次 1、在装、到期时刻 = 装载时刻+5000。
        ch.writeInbound(timer(9, TimerOp.TIMER_OP_QUERY, "t", null, 0));
        Envelope q = out(ch);
        assertThat(q.getTimerOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(q.getTimerOpResponse().getGeneration()).isEqualTo(1);
        assertThat(q.getTimerOpResponse().getArmed()).isTrue();
        assertThat(q.getTimerOpResponse().getFireAtMs())
                .isEqualTo(1_000_000L + 5_000L);
        assertThat(q.getTimerOpResponse().getMarked()).isFalse();
    }

    @Test
    void horizonParameterLineAndDepthResourceLineSeparate() {
        // 小护栏引擎：horizon=1000、深度=1。
        ServerSessionRegistry reg2 = new ServerSessionRegistry();
        AtomicLong now = new AtomicLong(1_000_000L);
        CoreEngine small = new CoreEngine(
                new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                        CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                        CoreConfig.MAX_KEY_LENGTH, 1, 1024, 1_000L),
                now::get, (s, r, k) -> {
                });
        RequestDispatcher d2 = new RequestDispatcher(small);
        EmbeddedChannel a = new EmbeddedChannel(new ServerSessionHandler(
                small, ServerConfig.defaults(), reg2, d2));
        hello(a, 11);
        // horizon 越界 → INVALID_REQUEST（参数线——租约越界判例，非 OVERLOADED）。
        assertThat(request(a, timer(2, TimerOp.TIMER_OP_SCHEDULE, "t", 1_001L, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(request(a, timer(3, TimerOp.TIMER_OP_SCHEDULE, "t", 1_000L, 0)))
                .isEqualTo(StatusCode.OK);
        // 深度=1：首个旁观挂起，第二个 → OVERLOADED（合并深度护栏）。
        assertThat(request(a, timer(4, TimerOp.TIMER_OP_AWAIT, "t", null, 0)))
                .isEqualTo(StatusCode.QUEUED);
        EmbeddedChannel b = new EmbeddedChannel(new ServerSessionHandler(
                small, ServerConfig.defaults(), reg2, d2));
        hello(b, 11);
        b.writeInbound(timer(5, TimerOp.TIMER_OP_AWAIT, "t", null, 0));
        assertThat(((Envelope) b.readOutbound()).getTimerOpResponse().getStatus())
                .isEqualTo(StatusCode.OVERLOADED);
    }

    @Test
    void standaloneScheduleAwaitTickWakeStickyRescheduleAndDisarm() {
        EmbeddedChannel a = handshake(11);
        EmbeddedChannel b = handshake(11);
        // 装载 5s：代次 1，未即刻共见。
        a.writeInbound(timer(1, TimerOp.TIMER_OP_SCHEDULE, "t", 5_000L, 0));
        assertThat(out(a).getTimerOpResponse().getGeneration()).isEqualTo(1);
        // 旁观挂起：QUEUED。
        b.writeInbound(timer(2, TimerOp.TIMER_OP_AWAIT, "t", null, 0));
        Envelope q = out(b);
        assertThat(q.getTimerOpResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(q.getTimerOpResponse().getMarked()).isFalse();
        // 时钟推进越点：tick 唤醒（单机扫描臂直调）→ AWAIT_NOTIFY(ref=2)。
        clockMs.addAndGet(5_000L);
        assertThat(core.wakeTimerReady()).isEqualTo(1);
        Envelope notify = out(b);
        assertThat(notify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
        assertThat(notify.getAwaitNotify().getRequestIdRef()).isEqualTo(2);
        // 同 rid 重发：谓词重评即刻 OK{marked=true}（了结即终结）。
        b.writeInbound(timer(2, TimerOp.TIMER_OP_AWAIT, "t", null, 0));
        Envelope done = out(b);
        assertThat(done.getTimerOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(done.getTimerOpResponse().getMarked()).isTrue();
        // 粘滞共见：fire 之后新到的等待者不经挂起即刻通过。
        EmbeddedChannel c = handshake(11);
        c.writeInbound(timer(3, TimerOp.TIMER_OP_AWAIT, "t", null, 0));
        assertThat(out(c).getTimerOpResponse().getStatus()).isEqualTo(StatusCode.OK);
        c.writeInbound(timer(4, TimerOp.TIMER_OP_QUERY, "t", null, 0));
        assertThat(out(c).getTimerOpResponse().getMarked()).isTrue();
        // 重装载：换代清钟（代次 2、marked 归伪、fireAt 重折算）。
        a.writeInbound(timer(5, TimerOp.TIMER_OP_SCHEDULE, "t", 10_000L, 0));
        Envelope re = out(a);
        assertThat(re.getTimerOpResponse().getGeneration()).isEqualTo(2);
        assertThat(re.getTimerOpResponse().getMarked()).isFalse();
        c.writeInbound(timer(6, TimerOp.TIMER_OP_QUERY, "t", null, 0));
        assertThat(out(c).getTimerOpResponse().getMarked()).isFalse();
        // DISARM：新等待者即刻 DENIED（等待不可满足的显式终态）；在等者收唤醒。
        c.writeInbound(timer(7, TimerOp.TIMER_OP_AWAIT, "t", null, 0));
        assertThat(out(c).getTimerOpResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(request(a, timer(8, TimerOp.TIMER_OP_DISARM, "t", null, 0)))
                .isEqualTo(StatusCode.OK);
        assertThat(out(c).getType()).isEqualTo(MessageType.AWAIT_NOTIFY); // ref=7
        c.writeInbound(timer(7, TimerOp.TIMER_OP_AWAIT, "t", null, 0));
        Envelope denied = out(c);
        assertThat(denied.getTimerOpResponse().getStatus()).isEqualTo(StatusCode.DENIED);
        assertThat(denied.getTimerOpResponse().getArmed()).isFalse();
        // 新到达者对代终结同样即刻 DENIED（DISARMED 优先于同代到期成立）。
        assertThat(request(b, timer(9, TimerOp.TIMER_OP_AWAIT, "t", null, 0)))
                .isEqualTo(StatusCode.DENIED);
        // 到期后到点扫描不再对无等待者键产出任何唤醒（谓词无观察者零事件）。
        clockMs.addAndGet(60_000L);
        assertThat(core.wakeTimerReady()).isZero();
    }

    @Test
    void crossFamilyMutualRejection() {
        EmbeddedChannel ch = handshake(11);
        // 锁 key 上 SCHEDULE 互拒。
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(11).setType(MessageType.LOCK_ACQUIRE).setRequestId(1)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("lk").setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1).setLeaseMs(30_000).setWaitMs(0))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(request(ch, timer(2, TimerOp.TIMER_OP_SCHEDULE, "lk", 1L, 0)))
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // timer key 上获取互拒且不扰动。
        assertThat(request(ch, timer(3, TimerOp.TIMER_OP_SCHEDULE, "tk", 1_000L, 0)))
                .isEqualTo(StatusCode.OK);
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(11).setType(MessageType.LOCK_ACQUIRE).setRequestId(4)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("tk").setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1).setLeaseMs(30_000).setWaitMs(0))
                .build());
        assertThat(out(ch).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        ch.writeInbound(timer(5, TimerOp.TIMER_OP_QUERY, "tk", null, 0));
        assertThat(out(ch).getTimerOpResponse().getGeneration()).isEqualTo(1);
        // 到期语义同题异机对照：timer key 上队列操作互拒（延时形态↔标记位）。
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(11).setType(MessageType.QUEUE_OP).setRequestId(6)
                .setQueueOpRequest(io.github.lamspace.openlatch.protocol.QueueOpRequest
                        .newBuilder().setKey("tk").setOp(
                                io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT)
                        .setLockType(LockType.LOCK_TYPE_DELAY_QUEUE).setBlocking(false)
                        .setCapacity(2).setElementBytes(com.google.protobuf.ByteString.EMPTY))
                .build());
        assertThat(out(ch).getQueueOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
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
