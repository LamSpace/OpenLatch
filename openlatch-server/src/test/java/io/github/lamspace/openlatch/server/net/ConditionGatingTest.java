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
import io.github.lamspace.openlatch.protocol.ConditionOp;
import io.github.lamspace.openlatch.protocol.ConditionOpRequest;
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
 * 协议层用例（v9，单机形态）：CONDITION_OP 与带 {@code condition} 的 ACQUIRE 的
 * v9 门控（v≤8 会话同型 {@code INVALID_REQUEST} 拒绝、不断连）、折叠形状与
 * op×字段携带矩阵入口拒绝（等待集/队列/持有零扰动）、signal 权限
 * {@code NOT_HELD}、非 LOCK 家族 key 的 {@code INVALID_REQUEST} 线路映射、
 * 合并深度护栏（等待队列+条件集合计达上限 → {@code OVERLOADED} 且登记零发生）、
 * 单机 await-signal-release-重获取闭环（返回时持锁的队首重发形态）。
 * 判例 {@code TopicGatingTest}/{@code QueueGatingTest}；集群面门控见
 * {@code ClusterConditionGatingTest}。
 */
class ConditionGatingTest {

    /** 共享会话注册表。 */
    private final ServerSessionRegistry registry = new ServerSessionRegistry();
    /** 共享核心（唤醒桥恒静默，计数断言不依赖推送）。 */
    private final CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(),
            (sessionId, requestId, key) -> {
            });
    /** 默认限额分发器。 */
    private final RequestDispatcher dispatcher = new RequestDispatcher(core, null,
            ServerConfig.DEFAULT_MAX_VALUE_BYTES, ServerConfig.DEFAULT_MAX_QUEUE_CAPACITY,
            ServerConfig.DEFAULT_MAX_DRAIN_BYTES, ServerConfig.DEFAULT_MAX_KEY_LENGTH, null);

    /**
     * 建立已握手的嵌入通道。
     *
     * @param version 客户端协议版本
     * @return 通道
     */
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

    /**
     * 以独立小深度核心建立握手通道（合并深度护栏用例专用：
     * {@code maxQueueDepthPerKey=2}）。
     *
     * @param target 分发器落位的独立核心（断言条件集读数用）
     * @return 通道
     */
    private EmbeddedChannel handshakeSmallDepth(CoreEngine target) {
        RequestDispatcher small = new RequestDispatcher(target, null,
                ServerConfig.DEFAULT_MAX_VALUE_BYTES, ServerConfig.DEFAULT_MAX_QUEUE_CAPACITY,
                ServerConfig.DEFAULT_MAX_DRAIN_BYTES, ServerConfig.DEFAULT_MAX_KEY_LENGTH, null);
        EmbeddedChannel ch = new EmbeddedChannel(new ServerSessionHandler(
                target, ServerConfig.defaults(), registry, small));
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(9)
                .setType(MessageType.HELLO)
                .setRequestId(1)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(9))
                .build());
        assertThat(((Envelope) ch.readOutbound()).getHelloResponse().getStatus())
                .isEqualTo(StatusCode.OK);
        return ch;
    }

    /** 构造获取信封。 */
    private static Envelope acquire(long rid, int version, String key, LockType type,
            long threadId, long waitMs, String condition) {
        AcquireRequest.Builder b = AcquireRequest.newBuilder()
                .setKey(key).setLockType(type).setThreadId(threadId).setWaitMs(waitMs);
        if (condition != null) {
            b.setCondition(condition);
        }
        return Envelope.newBuilder().setProtocolVersion(version)
                .setType(MessageType.LOCK_ACQUIRE).setRequestId(rid)
                .setAcquireRequest(b).build();
    }

    /** 构造 CONDITION_OP 信封。 */
    private static Envelope condOp(long rid, int version, String key, ConditionOp op,
            String condition, long threadId, long awaitRequestId) {
        ConditionOpRequest.Builder b = ConditionOpRequest.newBuilder()
                .setKey(key).setOp(op).setCondition(condition)
                .setThreadId(threadId).setAwaitRequestId(awaitRequestId);
        return Envelope.newBuilder().setProtocolVersion(version)
                .setType(MessageType.CONDITION_OP).setRequestId(rid)
                .setConditionOpRequest(b).build();
    }

    /** 发送并读出应答信封（按 request_id 匹配）。 */
    private static Envelope ask(EmbeddedChannel ch, Envelope req) {
        ch.writeInbound(req);
        Object out = ch.readOutbound();
        assertThat(out).isInstanceOf(Envelope.class);
        Envelope e = (Envelope) out;
        assertThat(e.getRequestId()).isEqualTo(req.getRequestId());
        return e;
    }

    @Test
    void v8SessionBothShapesRejectedWithoutDisconnect() {
        EmbeddedChannel ch = handshake(8);

        // CONDITION_OP：同型 condition_op_response、op 回显、不断连（判例 v3-v8 门）。
        Envelope sig = ask(ch, condOp(2, 8, "k", ConditionOp.CONDITION_OP_SIGNAL,
                "x", 1L, 0L));
        assertThat(sig.getType()).isEqualTo(MessageType.CONDITION_OP);
        assertThat(sig.hasConditionOpResponse()).isTrue();
        assertThat(sig.getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(sig.getConditionOpResponse().getOp())
                .isEqualTo(ConditionOp.CONDITION_OP_SIGNAL);
        assertThat(ch.isOpen()).isTrue();

        // 折叠 ACQUIRE：同型 acquire_response INVALID、不断连。
        Envelope fold = ask(ch, acquire(3, 8, "k", LockType.LOCK_TYPE_REENTRANT,
                1L, -1L, "x"));
        assertThat(fold.hasAcquireResponse()).isTrue();
        assertThat(fold.getAcquireResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        assertThat(core.conditionWaiterCount("k")).isZero();

        // 既有能力回归：v8 会话的 ACQUIRE 照常服务。
        Envelope acq = ask(ch, acquire(4, 8, "k", LockType.LOCK_TYPE_REENTRANT,
                1L, 0L, null));
        assertThat(acq.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
    }

    @Test
    void foldShapeMatrixRejectedZeroDisturbance() {
        EmbeddedChannel ch = handshake(9);
        long rid = 10;

        // 立即式（wait_ms=0）携带 condition 违例——await 恒为挂起形态。
        assertThat(ask(ch, acquire(rid++, 9, "s1", LockType.LOCK_TYPE_REENTRANT,
                1L, 0L, "x")).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // 读/写形态携带 condition 违例（v1 支持面仅互斥三型）。
        assertThat(ask(ch, acquire(rid++, 9, "s1", LockType.LOCK_TYPE_READ,
                1L, -1L, "x")).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ask(ch, acquire(rid++, 9, "s1", LockType.LOCK_TYPE_WRITE,
                1L, -1L, "x")).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // 非 LOCK 家族携带 condition 违例。
        assertThat(ask(ch, acquire(rid++, 9, "s1", LockType.LOCK_TYPE_SEMAPHORE,
                1L, -1L, "x")).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ask(ch, acquire(rid++, 9, "s1", LockType.LOCK_TYPE_LATCH,
                1L, -1L, "x")).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        for (String k : new String[]{"s1"}) {
            assertThat(core.conditionWaiterCount(k)).as("形状违例零登记 " + k).isZero();
        }
        // 违例不建立条目/不触登记：普通获取照常。
        assertThat(ask(ch, acquire(rid, 9, "s1", LockType.LOCK_TYPE_SIMPLE,
                1L, 0L, null)).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
    }

    @Test
    void conditionOpShapeMatrixRejectedZeroDisturbance() {
        EmbeddedChannel ch = handshake(9);
        long rid = 10;

        // SIGNAL/SIGNAL_ALL 携非零 await_request_id 违例。
        assertThat(ask(ch, condOp(rid++, 9, "c", ConditionOp.CONDITION_OP_SIGNAL,
                "x", 1L, 5L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ask(ch, condOp(rid++, 9, "c", ConditionOp.CONDITION_OP_SIGNAL_ALL,
                "x", 1L, 5L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // LEAVE 缺 await_request_id 违例。
        assertThat(ask(ch, condOp(rid++, 9, "c", ConditionOp.CONDITION_OP_LEAVE,
                "x", 0L, 0L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // LEAVE 携非零 thread_id 违例。
        assertThat(ask(ch, condOp(rid++, 9, "c", ConditionOp.CONDITION_OP_LEAVE,
                "x", 1L, 5L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // LEAVE 空条件名违例。
        assertThat(ask(ch, condOp(rid++, 9, "c", ConditionOp.CONDITION_OP_LEAVE,
                "", 0L, 5L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // op 回显（同型拒绝应答）。
        assertThat(ask(ch, condOp(rid, 9, "c", ConditionOp.CONDITION_OP_SIGNAL,
                "x", 1L, 5L)).getConditionOpResponse().getOp())
                .isEqualTo(ConditionOp.CONDITION_OP_SIGNAL);
        assertThat(core.conditionWaiterCount("c")).isZero();
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void signalPermissionAndFamilyMismatchLines() {
        EmbeddedChannel ch = handshake(9);
        // 无人持有："fk" 上空集 signal → NOT_HELD（权限服务端权威）。
        assertThat(ask(ch, condOp(2, 9, "fk", ConditionOp.CONDITION_OP_SIGNAL,
                "x", 1L, 0L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.NOT_HELD);
        // 非 LOCK 家族 key：既有家族不匹配拒绝的线路映射 INVALID_REQUEST。
        assertThat(ask(ch, Envelope.newBuilder().setProtocolVersion(9)
                .setType(MessageType.LOCK_ACQUIRE).setRequestId(3)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey("sm").setLockType(LockType.LOCK_TYPE_SEMAPHORE)
                        .setThreadId(1L).setPermits(2).setPermitsTotal(2).setWaitMs(0))
                .build()).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(ask(ch, condOp(4, 9, "sm", ConditionOp.CONDITION_OP_SIGNAL,
                "x", 1L, 0L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        // 空集/无此条件名 + 恰为持有归属：恒 OK 无操作（JDK 对齐 ghost 无害面）。
        assertThat(ask(ch, acquire(5, 9, "hk", LockType.LOCK_TYPE_REENTRANT,
                1L, 0L, null)).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(ask(ch, condOp(6, 9, "hk", ConditionOp.CONDITION_OP_SIGNAL,
                "nobody", 1L, 0L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.OK);
        assertThat(ask(ch, condOp(7, 9, "hk", ConditionOp.CONDITION_OP_LEAVE,
                "nobody", 0L, 123L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.OK);
    }

    @Test
    void mergedDepthGuardOverloadsAtEntry() {
        CoreEngine small = new CoreEngine(
                new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                        CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                        CoreConfig.MAX_KEY_LENGTH, 2),
                new SystemClock(), (sessionId, requestId, key) -> {
                });
        EmbeddedChannel holder = handshakeSmallDepth(small);
        EmbeddedChannel w1 = handshakeSmallDepth(small);
        EmbeddedChannel w2 = handshakeSmallDepth(small);
        EmbeddedChannel w3 = handshakeSmallDepth(small);

        assertThat(ask(holder, acquire(2, 9, "gk", LockType.LOCK_TYPE_REENTRANT,
                1L, 0L, null)).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        // 前两位折叠 await 正常登记（位次=本 key 等待项合计读数）。
        Envelope q1 = ask(w1, acquire(2, 9, "gk", LockType.LOCK_TYPE_REENTRANT,
                11L, -1L, "x"));
        assertThat(q1.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(q1.getAcquireResponse().getQueuePosition()).isEqualTo(1);
        Envelope q2 = ask(w2, acquire(2, 9, "gk", LockType.LOCK_TYPE_REENTRANT,
                21L, -1L, "x"));
        assertThat(q2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(q2.getAcquireResponse().getQueuePosition()).isEqualTo(2);
        // 合计达上限：同型 OVERLOADED、queue_position 零值、既有等待项零扰动。
        Envelope over = ask(w3, acquire(2, 9, "gk", LockType.LOCK_TYPE_REENTRANT,
                31L, -1L, "x"));
        assertThat(over.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OVERLOADED);
        assertThat(over.getAcquireResponse().getQueuePosition()).isZero();
        assertThat(small.conditionWaiterCount("gk")).isEqualTo(2);
        // 幂等重挂不挤护栏：已登记身份重发同折叠信封照常 QUEUED（不双登记）。
        assertThat(ask(w1, acquire(2, 9, "gk", LockType.LOCK_TYPE_REENTRANT,
                11L, -1L, "x")).getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(small.conditionWaiterCount("gk")).isEqualTo(2);
    }

    @Test
    void awaitSignalReleaseReacquireLoop() {
        EmbeddedChannel holder = handshake(9);
        EmbeddedChannel waiter = handshake(9);

        Envelope hold = ask(holder, acquire(2, 9, "lk", LockType.LOCK_TYPE_REENTRANT,
                1L, 0L, null));
        assertThat(hold.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        // await：登记半程在条目关键区内随折叠一体完成（非持有者重挂属常态）。
        Envelope await = ask(waiter, acquire(7, 9, "lk", LockType.LOCK_TYPE_REENTRANT,
                11L, -1L, "x"));
        assertThat(await.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(core.conditionWaiterCount("lk")).isEqualTo(1);

        // signal 须持锁：等待者自身 signal → NOT_HELD；持有者 signal → OK 且搬运。
        assertThat(ask(waiter, condOp(8, 9, "lk", ConditionOp.CONDITION_OP_SIGNAL,
                "x", 11L, 0L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.NOT_HELD);
        assertThat(ask(holder, condOp(3, 9, "lk", ConditionOp.CONDITION_OP_SIGNAL,
                "x", 1L, 0L)).getConditionOpResponse().getStatus())
                .isEqualTo(StatusCode.OK);
        assertThat(core.conditionWaiterCount("lk")).isZero(); // 已转入等待队列

        // 释放接力（搬运后队首通知由既有桥在条目关键区内评估）。
        assertThat(ask(holder, Envelope.newBuilder().setProtocolVersion(9)
                .setType(MessageType.LOCK_RELEASE).setRequestId(4)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey("lk")
                        .setThreadId(1L).setLeaseToken(hold.getAcquireResponse()
                                .getLeaseToken()))
                .build()).getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);

        // 等待项经队首重发（condition 已清除的同 rid 信封）被授予——
        // 返回时持锁、重入 1 级（队首重发纪律）。
        Envelope re = ask(waiter, acquire(7, 9, "lk", LockType.LOCK_TYPE_REENTRANT,
                11L, -1L, null));
        assertThat(re.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(core.conditionWaiterCount("lk")).isZero();
    }
}
