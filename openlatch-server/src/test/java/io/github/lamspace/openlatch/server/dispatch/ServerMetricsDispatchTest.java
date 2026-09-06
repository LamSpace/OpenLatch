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

package io.github.lamspace.openlatch.server.dispatch;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.SystemClock;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LatchAwaitRequest;
import io.github.lamspace.openlatch.protocol.LatchCountDownRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.metrics.ServerMetrics;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 T2 单机路径 L1 埋点断言（spec"请求埋点覆盖单机与集群双路径"）：驱动
 * {@link RequestDispatcher} 消息面，逐项核对计数、耗时 result 折算、
 * gauge 联动读数与"每应答恰好一次"口径；含 LATCH 消息的映射口径与
 * 不埋点构造（null 门面）行为不变。
 */
class ServerMetricsDispatchTest {

    /** 被测核心（系统时钟 + no-op 通知桥）。 */
    private CoreEngine core;
    /** 被测词表。 */
    private ServerMetrics metrics;
    /** 被测分发器（挂指标）。 */
    private RequestDispatcher dispatcher;
    /** 会话注册表（sessions gauge 数据源）。 */
    private ServerSessionRegistry registry;

    @BeforeEach
    void setUp() {
        core = new CoreEngine(new CoreConfig(), new SystemClock(), (s, r, k) -> { });
        metrics = new ServerMetrics();
        registry = new ServerSessionRegistry();
        metrics.bindStandaloneGauges(core, registry);
        dispatcher = new RequestDispatcher(core, metrics);
    }

    /** 已握手会话（v1 默认形态）。 */
    private ServerSession session(int version) {
        ServerSession s = new ServerSession(null);
        s.activate(core.sessionOpened(), version);
        return s;
    }

    private static Envelope acquire(long rid, String key, long waitMs) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key)
                        .setLockType(LockType.LOCK_TYPE_REENTRANT).setThreadId(1)
                        .setLeaseMs(0).setWaitMs(waitMs))
                .build();
    }

    private static Envelope release(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey(key).setLeaseToken(token)
                        .setThreadId(1))
                .build();
    }

    private static Envelope renew(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LEASE_RENEW)
                .setRequestId(rid)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder().setKey(key).setLeaseToken(token))
                .build();
    }

    private static Envelope latchAwait(long rid, String key, long total) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LATCH_AWAIT)
                .setRequestId(rid)
                .setLatchAwaitRequest(LatchAwaitRequest.newBuilder().setKey(key).setTotal(total))
                .build();
    }

    private static Envelope latchCountDown(long rid, String key, long count, long total) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LATCH_COUNT_DOWN)
                .setRequestId(rid)
                .setLatchCountDownRequest(LatchCountDownRequest.newBuilder()
                        .setKey(key).setCount(count).setTotal(total))
                .build();
    }

    private double counter(String name, String tag, String value) {
        var c = metrics.registry().find(name).tag(tag, value).counter();
        return c == null ? 0d : c.count();
    }

    private double timerCount(String result) {
        var t = metrics.registry().find(ServerMetrics.ACQUIRE_DURATION).tag("result", result).timer();
        return t == null ? 0d : t.count();
    }

    @Test
    void acquireReleaseRenewCountsAndGauges() {
        ServerSession a = session(3);
        Envelope granted = dispatcher.dispatch(a, acquire(1, "k1", -1));
        assertThat(granted.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        long token = granted.getAcquireResponse().getLeaseToken();

        assertThat(counter(ServerMetrics.ACQUIRE_TOTAL, "status", "OK")).isEqualTo(1);
        assertThat(timerCount("granted")).isEqualTo(1);
        assertThat(metrics.registry().get(ServerMetrics.LOCKS_HELD).tag("type", "lock").gauge().value())
                .isEqualTo(1);

        // 排队与立即拒绝各落一档 result。
        ServerSession b = session(3);
        assertThat(dispatcher.dispatch(b, acquire(2, "k1", -1)).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.QUEUED);
        ServerSession c = session(3);
        assertThat(dispatcher.dispatch(c, acquire(3, "k1", 0)).getAcquireResponse().getStatus())
                .isEqualTo(StatusCode.DENIED);
        assertThat(counter(ServerMetrics.ACQUIRE_TOTAL, "status", "QUEUED")).isEqualTo(1);
        assertThat(counter(ServerMetrics.ACQUIRE_TOTAL, "status", "DENIED")).isEqualTo(1);
        assertThat(timerCount("queued")).isEqualTo(1);
        assertThat(timerCount("denied")).isEqualTo(1);
        assertThat(metrics.registry().get(ServerMetrics.WAITERS).gauge().value()).isEqualTo(1);
        assertThat(metrics.registry().get(ServerMetrics.QUEUE_DEPTH_MAX).gauge().value()).isEqualTo(1);

        assertThat(dispatcher.dispatch(a, release(4, "k1", token)).getReleaseResponse().getStatus())
                .isEqualTo(StatusCode.OK);
        assertThat(counter(ServerMetrics.RELEASE_TOTAL, "status", "OK")).isEqualTo(1);
        // 重复释放：NOT_HELD 计数（条目已回收）。
        assertThat(dispatcher.dispatch(a, release(5, "k1", token)).getReleaseResponse().getStatus())
                .isEqualTo(StatusCode.NOT_HELD);
        assertThat(counter(ServerMetrics.RELEASE_TOTAL, "status", "NOT_HELD")).isEqualTo(1);

        // 续租未知凭证失败。
        assertThat(dispatcher.dispatch(a, renew(6, "k1", 999)).getLeaseRenewResponse().getStatus())
                .isEqualTo(StatusCode.NOT_HELD);
        assertThat(counter(ServerMetrics.RENEW_TOTAL, "status", "NOT_HELD")).isEqualTo(1);
    }

    @Test
    void sessionsGaugeFollowsRegistry() {
        registry.register(session(3));
        registry.register(session(3));
        assertThat(metrics.registry().get(ServerMetrics.SESSIONS).gauge().value()).isEqualTo(2);
    }

    @Test
    void latchMessagesMapToAcquireAndReleaseFamilies() {
        ServerSession a = session(3);
        // await 建立屏障并挂起 → acquire 家族（queued 档）。
        assertThat(dispatcher.dispatch(a, latchAwait(1, "l1", 2)).getLatchAwaitResponse().getStatus())
                .isEqualTo(StatusCode.QUEUED);
        assertThat(counter(ServerMetrics.ACQUIRE_TOTAL, "status", "QUEUED")).isEqualTo(1);
        assertThat(timerCount("queued")).isEqualTo(1);
        // countDown 归零 → release 家族计数（OK）。
        assertThat(dispatcher.dispatch(a, latchCountDown(2, "l1", 2, 0)).getLatchCountDownResponse().getStatus())
                .isEqualTo(StatusCode.OK);
        assertThat(counter(ServerMetrics.RELEASE_TOTAL, "status", "OK")).isEqualTo(1);
        // v1 会话的 LATCH 消息 → 协议拒绝也计入对应家族（有应答即有计数）。
        ServerSession v1 = session(1);
        assertThat(dispatcher.dispatch(v1, latchAwait(3, "l1", 0)).getLatchAwaitResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(counter(ServerMetrics.ACQUIRE_TOTAL, "status", "INVALID_REQUEST")).isEqualTo(1);
    }

    @Test
    void answeredRequestsCountedExactlyOnce() {
        ServerSession a = session(3);
        int requests = 8;
        for (int i = 0; i < requests; i++) {
            Envelope resp = dispatcher.dispatch(a, acquire(100 + i, "once-" + (i % 3), i % 2 == 0 ? -1 : 0));
            assertThat(resp.hasAcquireResponse()).isTrue();
        }
        double total = metrics.registry().find(ServerMetrics.ACQUIRE_TOTAL).counters().stream()
                .mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
        double durationSamples = metrics.registry().find(ServerMetrics.ACQUIRE_DURATION).timers().stream()
                .mapToLong(io.micrometer.core.instrument.Timer::count).sum();
        assertThat(total).isEqualTo(requests);
        assertThat(durationSamples).isEqualTo(requests);
    }

    @Test
    void pingAndPayloadLessRepliesNotCounted() {
        ServerSession a = session(3);
        Envelope ping = Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.PING)
                .setRequestId(1).build();
        assertThat(dispatcher.dispatch(a, ping)).isNull();
        // 未知类型：有错误信封（无对应指标族 payload）但不落入任何计数。
        Envelope weird = Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.AWAIT_NOTIFY)
                .setRequestId(2).build();
        assertThat(dispatcher.dispatch(a, weird)).isNotNull();
        assertThat(metrics.registry().find(ServerMetrics.ACQUIRE_TOTAL).counters()).isEmpty();
        assertThat(metrics.registry().find(ServerMetrics.RELEASE_TOTAL).counters()).isEmpty();
        assertThat(metrics.registry().find(ServerMetrics.RENEW_TOTAL).counters()).isEmpty();
    }

    @Test
    void nullMetricsKeepsLegacyBehavior() {
        RequestDispatcher plain = new RequestDispatcher(core);
        ServerSession a = session(3);
        Envelope resp = plain.dispatch(a, acquire(1, "legacy", -1));
        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(counter(ServerMetrics.ACQUIRE_TOTAL, "status", "OK")).isZero();
    }
}
