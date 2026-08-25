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
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.session.ServerSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §5.4 分发表（design.md D5）：逐消息映射、requestId 回显、非法消息裁决。纯单元，无 Netty。
 */
class RequestDispatcherTest {

    private CoreEngine core;
    private RequestDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        core = new CoreEngine(new CoreConfig(), new SystemClock(), (s, r, k) -> { });
        dispatcher = new RequestDispatcher(core);
    }

    private ServerSession newSession() {
        ServerSession session = new ServerSession(null);
        session.activate(core.sessionOpened());
        return session;
    }

    private static Envelope acquire(long requestId, String key, long waitMs) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(key)
                        .setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1)
                        .setLeaseMs(0)
                        .setWaitMs(waitMs))
                .build();
    }

    private static Envelope release(long requestId, String key, long token) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(requestId)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey(key).setLeaseToken(token).setThreadId(1))
                .build();
    }

    private static Envelope renew(long requestId, String key, long token) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LEASE_RENEW)
                .setRequestId(requestId)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder()
                        .setKey(key).setLeaseToken(token).setLeaseMs(30_000))
                .build();
    }

    @Test
    void acquire_idle_key_granted_with_token_and_expiry() {
        ServerSession session = newSession();
        long before = System.currentTimeMillis();

        Envelope resp = dispatcher.dispatch(session, acquire(11, "k", -1));

        assertThat(resp.getType()).isEqualTo(MessageType.LOCK_ACQUIRE);
        assertThat(resp.getRequestId()).isEqualTo(11);
        AcquireResponse ar = resp.getAcquireResponse();
        assertThat(ar.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(ar.getLeaseToken()).isPositive();
        assertThat(ar.getGrantedLeaseMs()).isEqualTo(CoreConfig.DEFAULT_LEASE_MS);
        assertThat(ar.getLeaseExpiresAtMs() - ar.getGrantedLeaseMs())
                .isBetween(before - 1000, before + 10_000);
    }

    @Test
    void acquire_held_key_queued_with_position() {
        ServerSession holder = newSession();
        ServerSession waiter = newSession();
        dispatcher.dispatch(holder, acquire(1, "k", -1));

        Envelope resp = dispatcher.dispatch(waiter, acquire(2, "k", -1));

        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        assertThat(resp.getAcquireResponse().getQueuePosition()).isEqualTo(1);
    }

    @Test
    void acquire_wait_ms_positive_queues_like_minus_one() {
        ServerSession holder = newSession();
        ServerSession waiter = newSession();
        dispatcher.dispatch(holder, acquire(1, "k", -1));

        Envelope resp = dispatcher.dispatch(waiter, acquire(2, "k", 5000));

        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
    }

    @Test
    void acquire_held_key_try_lock_denied() {
        ServerSession holder = newSession();
        ServerSession other = newSession();
        dispatcher.dispatch(holder, acquire(1, "k", -1));

        Envelope resp = dispatcher.dispatch(other, acquire(2, "k", 0));

        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.DENIED);
    }

    @Test
    void acquire_empty_key_rejected() {
        ServerSession session = newSession();

        Envelope resp = dispatcher.dispatch(session, acquire(1, "", -1));

        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.KEY_EMPTY);
    }

    @Test
    void acquire_oversized_key_rejected() {
        ServerSession session = newSession();

        Envelope resp = dispatcher.dispatch(session, acquire(1, "k".repeat(513), -1));

        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.KEY_TOO_LONG);
    }

    @Test
    void acquire_unknown_session_rejected() {
        ServerSession ghost = new ServerSession(null);
        ghost.activate(123456789L);

        Envelope resp = dispatcher.dispatch(ghost, acquire(1, "k", -1));

        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.SESSION_EXPIRED);
    }

    @Test
    void release_with_valid_token_fully_releases() {
        ServerSession session = newSession();
        Envelope granted = dispatcher.dispatch(session, acquire(1, "k", -1));
        long token = granted.getAcquireResponse().getLeaseToken();

        Envelope resp = dispatcher.dispatch(session, release(2, "k", token));

        assertThat(resp.getType()).isEqualTo(MessageType.LOCK_RELEASE);
        assertThat(resp.getRequestId()).isEqualTo(2);
        assertThat(resp.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(resp.getReleaseResponse().getFullyReleased()).isTrue();
    }

    @Test
    void release_with_wrong_token_rejected() {
        ServerSession session = newSession();
        dispatcher.dispatch(session, acquire(1, "k", -1));

        Envelope resp = dispatcher.dispatch(session, release(2, "k", 99999L));

        assertThat(resp.getReleaseResponse().getStatus()).isEqualTo(StatusCode.INVALID_TOKEN);
    }

    @Test
    void release_not_held_rejected() {
        ServerSession session = newSession();

        Envelope resp = dispatcher.dispatch(session, release(1, "free-key", 1));

        assertThat(resp.getReleaseResponse().getStatus()).isEqualTo(StatusCode.NOT_HELD);
    }

    @Test
    void renew_with_valid_token_extends_expiry() {
        ServerSession session = newSession();
        Envelope granted = dispatcher.dispatch(session, acquire(1, "k", -1));
        long token = granted.getAcquireResponse().getLeaseToken();

        Envelope resp = dispatcher.dispatch(session, renew(2, "k", token));

        assertThat(resp.getType()).isEqualTo(MessageType.LEASE_RENEW);
        assertThat(resp.getLeaseRenewResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(resp.getLeaseRenewResponse().getLeaseExpiresAtMs()).isPositive();
    }

    @Test
    void renew_with_wrong_token_rejected() {
        ServerSession session = newSession();
        dispatcher.dispatch(session, acquire(1, "k", -1));

        Envelope resp = dispatcher.dispatch(session, renew(2, "k", 99999L));

        assertThat(resp.getLeaseRenewResponse().getStatus()).isEqualTo(StatusCode.INVALID_TOKEN);
    }

    @Test
    void ping_is_not_answered() {
        ServerSession session = newSession();
        Envelope ping = Envelope.newBuilder()
                .setProtocolVersion(1).setType(MessageType.PING).setRequestId(5).build();

        assertThat(dispatcher.dispatch(session, ping)).isNull();
    }

    @Test
    void type_payload_mismatch_rejected_without_disconnect_marker() {
        ServerSession session = newSession();
        // LOCK_ACQUIRE 类型却携带 ReleaseRequest payload。
        Envelope mismatched = Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(8)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey("k"))
                .build();

        Envelope resp = dispatcher.dispatch(session, mismatched);

        assertThat(resp.getType()).isEqualTo(MessageType.LOCK_ACQUIRE);
        assertThat(resp.getRequestId()).isEqualTo(8);
        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
    }

    @Test
    void unknown_and_server_push_types_rejected() {
        ServerSession session = newSession();
        Envelope unknown = Envelope.newBuilder()
                .setProtocolVersion(1).setType(MessageType.MESSAGE_TYPE_UNKNOWN).setRequestId(1).build();
        Envelope notify = Envelope.newBuilder()
                .setProtocolVersion(1).setType(MessageType.AWAIT_NOTIFY).setRequestId(2).build();

        Envelope unknownResp = dispatcher.dispatch(session, unknown);
        assertThat(unknownResp.getType()).isEqualTo(MessageType.MESSAGE_TYPE_UNKNOWN);
        assertThat(unknownResp.getRequestId()).isEqualTo(1);
        assertThat(unknownResp.hasAcquireResponse()).isFalse();

        Envelope notifyResp = dispatcher.dispatch(session, notify);
        assertThat(notifyResp.getRequestId()).isEqualTo(2);
    }

    @Test
    void outcome_to_status_table_is_exhaustive() {
        assertThat(RequestDispatcher.toAcquireStatus(Outcome.GRANTED)).isEqualTo(StatusCode.OK);
        assertThat(RequestDispatcher.toAcquireStatus(Outcome.QUEUED)).isEqualTo(StatusCode.QUEUED);
        assertThat(RequestDispatcher.toAcquireStatus(Outcome.DENIED)).isEqualTo(StatusCode.DENIED);
        assertThat(RequestDispatcher.toAcquireStatus(Outcome.REJECT_KEY_EMPTY)).isEqualTo(StatusCode.KEY_EMPTY);
        assertThat(RequestDispatcher.toAcquireStatus(Outcome.REJECT_KEY_TOO_LONG)).isEqualTo(StatusCode.KEY_TOO_LONG);
        assertThat(RequestDispatcher.toAcquireStatus(Outcome.REJECT_QUEUE_FULL)).isEqualTo(StatusCode.OVERLOADED);
        assertThat(RequestDispatcher.toAcquireStatus(Outcome.REJECT_SESSION)).isEqualTo(StatusCode.SESSION_EXPIRED);
    }

    @Test
    void release_status_to_code_table_is_exhaustive() {
        assertThat(RequestDispatcher.toCommonStatus(ReleaseStatus.OK)).isEqualTo(StatusCode.OK);
        assertThat(RequestDispatcher.toCommonStatus(ReleaseStatus.INVALID_TOKEN)).isEqualTo(StatusCode.INVALID_TOKEN);
        assertThat(RequestDispatcher.toCommonStatus(ReleaseStatus.NOT_HELD)).isEqualTo(StatusCode.NOT_HELD);
        assertThat(RequestDispatcher.toCommonStatus(ReleaseStatus.REJECT_SESSION)).isEqualTo(StatusCode.SESSION_EXPIRED);
    }
}
