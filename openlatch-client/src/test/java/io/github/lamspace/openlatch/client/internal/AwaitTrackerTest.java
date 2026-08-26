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

package io.github.lamspace.openlatch.client.internal;

import io.github.lamspace.openlatch.client.AcquireSpec;
import io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException;
import io.github.lamspace.openlatch.client.LockGrant;
import io.github.lamspace.openlatch.client.LockType;
import io.github.lamspace.openlatch.client.OpenLatchException;
import io.github.lamspace.openlatch.client.ServerUnavailableException;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.HashedWheelTimer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 等待跟踪单测（tasks 4.1–4.5）：§6.5 边界场景表逐项覆盖，含
 * 重发超时保持挂起（D1）与重复/孤儿授予补偿归还（D3）。
 */
class AwaitTrackerTest {

    /** 默认每请求超时（毫秒）：多数用例不希望它触发。 */
    private static final long REQUEST_TIMEOUT_MS = 2000;
    /** 测试用锁键。 */
    private static final String KEY = "orders/42";
    /** 测试用线程标识。 */
    private static final long THREAD_ID = 7L;

    /** 细刻度定时器。 */
    private HashedWheelTimer timer;
    /** 内存通道。 */
    private EmbeddedChannel channel;
    /** 多路复用器。 */
    private RequestMultiplexer multiplexer;
    /** 授予回调捕获。 */
    private AtomicReference<AcquireSpec> grantedSpec;
    /** 授予回调捕获。 */
    private AtomicReference<LockGrant> grantedGrant;
    /** 被测对象。 */
    private AwaitTracker tracker;

    /**
     * 装配：真实多路复用器 + 内存通道，孤儿响应路由到被测对象。
     */
    @BeforeEach
    void setUp() {
        setUpTracker(REQUEST_TIMEOUT_MS);
    }

    /**
     * 释放资源。
     */
    @AfterEach
    void tearDown() {
        timer.stop();
        channel.close();
    }

    /** 授予：future 以凭据完成，onGranted 被回调。 */
    @Test
    void grantCompletesFutureAndInvokesCallback() throws Exception {
        CompletableFuture<LockGrant> future = startAcquire(30_000);
        Envelope out = channel.readOutbound();

        multiplexer.onResponse(acquireResponse(out.getRequestId(), StatusCode.OK, 77, 30_000));

        LockGrant grant = future.get(1, TimeUnit.SECONDS);
        assertThat(grant.leaseToken()).isEqualTo(77);
        assertThat(grant.grantedLeaseMs()).isEqualTo(30_000);
        assertThat(grantedSpec.get().key()).isEqualTo(KEY);
        assertThat(grantedGrant.get()).isEqualTo(grant);
    }

    /** 排队 → 通知 → 同 id 重发 → 授予。 */
    @Test
    void queuedThenNotifyResendGrants() throws Exception {
        CompletableFuture<LockGrant> future = startAcquire(30_000);
        Envelope out = channel.readOutbound();
        long requestId = out.getRequestId();

        multiplexer.onResponse(acquireResponse(requestId, StatusCode.QUEUED, 0, 0));
        assertThat(future).isNotDone();

        tracker.onNotify(AwaitNotify.newBuilder().setKey(KEY).setRequestIdRef(requestId).build());
        Envelope resend = channel.readOutbound();
        assertThat(resend.getRequestId()).isEqualTo(requestId);
        assertThat(resend.getType()).isEqualTo(MessageType.LOCK_ACQUIRE);

        multiplexer.onResponse(acquireResponse(requestId, StatusCode.OK, 88, 30_000));
        assertThat(future.get(1, TimeUnit.SECONDS).leaseToken()).isEqualTo(88);
    }

    /** 立即式被拒：以 LockDeniedException 失败。 */
    @Test
    void deniedFailsFuture() {
        CompletableFuture<LockGrant> future = startAcquire(0);
        Envelope out = channel.readOutbound();

        multiplexer.onResponse(acquireResponse(out.getRequestId(), StatusCode.DENIED, 0, 0));

        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(LockDeniedException.class);
    }

    /** 错误状态码原样上抛。 */
    @Test
    void errorStatusFailsWithStatusCode() {
        CompletableFuture<LockGrant> future = startAcquire(30_000);
        Envelope out = channel.readOutbound();

        multiplexer.onResponse(acquireResponse(out.getRequestId(), StatusCode.KEY_TOO_LONG, 0, 0));

        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .satisfies(e -> {
                    OpenLatchException cause = (OpenLatchException) e.getCause();
                    assertThat(cause.status()).isEqualTo(StatusCode.KEY_TOO_LONG);
                });
    }

    /** 总超时失败；晚到的通知被忽略（不重发）。 */
    @Test
    void totalTimeoutFailsAndLateNotifyIgnored() throws Exception {
        CompletableFuture<LockGrant> future = startAcquire(100);
        Envelope out = channel.readOutbound();
        long requestId = out.getRequestId();
        multiplexer.onResponse(acquireResponse(requestId, StatusCode.QUEUED, 0, 0));

        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                .hasCauseInstanceOf(LockAcquisitionTimeoutException.class);

        tracker.onNotify(AwaitNotify.newBuilder().setKey(KEY).setRequestIdRef(requestId).build());
        Thread.sleep(50);
        assertThat(channel.outboundMessages()).isEmpty();
    }

    /** 重发无响应（请求超时）：保持挂起，再次通知可再次重发（D1）。 */
    @Test
    void resendTimeoutKeepsWaitingForNextNotify() throws Exception {
        tearDown();
        setUpTracker(80);
        CompletableFuture<LockGrant> future = startAcquire(10_000);
        Envelope out = channel.readOutbound();
        long requestId = out.getRequestId();
        multiplexer.onResponse(acquireResponse(requestId, StatusCode.QUEUED, 0, 0));

        tracker.onNotify(AwaitNotify.newBuilder().setKey(KEY).setRequestIdRef(requestId).build());
        assertThat((Envelope) channel.readOutbound()).isNotNull();

        // 重发无响应：请求超时后 future 仍挂起
        Thread.sleep(200);
        assertThat(future).isNotDone();

        // 第二次通知触发再次重发
        tracker.onNotify(AwaitNotify.newBuilder().setKey(KEY).setRequestIdRef(requestId).build());
        Envelope secondResend = channel.readOutbound();
        assertThat(secondResend.getRequestId()).isEqualTo(requestId);
    }

    /** 重复通知导致重复授予：首个交付调用方，重复者归还（D3）。 */
    @Test
    void duplicateGrantCompensatedWithRelease() throws Exception {
        CompletableFuture<LockGrant> future = startAcquire(30_000);
        Envelope out = channel.readOutbound();
        long requestId = out.getRequestId();
        multiplexer.onResponse(acquireResponse(requestId, StatusCode.QUEUED, 0, 0));

        tracker.onNotify(AwaitNotify.newBuilder().setKey(KEY).setRequestIdRef(requestId).build());
        tracker.onNotify(AwaitNotify.newBuilder().setKey(KEY).setRequestIdRef(requestId).build());
        assertThat((Envelope) channel.readOutbound()).isNotNull();
        assertThat((Envelope) channel.readOutbound()).isNotNull();

        multiplexer.onResponse(acquireResponse(requestId, StatusCode.OK, 55, 30_000));
        assertThat(future.get(1, TimeUnit.SECONDS).leaseToken()).isEqualTo(55);

        // 第二个授予到达：无挂起项，走孤儿路径 → 补偿释放
        multiplexer.onResponse(acquireResponse(requestId, StatusCode.OK, 55, 30_000));
        Envelope compensation = channel.readOutbound();
        assertThat(compensation).isNotNull();
        assertThat(compensation.getType()).isEqualTo(MessageType.LOCK_RELEASE);
        assertThat(compensation.getReleaseRequest().getKey()).isEqualTo(KEY);
        assertThat(compensation.getReleaseRequest().getLeaseToken()).isEqualTo(55);
        assertThat(compensation.getReleaseRequest().getThreadId()).isEqualTo(THREAD_ID);
    }

    /** 重发的请求超时后响应才到：等待仍活跃，按正常授予完成。 */
    @Test
    void lateOkAfterResendRequestTimeoutStillGrants() throws Exception {
        tearDown();
        setUpTracker(80);
        CompletableFuture<LockGrant> future = startAcquire(10_000);
        Envelope out = channel.readOutbound();
        long requestId = out.getRequestId();
        multiplexer.onResponse(acquireResponse(requestId, StatusCode.QUEUED, 0, 0));

        tracker.onNotify(AwaitNotify.newBuilder().setKey(KEY).setRequestIdRef(requestId).build());
        assertThat((Envelope) channel.readOutbound()).isNotNull();
        Thread.sleep(200); // 重发请求超时，inflight 摘除

        multiplexer.onResponse(acquireResponse(requestId, StatusCode.OK, 66, 30_000));
        assertThat(future.get(1, TimeUnit.SECONDS).leaseToken()).isEqualTo(66);
    }

    /** 总超时后在途重发被授予：归还（D3）。 */
    @Test
    void grantAfterTotalTimeoutCompensatedWithRelease() throws Exception {
        CompletableFuture<LockGrant> future = startAcquire(150);
        Envelope out = channel.readOutbound();
        long requestId = out.getRequestId();
        multiplexer.onResponse(acquireResponse(requestId, StatusCode.QUEUED, 0, 0));

        tracker.onNotify(AwaitNotify.newBuilder().setKey(KEY).setRequestIdRef(requestId).build());
        assertThat((Envelope) channel.readOutbound()).isNotNull();

        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
                .hasCauseInstanceOf(LockAcquisitionTimeoutException.class);

        multiplexer.onResponse(acquireResponse(requestId, StatusCode.OK, 99, 30_000));
        Envelope compensation = channel.readOutbound();
        assertThat(compensation).isNotNull();
        assertThat(compensation.getType()).isEqualTo(MessageType.LOCK_RELEASE);
        assertThat(compensation.getReleaseRequest().getLeaseToken()).isEqualTo(99);
    }

    /** 错型响应（同 requestId 的续租响应）不得按默认 AcquireResponse 误授予。 */
    @Test
    void wrongTypeResponseNeverGrantsWithDefaults() {
        CompletableFuture<LockGrant> future = startAcquire(30_000);
        Envelope out = channel.readOutbound();

        // 默认 AcquireResponse 实例的 status 恰为枚举零值 OK、token 为 0：
        // 若不校验信封类型，错型响应会被误判为"零凭据授予"。
        multiplexer.onResponse(Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LEASE_RENEW)
                .setRequestId(out.getRequestId())
                .setLeaseRenewResponse(io.github.lamspace.openlatch.protocol.LeaseRenewResponse
                        .newBuilder().setStatus(StatusCode.OK))
                .build());

        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(OpenLatchException.class);
        assertThat(grantedGrant.get()).isNull();
    }

    /** 断连清空：挂起等待以指定原因快速失败。 */
    @Test
    void failAllFailsPendingWaits() {
        CompletableFuture<LockGrant> future = startAcquire(30_000);
        Envelope out = channel.readOutbound();
        multiplexer.onResponse(acquireResponse(out.getRequestId(), StatusCode.QUEUED, 0, 0));

        tracker.failAll(new ServerUnavailableException("connection lost"));

        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                .hasCauseInstanceOf(ServerUnavailableException.class);
    }

    /**
     * 以给定每请求超时重建被测对象（用例需要短超时时调用）。
     *
     * @param requestTimeoutMs 每请求超时（毫秒）
     */
    private void setUpTracker(long requestTimeoutMs) {
        timer = new HashedWheelTimer(r -> {
            Thread t = new Thread(r, "test-tracker-timer");
            t.setDaemon(true);
            return t;
        }, 10, TimeUnit.MILLISECONDS);
        channel = new EmbeddedChannel();
        SessionContext session = new SessionContext(1L);
        multiplexer = new RequestMultiplexer(timer, () -> channel, () -> session);
        grantedSpec = new AtomicReference<>();
        grantedGrant = new AtomicReference<>();
        tracker = new AwaitTracker(timer, multiplexer, requestTimeoutMs,
                (spec, grant) -> {
                    grantedSpec.set(spec);
                    grantedGrant.set(grant);
                });
        multiplexer.setOrphanSink(tracker::onOrphanResponse);
    }

    /**
     * 发起一次排队式获取。
     *
     * @param totalTimeoutMs 等待总超时（毫秒）
     * @return 用户 future
     */
    private CompletableFuture<LockGrant> startAcquire(long totalTimeoutMs) {
        AcquireSpec spec = new AcquireSpec(KEY, LockType.REENTRANT, THREAD_ID, 0, -1);
        Envelope envelope = Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(100)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(KEY))
                .build();
        CompletableFuture<LockGrant> future = new CompletableFuture<>();
        tracker.startAcquire(100, envelope, spec, future, totalTimeoutMs);
        return future;
    }

    /**
     * 构造获取响应信封。
     *
     * @param requestId 请求 id
     * @param status    状态码
     * @param token     租约凭证
     * @param leaseMs   实际生效租约
     * @return 响应信封
     */
    private static Envelope acquireResponse(long requestId, StatusCode status, long token, long leaseMs) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireResponse(AcquireResponse.newBuilder()
                        .setStatus(status)
                        .setLeaseToken(token)
                        .setGrantedLeaseMs(leaseMs))
                .build();
    }
}
