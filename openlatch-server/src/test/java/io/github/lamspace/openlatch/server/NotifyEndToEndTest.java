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

package io.github.lamspace.openlatch.server;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 通知端到端（规格"队首通知推送"与"租约到期扫描驱动"）：
 * 真实端口、真实协议收发，通知 → 重发 → 授予闭环。
 */
class NotifyEndToEndTest {

    /** 被测内嵌服务器：各用例自行启动、{@code tearDown} 统一关停。 */
    private OpenLatchServer server;

    /** 关停本用例启动的服务器（未启动时跳过）。 */
    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    /**
     * 构造获取请求信封：重入式、threadId=1；leaseMs 显式传入
     * （短租约到期用例依赖此参数自设 100ms 租约）。
     *
     * @param requestId 请求 id
     * @param key       锁键
     * @param leaseMs   请求租约时长（0 为服务端默认）
     * @param waitMs    等待时长（-1 为排队式）
     * @return 信封
     */
    private static Envelope acquire(long requestId, String key, long leaseMs, long waitMs) {
        return Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(key)
                        .setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1)
                        .setLeaseMs(leaseMs)
                        .setWaitMs(waitMs))
                .build();
    }

    /**
     * 构造释放请求信封（threadId 固定 1，与 acquire 的持有线程一致）。
     *
     * @param requestId 请求 id
     * @param key       锁键
     * @param token     租约凭证
     * @return 信封
     */
    private static Envelope release(long requestId, String key, long token) {
        return Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(requestId)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey(key).setLeaseToken(token).setThreadId(1))
                .build();
    }

    @Test
    @Timeout(30)
    void release_notifies_head_waiter_and_retry_grants() throws Exception {
        server = TestServers.start(TestServers.config(0));

        try (TestProtocolClient holder = new TestProtocolClient();
             TestProtocolClient waiter = new TestProtocolClient()) {
            holder.connect("127.0.0.1", server.port());
            waiter.connect("127.0.0.1", server.port());
            holder.hello();
            waiter.hello();

            Envelope granted = holder.sendAndAwait(acquire(holder.nextRequestId(), "order", 0, -1));
            assertThat(granted.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long token = granted.getAcquireResponse().getLeaseToken();

            long waiterRequestId = waiter.nextRequestId();
            Envelope queued = waiter.sendAndAwait(acquire(waiterRequestId, "order", 0, -1));
            assertThat(queued.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(queued.getAcquireResponse().getQueuePosition()).isEqualTo(1);

            Envelope released = holder.sendAndAwait(release(holder.nextRequestId(), "order", token));
            assertThat(released.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(released.getReleaseResponse().getFullyReleased()).isTrue();

            Envelope push = waiter.awaitPush(5000);
            assertThat(push).isNotNull();
            assertThat(push.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(push.getRequestId()).isZero();
            assertThat(push.getAwaitNotify().getRequestIdRef()).isEqualTo(waiterRequestId);
            assertThat(push.getAwaitNotify().getKey()).isEqualTo("order");

            // 以同一 request_id 重发：服务端幂等授予（§4.8 规则 7）。
            Envelope retry = waiter.sendAndAwait(acquire(waiterRequestId, "order", 0, -1));
            assertThat(retry.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(retry.getAcquireResponse().getLeaseToken()).isPositive();
        }
    }

    @Test
    @Timeout(30)
    void lease_expiry_releases_and_notifies_waiter() throws Exception {
        server = TestServers.start(TestServers.fastExpiryConfig(0));

        try (TestProtocolClient holder = new TestProtocolClient();
             TestProtocolClient waiter = new TestProtocolClient()) {
            holder.connect("127.0.0.1", server.port());
            waiter.connect("127.0.0.1", server.port());
            holder.hello();
            waiter.hello();

            // 短租约 100ms，持有者不续租也不释放。
            Envelope granted = holder.sendAndAwait(acquire(holder.nextRequestId(), "expiring", 100, -1));
            assertThat(granted.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(granted.getAcquireResponse().getGrantedLeaseMs()).isEqualTo(100);

            long waiterRequestId = waiter.nextRequestId();
            Envelope queued = waiter.sendAndAwait(acquire(waiterRequestId, "expiring", 0, -1));
            assertThat(queued.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);

            Envelope push = waiter.awaitPush(5000);
            assertThat(push).isNotNull();
            assertThat(push.getAwaitNotify().getRequestIdRef()).isEqualTo(waiterRequestId);

            Envelope retry = waiter.sendAndAwait(acquire(waiterRequestId, "expiring", 0, -1));
            assertThat(retry.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        }
    }
}
