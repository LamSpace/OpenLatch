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
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 规格"断连会话清理与空闲检测"：持锁断连即时释放、等待中断连摘除、空闲连接被断开。
 */
class DisconnectEndToEndTest {

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
     * 构造获取请求信封：硬编码重入式、threadId=1、leaseMs=0（交服务端默认
     * 租约裁决）——"默认租约 30s 内即刻可获取"类断言因此成立。
     *
     * @param requestId 请求 id
     * @param key       锁键
     * @param waitMs    等待时长（-1 为排队式）
     * @return 信封
     */
    private static Envelope acquire(long requestId, String key, long waitMs) {
        return Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
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

    @Test
    @Timeout(30)
    void holder_disconnect_releases_lock_immediately() throws Exception {
        // 默认租约 30s：若 3 秒内可获取，必然是断连即时清理而非租约到期。
        server = TestServers.start(TestServers.config(0));

        try (TestProtocolClient holder = new TestProtocolClient();
             TestProtocolClient other = new TestProtocolClient()) {
            holder.connect("127.0.0.1", server.port());
            other.connect("127.0.0.1", server.port());
            holder.hello();
            other.hello();

            Envelope granted = holder.sendAndAwait(acquire(holder.nextRequestId(), "k", -1));
            assertThat(granted.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            holder.disconnectAbruptly();

            Envelope regnant = other.sendAndAwait(acquire(other.nextRequestId(), "k", -1), 3000);
            assertThat(regnant.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        }
    }

    @Test
    @Timeout(30)
    void waiter_disconnect_is_removed_from_queue() throws Exception {
        server = TestServers.start(TestServers.config(0));

        try (TestProtocolClient holder = new TestProtocolClient();
             TestProtocolClient abandoner = new TestProtocolClient();
             TestProtocolClient survivor = new TestProtocolClient()) {
            holder.connect("127.0.0.1", server.port());
            abandoner.connect("127.0.0.1", server.port());
            survivor.connect("127.0.0.1", server.port());
            holder.hello();
            abandoner.hello();
            survivor.hello();

            holder.sendAndAwait(acquire(holder.nextRequestId(), "k", -1));
            abandoner.sendAndAwait(acquire(abandoner.nextRequestId(), "k", -1));
            survivor.sendAndAwait(acquire(survivor.nextRequestId(), "k", -1));

            abandoner.disconnectAbruptly();

            // 持有者释放（经断连清理路径）：通知应直接到达 survivor，跳过已断连的放弃者。
            holder.disconnectAbruptly();

            Envelope push = survivor.awaitPush(5000);
            assertThat(push).isNotNull();
            assertThat(push.getAwaitNotify().getRequestIdRef()).isPositive();

            Envelope retry = survivor.sendAndAwait(
                    acquire(push.getAwaitNotify().getRequestIdRef(), "k", -1));
            assertThat(retry.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        }
    }

    @Test
    @Timeout(30)
    void idle_connection_is_closed_and_session_cleaned() throws Exception {
        // 空闲时限 800ms（fastIdleConfig）；默认租约 30s，锁的释放必因空闲断连。
        server = TestServers.start(TestServers.fastIdleConfig(0));

        try (TestProtocolClient idleHolder = new TestProtocolClient();
             TestProtocolClient other = new TestProtocolClient()) {
            idleHolder.connect("127.0.0.1", server.port());
            idleHolder.hello();

            Envelope granted = idleHolder.sendAndAwait(acquire(idleHolder.nextRequestId(), "k", -1));
            assertThat(granted.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            // idleHolder 静默：无后续读入，等待空闲断连发生。
            long deadline = System.currentTimeMillis() + 5000;
            while (idleHolder.isConnected() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertThat(idleHolder.isConnected()).isFalse();

            // 会话已清理（默认租约 30s 未到期，释放必因空闲断连）：新连接即刻可获取。
            other.connect("127.0.0.1", server.port());
            other.hello();
            Envelope regnant = other.sendAndAwait(acquire(other.nextRequestId(), "k", -1), 3000);
            assertThat(regnant.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        }
    }
}
