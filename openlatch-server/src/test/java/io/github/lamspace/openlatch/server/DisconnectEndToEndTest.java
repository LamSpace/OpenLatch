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

    private OpenLatchServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

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
