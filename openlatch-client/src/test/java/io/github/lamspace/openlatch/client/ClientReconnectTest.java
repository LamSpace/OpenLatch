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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 重连与锁丢失时序用例（tasks 7.1–7.7）：断连快速失败、重连成功即触发
 * 旧锁丢失回调、失锁时刻到达仍未重连则到时回调、关停后拒绝请求。
 */
class ClientReconnectTest {

    /** 断连后等待回调/事件的统一时限。 */
    private static final long EVENT_WAIT_SECONDS = 6;

    /** 持锁中断连且服务器重启：重连成功即触发旧锁丢失回调，本地状态清除。 */
    @Test
    void lockLostFiredOnReconnectAfterServerRestart() throws Exception {
        // 固定端口：服务器须在原端口重启以验证同地址重连，不适用临时端口约定
        int port = 19410;
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.config(port));
        OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + port)
                .reconnectInitialBackoff(Duration.ofMillis(50))
                .reconnectMaxBackoff(Duration.ofMillis(300))
                .build();
        BlockingQueue<String> lostKeys = new LinkedBlockingQueue<>();
        client.addLockLostListener((key, cause) -> lostKeys.offer(key));
        try {
            client.connectAsync().get(5, TimeUnit.SECONDS);
            OLock lock = client.newReentrantLock("restart-key");
            lock.lock();
            assertThat(lock.isHeldByCurrentThread()).isTrue();

            server.stop();
            OpenLatchServer restarted = ClientTestServers.start(ClientTestServers.config(port));
            try {
                assertThat(lostKeys.poll(EVENT_WAIT_SECONDS, TimeUnit.SECONDS))
                        .isEqualTo("restart-key");
                assertThat(lock.isHeldByCurrentThread()).isFalse();

                // 重连后可重新竞争
                Thread.sleep(100);
                assertThat(lock.tryLock()).isTrue();
                lock.unlock();
            } finally {
                restarted.stop();
            }
        } finally {
            client.shutdown();
        }
    }

    /** 持锁中断连且不再恢复：失锁时刻（租约耗尽）到达时触发回调。 */
    @Test
    void lockLostFiredAtLostAtWhenNeverReconnected() throws Exception {
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.config(0));
        int port = server.port();
        OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + port)
                .reconnectInitialBackoff(Duration.ofMillis(50))
                .reconnectMaxBackoff(Duration.ofMillis(300))
                .build();
        BlockingQueue<String> lostKeys = new LinkedBlockingQueue<>();
        client.addLockLostListener((key, cause) -> lostKeys.offer(key));
        try {
            client.connectAsync().get(5, TimeUnit.SECONDS);
            OLock lock = client.newReentrantLock("lost-at-key");
            // 以 1s 短租约获取：失锁时刻 = 上次续租 + 1s
            client.acquireAsync(new AcquireSpec("lost-at-key", LockType.REENTRANT,
                    Thread.currentThread().threadId(), 1000, 0))
                    .get(3, TimeUnit.SECONDS);

            server.stop(); // 不再重启

            long start = System.currentTimeMillis();
            assertThat(lostKeys.poll(EVENT_WAIT_SECONDS, TimeUnit.SECONDS))
                    .isEqualTo("lost-at-key");
            long elapsed = System.currentTimeMillis() - start;
            // 回调应在失锁时刻附近（约 1s 租约），远早于等待上限
            assertThat(elapsed).isLessThan(TimeUnit.SECONDS.toMillis(EVENT_WAIT_SECONDS) - 1000);
            assertThat(lock.isHeldByCurrentThread()).isFalse();
        } finally {
            client.shutdown();
        }
    }

    /** 等待中断连：挂起的获取快速失败（不等到自身超时）。 */
    @Test
    void pendingAcquireFailsFastOnDisconnect() throws Exception {
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.config(0));
        int port = server.port();
        OpenLatchClient blocker = OpenLatchClient.builder().address("127.0.0.1:" + port).build();
        OpenLatchClient waiter = OpenLatchClient.builder()
                .address("127.0.0.1:" + port)
                .reconnectInitialBackoff(Duration.ofMillis(50))
                .build();
        try {
            blocker.connectAsync().get(5, TimeUnit.SECONDS);
            waiter.connectAsync().get(5, TimeUnit.SECONDS);

            blocker.newReentrantLock("pending-key").lock();
            java.util.concurrent.CompletableFuture<Boolean> waiting =
                    waiter.newReentrantLock("pending-key").tryLockAsync(10, TimeUnit.SECONDS);
            Thread.sleep(300); // 确保已排队

            server.stop();

            // 关停序列存在合法竞态：若阻塞者的会话先被清理，服务端会先把锁
            // 授予等待者、再断开等待者连接——此时等待以 true 完成（随后锁丢失
            // 回调兜底）；否则等待以 ServerUnavailableException 快速失败。
            // 两种结局都满足"快速决出、无死等"的契约。
            long start = System.currentTimeMillis();
            try {
                Boolean granted = waiting.get(3, TimeUnit.SECONDS);
                assertThat(granted).isTrue();
            } catch (java.util.concurrent.ExecutionException e) {
                assertThat(unwrapCause(e)).isInstanceOf(ServerUnavailableException.class);
            }
            assertThat(System.currentTimeMillis() - start).isLessThan(3000);
        } finally {
            blocker.shutdown();
            waiter.shutdown();
        }
    }

    /**
     * 解包 {@link java.util.concurrent.ExecutionException} 与
     * {@link java.util.concurrent.CompletionException} 至真实原因。
     *
     * @param t 待解包异常
     * @return 真实原因
     */
    private static Throwable unwrapCause(Throwable t) {
        Throwable cause = t;
        while ((cause instanceof java.util.concurrent.ExecutionException
                || cause instanceof java.util.concurrent.CompletionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    /** 关停后：新请求被拒绝，且关停幂等。 */
    @Test
    void shutdownRejectsNewRequests() throws Exception {
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.config(0));
        OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).build();
        try {
            client.connectAsync().get(5, TimeUnit.SECONDS);
            client.shutdown();

            java.util.concurrent.CompletableFuture<LockGrant> rejected =
                    client.acquireAsync(new AcquireSpec("closed", LockType.REENTRANT, 1L, 0, 0));
            try {
                rejected.get(1, TimeUnit.SECONDS);
                throw new AssertionError("expected rejection");
            } catch (java.util.concurrent.ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(IllegalStateException.class);
            }
        } finally {
            client.shutdown();
            server.stop();
        }
    }
}
