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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 故障注入套件（§10.4，tasks 9.1/9.2/9.4）：持锁中断连、等待中断连、
 * 半开连接的服务端空闲清理。
 */
class ClientFaultInjectionIT {

    /** 短租约快扫描服务器配置：持锁断连用例（§10.4 用 1–2s 短租约）。 */
    private static ServerConfig fastExpiry(int port) {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(port, d.workerThreads(), d.idleTimeoutMs(), 200L,
                100L, d.maxLeaseMs(), 100L, d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    /** 短空闲时限服务器配置：半开连接用例（2s 读空闲即断连）。 */
    private static ServerConfig fastIdle(int port) {
        ServerConfig d = ServerConfig.defaults();
        return new ServerConfig(port, d.workerThreads(), 2000L, d.defaultLeaseMs(),
                d.minLeaseMs(), d.maxLeaseMs(), d.leaseTickIntervalMs(), d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection());
    }

    /**
     * 9.1 持锁中断连：直接关闭通道（不释放），服务端断连清理在一个租约期内
     * 释放锁，其他客户端可获取。
     */
    @Test
    void heldLockReleasedOnAbruptDisconnect() throws Exception {
        OpenLatchServer server = ClientTestServers.start(fastExpiry(0));
        OpenLatchClient holder = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).build();
        OpenLatchClient rival = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).build();
        try {
            holder.connectAsync().get(5, TimeUnit.SECONDS);
            rival.connectAsync().get(5, TimeUnit.SECONDS);

            // 以 200ms 短租约获取后直接断连（不 unlock）
            OLock holderLock = holder.newReentrantLock("abrupt");
            holder.acquireAsync(new AcquireSpec("abrupt", LockType.REENTRANT,
                    Thread.currentThread().threadId(), 200, 0)).get(3, TimeUnit.SECONDS);
            holder.connectionManager().activeChannel().close().syncUninterruptibly();

            // 服务端即时断连清理释放锁（远快于一个租约期）
            long start = System.currentTimeMillis();
            assertThat(rival.newReentrantLock("abrupt").tryLock(3, TimeUnit.SECONDS)).isTrue();
            assertThat(System.currentTimeMillis() - start).isLessThan(2000);
            rival.newReentrantLock("abrupt").unlock();
        } finally {
            holder.shutdown();
            rival.shutdown();
            server.stop();
        }
    }

    /**
     * 9.2 等待中断连：排队后关闭通道，挂起 future 快速失败且队列无残留
     * （后续授予顺序不受影响）。
     */
    @Test
    void waiterDisconnectFailsFastWithoutResidue() throws Exception {
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.config(0));
        OpenLatchClient holder = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).build();
        OpenLatchClient waiter = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).build();
        OpenLatchClient rival = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).build();
        try {
            holder.connectAsync().get(5, TimeUnit.SECONDS);
            waiter.connectAsync().get(5, TimeUnit.SECONDS);
            rival.connectAsync().get(5, TimeUnit.SECONDS);

            holder.newReentrantLock("wait-abandon").lock();
            java.util.concurrent.CompletableFuture<Boolean> waiting =
                    waiter.newReentrantLock("wait-abandon").tryLockAsync(10, TimeUnit.SECONDS);
            Thread.sleep(300); // 确保已排队

            waiter.connectionManager().activeChannel().close().syncUninterruptibly();

            try {
                waiting.get(3, TimeUnit.SECONDS);
                throw new AssertionError("expected fast failure");
            } catch (ExecutionException e) {
                assertThat(unwrap(e)).isInstanceOf(ServerUnavailableException.class);
            }

            // 队列无残留：持有者释放后，后来者直接获取（不被已断连的等待项阻塞）
            holder.newReentrantLock("wait-abandon").unlock();
            long start = System.currentTimeMillis();
            assertThat(rival.newReentrantLock("wait-abandon").tryLock(2, TimeUnit.SECONDS)).isTrue();
            assertThat(System.currentTimeMillis() - start).isLessThan(1000);
            rival.newReentrantLock("wait-abandon").unlock();
        } finally {
            holder.shutdown();
            waiter.shutdown();
            rival.shutdown();
            server.stop();
        }
    }

    /**
     * 9.4 半开连接：暂停客户端出站写，服务端读空闲检测断连并清理会话，
     * 锁随之释放（之后连接的新客户端可获取）。
     */
    @Test
    void halfOpenConnectionCleanedUpByServerIdleDetection() throws Exception {
        OpenLatchServer server = ClientTestServers.start(fastIdle(0));
        OpenLatchClient holder = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).build();
        try {
            holder.connectAsync().get(5, TimeUnit.SECONDS);
            holder.newReentrantLock("half-open").lock();

            // 暂停出站写：客户端不再发送任何内容（含续租），服务端读空闲触发断连
            holder.requestMultiplexer().setOutboundGate(envelope -> false);

            // 服务端 2s 空闲检测断连 → 会话清理 → 锁释放
            Thread.sleep(2500);
            assertThat(server.sessions().size()).isZero();

            // 清理后新连接的客户端直接走快路径获取（锁已释放）
            OpenLatchClient rival = OpenLatchClient.builder()
                    .address("127.0.0.1:" + server.port()).build();
            try {
                rival.connectAsync().get(5, TimeUnit.SECONDS);
                assertThat(rival.newReentrantLock("half-open").tryLock(2, TimeUnit.SECONDS)).isTrue();
                rival.newReentrantLock("half-open").unlock();
            } finally {
                rival.shutdown();
            }
        } finally {
            holder.shutdown();
            server.stop();
        }
    }

    /**
     * 解包 Execution/Completion 异常至真实原因。
     *
     * @param t 待解包异常
     * @return 真实原因
     */
    private static Throwable unwrap(Throwable t) {
        Throwable cause = t;
        while ((cause instanceof ExecutionException
                || cause instanceof java.util.concurrent.CompletionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
