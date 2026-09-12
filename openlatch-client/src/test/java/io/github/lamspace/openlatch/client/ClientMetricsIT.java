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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 客户端可选监控指标端到端：注入注册表的客户端
 * 按固定脚本逐项核对 {type,status} 计数与耗时样本；未注入的客户端行为
 * 一致（默认关闭）；停服断连场景钉住 {@code reconnect.total} 与
 * {@code locks.lost.total}（失锁裁决至多一次）。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ClientMetricsIT {

    /** 计数读数（未注册计 0）。 */
    private static double counter(MeterRegistry r, String type, String status) {
        var c = r.find("openlatch.client.requests.total")
                .tag("type", type).tag("status", status).counter();
        return c == null ? 0d : c.count();
    }

    private static double counter(MeterRegistry r, String name) {
        var c = r.find(name).counter();
        return c == null ? 0d : c.count();
    }

    @Test
    void scriptedRequestsCountedByTypeAndStatus() throws Exception {
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.config(0));
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        OpenLatchClient measured = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).meterRegistry(reg).build();
        OpenLatchClient silent = OpenLatchClient.builder()   // 默认关闭形态：行为一致
                .address("127.0.0.1:" + server.port()).build();
        try {
            measured.connectAsync().get(5, TimeUnit.SECONDS);
            silent.connectAsync().get(5, TimeUnit.SECONDS);

            // 立即式授予（OK）→ 释放（OK）。
            assertThat(silent.newReentrantLock("busy").tryLock()).isTrue();
            OLock q = measured.newReentrantLock("busy");
            CompletableFuture<Boolean> waiting = q.tryLockAsync(10, TimeUnit.SECONDS);
            Thread.sleep(300); // 确保 QUEUED 已落
            silent.newReentrantLock("busy").unlock();
            assertThat(waiting.get(10, TimeUnit.SECONDS)).isTrue(); // 重发终局 OK

            // 空闲 key 立即式拒绝路径不产生（tryLock 空即授予）；改断言直数。
            assertThat(measured.newReentrantLock("idle").tryLock()).isTrue();
            measured.newReentrantLock("idle").unlock();

            assertThat(counter(reg, "LOCK_ACQUIRE", "QUEUED")).isEqualTo(1);
            assertThat(counter(reg, "LOCK_ACQUIRE", "OK"))
                    .as("排队重发 1 + 空 key 授予 1")
                    .isEqualTo(2);
            assertThat(counter(reg, "LOCK_RELEASE", "OK")).isEqualTo(1);
            // 无标签耗时汇总：锁类 4 样本之外 HELLO 建连请求同样计入。
            assertThat(reg.get("openlatch.client.request.duration").timer().count())
                    .as("每个完成请求伴一个耗时样本")
                    .isGreaterThanOrEqualTo(4);
        } finally {
            measured.shutdown();
            silent.shutdown();
            server.stop();
        }
    }

    @Test
    void serverDownCountsReconnectAndLockLostExactlyOnce() throws Exception {
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.fastExpiryConfig(0));
        SimpleMeterRegistry reg = new SimpleMeterRegistry();
        OpenLatchClient holder = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port()).meterRegistry(reg).build();
        CountDownLatch lost = new CountDownLatch(1);
        holder.addLockLostListener((key, cause) -> lost.countDown());
        try {
            holder.connectAsync().get(5, TimeUnit.SECONDS);
            holder.acquireAsync(new AcquireSpec("dying", LockType.REENTRANT,
                    Thread.currentThread().threadId(), 2_000, 0)).get(3, TimeUnit.SECONDS);

            server.stop(); // 停服：断连 → 重连发起；未在窗口内重连成功 → 失锁裁决
            server = null;

            assertThat(lost.await(30, TimeUnit.SECONDS)).as("失锁通知抵达").isTrue();
            Thread.sleep(1_000); // 容忍并发裁决窗口，确认"至多一次"
            assertThat(counter(reg, "openlatch.client.locks.lost.total")).isEqualTo(1);
            assertThat(counter(reg, "openlatch.client.reconnect.total"))
                    .as("断连后至少一次重连发起")
                    .isGreaterThanOrEqualTo(1);
        } finally {
            holder.shutdown();
            if (server != null) {
                server.stop();
            }
        }
    }
}
