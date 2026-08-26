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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 看门狗端到端（§10.3，task 8.4）：3s 租约持有 10s，期间续租生效、
 * 锁不过期；释放后可被其他客户端获取。
 */
class ClientWatchdogIT {

    /** 短租约（毫秒）：端到端用例的授予租约。 */
    private static final long SHORT_LEASE_MS = 3000;
    /** 持有时长（毫秒）：远超租约期，验证续租生效。 */
    private static final long HOLD_MS = 10_000;

    /** 被测服务器。 */
    private OpenLatchServer server;
    /** 持有者客户端。 */
    private OpenLatchClient holder;
    /** 竞争者客户端。 */
    private OpenLatchClient rival;

    /**
     * 启动服务器与两个客户端。
     *
     * @throws Exception 建连失败
     */
    @BeforeEach
    void setUp() throws Exception {
        // 3s 租约在默认钳制区间 [1s, 1h] 内，使用默认服务器配置即可
        server = ClientTestServers.start(ClientTestServers.config(0));
        holder = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        rival = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        holder.connectAsync().get(5, TimeUnit.SECONDS);
        rival.connectAsync().get(5, TimeUnit.SECONDS);
    }

    /**
     * 关停资源。
     */
    @AfterEach
    void tearDown() {
        holder.shutdown();
        rival.shutdown();
        server.stop();
    }

    /** 3s 租约持有 10s：期间锁不过期，释放后可被获取。 */
    @Test
    void watchdogKeepsLockAliveBeyondLease() throws Exception {
        String key = "watchdog-e2e";
        long threadId = Thread.currentThread().threadId();

        LockGrant grant = holder.acquireAsync(
                new AcquireSpec(key, LockType.REENTRANT, threadId, SHORT_LEASE_MS, 0))
                .get(3, TimeUnit.SECONDS);
        assertThat(grant.grantedLeaseMs()).isEqualTo(SHORT_LEASE_MS);

        OLock rivalLock = rival.newReentrantLock(key);
        // 多个时间点抽查：每个点都已越过至少一个原始租约期，锁仍被持有
        for (long elapsed : new long[]{4000, 7000, 9500}) {
            Thread.sleep(elapsed == 4000 ? 4000 : elapsed - previousOf(elapsed));
            assertThat(rivalLock.tryLock())
                    .as("lock must still be held at %dms", elapsed)
                    .isFalse();
        }

        // 释放后竞争者可获取
        holder.releaseAsync(key, grant.leaseToken(), threadId).get(3, TimeUnit.SECONDS);
        assertThat(rivalLock.tryLock()).isTrue();
        rivalLock.unlock();
    }

    /**
     * 返回检查点序列中的前一个时间点。
     *
     * @param elapsed 当前检查点
     * @return 前一检查点（首项为 0）
     */
    private static long previousOf(long elapsed) {
        return elapsed == 7000 ? 4000 : elapsed == 9500 ? 7000 : 0;
    }
}
