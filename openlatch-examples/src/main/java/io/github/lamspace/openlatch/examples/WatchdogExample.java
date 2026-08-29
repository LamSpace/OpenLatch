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

package io.github.lamspace.openlatch.examples;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;

/**
 * 示例 4：看门狗续租与锁丢失回调（详设 §9）。
 *
 * <p>两段演示，服务器以短租约档位启动（默认 2s）：
 * <ol>
 *   <li><b>续租生效</b>：任务持有 6s（三倍租约），看门狗按 lease/3 周期
 *       续租，全程无锁丢失回调；</li>
 *   <li><b>人为停止续租</b>：持有期间直接关停服务器进程，断连使续租
 *       中止，客户端按失锁时刻（上次成功续租 + 生效租约）挂定时裁决，
 *       锁丢失回调在数秒内触发——断连期间续租不计数（半开连接下才走
 *       连续续租超时判定，详设 §6.2/§6.6）；若客户端稍后重连成功，
 *       重连瞬间也会裁决旧会话锁已失效。</li>
 * </ol>
 *
 * <p>运行：{@code mvn -pl openlatch-examples exec:java
 * -Dexec.mainClass=io.github.lamspace.openlatch.examples.WatchdogExample}
 */
public final class WatchdogExample {

    /**
     * 私有构造：入口类。
     */
    private WatchdogExample() {
    }

    /**
     * 入口。
     *
     * @param args 未使用
     * @throws Exception 连接异常
     */
    public static void main(String[] args) throws Exception {
        OpenLatchServer server = ExampleServers.startFastExpiry();
        CountDownLatch lost = new CountDownLatch(1);
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port())
                .requestTimeout(Duration.ofSeconds(1))
                .build()) {
            client.addLockLostListener((key, cause) -> {
                System.out.printf("[lost ] lock '%s' lost: %s%n", key, cause.getMessage());
                lost.countDown();
            });
            client.connectAsync().get(5, TimeUnit.SECONDS);

            OLock job = client.newReentrantLock("watchdog:long-job");
            job.lock();
            System.out.println("[job  ] acquired (lease 2s), working 6s — "
                    + "watchdog renews every ~667ms");
            Thread.sleep(6_000);
            job.unlock();
            System.out.println("[job  ] held 6s > 3x lease without loss -> "
                    + "renewal effective");

            OLock doomed = client.newReentrantLock("watchdog:doomed");
            doomed.lock();
            System.out.println("[doomed] acquired, killing server to stop renewal...");
            server.stop();
            boolean fired = lost.await(15, TimeUnit.SECONDS);
            System.out.println("[doomed] lock-lost callback fired = " + fired
                    + " (expected true)");
        } finally {
            server.stop();
        }
        System.exit(0);
    }
}
