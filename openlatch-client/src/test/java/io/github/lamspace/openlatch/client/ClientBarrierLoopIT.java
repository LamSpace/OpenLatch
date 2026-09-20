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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 高频世代循环回归（v5 屏障等待-通知-重发闭环时序）：单客户端双线程与
 * 双客户端各一线两形态下，多方连续多世代会合 MUST 全部经推送即时唤醒——
 * 本用例钉死"等待期间保持通知登记"契约（曾因应答后即刻摘登记使全部
 * 通知落空、每世代退化为请求超时兜底重发；Latch 通道同缺陷同修复）。
 * 断言含时界：20 世代双方案总耗时 MUST 远小于两轮请求超时（5s×2），
 * 以证明确由推送唤醒而非兜底重发。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClientBarrierLoopIT {

    @ParameterizedTest(name = "singleClient={0}")
    @ValueSource(booleans = {true, false})
    void loopGenerations(boolean singleClient) throws Exception {
        OpenLatchServer server = ClientTestServers.start(ClientTestServers.config(0));
        String address = "127.0.0.1:" + server.port();
        OpenLatchClient ca = OpenLatchClient.builder().address(address).build();
        OpenLatchClient cb = singleClient ? ca
                : OpenLatchClient.builder().address(address).build();
        ca.connectAsync().get(5, TimeUnit.SECONDS);
        if (!singleClient) {
            cb.connectAsync().get(5, TimeUnit.SECONDS);
        }
        AtomicLong doneA = new AtomicLong();
        AtomicLong doneB = new AtomicLong();
        AtomicReference<Throwable> err = new AtomicReference<>();
        AtomicLong slowA = new AtomicLong();
        AtomicLong slowB = new AtomicLong();
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);
        Runnable ra = () -> {
            OBarrier h = ca.newBarrier("loop" + singleClient, 2);
            ready.countDown();
            try {
                go.await(5, TimeUnit.SECONDS);
                for (int i = 0; i < 20; i++) {
                    long st = System.nanoTime();
                    if (!h.await(10, TimeUnit.SECONDS)) {
                        return;
                    }
                    if ((System.nanoTime() - st) / 1000 > 50_000) {
                        slowA.incrementAndGet();
                    }
                    doneA.incrementAndGet();
                }
            } catch (Throwable t) { err.compareAndSet(null, t); }
        };
        Runnable rb = () -> {
            OBarrier h = cb.newBarrier("loop" + singleClient, 2);
            ready.countDown();
            try {
                go.await(5, TimeUnit.SECONDS);
                for (int i = 0; i < 20; i++) {
                    long st = System.nanoTime();
                    if (!h.await(10, TimeUnit.SECONDS)) {
                        return;
                    }
                    if ((System.nanoTime() - st) / 1000 > 50_000) {
                        slowB.incrementAndGet();
                    }
                    doneB.incrementAndGet();
                }
            } catch (Throwable t) { err.compareAndSet(null, t); }
        };
        Thread t0 = new Thread(ra);
        Thread t1 = new Thread(rb);
        t0.start(); t1.start();
        ready.await(5, TimeUnit.SECONDS);
        go.countDown();
        t0.join(60_000); t1.join(60_000);
        if (!singleClient) { cb.shutdown(); }
        ca.shutdown();
        server.stop();
        assertThat(err.get()).isNull();
        assertThat(doneA.get()).isEqualTo(20);
        assertThat(doneB.get()).isEqualTo(20);
        // 唤醒时界：每世代均应为推送唤醒（毫秒级），不得出现 5s 兜底重发。
        assertThat(slowA.get()).isZero();
        assertThat(slowB.get()).isZero();
    }
}
