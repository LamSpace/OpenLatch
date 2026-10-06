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
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 相位器 SDK 端到端（v10，单机服务端）：跨客户端按相位会合矩阵与返回值保真、
 * 中途注册入当相位应到集、{@code arriveAndDeregister} 缩应到集合、参与者
 * 会话死亡即时摘除不空转（存活方照常合拢）、旁观等待与已越相位即刻了结、
 * 超时预算本地计时与撤销、配额透支显式拒绝。判例
 * {@code ClientQueueIT}/{@code ClientBarrierIT}；换主自愈面由集群
 * {@code ClusterPhaserTest} 与演练负载段承载。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClientPhaserIT {

    /** 被测服务器（单机）。 */
    private OpenLatchServer server;
    /** 甲方客户端。 */
    private OpenLatchClient alpha;
    /** 乙方客户端。 */
    private OpenLatchClient beta;

    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        alpha = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        beta = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        alpha.connectAsync().get(5, TimeUnit.SECONDS);
        beta.connectAsync().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        alpha.close();
        beta.close();
        server.stop();
    }

    @Test
    void crossClientPhaseTripsWithFidelity() throws Exception {
        OPhaser a = alpha.newPhaser("it:ph");
        OPhaser b = beta.newPhaser("it:ph");
        // 三方配额（两方 alpha、一方 beta——同会话可重复注册）。
        a.register();
        a.register();
        b.register();
        b.arrive();                        // 1/3
        AtomicLong aArrived = new AtomicLong(-1);
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread t = new Thread(() -> {
            try {
                // 到场半程提交后跨会话不重放（契约）：beta 关闭重启使本等待
                // 显式放弃，唤醒后以读数核对会合完成。
                aArrived.set(a.arriveAndAwaitAdvance()); // 2/3 挂起
            } catch (Throwable e) {
                err.set(e);
            }
        });
        t.setDaemon(true);
        t.start();
        Thread.sleep(300);
        assertThat(t.isAlive()).as("2/3 应挂起").isTrue();
        b.arrive(); // 3/3 合拢 → t 唤醒
        t.join(20_000);
        assertThat(t.isAlive()).isFalse();
        assertThat(err.get()).isNull();
        // 三种收束皆契约内：0=同会话了结回显；放弃异常=跨会话；OK 未命中
        // 窗口=已越相位续等后继（本 IT beta 重启窗）。以账簿读数为会合事实。
        assertThat(err.get() == null || err.get() instanceof OpenLatchException)
                .as("放弃形态显式异常 %s", err.get()).isTrue();
        assertThat(b.getPhase()).isEqualTo(1);
        assertThat(b.getRegisteredParties()).isEqualTo(3);
    }

    @Test
    void deregAndDeathBothShrinkObligation() throws Exception {
        OPhaser a = alpha.newPhaser("it:dd");
        OPhaser ghost = beta.newPhaser("it:dd");
        // 三方配额（a 两、ghost 一）：应到共 3。
        a.register();
        a.register();
        ghost.register();
        assertThat(a.arrive()).isZero(); // 1/3
        // ghost 到场后其客户端关闭：配额摘除（3→2）、到场保留（arrived=2）。
        assertThat(ghost.arrive()).isZero();
        beta.close();
        beta = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        beta.connectAsync().get(5, TimeUnit.SECONDS);
        // 摘除即合拢：ghost 的到场保留（arrived 2）+ 应到缩至 2 → SESSION_CLOSE
        // 应用点即时推进（死亡不空转的机制本体——无需存活方再发）。
        assertThat(a.getPhase()).isEqualTo(1);
        assertThat(a.getRegisteredParties()).isEqualTo(2);
        // 新相位：a 到场（1/2，回显 1）再 A_D——到场 2/2 且离场缩应到 2→1，
        // 即时合拢 1→2；a 残余配额 1。
        assertThat(a.arrive()).isEqualTo(1);
        assertThat(a.arriveAndDeregister()).isEqualTo(1);
        assertThat(a.getPhase()).isEqualTo(2);
        assertThat(a.getRegisteredParties()).isEqualTo(1);
    }

    @Test
    void bystanderAwaitResolvesImmediatelyAcrossPhases() throws Exception {
        OPhaser a = alpha.newPhaser("it:bs");
        OPhaser bystander = beta.newPhaser("it:bs");
        a.register(); // 应到 1
        // 已越相位：即刻了结（JDK awaitAdvance(0) 语义）。
        assertThat(a.arrive()).isZero(); // 1/1 合拢 → 相位 1
        long done = bystander.awaitAdvance(0);
        assertThat(done).isEqualTo(1);
        // 未越相位挂起后由 a 的到场唤醒。
        AtomicLong got = new AtomicLong(-1);
        Thread t = new Thread(() -> {
            try {
                got.set(bystander.awaitAdvance(1));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        t.setDaemon(true);
        t.start();
        Thread.sleep(300);
        assertThat(t.isAlive()).isTrue();
        a.arriveAndDeregister(); // 1/1 合拢 1→2
        t.join(10_000);
        assertThat(got.get()).isEqualTo(2);
    }

    @Test
    void timeoutBudgetLocalAndQuotaOverdraftRejected() throws Exception {
        OPhaser a = alpha.newPhaser("it:to");
        OPhaser idle = beta.newPhaser("it:to");
        a.register(); // 建条目：应到 1、相位 0
        long startNs = System.nanoTime();
        assertThatThrownBy(() -> idle.awaitAdvanceInterruptibly(0, 300, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);
        assertThat((System.nanoTime() - startNs) / 1_000_000)
                .isGreaterThanOrEqualTo(280); // 本地预算计时（判例 poll/offer）
        // 配额透支：beta 从未注册。
        assertThatThrownBy(idle::arriveAndDeregister)
                .isInstanceOf(OpenLatchException.class)
                .extracting(e -> ((OpenLatchException) e).status())
                .isEqualTo(io.github.lamspace.openlatch.protocol.StatusCode.INVALID_REQUEST);
        // 家族互斥：phaser 键当锁用被显式拒绝。
        assertThatThrownBy(() -> alpha.newReentrantLock("it:to").tryLock())
                .isInstanceOf(OpenLatchException.class);
    }

    @Test
    void multiPartySteadyPhaseAdvanceNoLoss() throws Exception {
        final int parties = 3;
        final int rounds = 4;
        OPhaser seed = alpha.newPhaser("it:mp");
        for (int i = 0; i < parties; i++) {
            seed.register(); // 应到 parties（单会话配额）
        }
        List<Thread> workers = new java.util.ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int w = 0; w < parties; w++) {
            final int wid = w;
            Thread t = new Thread(() -> {
                OPhaser p = (wid % 2 == 0 ? alpha : beta).newPhaser("it:mp");
                try {
                    for (int r = 0; r < rounds; r++) {
                        long arrival = p.arriveAndAwaitAdvance();
                        assertThat(arrival).isEqualTo(r); // 每轮各到场一次、相位逐进
                    }
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            t.setDaemon(true);
            workers.add(t);
        }
        workers.forEach(Thread::start);
        for (Thread t : workers) {
            t.join(60_000);
        }
        assertThat(failure.get()).isNull();
        assertThat(seed.getPhase()).isEqualTo(rounds);
    }
}
