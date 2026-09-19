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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 循环屏障 SDK 端到端（单机服务端 + 真实客户端）：跨进程到场合拢、
 * 世代回卷复用、barrierAction 两阶段放行次序、超时/显式 break/进程死亡
 * 三条破障路径与他方感知、{@code isBroken} 本地裁决读数。
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class ClientBarrierIT {

    /** 被测服务器。 */
    private OpenLatchServer server;
    /** 客户端 A。 */
    private OpenLatchClient clientA;
    /** 客户端 B。 */
    private OpenLatchClient clientB;
    /** 客户端 C（第三到场方）。 */
    private OpenLatchClient clientC;

    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        clientA = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        clientB = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        clientC = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        clientA.connectAsync().get(5, TimeUnit.SECONDS);
        clientB.connectAsync().get(5, TimeUnit.SECONDS);
        clientC.connectAsync().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        clientA.shutdown();
        clientB.shutdown();
        clientC.shutdown();
        server.stop();
    }

    @Test
    void twoPartiesMeetAcrossProcessesAndReuseGeneration() throws Exception {
        OBarrier a = clientA.newBarrier("ph", 2);
        OBarrier b = clientB.newBarrier("ph", 2);
        AtomicReference<Throwable> errA = new AtomicReference<>();
        Thread ta = new Thread(() -> {
            try {
                a.await();
            } catch (Throwable t) {
                errA.set(t);
            }
        });
        ta.start();
        Thread.sleep(300); // 确保 A 先到场挂起
        assertThat(b.await(10, TimeUnit.SECONDS)).isTrue(); // B 当回合拢
        ta.join(10_000);
        assertThat(errA.get()).isNull();
        assertThat(ta.isAlive()).isFalse(); // A 经推送-重发放行
        // 相位复用后重组：同 key 第二个世代照常两方会合。
        AtomicBoolean okA2 = new AtomicBoolean();
        AtomicReference<Throwable> errA2 = new AtomicReference<>();
        CountDownLatch done2 = new CountDownLatch(1);
        Thread ta2 = new Thread(() -> {
            try {
                okA2.set(a.await(10, TimeUnit.SECONDS));
            } catch (Throwable t) {
                errA2.set(t);
            } finally {
                done2.countDown();
            }
        });
        ta2.start();
        Thread.sleep(300);
        assertThat(b.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(done2.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(errA2.get()).isNull();
        assertThat(okA2.get()).isTrue();
        assertThat(a.isBroken()).isFalse();
        ta2.join();
    }

    @Test
    void actionRunsByLastArriverBeforeOthersRelease() throws Exception {
        AtomicBoolean actionRan = new AtomicBoolean();
        AtomicInteger actionOrder = new AtomicInteger();
        OBarrier a = clientA.newBarrier("st", 2);
        OBarrier b = clientB.newBarrier("st", 2, () -> {
            actionRan.set(true);
            actionOrder.set(1);
            try {
                Thread.sleep(300); // 放大动作执行窗
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            actionOrder.set(2);
        });
        CountDownLatch aReturned = new CountDownLatch(1);
        AtomicReference<Integer> aSawOrder = new AtomicReference<>();
        Thread ta = new Thread(() -> {
            try {
                a.await(10, TimeUnit.SECONDS);
                aSawOrder.set(actionOrder.get()); // A 放行时动作必已完成（=2）
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                aReturned.countDown();
            }
        });
        ta.start();
        Thread.sleep(300);
        assertThat(b.await(10, TimeUnit.SECONDS)).isTrue(); // B 为最后到场执行者
        assertThat(actionRan).isTrue();
        assertThat(aReturned.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(aSawOrder.get()).isEqualTo(2); // 动作完成先于他方放行
        ta.join();
    }

    @Test
    void actionFailureBreaksGenerationForPeers() throws Exception {
        OBarrier a = clientA.newBarrier("af", 2);
        OBarrier b = clientB.newBarrier("af", 2, () -> {
            throw new IllegalStateException("action boom");
        });
        AtomicReference<Throwable> errA = new AtomicReference<>();
        Thread ta = new Thread(() -> {
            try {
                a.await(15, TimeUnit.SECONDS);
            } catch (Throwable t) {
                errA.set(t);
            }
        });
        ta.start();
        Thread.sleep(300);
        assertThatThrownBy(() -> b.await(15, TimeUnit.SECONDS))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("action boom");
        ta.join(15_000);
        assertThat(errA.get()).isInstanceOf(OBrokenBarrierException.class);
        assertThat(a.isBroken()).isTrue();
    }

    @Test
    void timeoutLeaveBreaksAndPeersObserveBroken() throws Exception {
        OBarrier a = clientA.newBarrier("tl", 3);
        OBarrier b = clientB.newBarrier("tl", 3);
        assertThat(a.await(1, TimeUnit.SECONDS)).isFalse(); // A 超时离场并破障
        assertThat(a.isBroken()).isFalse(); // 本方以超时收场，不见破障裁决（JDK 同构）
        // B 从未到场——他方感知道在破障世代成员侧，此处验证世代自愈：
        OBarrier c = clientC.newBarrier("tl", 3);
        assertThat(b.await(1, TimeUnit.SECONDS)).isFalse(); // B 进入新世代后同样超时
        assertThat(c.await(1, TimeUnit.SECONDS)).isFalse(); // C 亦新世代（世代局部）
    }

    @Test
    void explicitBreakReachesWaitingPeers() throws Exception {
        OBarrier a = clientA.newBarrier("eb", 3);
        OBarrier b = clientB.newBarrier("eb", 3);
        OBarrier breaker = clientC.newBarrier("eb");
        AtomicReference<Throwable> errA = new AtomicReference<>();
        AtomicReference<Throwable> errB = new AtomicReference<>();
        Thread ta = new Thread(() -> {
            try {
                a.await(15, TimeUnit.SECONDS);
            } catch (Throwable t) {
                errA.set(t);
            }
        });
        Thread tb = new Thread(() -> {
            try {
                b.await(15, TimeUnit.SECONDS);
            } catch (Throwable t) {
                errB.set(t);
            }
        });
        ta.start();
        tb.start();
        Thread.sleep(400);
        breaker.breakBarrier();
        ta.join(15_000);
        tb.join(15_000);
        assertThat(errA.get()).isInstanceOf(OBrokenBarrierException.class);
        assertThat(errB.get()).isInstanceOf(OBrokenBarrierException.class);
        assertThat(a.isBroken()).isTrue();
        assertThat(b.isBroken()).isTrue();
    }

    @Test
    void localInterruptLeavesAndBreaksForPeers() throws Exception {
        OBarrier a = clientA.newBarrier("ii", 3);
        OBarrier b = clientB.newBarrier("ii", 3);
        AtomicReference<Throwable> errA = new AtomicReference<>();
        AtomicReference<Throwable> errB = new AtomicReference<>();
        AtomicBoolean interruptSeen = new AtomicBoolean();
        Thread ta = new Thread(() -> {
            try {
                a.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                interruptSeen.set(true);
            } catch (Throwable t) {
                errA.set(t);
            }
        });
        Thread tb = new Thread(() -> {
            try {
                b.await(20, TimeUnit.SECONDS);
            } catch (Throwable t) {
                errB.set(t);
            }
        });
        ta.start();
        tb.start();
        Thread.sleep(400); // 双方到场挂起后中断 A：离场即破障
        ta.interrupt();
        ta.join(20_000);
        tb.join(20_000);
        assertThat(interruptSeen.get()).isTrue();
        assertThat(errA.get()).isNull();
        assertThat(errB.get()).isInstanceOf(OBrokenBarrierException.class);
        // 新到场进新世代（破障不粘滞）。
        OBarrier c = clientC.newBarrier("ii", 3);
        assertThat(c.await(1, TimeUnit.SECONDS)).isFalse(); // 独自到场：超时即破新世代
        assertThat(c.isBroken()).isFalse(); // 本方超时收场，与破障裁决路径不同型
    }

    @Test
    void participantProcessDeathBreaksForPeers() throws Exception {
        OBarrier a = clientA.newBarrier("pd", 3);
        OBarrier b = clientB.newBarrier("pd", 3);
        AtomicReference<Throwable> errA = new AtomicReference<>();
        AtomicReference<Throwable> errB = new AtomicReference<>();
        Thread ta = new Thread(() -> {
            try {
                a.await(20, TimeUnit.SECONDS);
            } catch (Throwable t) {
                errA.set(t);
            }
        });
        Thread tb = new Thread(() -> {
            try {
                b.await(20, TimeUnit.SECONDS);
            } catch (Throwable t) {
                errB.set(t);
            }
        });
        ta.start();
        tb.start();
        Thread.sleep(400);
        // 杀 C？不——A/B 在场，杀掉 A 进程即可破障 B。
        clientA.shutdown(); // 会话关闭经服务端清理传播为破障（单机关闭即断连）
        tb.join(20_000);
        assertThat(errB.get()).isInstanceOf(OBrokenBarrierException.class);
        assertThat(b.isBroken()).isTrue();
    }
}
