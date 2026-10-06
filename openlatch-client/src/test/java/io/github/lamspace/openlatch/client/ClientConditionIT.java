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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 条件变量 SDK 端到端（v9，单机真实服务端）：生产-消费守卫闭环
 * （跨客户端 await/signal，返回时持锁、谓词复查退出）、超时 await 自救
 * 返回 {@code false} 且重入 1 级（单次 unlock 即全释放）、signal 无等待者
 * 的 JDK 对齐无操作回执。
 *
 * <p>单节点无换主窗——服务端"登记先于释放可见"保证下 signal 不丢，
 * 时序断言均为有界轮询形态（沿换主暴露面纪律，见规格 guard loop 义务）。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClientConditionIT {

    /** 被测服务器（单机）。 */
    private OpenLatchServer server;
    /** 消费端（等待方）。 */
    private OpenLatchClient consumer;
    /** 生产端（唤醒方）。 */
    private OpenLatchClient producer;

    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        consumer = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        producer = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        consumer.connectAsync().get(5, TimeUnit.SECONDS);
        producer.connectAsync().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
        producer.close();
        server.stop();
    }

    @Test
    void guardLoopAwakensOnCrossClientSignalAndHoldsOnReturn() throws Exception {
        String key = "it:cond";
        OLock cLock = consumer.newReentrantLock(key);
        OCondition cCond = cLock.newCondition("ready");
        OLock pLock = producer.newReentrantLock(key);
        OCondition pCond = pLock.newCondition("ready");

        AtomicBoolean ready = new AtomicBoolean();
        AtomicBoolean awakened = new AtomicBoolean();
        AtomicReference<Throwable> error = new AtomicReference<>();
        Thread worker = new Thread(() -> {
            try {
                cLock.lock();
                try {
                    // 标准 guard loop 惯用法：谓词 while 复查（虚假唤醒义务面）。
                    long end = System.currentTimeMillis() + 60_000;
                    while (!ready.get() && System.currentTimeMillis() < end) {
                        cCond.await(2, TimeUnit.SECONDS); // 超时形态自救兜底（推荐惯用法）
                    }
                    awakened.set(ready.get());
                    // 返回时持锁且重入 1 级：此刻仍持有（unlock 不抛 IMS）。
                } finally {
                    cLock.unlock();
                }
            } catch (Throwable t) {
                error.set(t);
            }
        });
        worker.setDaemon(true);
        worker.start();

        long deadline = System.currentTimeMillis() + 40_000;
        while (!awakened.get() && System.currentTimeMillis() < deadline) {
            pLock.lock();
            try {
                ready.set(true);
                pCond.signal();
            } finally {
                pLock.unlock();
            }
            Thread.sleep(200);
        }
        worker.join(10_000);

        assertThat(error.get()).isNull();
        assertThat(awakened).isTrue();
        // 全释放后锁可被再次获取（await 重入 1 级算术：单次 unlock 收口）。
        assertThat(consumer.newReentrantLock(key).tryLock()).isTrue();
    }

    @Test
    void timedAwaitWithoutSignalReturnsFalseStillHoldingOneLevel() throws Exception {
        String key = "it:cond-tmo";
        OLock lock = consumer.newReentrantLock(key);
        OCondition cond = lock.newCondition("never");

        lock.lock();
        boolean signalled = cond.await(500, TimeUnit.MILLISECONDS); // 无人 signal → 超时自救
        assertThat(signalled).isFalse();
        // 返回时持锁（LEAVE → 常规重获取收束），且重入计数为 1：
        // 单次 unlock 即全释放，对端立即可取。
        assertThat(lock.isHeldByCurrentThread()).isTrue();
        lock.unlock();
        assertThat(producer.newReentrantLock(key).tryLock()).isTrue();
    }

    @Test
    void signalWithoutWaitersIsNoOpAndReentrantCallsConverge() throws Exception {
        String key = "it:cond-empty";
        OLock lock = consumer.newReentrantLock(key);
        OCondition cond = lock.newCondition("empty");

        lock.lock();
        cond.signal();   // 空集无操作正常返回（JDK 对齐）
        cond.signalAll();
        lock.unlock();

        // 命名寻址跨句柄等价：两处 newCondition 同 name 绑同一等待集。
        OCondition same = consumer.newReentrantLock(key).newCondition("empty");
        lock.lock();
        try {
            same.signal();
        } finally {
            lock.unlock();
        }
    }
}
