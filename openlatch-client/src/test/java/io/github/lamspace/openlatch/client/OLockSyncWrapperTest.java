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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * JUC 风格同步包装语义用例（tasks 5.2–5.5）：对真实服务器验证互斥、
 * 重入计数、非法解锁守卫、持锁查询与限时等待。
 */
class OLockSyncWrapperTest {

    /** 被测服务器。 */
    private OpenLatchServer server;
    /** 客户端 A。 */
    private OpenLatchClient clientA;
    /** 客户端 B。 */
    private OpenLatchClient clientB;

    /**
     * 启动服务器与两个客户端。
     *
     * @throws Exception 建连失败
     */
    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        clientA = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        clientB = OpenLatchClient.builder().address("127.0.0.1:" + server.port())
                .defaultWaitTimeout(Duration.ofMillis(500)).build();
        clientA.connectAsync().get(5, TimeUnit.SECONDS);
        clientB.connectAsync().get(5, TimeUnit.SECONDS);
    }

    /**
     * 关停客户端与服务器。
     */
    @AfterEach
    void tearDown() {
        clientA.shutdown();
        clientB.shutdown();
        server.stop();
    }

    /** 互斥获取与释放。 */
    @Test
    void mutexAcquireAndRelease() throws Exception {
        OLock lockA = clientA.newReentrantLock("mutex");
        OLock lockB = clientB.newReentrantLock("mutex");

        assertThat(lockA.tryLock()).isTrue();
        assertThat(lockB.tryLock()).isFalse();

        lockA.unlock();
        assertThat(lockB.tryLock()).isTrue();
        lockB.unlock();
    }

    /** 重入计数由服务端维护：逐次解锁，计数归零前他人不可获取。 */
    @Test
    void reentrantCountMaintainedServerSide() throws Exception {
        OLock lockA = clientA.newReentrantLock("reentrant");
        OLock lockB = clientB.newReentrantLock("reentrant");

        lockA.lock();
        assertThat(lockA.tryLock()).isTrue();
        assertThat(lockB.tryLock()).isFalse();

        lockA.unlock();
        assertThat(lockB.tryLock()).isFalse();
        lockA.unlock();
        assertThat(lockB.tryLock()).isTrue();
        lockB.unlock();
    }

    /** 非持锁线程解锁抛 IllegalMonitorStateException，且不产生服务端副作用。 */
    @Test
    void unlockByNonHolderThrows() throws Exception {
        OLock lockA = clientA.newReentrantLock("guarded");
        OLock lockB = clientB.newReentrantLock("guarded");
        lockA.lock();

        assertThatThrownBy(lockB::unlock).isInstanceOf(IllegalMonitorStateException.class);
        lockA.unlock();
        // 误解锁未影响锁状态：锁已可正常获取
        assertThat(lockB.tryLock()).isTrue();
        lockB.unlock();
    }

    /** isHeldByCurrentThread 反映当前线程持有状态。 */
    @Test
    void isHeldByCurrentThread() throws Exception {
        OLock lock = clientA.newReentrantLock("held-check");
        assertThat(lock.isHeldByCurrentThread()).isFalse();

        lock.lock();
        assertThat(lock.isHeldByCurrentThread()).isTrue();

        AtomicBoolean otherThreadSeesHeld = new AtomicBoolean();
        Thread other = new Thread(() -> otherThreadSeesHeld.set(lock.isHeldByCurrentThread()));
        other.start();
        other.join(2000);
        assertThat(otherThreadSeesHeld.get()).isFalse();

        lock.unlock();
        assertThat(lock.isHeldByCurrentThread()).isFalse();
    }

    /** 限时等待：等待期内锁被释放则获取成功。 */
    @Test
    void tryLockWithWaitSucceedsWhenReleasedInTime() throws Exception {
        OLock lockA = clientA.newReentrantLock("timed");
        OLock lockB = clientB.newReentrantLock("timed");
        lockA.lock();

        CountDownLatch started = new CountDownLatch(1);
        AtomicBoolean acquired = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            started.countDown();
            try {
                if (lockB.tryLock(3, TimeUnit.SECONDS)) {
                    acquired.set(true);
                    lockB.unlock(); // 解锁须由持锁线程执行
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        started.await(1, TimeUnit.SECONDS);
        Thread.sleep(200);
        lockA.unlock();
        waiter.join(5000);

        assertThat(acquired.get()).isTrue();
    }

    /** 限时等待：到时未授予返回 false（而非抛异常）。 */
    @Test
    void tryLockWithWaitReturnsFalseOnTimeout() throws Exception {
        OLock lockA = clientA.newReentrantLock("timed-out");
        OLock lockB = clientB.newReentrantLock("timed-out");
        lockA.lock();

        try {
            assertThat(lockB.tryLock(300, TimeUnit.MILLISECONDS)).isFalse();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        } finally {
            lockA.unlock();
        }
    }

    /** lock() 受兜底超时约束：到时抛超时异常，不存在无限阻塞。 */
    @Test
    void lockThrowsOnDefaultWaitTimeout() throws Exception {
        OLock lockA = clientA.newReentrantLock("blocked");
        OLock lockB = clientB.newReentrantLock("blocked");
        lockA.lock();

        try {
            assertThatThrownBy(lockB::lock)
                    .isInstanceOf(LockAcquisitionTimeoutException.class);
        } finally {
            lockA.unlock();
        }
    }

    /** 不可重入锁：同持有者再次获取被拒（立即式返回 false）。 */
    @Test
    void simpleLockRejectsSameOwnerReacquire() throws Exception {
        OLock lockA = clientA.newSimpleLock("simple");

        assertThat(lockA.tryLock()).isTrue();
        assertThat(lockA.tryLock()).isFalse();
        lockA.unlock();
        assertThat(lockA.tryLock()).isTrue();
        lockA.unlock();
    }

    /** 读写锁基本互斥：读者并发、写者与读者互斥。 */
    @Test
    void readWriteLockBasicExclusion() throws Exception {
        OReadWriteLock rwA = clientA.newReadWriteLock("rw");
        OReadWriteLock rwB = clientB.newReadWriteLock("rw");

        assertThat(rwA.readLock().tryLock()).isTrue();
        assertThat(rwB.readLock().tryLock()).isTrue();
        assertThat(rwB.writeLock().tryLock()).isFalse();

        rwA.readLock().unlock();
        assertThat(rwB.writeLock().tryLock()).isFalse();
        rwB.readLock().unlock();
        assertThat(rwB.writeLock().tryLock()).isTrue();
        rwB.writeLock().unlock();
    }

    /** lockAsync 授予携带凭据。 */
    @Test
    void lockAsyncCompletesWithGrant() throws Exception {
        OLock lock = clientA.newReentrantLock("async");
        AtomicReference<LockGrant> grant = new AtomicReference<>();
        lock.lockAsync().thenAccept(grant::set).get(3, TimeUnit.SECONDS);
        assertThat(grant.get()).isNotNull();
        assertThat(grant.get().leaseToken()).isNotZero();
        lock.unlock();
    }
}
