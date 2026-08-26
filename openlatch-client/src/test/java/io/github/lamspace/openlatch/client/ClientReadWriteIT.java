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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 读写并发组合矩阵（§10.3，task 8.3）：读者并发、写者互斥、严格 FIFO
 * 杜绝写者饥饿与读者越位。
 */
class ClientReadWriteIT {

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
        clientB = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        clientA.connectAsync().get(5, TimeUnit.SECONDS);
        clientB.connectAsync().get(5, TimeUnit.SECONDS);
    }

    /**
     * 关停资源。
     */
    @AfterEach
    void tearDown() {
        clientA.shutdown();
        clientB.shutdown();
        server.stop();
    }

    /** 多读者并发持有；写者被全部读者互斥。 */
    @Test
    void multipleReadersConcurrentWriterExcluded() throws Exception {
        String key = "rw-matrix";
        OLock readerA = clientA.newReadWriteLock(key).readLock();
        OLock readerB = clientB.newReadWriteLock(key).readLock();
        OLock writerB = clientB.newReadWriteLock(key).writeLock();

        assertThat(readerA.tryLock()).isTrue();
        assertThat(readerB.tryLock()).isTrue();
        assertThat(writerB.tryLock()).isFalse();

        readerA.unlock();
        assertThat(writerB.tryLock()).isFalse(); // 仍有读者
        readerB.unlock();
        assertThat(writerB.tryLock()).isTrue();  // 读者全部释放
        writerB.unlock();
    }

    /** 写者持有时：新读者与写者都排队/拒绝。 */
    @Test
    void writerHoldingBlocksReadersAndWriters() {
        String key = "rw-writer-first";
        OLock writerA = clientA.newReadWriteLock(key).writeLock();
        OLock readerB = clientB.newReadWriteLock(key).readLock();
        OLock writerB = clientB.newReadWriteLock(key).writeLock();

        assertThat(writerA.tryLock()).isTrue();
        assertThat(readerB.tryLock()).isFalse();
        assertThat(writerB.tryLock()).isFalse();
        writerA.unlock();
    }

    /** 严格 FIFO：写者排队后到达的读者不得越过写者。 */
    @Test
    void readersDoNotOvertakeQueuedWriter() throws Exception {
        String key = "rw-fifo";
        OLock readerA = clientA.newReadWriteLock(key).readLock();
        OLock writerB = clientB.newReadWriteLock(key).writeLock();
        OLock readerB = clientB.newReadWriteLock(key).readLock();

        assertThat(readerA.tryLock()).isTrue();

        // 写者限时等待入队
        CountDownLatch writerStarted = new CountDownLatch(1);
        AtomicInteger writerResult = new AtomicInteger(-1);
        Thread writer = new Thread(() -> {
            writerStarted.countDown();
            try {
                if (writerB.tryLock(10, TimeUnit.SECONDS)) {
                    writerResult.set(1);
                    writerB.unlock(); // 解锁须由持锁线程执行
                } else {
                    writerResult.set(0);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        writer.start();
        writerStarted.await(2, TimeUnit.SECONDS);
        Thread.sleep(300); // 确保写者已排队

        // 写者之后的读者立即式获取必须被拒（不得越位）
        assertThat(readerB.tryLock()).isFalse();

        readerA.unlock();          // 放行队首写者
        writer.join(12_000);
        assertThat(writerResult.get()).isEqualTo(1);

        // 写者释放后读者可获取
        assertThat(readerB.tryLock()).isTrue();
        readerB.unlock();
    }
}
