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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import io.github.lamspace.openlatch.client.OLock;
import io.github.lamspace.openlatch.client.OReadWriteLock;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;

/**
 * 示例 3：读写锁并发矩阵（详设 §9）——读读共享、读写互斥。
 *
 * <p>运行：{@code mvn -pl openlatch-examples exec:java
 * -Dexec.mainClass=io.github.lamspace.openlatch.examples.ReadWriteExample}
 */
public final class ReadWriteExample {

    /**
     * 私有构造：入口类。
     */
    private ReadWriteExample() {
    }

    /**
     * 入口：双读者屏障汇合（读读共享）→ 写者持有时读者被拒 →
     * 读者持有时写者被拒。
     *
     * @param args 未使用
     * @throws Exception 连接或同步异常
     */
    public static void main(String[] args) throws Exception {
        OpenLatchServer server = ExampleServers.startDefault();
        try (OpenLatchClient client = OpenLatchClient.builder()
                .address("127.0.0.1:" + server.port())
                .build()) {
            client.connectAsync().get(5, TimeUnit.SECONDS);
            OReadWriteLock rw = client.newReadWriteLock("rw:doc");
            OLock read = rw.readLock();
            OLock write = rw.writeLock();

            CyclicBarrier readersMeet = new CyclicBarrier(2);
            Thread r1 = new Thread(() -> holdReadWithBarrier(read, readersMeet), "reader-1");
            Thread r2 = new Thread(() -> holdReadWithBarrier(read, readersMeet), "reader-2");
            r1.start();
            r2.start();
            r1.join();
            r2.join();
            System.out.println("[rw   ] two readers met at the barrier while both holding "
                    + "READ -> read-read sharing works");

            Thread writer = new Thread(() -> {
                try {
                    write.lock();
                    boolean readerGot = read.tryLock();
                    System.out.println("[writer] holds WRITE, reader tryLock -> " + readerGot
                            + " (expected false: write excludes read)");
                    if (readerGot) {
                        read.unlock();
                    }
                    TimeUnit.MILLISECONDS.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    write.unlock();
                }
            }, "writer");
            writer.start();
            writer.join();

            read.lock();
            boolean writerGot = write.tryLock();
            System.out.println("[read ] holds READ, writer tryLock -> " + writerGot
                    + " (expected false: read excludes write)");
            if (writerGot) {
                write.unlock();
            }
            read.unlock();
            System.out.println("[done ] ReadWriteExample finished");
        } finally {
            server.stop();
        }
        System.exit(0);
    }

    /**
     * 读者体：获取读锁，在屏障处与其他读者汇合（证明共享持有），
     * 短暂停留后释放。屏障超时即说明读读并未共享。
     *
     * @param read 读锁句柄
     * @param gate 双读者屏障
     */
    private static void holdReadWithBarrier(OLock read, CyclicBarrier gate) {
        try {
            read.lock();
            try {
                gate.await(3, TimeUnit.SECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                System.out.println("[reader] barrier timed out -> read-read NOT shared");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.out.println("[reader] failed: " + e);
        } finally {
            read.unlock();
        }
    }
}
