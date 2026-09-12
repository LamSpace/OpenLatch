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

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Semaphore 客户端端到端（单机档）：许可计数与互斥、等待-通知-重发
 * 闭环（阻塞 acquire 经 AWAIT_NOTIFY 获授）、重入累加与对称释放、断连归还
 * 许可不泄漏（裸 socket 会话强制断开，服务端会话清理生效）。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ClientSemaphoreIT {

    /** 被测服务器（单机）。 */
    private OpenLatchServer server;
    /** 客户端 A。 */
    private OpenLatchClient clientA;
    /** 客户端 B。 */
    private OpenLatchClient clientB;

    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        clientA = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        clientB = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        clientA.connectAsync().get(5, TimeUnit.SECONDS);
        clientB.connectAsync().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        clientA.shutdown();
        clientB.shutdown();
        server.stop();
    }

    @Test
    void countingAndMutualExclusion() throws Exception {
        OSemaphore sem = clientA.newSemaphore("sem-count", 2);
        assertThat(sem.tryAcquire()).isTrue();
        assertThat(sem.tryAcquire(1)).isTrue();
        assertThat(sem.tryAcquire()).isFalse();       // 池尽
        assertThat(sem.tryAcquire(2)).isFalse();      // 需 2 无余量，立即式被拒
        sem.release();
        assertThat(sem.tryAcquire(2)).isFalse();      // 仅余 1：2 许可请求不满足
        assertThat(sem.tryAcquire()).isTrue();        // 回到持满
        sem.release(2);
    }

    @Test
    void blockedAcquireGrantsViaNotifyResend() throws Exception {
        OSemaphore a = clientA.newSemaphore("sem-block", 1);
        a.acquire(); // 池尽

        CountDownLatch started = new CountDownLatch(1);
        boolean[] granted = new boolean[1];
        Thread waiter = new Thread(() -> {
            started.countDown();
            OSemaphore semB = clientB.newSemaphore("sem-block", 1);
            try {
                granted[0] = semB.tryAcquire(10, TimeUnit.SECONDS);
                if (granted[0]) {
                    semB.release(); // 归还留在获授线程（归属 = 线程）
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        waiter.start();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(300); // 让 B 完成排队（QUEUED 挂起）
        assertThat(granted[0]).isFalse(); // 尚未获授（无轮询放大）

        a.release(); // 归还 → 通知 B → 客户端重发获授
        waiter.join(10_000);
        assertThat(granted[0]).isTrue();
    }

    @Test
    void reentrantAccumulatesAndReleasesSymmetrically() throws Exception {
        OSemaphore a = clientA.newSemaphore("sem-reentry", 4);
        a.acquire(2);
        a.acquire(1); // 同线程重入累加至 3
        assertThat(clientB.newSemaphore("sem-reentry", 4).tryAcquire(2)).isFalse(); // 仅余 1
        a.release(1);
        a.release(2);
        assertThat(clientB.newSemaphore("sem-reentry", 4).tryAcquire(4)).isTrue(); // 全部归还
        clientB.newSemaphore("sem-reentry", 4).release(4);
    }

    @Test
    void disconnectReturnsPermitsWithoutLeak() throws Exception {
        String key = "sem-leak";
        try (Socket ghost = new Socket("127.0.0.1", server.port())) {
            ghost.setTcpNoDelay(true);
            DataOutputStream out = new DataOutputStream(ghost.getOutputStream());
            DataInputStream in = new DataInputStream(ghost.getInputStream());

            hello(out, in, 1);
            // 幽灵会话建条目并取尽 3 个许可。
            acquire(out, 2, key, 3, 3);
            Envelope g = read(in);
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            // 强制断开（无释放）：服务端会话清理归还全部许可。
            ghost.close();
        }
        OSemaphore sem = clientA.newSemaphore(key, 3);
        // 断连即恢复可用（无需等租约）；permits 在前，避免落入 tryAcquire(timeout) 重载。
        assertThat(sem.tryAcquire(2, 2, TimeUnit.SECONDS)).isTrue();
        sem.release(2);
    }

    /** 裸 socket 完成 v3 握手。 */
    private static void hello(DataOutputStream out, DataInputStream in, long rid) throws Exception {
        Envelope msg = Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.HELLO)
                .setRequestId(rid)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(3))
                .build();
        write(out, msg);
        Envelope resp = read(in);
        assertThat(resp.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
    }

    /** 裸 socket 发送 Semaphore 获取请求。 */
    private static void acquire(DataOutputStream out, long rid, String key,
            int permits, int total) throws Exception {
        write(out, Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(key).setLockType(LockType.LOCK_TYPE_SEMAPHORE)
                        .setThreadId(1).setWaitMs(-1)
                        .setPermits(permits).setPermitsTotal(total))
                .build());
    }

    /** 4 字节大端长度前缀写帧。 */
    private static void write(DataOutputStream out, Envelope msg) throws Exception {
        byte[] body = msg.toByteArray();
        out.writeInt(body.length);
        out.write(body);
        out.flush();
    }

    /** 读一帧。 */
    private static Envelope read(DataInputStream in) throws Exception {
        int len = in.readInt();
        byte[] body = new byte[len];
        in.readFully(body);
        return Envelope.parseFrom(body);
    }
}
