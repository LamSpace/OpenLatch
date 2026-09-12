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

import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LatchAwaitRequest;
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
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * CountDownLatch 客户端端到端（单机档）：初始化定型、跨客户端
 * 倒计数放行（等待-通知-重发闭环）、归零立即通过、限时等待超时、
 * 一次性无重置、await 无续租流量、断连等待者摘除后屏障照常放行。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class ClientLatchIT {

    /** 被测服务器。 */
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
    void countDownReleasesBlockedAwaitCrossClient() throws Exception {
        OCountDownLatch creator = clientA.newCountDownLatch("l-rel", 2);
        assertThat(creator.init()).isEqualTo(2);
        OCountDownLatch waiter = clientB.newCountDownLatch("l-rel", 2);

        boolean[] passed = new boolean[1];
        Thread awaiter = new Thread(() -> {
            try {
                passed[0] = waiter.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaiter.start();
        Thread.sleep(300); // await 完成排队挂起
        assertThat(passed[0]).isFalse();

        assertThat(creator.countDown()).isEqualTo(1); // 尚有剩余
        Thread.sleep(100);
        assertThat(passed[0]).as("未归零不得放行").isFalse();
        creator.countDown();                        // 归零：全体放行
        awaiter.join(10_000);
        assertThat(passed[0]).isTrue();
    }

    @Test
    void awaitAfterZeroPassesImmediatelyAndCountDownIsNoOp() throws Exception {
        OCountDownLatch l = clientA.newCountDownLatch("l-once", 1);
        assertThat(l.countDown()).isZero();                    // 直接扣减归零
        assertThat(l.await(100, TimeUnit.MILLISECONDS)).isTrue(); // 已归零：立即通过
        assertThat(l.countDown()).isZero();                    // 一次性：归零后无操作
        assertThat(l.init()).isZero();                         // 再定型：断言相符、剩余仍 0
    }

    @Test
    void timedAwaitTimesOutThenPassesAfterCountDown() throws Exception {
        OCountDownLatch l = clientA.newCountDownLatch("l-timeout", 1);
        l.init();
        assertThat(l.await(600, TimeUnit.MILLISECONDS)).isFalse(); // 限时未归零
        assertThat(l.countDown(1)).isZero();
        assertThat(l.await(5, TimeUnit.SECONDS)).isTrue();         // 已归零立即通过
    }

    @Test
    void pureJoinOnAbsentLatchIsRejected() {
        OCountDownLatch ghost = clientB.newCountDownLatch("l-absent");
        assertThatThrownBy(() -> ghost.countDown())
                .isInstanceOf(OpenLatchException.class);
    }

    @Test
    void awaiterDisconnectDoesNotBreakBarrier() throws Exception {
        String key = "l-disc";
        OCountDownLatch creator = clientA.newCountDownLatch(key, 1);
        creator.init();

        // 幽灵 awaiter：裸 socket 建立 v3 会话并 await（挂起），随后强制断开。
        Socket ghost = new Socket("127.0.0.1", server.port());
        DataOutputStream out = new DataOutputStream(ghost.getOutputStream());
        DataInputStream in = new DataInputStream(ghost.getInputStream());
        write(out, Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.HELLO)
                .setRequestId(1).setHelloRequest(HelloRequest.newBuilder()
                        .setClientProtocolVersion(3)).build());
        assertThat(read(in).getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
        write(out, Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LATCH_AWAIT)
                .setRequestId(2).setLatchAwaitRequest(LatchAwaitRequest.newBuilder()
                        .setKey(key).setTotal(0)).build());
        assertThat(read(in).getLatchAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
        ghost.close(); // 断连：awaiter 随会话摘除

        // 屏障不受影响：归零后创建者 await 即过。
        assertThat(creator.countDown(1)).isZero();
        assertThat(creator.await(5, TimeUnit.SECONDS)).isTrue();
    }

    /** 写帧。 */
    private static void write(DataOutputStream out, Envelope msg) throws Exception {
        byte[] body = msg.toByteArray();
        out.writeInt(body.length);
        out.write(body);
        out.flush();
    }

    /** 读帧。 */
    private static Envelope read(DataInputStream in) throws Exception {
        byte[] body = new byte[in.readInt()];
        in.readFully(body);
        return Envelope.parseFrom(body);
    }
}
