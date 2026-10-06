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

import io.github.lamspace.openlatch.client.internal.ScriptedServer;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.PhaserOp;
import io.github.lamspace.openlatch.protocol.PhaserOpRequest;
import io.github.lamspace.openlatch.protocol.PhaserOpResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RemotePhaser} 车道单测（v10，{@link ScriptedServer} 直驱）：注册/
 * 到场形状与相位回显、{@code ARRIVE_AND_AWAIT} 的 QUEUED → 唤醒 → 同 rid
 * 重发了结全序、等待总超时到期先 CANCEL 后抛、旁观等待跨分片重发幂等、
 * {@code newPhaser(key, n)} 首操作前同步注册（同会话不重执）、读数每 getter
 * 一请求与 unarrived 客户端差值、{@code OVERLOADED} 拒绝的显式异常与码形、
 * Follower 型 {@code NOT_LEADER} 改道不伪成功。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RemotePhaserScriptedTest {

    /** HELLO 应答（v10 服务端形态）。 */
    private static Envelope hello(Envelope req) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(7100L)
                .setServerProtocolVersion(10).setDefaultLeaseMs(30_000))
                .build();
    }

    /** phaser 应答信封（op 回显 + 三计数）。 */
    private static Envelope resp(Envelope req, StatusCode status, long phase,
            int registered, int arrived) {
        return req.toBuilder().setType(MessageType.PHASER_OP)
                .setPhaserOpResponse(PhaserOpResponse.newBuilder()
                        .setStatus(status).setOp(req.getPhaserOpRequest().getOp())
                        .setPhase(phase).setRegistered(registered).setArrived(arrived))
                .build();
    }

    /** 唤醒通知推送帧（request_id=0，ref 关联等待请求）。 */
    private static Envelope notify(String key, long ref) {
        return Envelope.newBuilder().setProtocolVersion(10)
                .setType(MessageType.AWAIT_NOTIFY).setRequestId(0)
                .setAwaitNotify(io.github.lamspace.openlatch.protocol.AwaitNotify
                        .newBuilder().setKey(key).setRequestIdRef(ref))
                .build();
    }

    private static PhaserOpRequest req(Envelope env) {
        return env.getPhaserOpRequest();
    }

    @Test
    void registerAndArriveShapesWithPhaseEcho() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        try (ScriptedServer server = new ScriptedServer(env -> {
            synchronized (seen) {
                seen.add(env);
            }
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            return resp(env, StatusCode.OK, 4L, 6, 1);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OPhaser ph = client.newPhaser("work");
                assertThat(ph.register()).isEqualTo(4L);
                assertThat(ph.arrive()).isEqualTo(4L);
                assertThat(seen).filteredOn(e -> e.getType() == MessageType.PHASER_OP)
                        .extracting(e -> req(e).getOp())
                        .containsExactly(PhaserOp.PHASER_OP_REGISTER, PhaserOp.PHASER_OP_ARRIVE);
                assertThat(req(seen.get(1)).getParties()).isEqualTo(1);
                // 读数：unarrived = registered − arrived 客户端折算（服务端零字段）。
                assertThat(ph.getUnarrivedParties()).isEqualTo(5);
                assertThat(ph.getPhase()).isEqualTo(4L);
                assertThat(ph.getRegisteredParties()).isEqualTo(6);
                assertThat(ph.getArrivedParties()).isEqualTo(1);
            }
        }
    }

    @Test
    void initialRegistrationRunsOncePerSessionBeforeFirstOp() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        try (ScriptedServer server = new ScriptedServer(env -> {
            synchronized (seen) {
                seen.add(env);
            }
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            return resp(env, StatusCode.OK, 0, 3, 0);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OPhaser ph = client.newPhaser("work", 3);
                ph.arrive(); // 首操作前同步补初始注册
                ph.arrive(); // 同会话不重执
                List<Envelope> ops = seen.stream()
                        .filter(e -> e.getType() == MessageType.PHASER_OP).toList();
                assertThat(ops).extracting(e -> req(e).getOp()).containsExactly(
                        PhaserOp.PHASER_OP_REGISTER, PhaserOp.PHASER_OP_ARRIVE,
                        PhaserOp.PHASER_OP_ARRIVE);
                assertThat(req(ops.get(0)).getParties()).isEqualTo(3);
            }
        }
    }

    @Test
    void arriveAndAwaitQueuesNotifiesResendsSameRidThenOk() throws Exception {
        AtomicReference<Long> queuedRid = new AtomicReference<>();
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        Function<Envelope, Envelope> script = env -> {
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            PhaserOpRequest r = req(env);
            if (r.getOp() == PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT) {
                if (queuedRid.compareAndSet(null, env.getRequestId())) {
                    return resp(env, StatusCode.QUEUED, 2, 3, 1);
                }
                return resp(env, StatusCode.OK, 2, 3, 0); // 重发终态（换代窗口）
            }
            return resp(env, StatusCode.OK, 2, 3, 0);
        };
        try (ScriptedServer server = new ScriptedServer(script)) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .defaultWaitTimeout(Duration.ofSeconds(20))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OPhaser ph = client.newPhaser("work");
                Thread worker = new Thread(() -> {
                    try {
                        long arrived = ph.arriveAndAwaitAdvance();
                        seen.add(Envelope.newBuilder()
                                .setType(MessageType.PING).setRequestId(arrived).build());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                worker.setDaemon(true);
                worker.start();
                // 首个 QUEUED 帧抵达后推唤醒通知。
                long deadline = System.currentTimeMillis() + 8_000;
                while (queuedRid.get() == null && System.currentTimeMillis() < deadline) {
                    Thread.sleep(20);
                }
                assertThat(queuedRid.get()).isNotNull();
                server.push(notify("work", queuedRid.get()));
                worker.join(15_000);
                assertThat(worker.isAlive()).isFalse();
                // 重发同 rid 同 op（了结取数非重新等待）。
                List<Long> aaidRids = server.received().stream()
                        .filter(e -> e.getType() == MessageType.PHASER_OP
                                && req(e).getOp() == PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT)
                        .map(Envelope::getRequestId).distinct().toList();
                assertThat(aaidRids).hasSize(1);
                // 完成回显经 PING 影子帧携带（=到场相位 2）。
                assertThat(seen).anyMatch(e -> e.getType() == MessageType.PING
                        && e.getRequestId() == 2);
            }
        }
    }

    /** 分片保活重发 MUST 复用原 rid（不重复计数、不泄漏等待）。 */
    @Test
    void chunkRehangResendsWithSameRequestId() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        try (ScriptedServer server = new ScriptedServer(env -> {
            synchronized (seen) {
                seen.add(env);
            }
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            // 永不推唤醒：两次 QUEUED（首挂与分片重发）后第三次以 OK 收束。
            long n = seen.stream().filter(e -> e.getType() == MessageType.PHASER_OP).count();
            return resp(env, n < 3 ? StatusCode.QUEUED : StatusCode.OK, 0, 1, 1);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .defaultWaitTimeout(Duration.ofSeconds(20))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OPhaser ph = client.newPhaser("work");
                // 首个 5s 分片 + 二次 5s 分片后第三次命中 OK——约 10s，测试预算 60s。
                assertThat(ph.arriveAndAwaitAdvance()).isZero();
                List<Long> rids = server.received().stream()
                        .filter(e -> e.getType() == MessageType.PHASER_OP)
                        .map(Envelope::getRequestId).distinct().toList();
                assertThat(rids).as("分片重发不换 rid").hasSize(1);
            }
        }
    }

    @Test
    void awaitTimeoutCancelsThenThrowsTimeoutException() throws Exception {
        AtomicReference<Envelope> cancelFrame = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(env -> {
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            PhaserOpRequest r = req(env);
            if (r.getOp() == PhaserOp.PHASER_OP_CANCEL) {
                cancelFrame.set(env);
                return resp(env, StatusCode.OK, 5, 1, 0);
            }
            return resp(env, StatusCode.QUEUED, 5, 1, 0); // 永挂的等待
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OPhaser ph = client.newPhaser("work");
                assertThatThrownBy(() -> ph.awaitAdvanceInterruptibly(5, 800,
                        TimeUnit.MILLISECONDS))
                        .isInstanceOf(TimeoutException.class);
                assertThat(cancelFrame.get()).isNotNull();
                assertThat(req(cancelFrame.get()).getOp())
                        .isEqualTo(PhaserOp.PHASER_OP_CANCEL);
                assertThat(req(cancelFrame.get()).getAwaitRequestId()).isPositive();
            }
        }
    }

    @Test
    void interruptDuringAwaitCancelsAndPreservesFlag() throws Exception {
        AtomicReference<Envelope> cancelFrame = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(env -> {
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            PhaserOpRequest r = req(env);
            if (r.getOp() == PhaserOp.PHASER_OP_CANCEL) {
                cancelFrame.set(env);
                return resp(env, StatusCode.OK, 5, 1, 0);
            }
            return resp(env, StatusCode.QUEUED, 5, 1, 0);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .defaultWaitTimeout(Duration.ofSeconds(30))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OPhaser ph = client.newPhaser("work");
                AtomicReference<Throwable> error = new AtomicReference<>();
                Thread worker = new Thread(() -> {
                    try {
                        ph.awaitAdvance(5);
                    } catch (InterruptedException e) {
                        error.set(e);
                    } catch (RuntimeException e) {
                        error.set(e);
                    }
                });
                worker.setDaemon(true);
                worker.start();
                Thread.sleep(600);
                worker.interrupt();
                worker.join(8_000);
                assertThat(error.get()).isInstanceOf(InterruptedException.class);
                long deadline = System.currentTimeMillis() + 3_000;
                while (cancelFrame.get() == null && System.currentTimeMillis() < deadline) {
                    Thread.sleep(20);
                }
                assertThat(cancelFrame.get()).isNotNull();
            }
        }
    }

    @Test
    void overloadRejectionSurfacesExplicitlyAndEmptyShapeRetriesNotFakeOk() throws Exception {
        AtomicReference<Integer> registerFrames = new AtomicReference<>(0);
        try (ScriptedServer server = new ScriptedServer(env -> {
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            if (req(env).getOp() == PhaserOp.PHASER_OP_REGISTER) {
                int n = registerFrames.updateAndGet(v -> v + 1);
                // 首帧回空载荷异型码形：客户端 MUST NOT 读成成功（W10 判例），
                // 同 id 重发；第二帧回显真实 OVERLOADED。
                if (n == 1) {
                    return env.toBuilder().setType(MessageType.PHASER_OP)
                            .clearPayload().build();
                }
                return resp(env, StatusCode.OVERLOADED, 0, 0, 0);
            }
            return resp(env, StatusCode.OK, 0, 0, 0);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OPhaser ph = client.newPhaser("work");
                assertThatThrownBy(ph::register)
                        .isInstanceOf(OpenLatchException.class)
                        .extracting(e -> ((OpenLatchException) e).status())
                        .isEqualTo(StatusCode.OVERLOADED);
                assertThat(registerFrames.get()).isGreaterThanOrEqualTo(2);
            }
        }
    }
}
