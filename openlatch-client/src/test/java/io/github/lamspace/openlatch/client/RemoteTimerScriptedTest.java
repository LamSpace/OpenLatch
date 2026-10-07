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
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TimerOp;
import io.github.lamspace.openlatch.protocol.TimerOpRequest;
import io.github.lamspace.openlatch.protocol.TimerOpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RemoteTimer} 车道单测（v11，{@link ScriptedServer} 直驱）：装载
 * 形状与代次回显（delay presence 经 {@code hasDelayMs} 承载）、{@code AWAIT}
 * 的 QUEUED → 唤醒 → 同 rid 重发以 {@code OK/marked} 了结全序、撤销唤醒的
 * {@code DENIED} 终态映射为异常（与超时 false 两形态不混读）、等待超时到期
 * 先 CANCEL（携被撤 rid）后抛、读数 getter 的查询形态与 remaining 本地差值、
 * {@code OVERLOADED}/{@code INVALID_REQUEST} 拒绝的显式异常与码形（默认实例
 * 伪成功防线，W10 判例）。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RemoteTimerScriptedTest {

    /** HELLO 应答（v11 服务端形态）。 */
    private static Envelope hello(Envelope req) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(7100L)
                .setServerProtocolVersion(11).setDefaultLeaseMs(30_000))
                .build();
    }

    /** timer 应答信封（op 回显 + 三元组 + marked）。 */
    private static Envelope resp(Envelope req, StatusCode status, long generation,
            boolean armed, long fireAtMs, boolean marked) {
        return req.toBuilder().setType(MessageType.TIMER_OP)
                .setTimerOpResponse(TimerOpResponse.newBuilder()
                        .setStatus(status).setOp(req.getTimerOpRequest().getOp())
                        .setGeneration(generation).setArmed(armed)
                        .setFireAtMs(fireAtMs).setMarked(marked))
                .build();
    }

    /** 唤醒通知推送帧（request_id=0，ref 关联等待请求）。 */
    private static Envelope notify(String key, long ref) {
        return Envelope.newBuilder().setProtocolVersion(11)
                .setType(MessageType.AWAIT_NOTIFY).setRequestId(0)
                .setAwaitNotify(io.github.lamspace.openlatch.protocol.AwaitNotify
                        .newBuilder().setKey(key).setRequestIdRef(ref))
                .build();
    }

    private static TimerOpRequest req(Envelope env) {
        return env.getTimerOpRequest();
    }

    @Test
    void scheduleShapeWithDelayPresenceAndGenerationEcho() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        try (ScriptedServer server = new ScriptedServer(env -> {
            synchronized (seen) {
                seen.add(env);
            }
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            return resp(env, StatusCode.OK, 7L, true, 123_456L, false);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OTimer t = client.newTimer("alarm");
                assertThat(t.schedule(3, TimeUnit.SECONDS)).isEqualTo(7L);
                TimerOpRequest first = req(seen.stream()
                        .filter(e -> e.getType() == MessageType.TIMER_OP).findFirst().orElseThrow());
                assertThat(first.getOp()).isEqualTo(TimerOp.TIMER_OP_SCHEDULE);
                assertThat(first.hasDelayMs()).isTrue();
                assertThat(first.getDelayMs()).isEqualTo(3_000L);
                // 读数三 getter 各一 QUERY 请求；remaining 客户端本地差值。
                assertThat(t.isFired()).isFalse();
                assertThat(t.isArmed()).isTrue();
                assertThat(t.getRemainingMillis()).isBetween(0L, 123_456L);
                assertThat(seen).filteredOn(e -> e.getType() == MessageType.TIMER_OP)
                        .extracting(e -> req(e).getOp())
                        .containsExactly(TimerOp.TIMER_OP_SCHEDULE, TimerOp.TIMER_OP_QUERY,
                                TimerOp.TIMER_OP_QUERY, TimerOp.TIMER_OP_QUERY);
                assertThatThrownBy(() -> t.schedule(-1, TimeUnit.SECONDS))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        }
    }

    @Test
    void awaitQueuedNotifyReissueSameRidSettlesOkMarked() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        // 首答 QUEUED 并在服务端侧推唤醒、重发答 OK{marked}——了结必须命中同一
        // request_id（v10 重挂误换 rid 缺陷的回归钉：换 rid 会绕过幂等面）。
        AtomicReference<Long> awaitRid = new AtomicReference<>(-1L);
        AtomicReference<ScriptedServer> serverRef = new AtomicReference<>();
        Function<Envelope, Envelope> script = env -> {
            synchronized (seen) {
                seen.add(env);
            }
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            if (req(env).getOp() != TimerOp.TIMER_OP_AWAIT) {
                return resp(env, StatusCode.OK, 1, true, 1_000L, false);
            }
            if (awaitRid.compareAndSet(-1L, env.getRequestId())) {
                serverRef.get().push(notify("gate", env.getRequestId()));
                return resp(env, StatusCode.QUEUED, 1, true, 1_000L, false);
            }
            assertThat(env.getRequestId()).isEqualTo(awaitRid.get());
            return resp(env, StatusCode.OK, 1, true, 1_000L, true);
        };
        try (ScriptedServer server = new ScriptedServer(script)) {
            serverRef.set(server);
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OTimer t = client.newTimer("gate");
                boolean fired = t.await(10, TimeUnit.SECONDS);
                assertThat(fired).isTrue();
                assertThat(seen).filteredOn(e -> e.getType() == MessageType.TIMER_OP
                                && req(e).getOp() == TimerOp.TIMER_OP_AWAIT)
                        .extracting(Envelope::getRequestId)
                        .allMatch(rid -> rid == awaitRid.get());
            }
        }
    }

    @Test
    void disarmedTerminalSurfacesAsExceptionNotFalse() throws Exception {
        try (ScriptedServer server = new ScriptedServer(env -> {
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            return resp(env, StatusCode.DENIED, 2, false, 1_000L, false);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OTimer t = client.newTimer("gone");
                // 代终结即刻 DENIED：异常收束而非布尔 false（两形态不混读契约）。
                assertThatThrownBy(() -> t.await(5, TimeUnit.SECONDS))
                        .isInstanceOf(OpenLatchException.class)
                        .hasMessageContaining("unsatisfiable");
                assertThatThrownBy(() -> t.await()).isInstanceOf(OpenLatchException.class);
            }
        }
    }

    @Test
    void awaitTimeoutCancelsWithTargetRidThenThrows() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        // 恒答 QUEUED 且从不唤醒：客户端兜底超时→CANCEL（携被撤 rid）→TimeoutException。
        try (ScriptedServer server = new ScriptedServer(env -> {
            synchronized (seen) {
                seen.add(env);
            }
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            if (req(env).getOp() == TimerOp.TIMER_OP_CANCEL) {
                return resp(env, StatusCode.OK, 1, true, 9_999L, false);
            }
            return resp(env, StatusCode.QUEUED, 1, true, 9_999L, false);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OTimer t = client.newTimer("slow");
                // 契约：限时 await 的超时形态由 false 承载（与被撤销的异常路径分轨）。
                assertThat(t.await(800, TimeUnit.MILLISECONDS)).isFalse();
                long awaitRid = seen.stream()
                        .filter(e -> req(e).getOp() == TimerOp.TIMER_OP_AWAIT)
                        .findFirst().orElseThrow().getRequestId();
                // 再限时等待同样以 false 收束（每分片到期均先 CANCEL 后返）。
                assertThat(t.await(300, TimeUnit.MILLISECONDS)).isFalse();
                // CANCEL 为 fire-and-forget：有界等待帧抵达后校验目标 rid 指向。
                TimerOpRequest cancel = null;
                long deadline = System.currentTimeMillis() + 5_000;
                while (System.currentTimeMillis() < deadline) {
                    var hit = seen.stream()
                            .filter(e -> req(e).getOp() == TimerOp.TIMER_OP_CANCEL)
                            .findFirst();
                    if (hit.isPresent()) {
                        cancel = hit.get().getTimerOpRequest();
                        break;
                    }
                    Thread.sleep(20);
                }
                assertThat(cancel).as("超时路径须尽力发 CANCEL（fire-and-forget）").isNotNull();
                assertThat(cancel.getAwaitRequestId()).isEqualTo(awaitRid);
            }
        }
    }

    @Test
    void overaloadAndGateRejectionsSurfaceAsExplicitException() throws Exception {
        try (ScriptedServer server = new ScriptedServer(env -> {
            if (env.getType() == MessageType.HELLO) {
                return hello(env);
            }
            return resp(env, req(env).getOp() == TimerOp.TIMER_OP_AWAIT
                    ? StatusCode.OVERLOADED : StatusCode.INVALID_REQUEST,
                    0, false, 0, false);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OTimer t = client.newTimer("x");
                assertThatThrownBy(() -> t.await(1, TimeUnit.SECONDS))
                        .isInstanceOf(OpenLatchException.class)
                        .hasMessageContaining("OVERLOADED");
                assertThatThrownBy(() -> t.schedule(1, TimeUnit.SECONDS))
                        .isInstanceOf(OpenLatchException.class)
                        .hasMessageContaining("INVALID_REQUEST");
            }
        }
    }
}
