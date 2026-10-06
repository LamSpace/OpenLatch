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
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.ConditionOp;
import io.github.lamspace.openlatch.protocol.ConditionOpRequest;
import io.github.lamspace.openlatch.protocol.ConditionOpResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
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
 * {@link RemoteCondition} 车道单测（v9，{@link ScriptedServer} 直驱）：
 * 折叠 await 的 {@code QUEUED → AWAIT_NOTIFY → 无条件清 {@code condition}
 * 同 request_id 重发 → 授予}全序、超时收束的 {@code LEAVE}+常规重获取
 * 形状与返回时持锁、{@code await(0)} 同型收束、非持有本地抛
 * {@code IllegalMonitorStateException}（零请求）、服务端 {@code NOT_HELD}
 * 双层同型映射、{@code signal}/{@code signalAll} 单帧形状（thread_id 携带、
 * await_request_id 为 0）、句柄工厂守门与读/写锁
 * {@code UnsupportedOperationException} 显式化。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RemoteConditionScriptedTest {

    /** HELLO 应答（v9 服务端形态）。 */
    private static Envelope hello(Envelope req) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(9200L)
                .setServerProtocolVersion(9).setDefaultLeaseMs(30_000))
                .build();
    }

    /** 获取应答信封。 */
    private static Envelope acquireResp(Envelope req, StatusCode status, long token) {
        return req.toBuilder().setType(MessageType.LOCK_ACQUIRE)
                .setAcquireResponse(AcquireResponse.newBuilder()
                        .setStatus(status).setLeaseToken(token).setGrantedLeaseMs(30_000))
                .build();
    }

    /** 条件应答信封（op 回显）。 */
    private static Envelope condResp(Envelope req, StatusCode status) {
        return req.toBuilder().setType(MessageType.CONDITION_OP)
                .setConditionOpResponse(ConditionOpResponse.newBuilder()
                        .setStatus(status).setOp(req.getConditionOpRequest().getOp()))
                .build();
    }

    /** 唤醒通知推送帧（request_id=0，按 ref 关联折叠 ACQUIRE）。 */
    private static Envelope notify(String key, long awaitRequestId) {
        return Envelope.newBuilder()
                .setType(MessageType.AWAIT_NOTIFY).setRequestId(0)
                .setAwaitNotify(AwaitNotify.newBuilder()
                        .setKey(key).setRequestIdRef(awaitRequestId))
                .build();
    }

    /**
     * 条件应答桩公共脚本：HELLO 照常、无条件 ACQUIRE 即授、有
     * {@code condition} 的 ACQUIRE 恒 {@code QUEUED}、CONDITION_OP 恒 OK。
     *
     * @param seen   入站业务帧记录
     * @param folded 首见折叠帧时写入其 request_id 的引用
     * @return 应答函数
     */
    private static Function<Envelope, Envelope> script(List<Envelope> seen,
            AtomicReference<Long> folded) {
        return req -> {
            switch (req.getType()) {
                case HELLO -> {
                    return hello(req);
                }
                case LOCK_ACQUIRE -> {
                    seen.add(req);
                    AcquireRequest ar = req.getAcquireRequest();
                    if (ar.hasCondition()) {
                        folded.compareAndSet(null, req.getRequestId());
                        return acquireResp(req, StatusCode.QUEUED, 0);
                    }
                    Long rid = folded.get();
                    // 折叠提交后的同 id 无信封重发=唤醒重发：授予 666；
                    // 其余无条件获取（首次 lock()/自救重获取）授予 555。
                    if (rid != null && rid == req.getRequestId()) {
                        return acquireResp(req, StatusCode.OK, 666);
                    }
                    return acquireResp(req, StatusCode.OK, 555);
                }
                case CONDITION_OP -> {
                    seen.add(req);
                    return condResp(req, StatusCode.OK);
                }
                default -> {
                    return null;
                }
            }
        };
    }

    /** 折叠 await 全序：QUEUED → 通知 → 同 id 无条件重发 → 授予，返回 true。 */
    @Test
    void foldedAwaitNotifyResendsConditionClearedThenGrants() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        AtomicReference<Long> folded = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(script(seen, folded))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OLock lock = client.newReentrantLock("orders/cond");
                OCondition cond = lock.newCondition("x");
                AtomicReference<Boolean> outcome = new AtomicReference<>();
                AtomicReference<Throwable> error = new AtomicReference<>();
                long[] tid = new long[1];
                Thread worker = new Thread(() -> {
                    tid[0] = Thread.currentThread().threadId();
                    try {
                        lock.lock();
                        outcome.set(cond.await(10, TimeUnit.SECONDS));
                    } catch (Throwable t) {
                        error.set(t);
                    }
                });
                worker.setDaemon(true);
                worker.start();

                // 折叠提交帧到达后按 ref 推唤醒通知。
                awaitUntil(() -> folded.get() != null, 5_000);
                long rid = folded.get();
                awaitUntil(() -> seen.size() >= 2, 5_000);
                server.push(notify("orders/cond", rid));

                worker.join(15_000);
                assertThat(error.get()).isNull();
                assertThat(outcome.get()).isTrue(); // 唤醒路径收束

                Envelope first = seen.get(0);  // lock()：普通获取，无条件
                Envelope second = seen.get(1); // 折叠 await
                Envelope third = seen.get(2);  // 唤醒重发：同 id、清 condition
                assertThat(first.getAcquireRequest().hasCondition()).isFalse();
                assertThat(second.getAcquireRequest().hasCondition()).isTrue();
                assertThat(second.getAcquireRequest().getCondition()).isEqualTo("x");
                assertThat(second.getAcquireRequest().getWaitMs()).isEqualTo(-1);
                assertThat(second.getAcquireRequest().getThreadId()).isEqualTo(tid[0]);
                assertThat(third.getRequestId()).isEqualTo(rid);
                assertThat(third.getAcquireRequest().hasCondition()).isFalse();
                // 授予以重入 1 级登记回本地持锁簿记。
                assertThat(client.heldLockRegistry().get("orders/cond", tid[0])).isNotNull();
                assertThat(client.heldLockRegistry().get("orders/cond", tid[0]).leaseToken())
                        .isEqualTo(666);
                // 唤醒路径全程无 LEAVE。
                assertThat(server.countType(MessageType.CONDITION_OP)).isZero();
            }
        }
    }

    /** 超时收束：LEAVE（thread_id=0、await_request_id=折叠 rid）+ 常规重获取，返回 false 且持锁。 */
    @Test
    void timedOutAwaitLeavesThenReacquiresAndReturnsFalse() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        AtomicReference<Long> folded = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(script(seen, folded))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OLock lock = client.newReentrantLock("orders/tmo");
                OCondition cond = lock.newCondition("x");
                AtomicReference<Boolean> outcome = new AtomicReference<>();
                long[] tid = new long[1];
                Thread worker = new Thread(() -> {
                    tid[0] = Thread.currentThread().threadId();
                    try {
                        lock.lock();
                        outcome.set(cond.await(300, TimeUnit.MILLISECONDS)); // 无人 signal
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                });
                worker.setDaemon(true);
                worker.start();
                worker.join(20_000);

                assertThat(outcome.get()).isFalse(); // 超时收束
                // 返回时持锁：本地簿记已随重获取登记。
                assertThat(client.heldLockRegistry().get("orders/tmo", tid[0])).isNotNull();

                // 帧全序：普通获取 → 折叠提交 → LEAVE → 常规重获取（无第三帧）。
                assertThat(seen).hasSize(4);
                assertThat(seen.get(1).getAcquireRequest().hasCondition()).isTrue();
                Envelope leave = seen.get(2);
                assertThat(leave.getType()).isEqualTo(MessageType.CONDITION_OP);
                ConditionOpRequest lq = leave.getConditionOpRequest();
                assertThat(lq.getOp()).isEqualTo(ConditionOp.CONDITION_OP_LEAVE);
                assertThat(lq.getCondition()).isEqualTo("x");
                assertThat(lq.getThreadId()).isZero();               // LEAVE 矩阵：thread_id 恒 0
                assertThat(lq.getAwaitRequestId()).isEqualTo(folded.get()); // 关联折叠 rid
                assertThat(seen.get(3).getType()).isEqualTo(MessageType.LOCK_ACQUIRE);
                assertThat(seen.get(3).getAcquireRequest().hasCondition()).isFalse();
                assertThat(seen.get(3).getRequestId()).isNotEqualTo(folded.get());
            }
        }
    }

    /** 不限时 await()：无预算挂起，通知到达后以授予收束返回（持锁登记）。 */
    @Test
    void untimedAwaitBlocksAndWakesOnNotify() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        AtomicReference<Long> folded = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(script(seen, folded))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OLock lock = client.newReentrantLock("orders/untimed");
                OCondition cond = lock.newCondition("x");
                AtomicReference<Boolean> returned = new AtomicReference<>();
                AtomicReference<Throwable> error = new AtomicReference<>();
                long[] tid = new long[1];
                Thread worker = new Thread(() -> {
                    tid[0] = Thread.currentThread().threadId();
                    try {
                        lock.lock();
                        cond.await(); // 不限时：无 signal 则永睡（本用例随后推送）
                        returned.set(Boolean.TRUE);
                    } catch (Throwable t) {
                        error.set(t);
                    }
                });
                worker.setDaemon(true);
                worker.start();

                awaitUntil(() -> folded.get() != null, 5_000);
                long rid = folded.get();
                // 不限时形态的折叠帧预算在线路上同为 wait_ms=-1。
                assertThat(seen.get(1).getAcquireRequest().getWaitMs()).isEqualTo(-1);
                server.push(notify("orders/untimed", rid));

                worker.join(15_000);
                assertThat(error.get()).isNull();
                assertThat(returned.get()).isTrue(); // 通知到达后收束返回
                assertThat(client.heldLockRegistry().get("orders/untimed", tid[0]))
                        .isNotNull();
                assertThat(server.countType(MessageType.CONDITION_OP)).isZero();
            }
        }
    }

    /** await(0) 即时到期：与超时同型收束（折叠受理→LEAVE→重获取），返回 false。 */
    @Test
    void zeroTimeoutAwaitConvergesLikeTimeoutPath() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        AtomicReference<Long> folded = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(script(seen, folded))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OLock lock = client.newReentrantLock("orders/zero");
                OCondition cond = lock.newCondition("x");
                AtomicReference<Boolean> outcome = new AtomicReference<>();
                long[] tid = new long[1];
                Thread worker = new Thread(() -> {
                    tid[0] = Thread.currentThread().threadId();
                    try {
                        lock.lock();
                        outcome.set(cond.await(0, TimeUnit.MILLISECONDS));
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                });
                worker.setDaemon(true);
                worker.start();
                worker.join(20_000);

                assertThat(outcome.get()).isFalse();
                assertThat(client.heldLockRegistry().get("orders/zero", tid[0])).isNotNull();
                // 同型三帧：折叠提交、LEAVE、常规重获取。
                assertThat(seen).hasSize(4);
                assertThat(seen.get(1).getAcquireRequest().hasCondition()).isTrue();
                assertThat(seen.get(2).getType()).isEqualTo(MessageType.CONDITION_OP);
                assertThat(seen.get(2).getConditionOpRequest().getOp())
                        .isEqualTo(ConditionOp.CONDITION_OP_LEAVE);
            }
        }
    }

    /** 非持有 await/signal/signalAll：本地抛 IMS，零请求。 */
    @Test
    void notHeldAwaitAndSignalThrowLocallyWithoutRequests() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        AtomicReference<Long> folded = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(script(seen, folded))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OLock lock = client.newReentrantLock("orders/unheld");
                OCondition cond = lock.newCondition("x");
                assertThatThrownBy(() -> cond.await(500, TimeUnit.MILLISECONDS))
                        .isInstanceOf(IllegalMonitorStateException.class);
                assertThatThrownBy(() -> cond.await())
                        .isInstanceOf(IllegalMonitorStateException.class);
                assertThatThrownBy(cond::signal)
                        .isInstanceOf(IllegalMonitorStateException.class);
                assertThatThrownBy(cond::signalAll)
                        .isInstanceOf(IllegalMonitorStateException.class);
                assertThat(seen).isEmpty(); // 本地裁决：不发任何请求
                assertThat(server.countType(MessageType.CONDITION_OP)).isZero();
            }
        }
    }

    /** 服务端权威层：NOT_HELD 应答映射为 IllegalMonitorStateException（双层同型）。 */
    @Test
    void serverNotHeldMapsToIllegalMonitorState() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        AtomicReference<Long> folded = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(req -> {
            Envelope ok = script(seen, folded).apply(req);
            if (ok != null && req.getType() == MessageType.CONDITION_OP) {
                return condResp(req, StatusCode.NOT_HELD); // signal 恒被服务端判非持有
            }
            return ok;
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OLock lock = client.newReentrantLock("orders/notheld");
                OCondition cond = lock.newCondition("x");
                lock.lock(); // 本地持有成立（桩授 555）
                assertThatThrownBy(cond::signal)
                        .isInstanceOf(IllegalMonitorStateException.class);
                assertThat(server.countType(MessageType.CONDITION_OP)).isEqualTo(1);
                // 连接与会话不受影响：既有形状纪律——拒绝仅码形回显。
                assertThat(seen.stream().filter(e -> e.getType() == MessageType.CONDITION_OP)
                        .findFirst().orElseThrow().getConditionOpRequest().getOp())
                        .isEqualTo(ConditionOp.CONDITION_OP_SIGNAL);
            }
        }
    }

    /** signal/signalAll 单帧形状：thread_id 携归属线程、await_request_id 恒 0。 */
    @Test
    void signalFamilyShapesSingleFrame() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        AtomicReference<Long> folded = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(script(seen, folded))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OLock lock = client.newReentrantLock("orders/sig");
                OCondition cond = lock.newCondition("gate");
                lock.lock();
                int before = seen.size();

                cond.signal();
                ConditionOpRequest sq = seen.get(before).getConditionOpRequest();
                assertThat(seen.get(before).getType()).isEqualTo(MessageType.CONDITION_OP);
                assertThat(sq.getOp()).isEqualTo(ConditionOp.CONDITION_OP_SIGNAL);
                assertThat(sq.getKey()).isEqualTo("orders/sig");
                assertThat(sq.getCondition()).isEqualTo("gate");
                assertThat(sq.getThreadId()).isEqualTo(Thread.currentThread().threadId());
                assertThat(sq.getAwaitRequestId()).isZero();

                cond.signalAll();
                ConditionOpRequest saq = seen.get(before + 1).getConditionOpRequest();
                assertThat(saq.getOp()).isEqualTo(ConditionOp.CONDITION_OP_SIGNAL_ALL);
                assertThat(saq.getThreadId()).isEqualTo(Thread.currentThread().threadId());
                assertThat(saq.getAwaitRequestId()).isZero();
                // 恰一帧/调用（signal 家族恒即时回执，零挂起环）。
                assertThat(seen).hasSize(before + 2);
            }
        }
    }

    /** 工厂守门：null/空名拒、读/写锁句柄 UnsupportedOperationException（本地裁决）。 */
    @Test
    void newConditionFactoryGuards() throws Exception {
        List<Envelope> seen = new CopyOnWriteArrayList<>();
        AtomicReference<Long> folded = new AtomicReference<>();
        try (ScriptedServer server = new ScriptedServer(script(seen, folded))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                assertThatThrownBy(() -> client.newReentrantLock("g").newCondition(null))
                        .isInstanceOf(NullPointerException.class);
                assertThatThrownBy(() -> client.newReentrantLock("g").newCondition(""))
                        .isInstanceOf(IllegalArgumentException.class);
                // 同 name 重复 newCondition 合法（命名寻址，句柄等价）。
                assertThat(client.newReentrantLock("g").newCondition("x")).isNotNull();
                assertThat(client.newFairLock("g").newCondition("x")).isNotNull();
                assertThat(client.newSimpleLock("g").newCondition("x")).isNotNull();
                // 读/写锁所得句柄：立即抛，不产生任何请求（本地裁决）。
                OReadWriteLock rw = client.newReadWriteLock("g");
                assertThatThrownBy(() -> rw.readLock().newCondition("x"))
                        .isInstanceOf(UnsupportedOperationException.class);
                assertThatThrownBy(() -> rw.writeLock().newCondition("x"))
                        .isInstanceOf(UnsupportedOperationException.class);
                assertThat(seen).isEmpty();
            }
        }
    }

    /** 自旋等待断言条件（不裸 sleep 定长）。 */
    private static void awaitUntil(java.util.function.BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("条件超时未达成");
    }
}
