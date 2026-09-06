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

package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LatchAwaitRequest;
import io.github.lamspace.openlatch.protocol.LatchCountDownRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 T1 P3-07 集群行为矩阵（协议档）：Semaphore 的许可感知队首推进
 * （部分释放驱动唤醒、池回收后纯加入显式拒绝、带断言重建）与 Latch 的
 * 计数复制/等待本地/归零广播/Leader 切换存续；副本摘要一致性全程钉住。
 */
@Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
class ClusterSemaphoreLatchTest {

    /** Semaphore 获取信封。 */
    private static Envelope semAcquire(long rid, String key, int permits, int total, long waitMs) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(key).setLockType(LockType.LOCK_TYPE_SEMAPHORE)
                        .setThreadId(7).setLeaseMs(60_000).setWaitMs(waitMs)
                        .setPermits(permits).setPermitsTotal(total))
                .build();
    }

    /** 释放信封（许可数）。 */
    private static Envelope release(long rid, String key, long token, int permits) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey(key).setLeaseToken(token).setThreadId(7).setPermits(permits))
                .build();
    }

    /** countDown 信封。 */
    private static Envelope countDown(long rid, String key, long count, long total) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LATCH_COUNT_DOWN)
                .setRequestId(rid)
                .setLatchCountDownRequest(LatchCountDownRequest.newBuilder()
                        .setKey(key).setCount(count).setTotal(total))
                .build();
    }

    /** await 信封。 */
    private static Envelope await(long rid, String key, long total) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LATCH_AWAIT)
                .setRequestId(rid)
                .setLatchAwaitRequest(LatchAwaitRequest.newBuilder().setKey(key).setTotal(total))
                .build();
    }

    /** 静默窗口断言：在时限内该连接不得有任何出站（推送/应答）。 */
    private static void quietFor(ClusterHarness.TestConn conn, long ms) throws InterruptedException {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            assertThat(conn.pollOutbound()).isNull();
            Thread.sleep(50);
        }
    }

    @Test
    void semaphorePermitsAwareFifoAcrossReplicatedPath() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 3);
            ClusterHarness.TestConn b = h.connect(leader);
            b.hello(2, 3);
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(3, 3);

            Envelope ga = a.request(semAcquire(10, "sem", 2, 3, -1));
            assertThat(ga.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long tokenA = ga.getAcquireResponse().getLeaseToken();

            assertThat(b.request(semAcquire(11, "sem", 2, 3, -1)).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED); // 池 1 < 2
            assertThat(c.request(semAcquire(12, "sem", 1, 0, -1)).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED); // 1 ≤ 池但仍非队首（FIFO）

            // A 归还 1（部分释放）：池 2 满足队首 B → 恰 B 收到通知。
            assertThat(a.request(release(13, "sem", tokenA, 1)).getReleaseResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            Envelope pushB = b.awaitOutbound(10_000);
            assertThat(pushB.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(pushB.getAwaitNotify().getRequestIdRef()).isEqualTo(11);
            quietFor(c, 500);

            // B 同 id 重发：池足量 → 复制授予。
            Envelope gb = b.request(semAcquire(11, "sem", 2, 3, -1));
            assertThat(gb.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long tokenB = gb.getAcquireResponse().getLeaseToken();

            // B 归还 2（A 仍持 1，条目存续）：部分释放再次驱动队首检查 → C 获通知。
            b.request(release(14, "sem", tokenB, 2));
            Envelope pushC = c.awaitOutbound(10_000);
            assertThat(pushC.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(pushC.getAwaitNotify().getRequestIdRef()).isEqualTo(12);
            Envelope gc = c.request(semAcquire(12, "sem", 1, 0, -1));
            assertThat(gc.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK); // 队首正常授予
            long tokenC = gc.getAcquireResponse().getLeaseToken();

            a.request(release(15, "sem", tokenA, 1)); // A 清零，C 仍持 1：条目存续
            c.request(release(16, "sem", tokenC, 1)); // 全体清零：条目回收

            // 池回收后纯加入重发：显式 INVALID_REQUEST（生命周期约定）；带断言重建即授予。
            assertThat(c.request(semAcquire(17, "sem", 3, 0, 0)).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST);
            assertThat(c.request(semAcquire(18, "sem", 3, 3, 0)).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.OK);

            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "Semaphore 序列后副本一致");
        }
    }

    @Test
    void latchReplicatedCountLeaderLocalWait() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 3);
            ClusterHarness.TestConn b = h.connect(leader);
            b.hello(2, 3);
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(3, 3);

            Envelope init = a.request(countDown(10, "lat", 0, 3)); // 纯初始化（进日志）
            assertThat(init.getLatchCountDownResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(init.getLatchCountDownResponse().getRemaining()).isEqualTo(3);

            assertThat(b.request(await(11, "lat", 0)).getLatchAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(c.request(await(12, "lat", 0)).getLatchAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(b.pollOutbound()).isNull();

            a.request(countDown(13, "lat", 1, 0)); // 剩 2：无广播
            quietFor(b, 1_500);
            quietFor(c, 500);

            assertThat(a.request(countDown(14, "lat", 5, 0)).getLatchCountDownResponse()
                    .getRemaining()).isZero(); // 下限 0，触发全体广播
            Envelope pb = b.awaitOutbound(10_000);
            Envelope pc = c.awaitOutbound(10_000);
            assertThat(pb.getAwaitNotify().getRequestIdRef()).isEqualTo(11);
            assertThat(pc.getAwaitNotify().getRequestIdRef()).isEqualTo(12);

            assertThat(b.request(await(11, "lat", 0)).getLatchAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.OK); // 重发命中归零
            assertThat(c.request(await(12, "lat", 0)).getLatchAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(a.request(await(15, "lat", 0)).getLatchAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.OK); // 一次性：晚到者直接放行

            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "Latch 序列后副本一致");
        }
    }

    @Test
    void primitivesSurviveLeaderFailover() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 800L)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 3);

            Envelope gs = a.request(semAcquire(10, "fsem", 1, 2, -1));
            assertThat(gs.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            Envelope gl = a.request(countDown(11, "flat", 0, 2));
            assertThat(gl.getLatchCountDownResponse().getRemaining()).isEqualTo(2);

            h.stopNode(leader.id);
            h.awaitTrue(() -> h.leader() != null && h.leader() != leader, 20_000, "重选主");
            ClusterHarness.TestConn onNew = h.connect(h.leader());
            onNew.hello(20, 3);

            // 持有与计数为复制状态：切换后可见——fsem 池仅剩 1，flat 剩 2。
            assertThat(onNew.request(semAcquire(21, "fsem", 2, 2, 0)).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.DENIED);
            assertThat(onNew.request(semAcquire(23, "fsem", 1, 2, 0)).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            Envelope cd = onNew.request(countDown(24, "flat", 2, 0));
            assertThat(cd.getLatchCountDownResponse().getRemaining()).isZero();
            assertThat(onNew.request(await(25, "flat", 0)).getLatchAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.OK);

            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "failover 后副本一致");
        }
    }
}
