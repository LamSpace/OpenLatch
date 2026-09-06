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
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.metrics.ServerMetrics;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 3 T2 集群路径 L1/L3 用例（spec"请求埋点覆盖单机与集群双路径"/
 * "Gauge 取值语义与采样安全"）：Leader 授予-排队-释放-续租失败的计数与
 * 耗时档位、复制态 gauge（held/waiters/队深/is_leader）、Follower 的
 * {@code NOT_LEADER} 计数（证明集群路径非盲，§3.4 勘误的回归锁定）、
 * 租约到期各副本恰 +1（在途重试与守卫空操作零计）。
 */
@Timeout(value = 120, unit = java.util.concurrent.TimeUnit.SECONDS)
class ClusterMetricsTest {

    /** 锁获取信封。 */
    private static Envelope acquire(long rid, String key, long threadId, long leaseMs, long waitMs) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key)
                        .setLockType(LockType.LOCK_TYPE_REENTRANT).setThreadId(threadId)
                        .setLeaseMs(leaseMs).setWaitMs(waitMs))
                .build();
    }

    private static Envelope release(long rid, String key, long token, long threadId) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey(key)
                        .setLeaseToken(token).setThreadId(threadId))
                .build();
    }

    private static Envelope renew(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LEASE_RENEW)
                .setRequestId(rid)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder().setKey(key).setLeaseToken(token))
                .build();
    }

    /** 计数读数（未注册/未命中计 0）。 */
    private static double counter(ServerMetrics m, String name, String tag, String value) {
        var c = m.registry().find(name).tag(tag, value).counter();
        return c == null ? 0d : c.count();
    }

    private static double timerCount(ServerMetrics m, String result) {
        var t = m.registry().find(ServerMetrics.ACQUIRE_DURATION).tag("result", result).timer();
        return t == null ? 0d : t.count();
    }

    private static double gauge(ServerMetrics m, String name, String tag, String value) {
        var g = m.registry().find(name).tag(tag, value).gauge();
        return g == null ? Double.NaN : g.value();
    }

    private static double gauge(ServerMetrics m, String name) {
        var g = m.registry().find(name).gauge();
        return g == null ? Double.NaN : g.value();
    }

    @Test
    void leaderWritePathCountsGaugesAndFollowerNotLeader() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node follower = h.nodes().stream()
                    .filter(x -> !x.equals(leader)).findFirst().orElseThrow();

            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 3);
            Envelope ga = a.request(acquire(10, "mk", 1, 60_000, -1));
            assertThat(ga.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long token = ga.getAcquireResponse().getLeaseToken();

            ServerMetrics lm = leader.metrics();
            assertThat(counter(lm, ServerMetrics.ACQUIRE_TOTAL, "status", "OK")).isEqualTo(1);
            assertThat(timerCount(lm, "granted")).isEqualTo(1);
            // 复制态 gauge：1 个锁家族条目持有（本副本已应用）。
            assertThat(gauge(lm, ServerMetrics.LOCKS_HELD, "type", "lock")).isEqualTo(1);
            assertThat(gauge(lm, ServerMetrics.LOCKS_HELD, "type", "semaphore")).isZero();
            assertThat(gauge(lm, ServerMetrics.CLUSTER_IS_LEADER, "node_id",
                    String.valueOf(leader.id))).isEqualTo(1);

            // 排队：Leader 队列 gauge。
            ClusterHarness.TestConn b = h.connect(leader);
            b.hello(2, 3);
            assertThat(b.request(acquire(11, "mk", 2, 60_000, -1)).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(counter(lm, ServerMetrics.ACQUIRE_TOTAL, "status", "QUEUED")).isEqualTo(1);
            assertThat(timerCount(lm, "queued")).isEqualTo(1);
            assertThat(gauge(lm, ServerMetrics.WAITERS)).isEqualTo(1);
            assertThat(gauge(lm, ServerMetrics.QUEUE_DEPTH_MAX)).isEqualTo(1);

            // 释放 + 续租失败（已释放后 NOT_HELD）。
            assertThat(a.request(release(12, "mk", token, 1)).getReleaseResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(counter(lm, ServerMetrics.RELEASE_TOTAL, "status", "OK")).isEqualTo(1);
            assertThat(a.request(renew(13, "mk", 999)).getLeaseRenewResponse().getStatus())
                    .isEqualTo(StatusCode.NOT_HELD);
            assertThat(counter(lm, ServerMetrics.RENEW_TOTAL, "status", "NOT_HELD")).isEqualTo(1);

            // Follower：写请求经角色门拒为 NOT_LEADER——该节点计数非盲。
            ClusterHarness.TestConn f = h.connect(follower);
            f.hello(3, 3);
            assertThat(f.request(acquire(14, "other", 3, 60_000, -1)).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.NOT_LEADER);
            ServerMetrics fm = follower.metrics();
            assertThat(counter(fm, ServerMetrics.ACQUIRE_TOTAL, "status", "NOT_LEADER")).isEqualTo(1);
            assertThat(timerCount(fm, "denied")).isEqualTo(1);
            assertThat(gauge(fm, ServerMetrics.CLUSTER_IS_LEADER, "node_id",
                    String.valueOf(follower.id))).isZero();
        }
    }

    @Test
    void leaseExpiredCountsOncePerReplica() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 3);
            Envelope ga = a.request(acquire(10, "exp", 1, 1_000, 0));
            assertThat(ga.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            // 到期扫描（tick 200ms）→ 条目复制落地：各副本 +1，不随在途重试虚增。
            h.awaitTrue(() -> h.nodes().stream().allMatch(x -> expiredOf(x) >= 1),
                    30_000, "全副本到期计数");
            for (ClusterHarness.Node x : h.nodes()) {
                assertThat(expiredOf(x)).as("node " + x.id).isEqualTo(1);
            }
            // 抑制解除后的空扫描与守卫空操作不得再计。
            Thread.sleep(2_000);
            for (ClusterHarness.Node x : h.nodes()) {
                assertThat(expiredOf(x)).as("node " + x.id + " 稳定").isEqualTo(1);
            }
        }
    }

    /** 某节点到期计数读数。 */
    private static double expiredOf(ClusterHarness.Node x) {
        var c = x.metrics().registry().find(ServerMetrics.LEASE_EXPIRED_TOTAL).counter();
        return c == null ? 0d : c.count();
    }

    @Test
    void isLeaderFlipsAfterLeadershipTransfer() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 500L)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node target = h.nodes().stream()
                    .filter(x -> !x.equals(leader)).findFirst().orElseThrow();
            assertThat(gauge(leader.metrics(), ServerMetrics.CLUSTER_IS_LEADER, "node_id",
                    String.valueOf(leader.id))).isEqualTo(1);

            h.transferLeadership(target.id);
            h.awaitTrue(() -> target.isLeader() && !leader.isLeader(), 20_000, "让位完成");
            // LeaderTracker 事件驱动 gauge：新 Leader 翻 1、旧 Leader 翻 0。
            h.awaitTrue(() -> gauge(target.metrics(), ServerMetrics.CLUSTER_IS_LEADER, "node_id",
                    String.valueOf(target.id)) == 1d
                    && gauge(leader.metrics(), ServerMetrics.CLUSTER_IS_LEADER, "node_id",
                    String.valueOf(leader.id)) == 0d, 20_000, "is_leader 随切换翻转");
        }
    }
}
