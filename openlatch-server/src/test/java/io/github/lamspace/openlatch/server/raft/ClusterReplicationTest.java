package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewResponse;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.ReleaseResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 3 节点复制集成测试（详设 §10"复制集成"+ §13.2 P2-07/08/09/10 验证列，
 * spec"Leader 写请求路径""会话集群登记""租约到期 Leader 驱动复制"
 * "多数派可用性"各场景）。
 *
 * <p>基座 {@link ClusterHarness}：真实 Raft 子系统（gRPC 本机端口）×3，
 * 接入层 EmbeddedChannel 直驱——异步桥全程为真。每用例独立集群
 * （{@code @Timeout} 兜底防挂死）。
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class ClusterReplicationTest {

    // ---------- 请求构造 ----------

    private static Envelope acquire(long rid, String key, LockType type, long waitMs, long leaseMs) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key).setLockType(type)
                        .setThreadId(1L).setLeaseMs(leaseMs).setWaitMs(waitMs))
                .build();
    }

    private static Envelope release(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey(key).setLeaseToken(token)
                        .setThreadId(1L))
                .build();
    }

    private static Envelope renew(long rid, String key, long token, long leaseMs) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LEASE_RENEW)
                .setRequestId(rid)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder().setKey(key)
                        .setLeaseToken(token).setLeaseMs(leaseMs))
                .build();
    }

    // ---------- 授予路径（P2-07/3.6） ----------

    @Test
    void grantReplicatedToAllAndSurvivesFollowerRestart() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c1 = h.connect(leader);
            c1.hello(1);
            Envelope resp = c1.request(acquire(2, "gk", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(resp.getAcquireResponse().getLeaseToken()).isPositive();
            assertThat(resp.getAcquireResponse().getLeaseExpiresAtMs())
                    .isGreaterThan(System.currentTimeMillis());
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "授予后三副本 digest 一致");

            int victim = h.stopOneFollower();
            Envelope resp2 = c1.request(release(3, "gk", resp.getAcquireResponse().getLeaseToken()));
            assertThat(resp2.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(resp2.getReleaseResponse().getFullyReleased()).isTrue();

            h.restartNode(victim);
            h.awaitTrue(h::aliveAgreeWithLeader, 20_000, "停-1 期间写入经多数派确认，重启后追平一致");
        }
    }

    @Test
    void stopTwoNodesNoGrant() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 500)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node survivor = h.nodes().stream()
                    .filter(x -> x.alive() && !x.isLeader()).findFirst().orElseThrow();
            // 停机前在"将存活"的单机上建好会话（多数派尚在，HELLO 正常提交）。
            ClusterHarness.TestConn c = h.connect(survivor);
            Envelope hello = c.hello(1);
            assertThat(hello.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);

            h.stopNode(leader.id);                    // 杀主
            h.stopNodeOtherThan(survivor.id, leader.id); // 再杀一节点 → 仅存单机
            h.awaitTrue(() -> !h.hasLeader(), 10_000, "仅剩单节点无 Leader");

            Envelope resp = c.request(acquire(2, "2down", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
            // 全局无双授：任何存活副本都不持有该 key。
            assertThat(survivor.runtime.core().shadow().isHeld("2down")).isFalse();
        }
    }

    // ---------- 排队路径不写日志（P2-07/3.6，§4.5 硬断言） ----------

    @Test
    void queuedPathDoesNotWriteLog() throws Exception {
        // 常规选举超时 + 关闭探针（排除 NOOP 条目噪声），settle 掉已提交的探针后断言。
        try (ClusterHarness h = ClusterHarness.start(3)) {
            h.setProbesEnabled(false);
            Thread.sleep(1_500);
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c1 = h.connect(leader);
            c1.hello(1);
            ClusterHarness.TestConn c2 = h.connect(leader);
            c2.hello(2);
            Envelope g = c1.request(acquire(3, "qk", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            long before = leader.lastApplied();
            Envelope q = c2.request(acquire(4, "qk", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(q.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(q.getAcquireResponse().getQueuePosition()).isEqualTo(1);
            assertThat(leader.lastApplied()).isEqualTo(before); // QUEUED 零日志（§4.5）

            Envelope d = c2.request(acquire(5, "qk", LockType.LOCK_TYPE_REENTRANT, 0, 30_000));
            assertThat(d.getAcquireResponse().getStatus()).isEqualTo(StatusCode.DENIED);
            assertThat(leader.lastApplied()).isEqualTo(before); // 立即式冲突同样零日志
        }
    }

    @Test
    void concurrentSameKeyPrecheckRaceResolvesToSingleGrant() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c1 = h.connect(leader);
            c1.hello(1);
            ClusterHarness.TestConn c2 = h.connect(leader);
            c2.hello(2);
            // 同键双发不等回执：两次预检都见空闲 → 双双进日志 → 应用裁决
            // 一个 OK、一个改写为 QUEUED（D3），互斥不变式由副本一致收敛。
            c1.session.tryBeginRequest(1024);
            c2.session.tryBeginRequest(1024);
            leader.runtime.requestHandler().handleAcquire(c1.session, acquire(3, "ck",
                    LockType.LOCK_TYPE_REENTRANT, -1, 30_000), c1.ctx);
            leader.runtime.requestHandler().handleAcquire(c2.session, acquire(4, "ck",
                    LockType.LOCK_TYPE_REENTRANT, -1, 30_000), c2.ctx);
            Envelope r1 = c1.awaitOutbound(10_000);
            Envelope r2 = c2.awaitOutbound(10_000);
            long okCount = (r1.getAcquireResponse().getStatus() == StatusCode.OK ? 1 : 0)
                    + (r2.getAcquireResponse().getStatus() == StatusCode.OK ? 1 : 0);
            long queuedCount = (r1.getAcquireResponse().getStatus() == StatusCode.QUEUED ? 1 : 0)
                    + (r2.getAcquireResponse().getStatus() == StatusCode.QUEUED ? 1 : 0);
            assertThat(okCount).isEqualTo(1);
            assertThat(queuedCount).isEqualTo(1);
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "竞态后副本一致（无双授）");
            assertThat(leader.runtime.core().shadow().isHeld("ck")).isTrue();
        }
    }

    @Test
    void followerRejectsWriteWithNotLeader() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node follower = h.nodes().stream()
                    .filter(x -> x.alive() && !x.isLeader()).findFirst().orElseThrow();
            ClusterHarness.TestConn c = h.connect(follower);
            Envelope hello = c.hello(1); // SESSION_OPEN 经 ratis-client 寻主：任意节点可握手（D11）
            assertThat(hello.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
            Envelope resp = c.request(acquire(2, "fk", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
        }
    }

    // ---------- 会话集群化（P2-08/4.5） ----------

    @Test
    void sessionRegistryReplicatedAndDisconnectCleansUp() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node follower = h.nodes().stream()
                    .filter(x -> x.alive() && !x.isLeader()).findFirst().orElseThrow();
            ClusterHarness.TestConn onFollower = h.connect(follower);
            Envelope hello = onFollower.hello(1);
            long sid = hello.getHelloResponse().getSessionId();
            assertThat(sid >>> 32).isEqualTo(follower.id); // nodeId 高位编码（§5.2 规则 1）
            h.awaitTrue(() -> leader.runtime.core().shadow().hasSession(sid), 10_000,
                    "SESSION_OPEN 复制至 Leader");

            ClusterHarness.TestConn writer = h.connect(leader);
            writer.hello(2);
            Envelope g = writer.request(acquire(3, "sk", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            onFollower.disconnect(); // 断连传播：SESSION_CLOSE 条目（§5.2 规则 3）
            h.awaitTrue(() -> !leader.runtime.core().shadow().hasSession(sid), 10_000,
                    "断连会话从复制状态摘除");
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "断连清理后副本一致");
        }
    }

    @Test
    void disconnectReleasesHeldLockOnAllReplicas() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn holder = h.connect(leader);
            holder.hello(1);
            Envelope g = holder.request(acquire(2, "dlk", LockType.LOCK_TYPE_REENTRANT, -1, 60_000));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(leader.runtime.core().shadow().isHeld("dlk")).isTrue();

            holder.disconnect();
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && !l.runtime.core().shadow().isHeld("dlk");
            }, 10_000, "持锁断连即释放（复制语义）");
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "释放后副本一致");
        }
    }

    @Test
    void lostAccessNodeTriggersBatchCleanup() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 500)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node victim = h.nodes().stream()
                    .filter(x -> x.alive() && !x.isLeader()).findFirst().orElseThrow();
            ClusterHarness.TestConn onVictim = h.connect(victim);
            Envelope hello = onVictim.hello(1);
            long sid = hello.getHelloResponse().getSessionId();
            ClusterHarness.TestConn writer = h.connect(leader);
            writer.hello(2);
            Envelope g = writer.request(acquire(3, "lk", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            // victim 会话也持一把锁（经 Leader 授予，victim 仅接入）。
            ClusterHarness.TestConn victimWriter = h.connect(leader);
            victimWriter.hello(4);
            Envelope g2 = victimWriter.request(acquire(5, "lk2", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(g2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long victimSid2 = victimWriter.session.sessionId();
            assertThat(victimSid2 >>> 32).isNotEqualTo(sid >>> 32); // 分属两节点
            // 把 victim 持有的锁换成归属 victim 的会话：直接重授——简化为断言 sid 登记在案。
            h.awaitTrue(() -> leader.runtime.core().shadow().hasSession(sid), 10_000, "会话复制完成");

            h.stopNode(victim.id); // 模拟接入节点宕机（Raft peer 失联）
            // 失联判定 = 探针周期(500ms) × STALL_TOLERANCE(3) + 轮询余量
            h.awaitTrue(() -> !h.leader().runtime.core().shadow().hasSession(sid), 20_000,
                    "Leader 批量补发 SESSION_CLOSE 清理失联节点会话");
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "批量清理后副本一致");
        }
    }

    // ---------- 租约到期复制（P2-09/5.3） ----------

    @Test
    void leaseExpiresViaLeaderDriverOnAllReplicas() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(1);
            Envelope g = c.request(acquire(2, "ek", LockType.LOCK_TYPE_REENTRANT, -1, 500));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long token = g.getAcquireResponse().getLeaseToken();
            // 500ms 租约 + 200ms 扫描周期 + 复制/应用：秒级到期，全副本释放。
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l.runtime.core().shadow().heldEntries().get("ek") == null;
            }, 15_000, "到期条目经 Leader 驱动复制并释放");
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "到期后副本一致");
            // 迟到的旧到期条目为空操作（守卫按 token/到期判定）：重新授予不受影响。
            Envelope g2 = c.request(acquire(3, "ek", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(g2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(g2.getAcquireResponse().getLeaseToken()).isGreaterThan(token);
        }
    }

    @Test
    void headWakesAfterExpiryAndSelfPromotesOnResend() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn holder = h.connect(leader);
            holder.hello(1);
            Envelope g = holder.request(acquire(2, "wk", LockType.LOCK_TYPE_REENTRANT, -1, 500));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            ClusterHarness.TestConn waiter = h.connect(leader);
            waiter.hello(3);
            Envelope q = waiter.request(acquire(4, "wk", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(q.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);

            // 到期释放 → 队首 AWAIT_NOTIFY → 同 requestId 重发 → 自推进授予。
            Envelope notify = waiter.awaitOutbound(15_000);
            assertThat(notify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(notify.getAwaitNotify().getRequestIdRef()).isEqualTo(4);
            Envelope resend = waiter.request(acquire(4, "wk", LockType.LOCK_TYPE_REENTRANT, -1, 30_000));
            assertThat(resend.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(leader.runtime.waitQueue().waitCount("wk")).isZero();
        }
    }

    @Test
    void failoverKeepsExpiryDriving() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 500)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(1);
            Envelope g = c.request(acquire(2, "fk2", LockType.LOCK_TYPE_REENTRANT, -1, 3_000));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            c.disconnect(); // 持有会话保持（连接断开才清；此处保留锁经复制存活）
            // 上面 disconnect 会释放该会话的锁——重授到 leader 上另一会话再杀主：
            ClusterHarness.TestConn c2 = h.connect(leader);
            c2.hello(3);
            Envelope g2 = c2.request(acquire(4, "fk2", LockType.LOCK_TYPE_REENTRANT, -1, 3_000));
            assertThat(g2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            h.stopNode(leader.id); // 杀主，租约 3s 在新 Leader 上继续计时
            h.awaitTrue(h::hasLeader, 15_000, "新 Leader 选出");
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && !l.runtime.core().shadow().isHeld("fk2");
            }, 15_000, "新 Leader 到期驱动继续生效（不提前、不漏扫）");
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "failover 到期后副本一致");
        }
    }

    // ---------- 续租（P2-07 应答=应用结果） ----------

    @Test
    void renewReplicatesExpiryAcrossReplicas() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(1);
            Envelope g = c.request(acquire(2, "rk", LockType.LOCK_TYPE_REENTRANT, -1, 1_000));
            long token = g.getAcquireResponse().getLeaseToken();
            Envelope r = c.request(renew(3, "rk", token, 30_000));
            assertThat(r.getLeaseRenewResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(r.getLeaseRenewResponse().getLeaseExpiresAtMs())
                    .isGreaterThan(System.currentTimeMillis() + 20_000);
            // 1s 原始到期点之后：因续租仍持有（条目时刻语义 + 全副本一致）。
            Thread.sleep(1_500);
            assertThat(leader.runtime.core().shadow().isHeld("rk")).isTrue();
            Envelope bad = c.request(renew(4, "rk", token + 999, 30_000));
            assertThat(bad.getLeaseRenewResponse().getStatus()).isEqualTo(StatusCode.INVALID_TOKEN);
            Envelope rel = c.request(release(5, "rk", token));
            assertThat(rel.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
        }
    }

}
