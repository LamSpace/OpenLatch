package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 成员变更用例（详设 §7.4/§10，S4/P2-17 验证列"加节点追赶、删节点用例通过"，
 * spec cluster-node-lifecycle"成员变更运维"三场景）。
 *
 * <p>编排口径与部署文档一致（design D6）：加=listener 加入→追赶收敛→升
 * voter；删=单步出组＋被移除节点会话的显式批量清理（同 §5.2 规则 4 车道）。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClusterMembershipTest {

    private static Envelope acquire(long rid, String key, long leaseMs) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key).setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1L).setLeaseMs(leaseMs).setWaitMs(0))
                .build();
    }

    private static Envelope release(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey(key).setLeaseToken(token)
                        .setThreadId(1L))
                .build();
    }

    /** 本集群 peerSpec 全集（id@127.0.0.1:raftPort）。 */
    private static List<String> specsOf(ClusterHarness h, List<ClusterHarness.Node> ns) {
        List<String> out = new ArrayList<>();
        for (ClusterHarness.Node x : ns) {
            out.add(x.id + "@127.0.0.1:" + x.raftPort);
        }
        return out;
    }

    @Test
    void addNodeCatchesUpPromotesAndCanLead() throws IOException {
        // 小 segment+低阈值：新节点空目录加入时 Leader 历史已截断 → 安装流追赶。
        try (ClusterHarness h = ClusterHarness.start(3, 1_000L, 40L, 4096)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(8001);
            long rid = 100;
            for (int round = 0; round < 80; round++) {
                Envelope g = c.request(acquire(rid++, "m-" + (round % 6), 60_000));
                assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
                c.request(release(rid++, "m-" + (round % 6), g.getAcquireResponse().getLeaseToken()));
            }
            h.awaitTrue(h::aliveAgreeWithLeader, 30_000, "基线一致");

            ClusterHarness.Node n4 = h.addNode(4);
            List<ClusterHarness.Node> three = h.nodes().stream().filter(x -> x.id != 4).toList();
            // listener 加入（投票者不变）。
            leader.runtime.subsystem().setMembers(specsOf(h, three), List.of(
                    "4@127.0.0.1:" + n4.raftPort));
            h.awaitTrue(() -> n4.digest().equals(leader.digest()), 90_000,
                    "新节点（listener）追赶至一致");

            // 升 voter：四投票者全集。
            leader.runtime.subsystem().setMembers(specsOf(h, h.nodes()), List.of());
            // 升票后可当选并服务（§7.4"先加后删"的验证端）。
            h.transferLeadership(4);
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l.id == 4;
            }, 60_000, "新节点当选 Leader");
            ClusterHarness.TestConn c4 = h.connect(n4);
            c4.hello(8002);
            Envelope g = c4.request(acquire(900, "after-promote", 60_000));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            c4.request(release(901, "after-promote", g.getAcquireResponse().getLeaseToken()));
            h.awaitTrue(h::aliveAgreeWithLeader, 60_000, "四节点收敛");
        }
    }

    @Test
    void removeVoterCleansSessionsAndLocksOfRemovedNode() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 1_000L)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node victim = h.nodes().stream()
                    .filter(x -> !x.isLeader()).findFirst().orElseThrow();

            // 让 victim 先当主：其接入的会话才能持有锁（ACQUIRE 角色门 §4.5）。
            h.transferLeadership(victim.id);
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l.id == victim.id;
            }, 30_000, "victim 当选");
            ClusterHarness.TestConn vc = h.connect(victim);
            vc.hello(8003);
            Envelope g = vc.request(acquire(910, "victim-held", 600_000));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long victimToken = g.getAcquireResponse().getLeaseToken();

            // 移回原 Leader，victim 以 Follower 身份出组。
            h.transferLeadership(leader.id);
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l.id == leader.id;
            }, 30_000, "主权重归");
            leader.runtime.removeMember(victim.id);
            h.stopNode(victim.id);

            // 被移除节点会话的持锁已随批量 SESSION_CLOSE 释放：同键可再授予。
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(8004);
            Envelope again = c.request(acquire(920, "victim-held", 600_000));
            assertThat(again.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(again.getAcquireResponse().getLeaseToken()).isNotEqualTo(victimToken);
            h.awaitTrue(h::aliveAgreeWithLeader, 30_000, "存活副本收敛");
        }
    }

    @Test
    void majorityGuardRejectsUnsafeSingleStepChanges() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 1_000L)) {
            ClusterHarness.Node leader = h.leader();
            List<String> all = specsOf(h, h.nodes());

            // 违例一：同时加删（先加后删必须两步提交）。
            List<String> swap = new ArrayList<>(all);
            swap.remove(0);
            swap.add("9@127.0.0.1:19411");
            assertThatThrownBy(() -> leader.runtime.subsystem().setMembers(swap, List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("护栏");

            // 违例二：单次净移除两个投票者。
            List<String> minusTwo = new ArrayList<>(all);
            minusTwo.remove(0);
            minusTwo.remove(0);
            assertThatThrownBy(() -> leader.runtime.subsystem().setMembers(minusTwo, List.of()))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("护栏");

            // 护栏在提交前生效：集群成员未受影响（仍可正常读写收敛）。
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(8005);
            assertThat(c.request(acquire(930, "guard-check", 60_000))
                    .getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        }
    }
}
