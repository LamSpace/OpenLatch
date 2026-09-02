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
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 恢复路径集群用例（详设 §7.3/§8"快照点前后重启"行，S4/P2-16 验证列
 * "恢复一致"；兼作 design D5 安装流 spike 的固化判据）：
 * <ul>
 *   <li>{@code restartFollowerLoadsLocalSnapshotAndConverges} —— 启动加载
 *       （initialize 读最新快照 + 尾部回放）；</li>
 *   <li>{@code severelyLaggingFollowerInstallsSnapshotFromLeader} —— Leader
 *       截断后重启严重落后 Follower，走库侧安装流（pause→发布→reload）而非
 *       本地全量回放；追赶窗口写请求 {@code NOT_LEADER}（§7.3-3）。</li>
 * </ul>
 *
 * <p>安装用例以 {@code logSegmentBytes=4096} 强制日志小块滚动 +
 * {@code purgeUptoSnapshotIndex}（装配层恒开），使"Leader 已截断 Follower
 * 所需日志"的位点差在数百条目内必然形成。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClusterSnapshotRecoveryTest {

    private static Envelope acquire(long rid, String key) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key).setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1L).setLeaseMs(60_000L).setWaitMs(0))
                .build();
    }

    private static Envelope release(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey(key).setLeaseToken(token)
                        .setThreadId(1L))
                .build();
    }

    /** 一轮获取+释放，断言均 OK（租约 60s：负载中途无自然到期扰动）。 */
    private static long grantAndRelease(ClusterHarness.TestConn c, long rid, String key) {
        Envelope g = c.request(acquire(rid, key));
        assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        Envelope r = c.request(release(rid + 1, key, g.getAcquireResponse().getLeaseToken()));
        assertThat(r.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
        return rid + 2;
    }

    private static long snapshotIndexOf(Path file) {
        String[] ti = file.getFileName().toString().split("\\.")[1].split("_");
        return Long.parseLong(ti[1]);
    }

    @Test
    void restartFollowerLoadsLocalSnapshotAndConverges() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 1_000L, 60L)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node victim = h.nodes().stream()
                    .filter(x -> !x.isLeader()).findFirst().orElseThrow();
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(7001);
            long rid = 100;
            for (int round = 0; round < 40; round++) {
                rid = grantAndRelease(c, rid, "load-" + (round % 5));
            }
            // 停机前确认 victim 已自产快照（启动加载路径的输入）。
            h.awaitTrue(() -> {
                try {
                    return !h.snapshotFiles(victim.id).isEmpty();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }, 20_000, "victim 产出本地快照");
            for (int round = 0; round < 80; round++) {
                rid = grantAndRelease(c, rid, "load-" + (round % 5));
            }
            h.stopNode(victim.id);
            for (int round = 0; round < 40; round++) {
                rid = grantAndRelease(c, rid, "load-" + (round % 5));
            }
            h.restartNode(victim.id);
            h.awaitTrue(h::aliveAgreeWithLeader, 60_000, "重启副本经快照+回放追平");

            // 追平后继续演化：新写入在三副本间一致收敛。
            for (int round = 0; round < 10; round++) {
                rid = grantAndRelease(c, rid, "load-" + (round % 5));
            }
            h.awaitTrue(h::aliveAgreeWithLeader, 30_000, "追平后继续一致");
        }
    }

    @Test
    void severelyLaggingFollowerInstallsSnapshotFromLeader() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 1_000L, 40L, 4096)) {
            ClusterHarness.Node leader = h.leader();
            // victim 停机前先握手（既有会话跨重启复用）：追赶窗口内 HELLO 的
            // 会话注册需提交至 Leader 并等待本节点应用，可能长于测试超时——
            // §3.1"会话注册除外"的既定语义，故角色门探针不经重新握手直发。
            ClusterHarness.Node preVictim = h.nodes().stream()
                    .filter(x -> !x.isLeader()).findFirst().orElseThrow();
            ClusterHarness.TestConn vc = h.connect(preVictim);
            assertThat(vc.hello(7003).hasHelloResponse()).isTrue();
            int victimId = h.stopOneFollower(); // 停服于日志起步段：本地无可用快照
            assertThat(victimId).isEqualTo(preVictim.id);
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(7002);
            long rid = 100;
            // 推 Leader 越过多个阈值：快照产出 + 小 segment 截断，落后位点差成型。
            for (int round = 0; round < 200; round++) {
                rid = grantAndRelease(c, rid, "install-" + (round % 7));
            }
            h.restartNode(victimId);
            ClusterHarness.Node victim = h.node(victimId);

            // 追赶窗口：victim 尚非 Leader，写请求一律 NOT_LEADER（§7.3-3）。
            Envelope probe = vc.request(acquire(500, "install-window"));
            assertThat(probe.getAcquireResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);

            h.awaitTrue(h::aliveAgreeWithLeader, 120_000, "严重落后副本安装快照后追平");

            // 安装确曾发生：victim 目录内出现由 Leader 下发的快照文件。
            Path installed = h.snapshotFiles(victimId).stream()
                    .max(java.util.Comparator.comparingLong(ClusterSnapshotRecoveryTest::snapshotIndexOf))
                    .orElseThrow(() -> new AssertionError("victim 无快照文件（安装流未触达？）"));
            assertThat(snapshotIndexOf(installed)).isPositive();

            // 安装位点之后的增量照常应用：继续演化并收敛。
            for (int round = 0; round < 10; round++) {
                rid = grantAndRelease(c, rid, "install-" + (round % 7));
            }
            h.awaitTrue(h::aliveAgreeWithLeader, 30_000, "安装后增量收敛");
        }
    }
}
