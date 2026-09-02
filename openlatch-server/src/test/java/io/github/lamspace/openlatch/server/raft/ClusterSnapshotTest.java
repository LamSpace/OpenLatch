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
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集群级快照测试（详设 §10"快照"层的集成半区，S4/P2-15 验证列"快照可加载；
 * 快照期间服务不受影响"）：真实 3 节点集群上验证小阈值自动触发、保留上限、
 * 触发位点合法性与手动触发通道（design D6）——负载与快照生成经不同线程
 * 真实交错（EmbeddedChannel 驱动线程 vs Ratis 应用线程）。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClusterSnapshotTest {

    /** 立即式获取请求（waitMs=0：无排队分支，应答恒为授予/拒绝二态）。 */
    private static Envelope acquire(long rid, String key) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key).setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1L).setLeaseMs(30_000L).setWaitMs(0))
                .build();
    }

    private static Envelope release(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey(key).setLeaseToken(token)
                        .setThreadId(1L))
                .build();
    }

    /** 执行一轮获取+释放并断言成功，返回本请求消耗的 rid 数（2）。 */
    private static void grantAndRelease(ClusterHarness.TestConn c, long rid, String key) {
        Envelope g = c.request(acquire(rid, key));
        assertThat(g.getAcquireResponse().getStatus())
                .as("acquire rid=%d", rid).isEqualTo(StatusCode.OK);
        long token = g.getAcquireResponse().getLeaseToken();
        Envelope r = c.request(release(rid + 1, key, token));
        assertThat(r.getReleaseResponse().getStatus())
                .as("release rid=%d", rid + 1).isEqualTo(StatusCode.OK);
    }

    /** 从 {@code snapshot.T_I} 文件名解析位点 I。 */
    private static long snapshotIndexOf(Path file) {
        String name = file.getFileName().toString();
        String[] parts = name.split("\\.");
        String[] ti = parts[1].split("_");
        return Long.parseLong(ti[1]);
    }

    @Test
    void autoSnapshotUnderLoadKeepsServiceAndRetentionBound() throws IOException {
        // 阈值 100 条目：约 50 轮获取+释放即越阈；观察保留数与位点。
        try (ClusterHarness h = ClusterHarness.start(3, 1_000L, 100L)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(9001);

            // 负载先行跨过阈值：期间任何请求失败即用例失败（grantAndRelease 内断言）。
            long rid = 100;
            for (int round = 0; round < 120; round++) {
                grantAndRelease(c, rid, "snap-" + (round % 8));
                rid += 2;
                if (round == 59) {
                    // 过半程确认已产出首份（不阻塞：此时约 120+ 条目已提交）。
                    h.awaitTrue(() -> {
                        try {
                            return !h.snapshotFiles(leader.id).isEmpty();
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }, 30_000, "Leader 自动快照产出");
                }
            }
            // 跨多个阈值后：保留上限钉死 2 份（清理在产出周期内由库完成）。
            h.awaitTrue(() -> {
                try {
                    return h.snapshotFiles(leader.id).size() == 2;
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }, 30_000, "保留收敛到 2 份");
            // 位点合法：不超过已应用位点、严格为正。
            long applied = leader.lastApplied();
            for (Path f : h.snapshotFiles(leader.id)) {
                assertThat(snapshotIndexOf(f)).isPositive().isLessThanOrEqualTo(applied);
            }
            // 快照多轮触发后集群仍一致（快照不改变复制状态）。
            h.awaitTrue(h::aliveAgreeWithLeader, 15_000, "停载后三副本摘要一致");
        }
    }

    @Test
    void manualTriggerOnAnyRoleProducesSnapshot() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 1_000L)) { // 默认大阈值：无自动干扰
            List<ClusterHarness.Node> ns = h.nodes();
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node follower = ns.stream().filter(x -> !x.isLeader()).findFirst().orElseThrow();
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(9002);
            grantAndRelease(c, 100, "manual-k");
            h.awaitTrue(h::aliveAgreeWithLeader, 15_000, "基线一致");

            long fIdx = follower.runtime.subsystem().triggerSnapshot();
            List<Path> fFiles = h.snapshotFiles(follower.id);
            assertThat(fIdx).isPositive();
            assertThat(fFiles).hasSize(1);
            assertThat(snapshotIndexOf(fFiles.get(0))).isEqualTo(fIdx);
            assertThat(fIdx).isLessThanOrEqualTo(follower.lastApplied());

            long lIdx = leader.runtime.subsystem().triggerSnapshot();
            assertThat(h.snapshotFiles(leader.id)).hasSize(1);
            assertThat(lIdx).isPositive();

            // 手动触发不扰动服务与一致性。
            grantAndRelease(c, 110, "manual-k2");
            h.awaitTrue(h::aliveAgreeWithLeader, 15_000, "触发后仍一致");
            assertThat(follower.runtime.subsystem().triggerSnapshot()).isGreaterThanOrEqualTo(fIdx);
        }
    }
}
