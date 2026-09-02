package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 少数派不可授予的进程内近似用例（详设 §8"网络分区（少数派侧）"行 /
 * §11-3 辅轨，design D7：失联近似的常规回归；真分区主轨见
 * {@code scripts/partition-drill.sh} 演练脚本）。
 *
 * <p>编排：让主到目标节点 → 停其余两节点（目标成为无多数派可及的单节点
 * Leader）→ 断言少数派侧写请求全部不生效（超时或错误，二者皆"不授予"）
 * → 复活缺席节点 → 自动收敛且写入恢复。<b>口径标注</b>：停止 ≠ 分区——
 * 停掉节点同时剥夺其"自认在线"能力，真分区下存活侧的旧 Leader 行为差异由
 * 脚本主轨覆盖；本用例保障的是多数派保证的服务端可观测形态（无法达成
 * 多数派即无法授予），该性质与分区/失联的具体故障载体无关。
 */
@Timeout(value = 240, unit = TimeUnit.SECONDS)
class MinorityQuorumTest {

    private static Envelope acquire(long rid, String key) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key).setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1L).setLeaseMs(30_000L).setWaitMs(0))
                .build();
    }

    @Test
    void singleNodeLeaderCannotGrantAndHealsOnRejoin() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 1_000L)) {
            ClusterHarness.Node survivor = h.nodes().stream()
                    .filter(x -> !x.isLeader()).findFirst().orElseThrow();
            h.transferLeadership(survivor.id);
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l.id == survivor.id;
            }, 30_000, "让主完成");

            // 少数派侧的既有会话：让主前建立（HELLO 需提交，停众后无法再建）。
            ClusterHarness.TestConn c = h.connect(survivor);
            c.hello(6001);
            Envelope held = c.request(acquire(650, "held-before"));
            assertThat(held.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long heldToken = held.getAcquireResponse().getLeaseToken();

            h.stopNodeOtherThan(survivor.id);
            h.stopNodeOtherThan(survivor.id);

            // 单节点 Leader 无法达成多数派：ACQUIRE 不产生授予——超时（无应答）
            // 与错误应答都计为"写失败"（§6.3 快速失败语义的两条合法路径）。
            long rid = 700;
            for (int attempt = 0; attempt < 3; attempt++) {
                boolean granted;
                try {
                    granted = c.request(acquire(rid++, "minority-k"))
                            .getAcquireResponse().getStatus() == StatusCode.OK;
                } catch (AssertionError timeout) {
                    granted = false;
                }
                assertThat(granted).as("第 %d 次少数派侧授予必须失败", attempt + 1).isFalse();
            }

            // 复活缺席节点：集群恢复多数派，写入恢复且三副本收敛。
            for (ClusterHarness.Node x : h.nodes()) {
                if (!x.alive()) {
                    h.restartNode(x.id);
                }
            }
            h.awaitTrue(h::hasLeader, 30_000, "复活后有 Leader");
            // 复活后的当值 Leader 可能是任一票（原主/ survivor）——在其上重试写入。
            h.awaitTrue(() -> {
                try {
                    ClusterHarness.TestConn t = h.connect(h.leader());
                    t.hello(6100);
                    Envelope r = t.request(acquire(6101, "minority-k"));
                    return r.getAcquireResponse().getStatus() == StatusCode.OK;
                } catch (AssertionError e) {
                    return false;
                }
            }, 90_000, "多数派恢复后可授予");
            h.awaitTrue(h::aliveAgreeWithLeader, 30_000, "复活后三副本收敛");
            // 少数派窗口前已确认授予的锁不丢（已提交条目不因窗口丢失）——
            // 同会话同凭证续租成功为证：无论 survivor 此刻为 Leader 或 Follower，
            // RENEW 均经转发车道/权威 Leader 复制执行（§4.5 分车道）。
            Envelope renewResp = c.request(renew(651, "held-before", heldToken));
            assertThat(renewResp.getLeaseRenewResponse().getStatus())
                    .as("leader=%s survivorLeader=%s", h.leader().id, survivor.isLeader())
                    .isEqualTo(StatusCode.OK);
        }
    }

    /** 续租请求（同凭证原租期）。 */
    private static Envelope renew(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(1).setType(MessageType.LEASE_RENEW)
                .setRequestId(rid)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder().setKey(key)
                        .setLeaseToken(token).setLeaseMs(30_000L))
                .build();
    }
}
