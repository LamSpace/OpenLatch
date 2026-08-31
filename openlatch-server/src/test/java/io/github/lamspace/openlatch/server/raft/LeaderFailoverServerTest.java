package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.ClusterView;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.NodeInfo;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.net.ServerSessionHandler;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * S3/P2-12 服务端角色语义集成测试（spec"Follower 写请求分车道"全场景 +
 * "Leader 提示的权威来源"一致性场景 + cluster-node-lifecycle"非 Leader
 * 节点的查询应答"）。
 *
 * <p>基座 {@link ClusterHarness}（v2 HELLO、合成 client-addresses 接入表、
 * Ratis transferLeadership 存活让位）。每用例独立集群，{@code @Timeout} 兜底。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class LeaderFailoverServerTest {

    // ---------- 请求构造（v2） ----------

    private static Envelope acquire(long rid, String key) {
        return Envelope.newBuilder().setProtocolVersion(2).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key)
                        .setLockType(io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1L).setLeaseMs(30_000).setWaitMs(-1))
                .build();
    }

    private static Envelope renew(long rid, String key, long token, long leaseMs) {
        return Envelope.newBuilder().setProtocolVersion(2).setType(MessageType.LEASE_RENEW)
                .setRequestId(rid)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder().setKey(key)
                        .setLeaseToken(token).setLeaseMs(leaseMs))
                .build();
    }

    private static Envelope release(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(2).setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(io.github.lamspace.openlatch.protocol.ReleaseRequest.newBuilder()
                        .setKey(key).setLeaseToken(token).setThreadId(1L))
                .build();
    }

    private static ClusterHarness.Node oneFollower(ClusterHarness h) {
        return h.nodes().stream().filter(x -> x.alive() && !x.isLeader()).findFirst().orElseThrow();
    }

    // ---------- spec 场景 1/4：Follower ACQUIRE 拒绝 + hint 正确且跟随换主 ----------

    @Test
    void followerAcquireRejectedWithAccurateHintThatTracksReelection() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            h.setProbesEnabled(false);
            Thread.sleep(1_500); // settle 掉探针遗留条目
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node follower = oneFollower(h);

            ClusterHarness.TestConn c = h.connect(follower);
            Envelope hello = c.hello(1);
            assertThat(hello.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
            // HELLO 提示（Leader 地址来自 client-addresses 合成表）
            assertThat(hello.getHelloResponse().getLeaderHint()).isEqualTo(leader.id);
            assertThat(hello.getHelloResponse().getLeaderAddress()).isEqualTo(h.clientAddressOf(leader.id));

            long before = leader.lastApplied();
            Envelope resp = c.request(acquire(2, "hk"));
            assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
            assertThat(resp.getAcquireResponse().getLeaderNodeId()).isEqualTo(leader.id);
            assertThat(resp.getAcquireResponse().getLeaderAddress()).isEqualTo(h.clientAddressOf(leader.id));
            assertThat(leader.lastApplied()).isEqualTo(before); // 拒绝零条目

            // 杀主重选：提示必须跟随新 Leader（原 follower 可能当选，故动态
            // 重选一个"当值 Follower"验证拒绝路径与 HELLO 两条路）
            h.stopNode(leader.id);
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l.id != leader.id;
            }, 30_000, "新 Leader 选出");
            ClusterHarness.Node newLeader = h.leader();
            ClusterHarness.Node postFollower = oneFollower(h);
            h.awaitTrue(() -> postFollower.runtime.leaderTracker().snapshot().leaderNodeId() == newLeader.id,
                    10_000, "Follower 提示视图跟随新 Leader");
            ClusterHarness.TestConn c2 = h.connect(postFollower);
            assertThat(c2.hello(3).getHelloResponse().getLeaderHint()).isEqualTo(newLeader.id);
            Envelope resp2 = c2.request(acquire(4, "hk"));
            assertThat(resp2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
            assertThat(resp2.getAcquireResponse().getLeaderNodeId()).isEqualTo(newLeader.id);
            assertThat(resp2.getAcquireResponse().getLeaderAddress())
                    .isEqualTo(h.clientAddressOf(newLeader.id));
        }
    }

    // ---------- spec 场景 2：home 会话跨"存活让位"续租/释放（转发车道） ----------

    @Test
    void relayRenewAndReleaseSurviveAliveLeaderStepdown() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 500)) {
            ClusterHarness.Node oldLeader = h.leader();
            ClusterHarness.TestConn holder = h.connect(oldLeader);
            holder.hello(1);
            Envelope g = holder.request(acquire(2, "rl"));
            long token = g.getAcquireResponse().getLeaseToken();
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            // 存活让位：原 Leader 进程/连接/会话全活，角色转 Follower（§8 行 2）
            int target = oneFollower(h).id;
            h.transferLeadership(target);
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l.id != oldLeader.id && !oldLeader.isLeader();
            }, 20_000, "Leadership 移交且原主降级存活");

            // home = 降级节点：RENEW/RELEASE 经转发车道由新主复制执行
            Envelope r = holder.request(renew(3, "rl", token, 30_000));
            assertThat(r.getLeaseRenewResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(r.getLeaseRenewResponse().getLeaseExpiresAtMs())
                    .isGreaterThan(System.currentTimeMillis() + 20_000);
            Envelope rel = holder.request(release(4, "rl", token));
            assertThat(rel.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(rel.getReleaseResponse().getFullyReleased()).isTrue();
            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "转发车道读写后副本一致");
        }
    }

    // ---------- 无多数派：写请求快速失败不悬挂 ----------

    @Test
    void minoritySurvivorRejectsWritesWithoutHanging() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3, 500)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node survivor = oneFollower(h);
            ClusterHarness.TestConn c = h.connect(survivor);
            assertThat(c.hello(1).getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);

            h.stopNode(leader.id);                       // 杀主
            h.stopNodeOtherThan(survivor.id, leader.id); // 再杀一 → 仅存单机无多数派
            h.awaitTrue(() -> !h.hasLeader(), 10_000, "仅剩单节点无 Leader");

            // ACQUIRE 10s 内必有 NOT_LEADER 应答（无悬挂）。提示为最后已知 Leader
            // 或 -1（视 Ratis 是否投 null 事件）——均不阻塞客户端（design D3：
            // 陈旧提示由客户端改连失败 + 强制发现兜底）。
            Envelope resp = c.request(acquire(2, "eg"));
            assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
            assertThat(resp.getAcquireResponse().getLeaderNodeId())
                    .isGreaterThanOrEqualTo(LeaderTracker.UNKNOWN_NODE_ID);
            // 视图仍可作答（成员表完整，诊断可用）
            ClusterView view = survivor.runtime.leaderTracker().clusterView();
            assertThat(view.getNodesCount()).isEqualTo(3);
            assertThat(view.getStatus()).isEqualTo(StatusCode.OK);
        }
    }

    // ---------- spec 场景 5：会话清理后转发被拒（本地预检 + 应用兜底映射） ----------

    @Test
    void relayRejectedForCleanedSessionWithoutNewEntry() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 500)) {
            ClusterHarness.Node survivor = oneFollower(h);
            ClusterHarness.TestConn c = h.connect(survivor);
            Envelope hello = c.hello(1);
            assertThat(hello.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
            long sid = hello.getHelloResponse().getSessionId();

            // 会话被清理（模拟失联批量清理路径）但连接仍开着：直接投 SESSION_CLOSE，
            // 不走 TestConn.disconnect（那会关 EmbeddedChannel 令后续出站不可读）。
            survivor.runtime.sessionCoordinator().submitClose(sid);
            h.awaitTrue(() -> !survivor.runtime.core().shadow().hasSession(sid),
                    15_000, "会话清理已应用至本副本");
            h.setProbesEnabled(false);
            Thread.sleep(1_500);

            long before = survivor.lastApplied();
            Envelope resp = c.request(renew(2, "whatever", 1L, 1_000L));
            assertThat(resp.getLeaseRenewResponse().getStatus()).isEqualTo(StatusCode.SESSION_EXPIRED);
            assertThat(survivor.lastApplied()).isEqualTo(before); // 本地预检拒绝，零转发条目
        }
    }

    /** Leader 应用点的未登记判定映射（REJECT_SESSION → SESSION_EXPIRED，纯函数）。 */
    @Test
    void applyLevelSessionRejectionMapsToSessionExpired() {
        Envelope msg = renew(9, "k", 5L, 1_000L);
        Envelope resp = ClusterRequestHandler.mapRenew(msg,
                ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build());
        assertThat(resp.getLeaseRenewResponse().getStatus()).isEqualTo(StatusCode.SESSION_EXPIRED);
    }

    // ---------- "单一数据源一致性"：HELLO/NOT_LEADER/CLUSTER_VIEW 三消费方 ----------

    @Test
    void threeConsumersReportSameLeader() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node follower = oneFollower(h);

            long viaHello = h.connect(follower).hello(1).getHelloResponse().getLeaderHint();
            long viaNotLeader = h.connect(follower).request(acquire(2, "sc"))
                    .getAcquireResponse().getLeaderNodeId();
            ClusterView view = follower.runtime.leaderTracker().clusterView();
            long viaView = view.getNodesList().stream()
                    .filter(NodeInfo::getIsLeader).map(NodeInfo::getNodeId).findFirst().orElse(-1L);

            assertThat(viaHello).isEqualTo(leader.id);
            assertThat(viaNotLeader).isEqualTo(viaHello);
            assertThat(viaView).isEqualTo(viaHello);
            // 地址同源：视图中 Leader 的地址与提示一致
            assertThat(view.getNodesList().stream().filter(n -> n.getNodeId() == leader.id)
                    .findFirst().orElseThrow().getAddress()).isEqualTo(h.clientAddressOf(leader.id));
        }
    }

    // ---------- cluster-node-lifecycle：经真实接入层（ServerSessionHandler）路由 ----------

    @Test
    void followerServesHelloPingAndClusterViewViaAccessLayer() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node follower = oneFollower(h);
            h.setProbesEnabled(false);
            Thread.sleep(1_500);

            ServerConfig config = ServerConfig.defaults();
            EmbeddedChannel ch = new EmbeddedChannel(new ServerSessionHandler(
                    null, config, follower.registry, null, follower.runtime));
            ch.pipeline().fireChannelActive();

            ch.writeInbound(Envelope.newBuilder()
                    .setProtocolVersion(2).setType(MessageType.HELLO).setRequestId(1L)
                    .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(2))
                    .build());
            Envelope hello = awaitOutbound(ch);
            assertThat(hello.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(hello.getProtocolVersion()).isEqualTo(2); // 版本回显
            assertThat(hello.getHelloResponse().getLeaderHint()).isEqualTo(leader.id);

            ch.writeInbound(Envelope.newBuilder()
                    .setProtocolVersion(2).setType(MessageType.CLUSTER_VIEW).setRequestId(2L)
                    .build());
            Envelope viewResp = awaitOutbound(ch);
            assertThat(viewResp.getType()).isEqualTo(MessageType.CLUSTER_VIEW);
            assertThat(viewResp.getRequestId()).isEqualTo(2L);
            assertThat(viewResp.getClusterView().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(viewResp.getClusterView().getNodesCount()).isEqualTo(3);
            assertThat(viewResp.getClusterView().getNodesList().stream()
                    .filter(NodeInfo::getIsLeader).map(NodeInfo::getNodeId).toList())
                    .containsExactly((long) leader.id);

            // PING 无应答但连接可用（活动信号）
            ch.writeInbound(Envelope.newBuilder()
                    .setProtocolVersion(2).setType(MessageType.PING).setRequestId(3L).build());
            Thread.sleep(300);
            ch.runPendingTasks();
            assertThat((Object) ch.readOutbound()).isNull();
            ch.finishAndReleaseAll();
        }
    }

    /** EmbeddedChannel 出站轮询（集群 HELLO 异步于应用回执）。 */
    private static Envelope awaitOutbound(EmbeddedChannel ch) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            ch.runPendingTasks();
            Object out = ch.readOutbound();
            if (out instanceof Envelope e) {
                return e;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new AssertionError("等待出站被中断");
            }
        }
        throw new AssertionError("出站应答超时");
    }
}
