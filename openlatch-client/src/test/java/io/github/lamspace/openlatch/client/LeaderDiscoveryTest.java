package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.client.internal.ScriptedServer;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.ClusterView;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.NodeInfo;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 客户端 Leader 发现与故障转移单元测试（详设 §6.3 逐分支，变更
 * s3-leader-discovery-failover 3.4）：以 {@link ScriptedServer} 真实 TCP 桩
 * 驱动完整 {@link OpenLatchClient}（含 {@code ConnectionManager} 与获取车道
 * 编排），不起真集群。"failover 期间持锁不丢"的转发车道端到端形态归 3.5。
 */
@Timeout(value = 40, unit = TimeUnit.SECONDS)
class LeaderDiscoveryTest {

    /** OK 授予响应。 */
    private static Envelope acquireOk(Envelope req, long token) {
        return req.toBuilder().setAcquireResponse(AcquireResponse.newBuilder()
                .setStatus(StatusCode.OK).setLeaseToken(token)
                .setGrantedLeaseMs(30_000)
                .setLeaseExpiresAtMs(System.currentTimeMillis() + 30_000))
                .build();
    }

    /** NOT_LEADER 拒绝（随附提示）。 */
    private static Envelope notLeader(Envelope req, long nodeId, String addr) {
        return req.toBuilder().setAcquireResponse(AcquireResponse.newBuilder()
                .setStatus(StatusCode.NOT_LEADER).setLeaderNodeId(nodeId)
                .setLeaderAddress(addr))
                .build();
    }

    /** HELLO 应答。 */
    private static Envelope hello(Envelope req, long sessionId, long leaderHint, String leaderAddr) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(sessionId)
                .setServerProtocolVersion(2).setDefaultLeaseMs(30_000)
                .setLeaderHint(leaderHint).setLeaderAddress(leaderAddr))
                .build();
    }

    /** 集群视图应答（单成员表）。 */
    private static Envelope view(Envelope req, long leaderNodeId, String leaderAddr) {
        return req.toBuilder().setClusterView(ClusterView.newBuilder()
                .setStatus(StatusCode.OK)
                .addNodes(NodeInfo.newBuilder().setNodeId(1)
                        .setAddress("127.0.0.1:1").setIsLeader(false))
                .addNodes(NodeInfo.newBuilder().setNodeId(leaderNodeId)
                        .setAddress(leaderAddr).setIsLeader(true))
                .build()).build();
    }

    private static AcquireSpec spec(String key) {
        return new AcquireSpec(key, LockType.REENTRANT,
                Thread.currentThread().getId(), 30_000, -1);
    }

    @Test
    void startupHelloHintConnectsDirectToLeader() throws Exception {
        // 种子 A 为 Follower：HELLO 提示 Leader=B → 新获取应直达 B，A 不见 ACQUIRE。
        ScriptedServer b = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 2000, 2, ""); // B 自指（无地址映射形态）
            case LOCK_ACQUIRE -> acquireOk(req, 77L);
            default -> null;
        });
        ScriptedServer a = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 1000, 2, b.address());
            default -> null;
        });
        try (a; b) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(a.address())
                    .defaultWaitTimeout(Duration.ofSeconds(15))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                assertThat(client.awaitAcquireLaneReady(10_000))
                        .as("启动直连发现：获取车道应在预算内建立").isTrue();
                LockGrant grant = client.acquireAsync(spec("k1")).get(20, TimeUnit.SECONDS);
                assertThat(grant.leaseToken()).isEqualTo(77L);
            }
            assertThat(b.countType(MessageType.LOCK_ACQUIRE)).isEqualTo(1);
            assertThat(a.countType(MessageType.LOCK_ACQUIRE)).isZero();
        }
    }

    @Test
    void notLeaderRedirectReplaysOnceWithSingleGrant() throws Exception {
        // home=A（启动提示不可用不改道）；ACQUIRE 收 NOT_LEADER+hint(B)
        // → 建获取车道 B 重放，调用方恰一次成功、无重复授予。
        ScriptedServer b = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 2000, -1, "");
            case LOCK_ACQUIRE -> acquireOk(req, 88L);
            default -> null;
        });
        ScriptedServer a = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 1000, -1, "");
            case LOCK_ACQUIRE -> notLeader(req, 2, b.address());
            default -> null;
        });
        try (a; b) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(a.address())
                    .defaultWaitTimeout(Duration.ofSeconds(15))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                LockGrant grant = client.acquireAsync(spec("k2")).get(20, TimeUnit.SECONDS);
                assertThat(grant.leaseToken()).isEqualTo(88L);
            }
            assertThat(a.countType(MessageType.LOCK_ACQUIRE)).isEqualTo(1);
            assertThat(b.countType(MessageType.LOCK_ACQUIRE)).isEqualTo(1);
        }
    }

    @Test
    void threeNotLeaderResponsesForceSeedClusterViewDiscovery() throws Exception {
        // A 连续回 hint=-1（选举空窗）：同 requestId 原地重发计满 3 次 →
        // 对种子扇出 CLUSTER_VIEW → 视图指出 B → 改道 B 授予，一次成功。
        AtomicInteger acquiresOnA = new AtomicInteger();
        ScriptedServer b = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 3000, -1, "");
            case LOCK_ACQUIRE -> acquireOk(req, 99L);
            default -> null;
        });
        // A 的 HELLO 报 hint=3 但地址空（服务端未配置 client-addresses）：
        // 启动不改道（无址）、发现探针走 CLUSTER_VIEW 自报兜底（design D4）。
        ScriptedServer a = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 1000, 3, "");
            case LOCK_ACQUIRE -> {
                acquiresOnA.incrementAndGet();
                yield notLeader(req, -1, "");
            }
            case CLUSTER_VIEW -> view(req, 3, b.address());
            default -> null;
        });
        try (a; b) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .seeds(a.address(), b.address())
                    .defaultWaitTimeout(Duration.ofSeconds(20))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                LockGrant grant = client.acquireAsync(spec("k3")).get(25, TimeUnit.SECONDS);
                assertThat(grant.leaseToken()).isEqualTo(99L);
            }
            assertThat(acquiresOnA.get()).isEqualTo(3);            // 阈值恰为 3
            assertThat(a.countType(MessageType.CLUSTER_VIEW)).isGreaterThanOrEqualTo(1);
            assertThat(b.countType(MessageType.LOCK_ACQUIRE)).isEqualTo(1); // 改道重放恰一次
        }
    }

    @Test
    void sameRequestIdReusedDuringElectionWindow() throws Exception {
        // hint=-1 原地重发口径：同一 requestId 复用于同会话重发（服务端幂等前提）。
        AtomicInteger seen = new AtomicInteger();
        ScriptedServer a = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req, 1000, -1, "");
            }
            if (req.getType() == MessageType.LOCK_ACQUIRE) {
                return seen.incrementAndGet() < 3
                        ? notLeader(req, -1, "") : acquireOk(req, 55L);
            }
            return null;
        });
        try (a) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(a.address())
                    .defaultWaitTimeout(Duration.ofSeconds(15))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                LockGrant grant = client.acquireAsync(spec("k4")).get(20, TimeUnit.SECONDS);
                assertThat(grant.leaseToken()).isEqualTo(55L);
            }
            assertThat(a.received().stream()
                    .filter(e -> e.getType() == MessageType.LOCK_ACQUIRE)
                    .map(Envelope::getRequestId).distinct().count())
                    .isEqualTo(1); // 三次尝试同 requestId（同会话幂等复用）
        }
    }

    @Test
    void fastFailsWithinRequestBudgetWhenNoLeader() {
        // 全部种子不可达：获取立即以"服务不可用"失败，不无界等待（快速失败优先）。
        OpenLatchClient client = OpenLatchClient.builder()
                .seeds("127.0.0.1:1", "127.0.0.1:2")
                .requestTimeout(Duration.ofSeconds(2))
                .defaultWaitTimeout(Duration.ofSeconds(3))
                .build();
        try {
            assertThatThrownBy(() -> client.acquireAsync(spec("never"))
                    .get(6, TimeUnit.SECONDS))
                    .hasCauseInstanceOf(ServerUnavailableException.class);
        } finally {
            client.close();
        }
    }
}
