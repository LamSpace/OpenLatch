package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.NodeInfo;
import io.github.lamspace.openlatch.server.ClusterConfig;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link LeaderTracker} 提示视图契约单元测试（s3 design D3/D4）：
 * {@code -1} 的确切语义（"本节点尚未取得 Leader 身份"——初始态或显式
 * no-leader 事件）、地址解析与缺映射降级、视图生成。
 *
 * <p>注意 leader 死亡过渡期 Ratis 不向 follower 投 null 事件，提示保持
 * 最后已知值属预期行为（陈旧性由客户端改连失败 + 强制发现兜底，design D3），
 * 本测试只钉死"事件→视图"的折算规则。
 */
class LeaderTrackerTest {

    /** 构造三节点配置（peers 恒备；地址表按开关）。 */
    private static ClusterConfig cfg(boolean withAddresses) {
        List<String> peers = List.of("1@h1:9411", "2@h2:9412", "3@h3:9413");
        List<String> addrs = withAddresses
                ? List.of("1@10.0.0.1:9410", "2@10.0.0.2:9410", "3@10.0.0.3:9410")
                : List.of();
        return new ClusterConfig(true, 1, peers, addrs, 9411, "/tmp/openlatch-tracker-test",
                1_000_000L, 3_000L);
    }

    @Test
    void initialStateIsUnknownHint() {
        LeaderTracker t = new LeaderTracker(cfg(true));
        assertThat(t.snapshot().unknown()).isTrue();
        assertThat(t.snapshot().leaderNodeId()).isEqualTo(LeaderTracker.UNKNOWN_NODE_ID);
        assertThat(t.snapshot().leaderAddress()).isEmpty();
        // 启动期（无任何 Leadership 事件）：视图无任何 leader 标记
        assertThat(t.clusterView().getNodesList()).noneMatch(NodeInfo::getIsLeader);
    }

    @Test
    void eventResolutionAndExplicitNoLeader() {
        LeaderTracker t = new LeaderTracker(cfg(true));
        t.onLeaderChanged(2);
        assertThat(t.snapshot().leaderNodeId()).isEqualTo(2);
        assertThat(t.snapshot().leaderAddress()).isEqualTo("10.0.0.2:9410");
        t.onLeaderChanged(LeaderTracker.UNKNOWN_NODE_ID);
        assertThat(t.snapshot().unknown()).isTrue();
        assertThat(t.snapshot().leaderAddress()).isEmpty();
    }

    @Test
    void addressDegradesToEmptyWithoutMapping() {
        LeaderTracker t = new LeaderTracker(cfg(false));
        t.onLeaderChanged(3);
        assertThat(t.snapshot().leaderNodeId()).isEqualTo(3);
        assertThat(t.snapshot().leaderAddress()).isEmpty(); // design D4 降级：空串 + 客户端种子发现
        assertThat(t.clusterView().getNodesList().stream()
                .filter(NodeInfo::getIsLeader).map(NodeInfo::getNodeId).toList())
                .containsExactly(3L);
        assertThat(t.clusterView().getNodesList().stream()
                .map(NodeInfo::getAddress).toList()).containsExactly("", "", "");
    }
}
