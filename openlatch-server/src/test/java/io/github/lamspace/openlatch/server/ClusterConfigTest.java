package io.github.lamspace.openlatch.server;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 集群配置解析与校验测试（详设 §9，spec"集群配置体系"三场景）。
 *
 * <p>判据：默认即单机（零配置回落）；{@code enabled=true} 必填项缺失/非法
 * 以指明配置键的异常快速失败（不静默降级）；合法值逐项解析。
 */
class ClusterConfigTest {

    private static Properties props(String... kv) {
        Properties p = new Properties();
        for (int i = 0; i < kv.length; i += 2) {
            p.setProperty(kv[i], kv[i + 1]);
        }
        return p;
    }

    @Test
    void defaultsAreSingleMode() {
        assertThat(ClusterConfig.disabled().enabled()).isFalse();
        assertThat(ClusterConfig.fromProperties(new Properties()).enabled()).isFalse();
        assertThat(ClusterConfig.fromProperties(new Properties()).raftPort())
                .isEqualTo(ClusterConfig.DEFAULT_RAFT_PORT);
    }

    @Test
    void enabledWithoutNodeIdFailsNamingKey() {
        Properties p = props(
                "openlatch.cluster.enabled", "true",
                "openlatch.cluster.peers", "1@localhost:9411");
        assertThatThrownBy(() -> ClusterConfig.fromProperties(p))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.cluster.node-id");
    }

    @Test
    void enabledWithoutPeersFailsNamingKey() {
        Properties p = props("openlatch.cluster.enabled", "true", "openlatch.cluster.node-id", "2");
        assertThatThrownBy(() -> ClusterConfig.fromProperties(p))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.cluster.peers");
    }

    @Test
    void peersMustIncludeSelf() {
        Properties p = props(
                "openlatch.cluster.enabled", "true",
                "openlatch.cluster.node-id", "1",
                "openlatch.cluster.peers", "2@h:9411,3@h:9412");
        assertThatThrownBy(() -> ClusterConfig.fromProperties(p))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("必须包含本节点");
    }

    @Test
    void malformedPeerRejected() {
        Properties p = props(
                "openlatch.cluster.enabled", "true",
                "openlatch.cluster.node-id", "1",
                "openlatch.cluster.peers", "1@h:abc");
        assertThatThrownBy(() -> ClusterConfig.fromProperties(p))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("openlatch.cluster.peers");
    }

    @Test
    void legalConfigParsesAllKeys() {
        Properties p = props(
                "openlatch.cluster.enabled", "true",
                "openlatch.cluster.node-id", "2",
                "openlatch.cluster.peers", "1@a:9411,2@b:9412,3@c:9413",
                "openlatch.cluster.raft-port", "9412",
                "openlatch.cluster.data-dir", "/tmp/ol2",
                "openlatch.cluster.snapshot-threshold", "5000",
                "openlatch.cluster.election-timeout-ms", "2000");
        ClusterConfig c = ClusterConfig.fromProperties(p);
        assertThat(c.enabled()).isTrue();
        assertThat(c.nodeId()).isEqualTo(2);
        assertThat(c.peers()).hasSize(3);
        assertThat(c.dataDir()).isEqualTo("/tmp/ol2");
        assertThat(c.snapshotThreshold()).isEqualTo(5000L);
        assertThat(c.electionTimeoutMs()).isEqualTo(2000L);
        assertThat(c.selfPeerId()).isEqualTo("n2");
    }

    @Test
    void disabledSkipsValidationOfClusterOnlyFields() {
        // enabled=false：node-id/peers 等不参与校验（单机零配置回退保证）。
        ClusterConfig c = new ClusterConfig(false, 0, List.of(), -1, " ", -5, 0);
        c.validate();
    }
}
