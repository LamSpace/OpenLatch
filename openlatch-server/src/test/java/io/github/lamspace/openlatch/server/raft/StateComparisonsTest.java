package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.raft.SnapshotHolder;
import io.github.lamspace.openlatch.protocol.raft.SnapshotLock;
import io.github.lamspace.openlatch.protocol.raft.SnapshotState;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 全量比对工具自测（6.1 交付判据："工具本身正确"——S4 快照比对以它为唯一
 * 裁判，裁判 MUST 先于使用被验证）。
 */
class StateComparisonsTest {

    private static SnapshotState state(String key, long token, long expires, int holderCount) {
        return SnapshotState.newBuilder()
                .addLocks(SnapshotLock.newBuilder()
                        .setKey(key).setLockType(io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_REENTRANT)
                        .setLeaseToken(token)
                        .setExpiresAtMs(expires).setLeaseMs(30_000)
                        .addHolders(SnapshotHolder.newBuilder()
                                .setSessionId(7).setThreadId(1).setCount(holderCount)))
                .addSessions(7)
                .build();
    }

    @Test
    void identicalStatesProduceEmptyDiff() {
        assertThat(StateComparisons.diff(state("k", 1, 100, 2), state("k", 1, 100, 2))).isEmpty();
    }

    @Test
    void fieldDivergencesAreListed() {
        List<String> d = StateComparisons.diff(state("k", 1, 100, 2), state("k", 9, 200, 3));
        assertThat(d).anyMatch(s -> s.contains("leaseToken"));
        assertThat(d).anyMatch(s -> s.contains("expiresAtMs"));
        assertThat(d).anyMatch(s -> s.contains("holders"));
    }

    @Test
    void missingAndExtraKeysAreListed() {
        assertThat(StateComparisons.diff(state("a", 1, 1, 1), state("b", 1, 1, 1)))
                .anyMatch(s -> s.startsWith("a:") && s.contains("缺失"))
                .anyMatch(s -> s.startsWith("b:") && s.contains("多余"));
    }

    @Test
    void digestAgreementAssertsAndWaits() {
        Supplier<String> ok = () -> "abc";
        StateComparisons.assertDigestsAgree(Map.of("n1", ok, "n2", ok));
        assertThatThrownBy(() -> StateComparisons.assertDigestsAgree(
                Map.of("n1", ok, "n2", () -> "xyz")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("不一致");
        // 收敛等待：第三源先"偏离"后立即一致不超时。
        long start = System.currentTimeMillis();
        StateComparisons.awaitDigestsAgree(
                Map.of("n1", ok, "n2", ok), 2_000);
        assertThat(System.currentTimeMillis() - start).isLessThan(500);
    }
}
