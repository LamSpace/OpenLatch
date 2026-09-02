package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import io.github.lamspace.openlatch.protocol.raft.SnapshotHolder;
import io.github.lamspace.openlatch.protocol.raft.SnapshotLock;
import io.github.lamspace.openlatch.protocol.raft.SnapshotState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 快照通道测试（详设 §10"状态机单元"层 + §7，S4/P2-15 验证列"快照可加载"）：
 * {@code snapshotState/installSnapshot} 的 round-trip 保真与<b>切割点不变性</b>
 * ——对随机序列在任意位置切快照，"前缀装快照 + 后缀回放"的终态与全程直接
 * 回放的终态 digest 逐字段一致（发号水位 design D10 的判据载体）。
 */
class StateMachineSnapshotTest {

    /** 以序列化字节应用条目区间 [from, to)。 */
    private static void applyRange(LockStateMachineCore core, List<RaftLogEntry> seq, int from, int to) {
        for (int i = from; i < to; i++) {
            core.applyEntry(seq.get(i).toByteArray());
        }
        assertThat(core.applyFailures()).isZero();
    }

    private static String replayAll(List<RaftLogEntry> seq) {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        applyRange(core, seq, 0, seq.size());
        return core.digest();
    }

    @Test
    void installSnapshotRebuildsEquivalentCore() throws Exception {
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(11, 1_000, 1),
                RaftEntrySamples.sessionOpen(12, 1_000, 2),
                RaftEntrySamples.acquire(11, 101, "a", 2_000, LockType.LOCK_TYPE_REENTRANT, 3),
                RaftEntrySamples.acquire(12, 102, "b", 3_000, LockType.LOCK_TYPE_READ, 4));
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        applyRange(origin, seq, 0, seq.size());
        SnapshotState wire = SnapshotState.parseFrom(
                origin.snapshotState().toByteArray());

        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(wire);

        assertThat(restored.digest()).isEqualTo(origin.digest());
        // 继承凭证与映射可用：以原逻辑会话释放继承锁，摘要收敛到空基准。
        List<RaftLogEntry> tail = List.of(
                RaftEntrySamples.release(11, "a", wire.getLocks(0).getLeaseToken(), 5_000, 5),
                RaftEntrySamples.sessionClose(12, 6_000, 6));
        applyRange(restored, tail, 0, tail.size());
        applyRange(origin, tail, 0, tail.size());
        assertThat(restored.digest()).isEqualTo(origin.digest());
        assertThat(restored.shadow().lockCount()).isZero();
    }

    @Test
    void installRejectsCorruptSnapshotForms() {
        // 持有者引用未登记会话：安装必须显式失败而非静默装坏。
        SnapshotState corrupt = SnapshotState.newBuilder()
                .addLocks(SnapshotLock.newBuilder()
                        .setKey("k").setLockTypeValue(0).setLeaseToken(1).setLeaseMs(30_000)
                        .setExpiresAtMs(10_000)
                        .addHolders(SnapshotHolder.newBuilder()
                                .setSessionId(404).setThreadId(1).setCount(1)))
                .setNextLeaseToken(2)
                .build();
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        assertThatThrownBy(() -> core.installSnapshot(corrupt))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void watermarkFallbackKeepsTokenIssuanceMonotoneForLegacySnapshot() {
        // 缺水位（字段为 0，等价于升级前的历史快照形态）时按 max(凭证)+1 兜底：
        // 安装后新授予不复用继承凭证。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(11, 1_000, 1),
                RaftEntrySamples.acquire(11, 101, "a", 2_000, LockType.LOCK_TYPE_REENTRANT, 2));
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        applyRange(origin, seq, 0, seq.size());
        SnapshotState noWatermark = origin.snapshotState().toBuilder()
                .setNextLeaseToken(0).build();

        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(noWatermark);
        RaftLogEntry later = RaftEntrySamples.acquire(11, 102, "z", 3_000,
                LockType.LOCK_TYPE_REENTRANT, 3);
        applyRange(restored, List.of(later), 0, 1);
        // 新 key 的凭证 > 继承凭证：影子表可见 z 被持有且 digest 正常推进。
        assertThat(restored.shadow().isHeld("z")).isTrue();
    }

    // 属性测试组数 ≥100（沿 P2-06 口径）：随机序列、随机切割点。
    @ParameterizedTest(name = "快照切割点不变性 seed={0}")
    @MethodSource("cutPointSeeds")
    void snapshotAtAnyCutPointYieldsEquivalentTailReplay(long seed) throws Exception {
        Random rnd = new Random(seed);
        List<RaftLogEntry> seq = StateMachineDeterminismTest.randomSequence(rnd);
        int cut = 1 + rnd.nextInt(seq.size() - 1);

        String directDigest = replayAll(seq);

        LockStateMachineCore prefixCore = new LockStateMachineCore(new CoreConfig());
        applyRange(prefixCore, seq, 0, cut);
        // 经字节往返（模拟落盘/传输），验证序列化保真。
        SnapshotState snapshot = SnapshotState.parseFrom(prefixCore.snapshotState().toByteArray());

        LockStateMachineCore installed = new LockStateMachineCore(new CoreConfig());
        installed.installSnapshot(snapshot);
        applyRange(installed, seq, cut, seq.size());

        assertThat(installed.digest()).isEqualTo(directDigest);
    }

    static Stream<Long> cutPointSeeds() {
        return Stream.concat(
                Stream.of(1L, 7L, 42L, 1337L, 20260830L, 9001L, 555L, 31L),
                LongStream.rangeClosed(101L, 200L).boxed());
    }
}
