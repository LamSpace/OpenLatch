package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.protocol.AtomicOp;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 有值引用复制路径用例组（v6）：引用写条目应用产载荷应答四元组、跨副本
 * 回放逐字节一致（摘要含载荷）、GET 条目重放零迁移、同槽重发应用层幂等、
 * 镜像登记、超限条目照常回放（状态机无钳制通道——分歧不可能性的构造性
 * 证明：apply 路径不接触 {@code maxValueBytes}，日志内容视为已钳制）。
 */
class StateMachineAtomicRefTest {

    /** 以序列化字节应用全序列到一个内核并返回之。 */
    private static LockStateMachineCore replayToCore(List<RaftLogEntry> seq) {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        for (RaftLogEntry e : seq) {
            core.applyEntry(e.toByteArray());
        }
        return core;
    }

    /** 生成 {@code n} 字节可判稳载荷。 */
    private static byte[] payload(int n, int seed) {
        byte[] v = new byte[n];
        for (int i = 0; i < n; i++) {
            v[i] = (byte) (i * 31 + seed);
        }
        return v;
    }

    @Test
    void refWriteAppliesWithPayloadQuad() throws Exception {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        core.applyEntry(RaftEntrySamples.sessionOpen(11, 1_000, 1).toByteArray());
        ApplyResult set = ApplyResult.parseFrom(core.applyEntry(RaftEntrySamples.atomicRefSample(
                11, 101, "r", AtomicOp.ATOMIC_SET, payload(128, 3), null, payload(1, 9),
                0, 1, 2_000, 2).toByteArray()));
        assertThat(set.getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(set.getAtomicApplied()).isTrue();
        assertThat(set.getAtomicOldValueBytes().toByteArray()).containsExactly(9);
        assertThat(set.getAtomicValueBytes().toByteArray()).containsExactly(payload(128, 3));
        assertThat(set.getAtomicVersion()).isEqualTo(1);
        assertThat(set.getAtomicValue()).isZero(); // 标量值位对引用形态恒不出现

        // 落 null 态：value_bytes 缺省、old 回显。
        ApplyResult cleared = ApplyResult.parseFrom(core.applyEntry(RaftEntrySamples
                .atomicRefSample(11, 102, "r", AtomicOp.ATOMIC_SET, null, null, null,
                        0, 2, 2_500, 3).toByteArray()));
        assertThat(cleared.getAtomicApplied()).isTrue();
        assertThat(cleared.hasAtomicValueBytes()).isFalse();
        assertThat(cleared.getAtomicOldValueBytes().toByteArray()).containsExactly(payload(128, 3));
        assertThat(cleared.getAtomicVersion()).isEqualTo(2);
    }

    @Test
    void crossReplicaPayloadReplayByteIdentical() throws Exception {
        byte[] big = payload(4096, 5);
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(21, 1_000, 1),
                RaftEntrySamples.sessionOpen(22, 1_000, 2),
                RaftEntrySamples.atomicRefSample(21, 101, "r", AtomicOp.ATOMIC_SET,
                        big, null, null, 0, 1, 2_000, 3),
                RaftEntrySamples.atomicRefSample(22, 102, "r", AtomicOp.ATOMIC_CAS,
                        payload(3, 1), big, null, 0, 1, 2_100, 4),
                RaftEntrySamples.atomicRefSample(21, 103, "r", AtomicOp.ATOMIC_CAS_STAMPED,
                        payload(0, 2), payload(3, 1), null, 2, 2, 2_200, 5),
                RaftEntrySamples.atomicRefSample(22, 104, "r", AtomicOp.ATOMIC_GET,
                        null, null, null, 0, 0, 2_300, 6));
        LockStateMachineCore a = replayToCore(seq);
        LockStateMachineCore b = replayToCore(seq);
        assertThat(a.applyFailures()).isZero();
        assertThat(b.applyFailures()).isZero();
        // 摘要含载荷字节：逐位一致即载荷跨副本确定。
        assertThat(a.digest()).isEqualTo(b.digest());
        // 终态：CAS_STAMPED 落空字节串（长度 0 但 presence 在），版本 3。
        assertThat(a.shadow().adminEntry("r").refValue()).isEmpty();
        assertThat(a.shadow().adminEntry("r").atomicVersion()).isEqualTo(3);
    }

    @Test
    void refGetEntriesAreDigestNeutral() {
        List<RaftLogEntry> withGets = List.of(
                RaftEntrySamples.sessionOpen(31, 1_000, 1),
                RaftEntrySamples.atomicRefSample(31, 101, "r", AtomicOp.ATOMIC_SET,
                        payload(64, 1), null, null, 0, 1, 2_000, 2),
                RaftEntrySamples.atomicRefSample(31, 102, "r", AtomicOp.ATOMIC_GET,
                        null, null, null, 0, 0, 2_100, 3),
                RaftEntrySamples.atomicRefSample(31, 103, "r", AtomicOp.ATOMIC_GET,
                        null, null, null, 0, 0, 2_200, 4));
        List<RaftLogEntry> withoutGets = List.of(
                RaftEntrySamples.sessionOpen(31, 1_000, 1),
                RaftEntrySamples.atomicRefSample(31, 101, "r", AtomicOp.ATOMIC_SET,
                        payload(64, 1), null, null, 0, 1, 2_000, 2));
        assertThat(replayToCore(withGets).digest())
                .isEqualTo(replayToCore(withoutGets).digest());
    }

    @Test
    void sameSlotReplayAtApplyLevelDoesNotDoubleApply() throws Exception {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        core.applyEntry(RaftEntrySamples.sessionOpen(41, 1_000, 1).toByteArray());
        byte[] big = payload(256, 7);
        ApplyResult first = ApplyResult.parseFrom(core.applyEntry(RaftEntrySamples
                .atomicRefSample(41, 101, "r", AtomicOp.ATOMIC_SET, big, null, null,
                        0, 9, 2_000, 2).toByteArray()));
        assertThat(first.getAtomicVersion()).isEqualTo(1);
        // 同 (session, opSeq) 异载荷条目重放：返回原应答，不重复落值。
        ApplyResult replay = ApplyResult.parseFrom(core.applyEntry(RaftEntrySamples
                .atomicRefSample(41, 102, "r", AtomicOp.ATOMIC_SET, payload(1, 8), null, null,
                        0, 9, 2_100, 3).toByteArray()));
        assertThat(replay.getAtomicApplied()).isTrue();
        assertThat(replay.getAtomicValueBytes().toByteArray()).containsExactly(big);
        assertThat(replay.getAtomicVersion()).isEqualTo(1);
        assertThat(core.shadow().adminEntry("r").refValue()).containsExactly(big);
    }

    @Test
    void oversizePayloadReplaysNormallyBecauseClampIsIngressOnly() throws Exception {
        // 构造性证明：状态机内核只接 CoreConfig（无 maxValueBytes 通道），
        // apply 引用条目时不存在尺寸判定路径——含 8KB 载荷（超默认 4KB 钳制
        // 两倍）的条目照常落值，任何副本回放结果一致（分歧不可能）。
        byte[] oversize = payload(8192, 11);
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(51, 1_000, 1),
                RaftEntrySamples.atomicRefSample(51, 101, "r", AtomicOp.ATOMIC_SET,
                        oversize, null, null, 0, 1, 2_000, 2));
        LockStateMachineCore a = replayToCore(seq);
        LockStateMachineCore b = replayToCore(seq);
        assertThat(a.applyFailures()).isZero();
        assertThat(a.digest()).isEqualTo(b.digest());
        assertThat(a.shadow().adminEntry("r").refValue()).containsExactly(oversize);
    }
}
