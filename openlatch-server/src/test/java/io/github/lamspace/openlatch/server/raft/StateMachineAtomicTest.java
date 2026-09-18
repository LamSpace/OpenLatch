package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.protocol.AtomicOp;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ATOMIC 复制路径用例组：写条目应用产应答四元组、跨副本回放逐位一致、
 * GET 条目重放零迁移（摘要不变）、同槽重发应用层幂等、镜像与影子表登记、
 * 到期事件不触原子条目（含 Latch 同型防御）、未知/坏载荷条目 error 路径。
 */
class StateMachineAtomicTest {

    /** 以序列化字节应用全序列到一个内核并返回之。 */
    private static LockStateMachineCore replayToCore(List<RaftLogEntry> seq) {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        for (RaftLogEntry e : seq) {
            core.applyEntry(e.toByteArray());
        }
        return core;
    }

    @Test
    void atomicWriteAppliesWithQuad() throws Exception {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        core.applyEntry(RaftEntrySamples.sessionOpen(11, 1_000, 1).toByteArray());
        ApplyResult set = ApplyResult.parseFrom(core.applyEntry(RaftEntrySamples.atomic(
                11, 101, "k", AtomicOp.ATOMIC_SET, LockType.LOCK_TYPE_ATOMIC_LONG,
                5, 0, 0, 100, 1, 2_000, 2).toByteArray()));
        assertThat(set.getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(set.getAtomicApplied()).isTrue();
        assertThat(set.getAtomicOldValue()).isEqualTo(100);
        assertThat(set.getAtomicValue()).isEqualTo(5);
        assertThat(set.getAtomicVersion()).isEqualTo(1);
        ApplyResult add = ApplyResult.parseFrom(core.applyEntry(RaftEntrySamples.atomic(
                11, 102, "k", AtomicOp.ATOMIC_ADD, LockType.LOCK_TYPE_ATOMIC_LONG,
                3, 0, 0, 0, 2, 2_500, 3).toByteArray()));
        assertThat(add.getAtomicOldValue()).isEqualTo(5);
        assertThat(add.getAtomicValue()).isEqualTo(8);
        assertThat(add.getAtomicVersion()).isEqualTo(2);
    }

    @Test
    void crossReplicaReplayIdentical() throws Exception {
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(21, 1_000, 1),
                RaftEntrySamples.sessionOpen(22, 1_000, 2),
                RaftEntrySamples.atomic(21, 101, "k", AtomicOp.ATOMIC_SET,
                        LockType.LOCK_TYPE_ATOMIC_LONG, 5, 0, 0, 0, 1, 2_000, 3),
                RaftEntrySamples.atomic(22, 102, "k", AtomicOp.ATOMIC_CAS,
                        LockType.LOCK_TYPE_ATOMIC_LONG, 9, 5, 0, 0, 1, 2_100, 4),
                RaftEntrySamples.atomic(21, 103, "k", AtomicOp.ATOMIC_CAS_STAMPED,
                        LockType.LOCK_TYPE_ATOMIC_LONG, 7, 9, 2, 0, 2, 2_200, 5),
                RaftEntrySamples.atomic(22, 104, "k", AtomicOp.ATOMIC_GET,
                        LockType.LOCK_TYPE_ATOMIC_LONG, 0, 0, 0, 0, 0, 2_300, 6));
        LockStateMachineCore a = replayToCore(seq);
        LockStateMachineCore b = replayToCore(seq);
        assertThat(a.applyFailures()).isZero();
        assertThat(b.applyFailures()).isZero();
        assertThat(a.digest()).isEqualTo(b.digest());
        assertThat(a.shadow().adminEntry("k")).isNotNull();
        assertThat(a.shadow().adminEntry("k").atomicVersion()).isEqualTo(3);
        assertThat(a.shadow().adminEntry("k").atomicValue()).isEqualTo(7);
    }

    @Test
    void getEntriesAreDigestNeutral() throws Exception {
        List<RaftLogEntry> withGets = List.of(
                RaftEntrySamples.sessionOpen(31, 1_000, 1),
                RaftEntrySamples.atomic(31, 101, "k", AtomicOp.ATOMIC_SET,
                        LockType.LOCK_TYPE_ATOMIC_LONG, 5, 0, 0, 0, 1, 2_000, 2),
                RaftEntrySamples.atomic(31, 102, "k", AtomicOp.ATOMIC_GET,
                        LockType.LOCK_TYPE_ATOMIC_LONG, 0, 0, 0, 0, 0, 2_100, 3),
                RaftEntrySamples.atomic(31, 103, "k", AtomicOp.ATOMIC_GET,
                        LockType.LOCK_TYPE_ATOMIC_LONG, 0, 0, 0, 0, 0, 2_200, 4));
        List<RaftLogEntry> withoutGets = List.of(
                RaftEntrySamples.sessionOpen(31, 1_000, 1),
                RaftEntrySamples.atomic(31, 101, "k", AtomicOp.ATOMIC_SET,
                        LockType.LOCK_TYPE_ATOMIC_LONG, 5, 0, 0, 0, 1, 2_000, 2));
        assertThat(replayToCore(withGets).digest()).isEqualTo(replayToCore(withoutGets).digest());
    }

    @Test
    void sameSlotReplayAtApplyLevelDoesNotDoubleAdvance() throws Exception {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        core.applyEntry(RaftEntrySamples.sessionOpen(41, 1_000, 1).toByteArray());
        RaftLogEntry add = RaftEntrySamples.atomic(41, 101, "k", AtomicOp.ATOMIC_ADD,
                LockType.LOCK_TYPE_ATOMIC_LONG, 1, 0, 0, 0, 7, 2_000, 2);
        ApplyResult first = ApplyResult.parseFrom(core.applyEntry(add.toByteArray()));
        // 换请求 id 但同 (session, op_seq) 的重发条目。
        RaftLogEntry retry = RaftEntrySamples.atomic(41, 999, "k", AtomicOp.ATOMIC_ADD,
                LockType.LOCK_TYPE_ATOMIC_LONG, 1, 0, 0, 0, 7, 2_500, 3);
        ApplyResult second = ApplyResult.parseFrom(core.applyEntry(retry.toByteArray()));
        assertThat(second.getAtomicApplied()).isTrue();
        assertThat(second.getAtomicValue()).isEqualTo(first.getAtomicValue());
        assertThat(second.getAtomicVersion()).isEqualTo(first.getAtomicVersion());
        assertThat(core.shadow().adminEntry("k").atomicValue()).isEqualTo(1);
    }

    @Test
    void expireEventDoesNotTouchAtomicOrLatch() throws Exception {
        // 混合序列：原子写、屏障定型、锁获取（短租约）+ 到期条目——到期清扫
        // 后原子值与屏障计数存续，摘要跨副本仍一致。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(51, 1_000, 1),
                RaftEntrySamples.atomic(51, 101, "ak", AtomicOp.ATOMIC_SET,
                        LockType.LOCK_TYPE_ATOMIC_LONG, 42, 0, 0, 0, 1, 2_000, 2),
                RaftEntrySamples.latchCountDown(51, "lk", 0, 3, 2_100, 3),
                RaftEntrySamples.acquireWithWait(51, 102, "mk", 2_200,
                        LockType.LOCK_TYPE_REENTRANT, 4, -1, 1_000, 7),
                RaftEntrySamples.expire("mk", 1, 10_000, 5));
        LockStateMachineCore core = replayToCore(seq);
        assertThat(core.applyFailures()).isZero();
        assertThat(core.shadow().adminEntry("ak").atomicValue()).isEqualTo(42);
        assertThat(core.shadow().latchCount("lk")).isEqualTo(3);
        assertThat(core.shadow().isHeld("mk")).isFalse();
        assertThat(replayToCore(seq).digest()).isEqualTo(core.digest());
    }

    @Test
    void malformedAtomicEntriesTakeErrorPath() throws Exception {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        core.applyEntry(RaftEntrySamples.sessionOpen(61, 1_000, 1).toByteArray());
        // 未登记会话。
        ApplyResult noSession = ApplyResult.parseFrom(core.applyEntry(RaftEntrySamples.atomic(
                999, 101, "k", AtomicOp.ATOMIC_SET, LockType.LOCK_TYPE_ATOMIC_LONG,
                1, 0, 0, 0, 1, 2_000, 2).toByteArray()));
        assertThat(noSession.getStatus()).isEqualTo(ApplyStatus.REJECT_SESSION);
        // 非原子形态（SEMAPHORE 打进原子通道）。
        ApplyResult wrongKind = ApplyResult.parseFrom(core.applyEntry(RaftEntrySamples.atomic(
                61, 102, "k", AtomicOp.ATOMIC_SET, LockType.LOCK_TYPE_SEMAPHORE,
                1, 0, 0, 0, 2, 2_000, 3).toByteArray()));
        assertThat(wrongKind.getStatus()).isEqualTo(ApplyStatus.INVALID_REQUEST);
        // 锁 key 上的原子操作：家族互拒。
        core.applyEntry(RaftEntrySamples.acquire(61, 103, "lockk", 2_100,
                LockType.LOCK_TYPE_REENTRANT, 4).toByteArray());
        ApplyResult crossFamily = ApplyResult.parseFrom(core.applyEntry(RaftEntrySamples.atomic(
                61, 104, "lockk", AtomicOp.ATOMIC_SET, LockType.LOCK_TYPE_ATOMIC_LONG,
                1, 0, 0, 0, 3, 2_200, 5).toByteArray()));
        assertThat(crossFamily.getStatus()).isEqualTo(ApplyStatus.INVALID_REQUEST);
        assertThat(core.applyFailures()).isZero();
    }
}
