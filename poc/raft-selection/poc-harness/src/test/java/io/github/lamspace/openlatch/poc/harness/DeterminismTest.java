package io.github.lamspace.openlatch.poc.harness;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.AcquirePayload;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftEntryType;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftLogEntry;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.SessionPayload;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 回放确定性与快照回灌单测（P2-01，spec「CoreEngine 零改动接入」场景）。
 */
class DeterminismTest {

    private static RaftLogEntry open(int sid, long wall) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.SESSION_OPEN).setSeq(1)
                .setWallClockMs(wall)
                .setCommandPayload(SessionPayload.newBuilder().setSessionId(sid).build().toByteString())
                .build();
    }

    private static RaftLogEntry acq(long sid, long reqId, String key, long wall, int lockType) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.LOCK_ACQUIRE_ENTRY).setSeq(reqId)
                .setWallClockMs(wall)
                .setCommandPayload(AcquirePayload.newBuilder()
                        .setSessionId(sid).setRequestId(reqId).setKey(key)
                        .setLockType(lockType).setThreadId(7).setRequestedLeaseMs(60_000)
                        .setQueueIfBusy(false)
                        .build().toByteString())
                .build();
    }

    private static String replay(List<RaftLogEntry> seq) {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        for (RaftLogEntry e : seq) {
            core.applyEntry(e.toByteArray());
        }
        return core.digest();
    }

    @Test
    void sameSequenceReplayedTwiceYieldsIdenticalDigest() {
        List<RaftLogEntry> seq = List.of(
                open(11, 1_000), open(12, 1_000),
                acq(11, 101, "a", 2_000, 0),
                acq(11, 102, "b", 3_000, 0),
                acq(12, 103, "c", 4_000, 2));
        assertThat(replay(seq)).isEqualTo(replay(seq));
    }

    @Test
    void leaseTimeComesFromEntryNotPhysicalClock() {
        List<RaftLogEntry> early = List.of(open(21, 5_000), acq(21, 201, "k", 5_000, 0));
        List<RaftLogEntry> late = List.of(open(21, 5_000_000), acq(21, 201, "k", 5_000_000, 0));
        // 同一逻辑条目、不同携带时刻 → 到期时刻不同（digest 不同）；
        // 各自重放稳定（与回放时的物理时钟无关）。
        assertThat(replay(early)).isNotEqualTo(replay(late));
        assertThat(replay(late)).isEqualTo(replay(late));
    }

    @Test
    void snapshotBundleRoundTripRestoresState() {
        List<RaftLogEntry> seq = List.of(
                open(31, 10_000),
                acq(31, 301, "x", 10_001, 0),
                acq(31, 302, "y", 10_002, 0),
                acq(31, 303, "z", 10_003, 2));
        LockStateMachineCore src = new LockStateMachineCore(new CoreConfig());
        for (RaftLogEntry e : seq) {
            src.applyEntry(e.toByteArray());
        }
        byte[] snap = src.snapshotBundle(1, 9).shadowBytes();

        LockStateMachineCore dst = new LockStateMachineCore(new CoreConfig());
        assertThat(dst.installSnapshot(snap)).isEqualTo(3);
        assertThat(dst.digest()).isEqualTo(src.digest());
        assertThat(dst.rebuildFailures).isZero();
    }
}
