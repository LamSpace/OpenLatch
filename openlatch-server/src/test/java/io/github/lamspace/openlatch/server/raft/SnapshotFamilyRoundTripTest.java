/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.SnapshotState;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 快照扩展往返（含新条目的恢复往返）：
 * Semaphore/Latch 条目经 {@code snapshotState → installSnapshot} 全量回灌后，
 * 许可池、屏障计数、租约驱动与摘要逐项一致；锁条目序列化字节形不受
 * v3 新字段扰动（缺省字段不出现）。
 */
class SnapshotFamilyRoundTripTest {

    @Test
    void semaphoreAndLatchSurviveSnapshotInstall() throws Exception {
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(31, 1_000, 1).toByteArray());
        origin.applyEntry(RaftEntrySamples.sessionOpen(32, 1_000, 2).toByteArray());
        ApplyResult gs = ApplyResult.parseFrom(origin.applyEntry(
                RaftEntrySamples.acquireSemaphore(31, 101, "sem", 2_000, 2, 3, 3, 60_000)
                        .toByteArray()));
        origin.applyEntry(RaftEntrySamples.acquireSemaphore(32, 102, "sem", 3_000, 1, 0, 4, 60_000)
                .toByteArray());
        origin.applyEntry(RaftEntrySamples.latchCountDown(31, "lat", 0, 4, 4_000, 5).toByteArray());
        origin.applyEntry(RaftEntrySamples.latchCountDown(32, "lat", 1, 0, 5_000, 6).toByteArray());
        assertThat(gs.getLeaseToken()).isPositive();

        SnapshotState snap = origin.snapshotState();

        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(snap);
        assertThat(restored.digest()).isEqualTo(origin.digest());
        assertThat(restored.shadow().permitsAvailable("sem")).isEqualTo(0);
        assertThat(restored.shadow().isSemaphore("sem")).isTrue();
        assertThat(restored.shadow().hasLatch("lat")).isTrue();
        assertThat(restored.shadow().latchCount("lat")).isEqualTo(3);
        assertThat(restored.shadow().isHeldBy(32, 7, "sem")).isTrue();

        // 恢复后的租约到期驱动照常：条目时刻晚于继承到期（62_000）时强制回收
        // 共享租约的全部持有（与锁读者群同口径），条目随之消失、池视同满量。
        restored.applyEntry(RaftEntrySamples.expire("sem", gs.getLeaseToken(), 70_000, 7)
                .toByteArray());
        assertThat(restored.shadow().isSemaphore("sem")).isFalse();
        assertThat(restored.shadow().permitsAvailable("sem"))
                .isEqualTo(Integer.MAX_VALUE); // 条目回收：重建语义
    }

    @Test
    void atomicValueVersionAndSlotSurviveSnapshotInstall() throws Exception {
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(51, 1_000, 1).toByteArray());
        origin.applyEntry(RaftEntrySamples.sessionOpen(52, 1_000, 2).toByteArray());
        // 建条目（非零初值主张）+ 同槽 CAS + 跨会话 GET。
        ApplyResult init = ApplyResult.parseFrom(origin.applyEntry(RaftEntrySamples.atomic(
                51, 101, "at", io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_INTEGER,
                10, 0, 0, 100, 1, 2_000, 3).toByteArray()));
        assertThat(init.getAtomicVersion()).isEqualTo(1);
        origin.applyEntry(RaftEntrySamples.atomic(52, 102, "at",
                io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_CAS_STAMPED,
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_INTEGER,
                20, 10, 1, 0, 7, 3_000, 4).toByteArray());
        origin.applyEntry(RaftEntrySamples.atomic(51, 103, "at",
                io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_GET,
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_INTEGER,
                0, 0, 0, 0, 0, 3_500, 5).toByteArray());

        SnapshotState snap = origin.snapshotState();
        // 序列化含 v4 家族字段：定型初值与当前值可解析。
        var atomicLock = snap.getLocksList().stream()
                .filter(l -> l.getKey().equals("at")).findFirst().orElseThrow();
        assertThat(atomicLock.getAtomicInitial()).isEqualTo(100);
        assertThat(atomicLock.getAtomicValue()).isEqualTo(20);
        assertThat(atomicLock.getAtomicVersion()).isEqualTo(2);
        assertThat(atomicLock.getAtomicSlotSession()).isPositive();
        assertThat(atomicLock.getAtomicSlotOpSeq()).isEqualTo(7);

        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(snap);
        assertThat(restored.digest()).isEqualTo(origin.digest());
        assertThat(restored.shadow().adminEntry("at").lockType())
                .isEqualTo(io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_INTEGER_VALUE);
        assertThat(restored.shadow().adminEntry("at").atomicValue()).isEqualTo(20);
        assertThat(restored.shadow().adminEntry("at").atomicVersion()).isEqualTo(2);

        // 重启后命中同槽的迟到重发：值与版本不双推进（应答与槽一致）。
        ApplyResult retry = ApplyResult.parseFrom(restored.applyEntry(RaftEntrySamples.atomic(
                52, 999, "at", io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_CAS_STAMPED,
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_INTEGER,
                20, 10, 1, 0, 7, 4_000, 6).toByteArray()));
        assertThat(retry.getStatus())
                .isEqualTo(io.github.lamspace.openlatch.protocol.raft.ApplyStatus.OK);
        assertThat(retry.getAtomicVersion()).isEqualTo(2);
        assertThat(retry.getAtomicValue()).isEqualTo(20);

        // 恢复后新写照常推进版本（会话登记存续）。
        ApplyResult next = ApplyResult.parseFrom(restored.applyEntry(RaftEntrySamples.atomic(
                52, 104, "at", io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_ADD,
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_INTEGER,
                1, 0, 0, 0, 8, 5_000, 7).toByteArray()));
        assertThat(next.getAtomicValue()).isEqualTo(21);
        assertThat(next.getAtomicVersion()).isEqualTo(3);
    }

    @Test
    void lockEntrySerializationBytesUnchangedByV3Fields() throws Exception {
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(41, 1_000, 1).toByteArray());
        origin.applyEntry(RaftEntrySamples.acquire(41, 201, "lk",
                2_000, io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_REENTRANT, 2)
                .toByteArray());
        SnapshotState snap = origin.snapshotState();
        assertThat(snap.getLocks(0).getPermitsTotal()).isZero();
        assertThat(snap.getLocks(0).getLatchTotal()).isZero();
        // v3 字段对锁条目不出现于序列化流（proto3 缺省省略）：字节长与 v2 形态一致。
        assertThat(snap.getLocks(0).toByteArray())
                .doesNotContain(new byte[] {0x38}); // field 7 varint tag 不出现
    }
}
