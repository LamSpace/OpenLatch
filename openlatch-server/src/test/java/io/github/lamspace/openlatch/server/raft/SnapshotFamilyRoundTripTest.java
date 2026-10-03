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
        // 强断言四元组逐位回放（applied+old 命中槽的证明——仅 value/version
        // 相等不足以区分"重放"与"重执行后恰合"）。
        assertThat(retry.getAtomicApplied()).isTrue();
        assertThat(retry.getAtomicOldValue()).isEqualTo(10);
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
    void referencePayloadNullEmptyAndSlotSurviveSnapshotInstall() throws Exception {
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(71, 1_000, 1).toByteArray());
        // 非 null 初值主张建条目 + 空字节串态 + 同槽重放占位。
        origin.applyEntry(RaftEntrySamples.atomicRefSample(71, 101, "rn",
                io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                new byte[] {1, 2}, null, new byte[] {9}, 0, 1, 2_000, 2).toByteArray());
        origin.applyEntry(RaftEntrySamples.atomicRefSample(71, 102, "rn",
                io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                new byte[0], null, null, 0, 2, 2_100, 3).toByteArray());
        // null 值条目（无主张创建后落 null）：载荷字段全部缺省。
        origin.applyEntry(RaftEntrySamples.atomicRefSample(71, 103, "rnull",
                io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                null, null, null, 0, 3, 2_200, 4).toByteArray());
        // 恰限 4KB 大载荷。
        byte[] big = new byte[4096];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 7 + 1);
        }
        origin.applyEntry(RaftEntrySamples.atomicRefSample(71, 104, "rbig",
                io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                big, null, null, 0, 4, 2_300, 5).toByteArray());

        SnapshotState snap = origin.snapshotState();
        var rn = snap.getLocksList().stream()
                .filter(l -> l.getKey().equals("rn")).findFirst().orElseThrow();
        assertThat(rn.hasAtomicRefInitial()).isTrue();
        assertThat(rn.getAtomicRefInitial().toByteArray()).containsExactly(9);
        assertThat(rn.getAtomicRefValue().toByteArray()).isEmpty(); // 空串=EMPTY 非缺省
        assertThat(rn.hasAtomicRefSlotValue()).isTrue();
        assertThat(rn.getAtomicSlotOpSeq()).isEqualTo(2);
        var rnull = snap.getLocksList().stream()
                .filter(l -> l.getKey().equals("rnull")).findFirst().orElseThrow();
        assertThat(rnull.hasAtomicRefInitial()).isFalse(); // 无主张：缺省
        assertThat(rnull.hasAtomicRefValue()).isFalse();   // null 态：缺省（非 EMPTY）
        assertThat(rnull.getAtomicSlotOpSeq()).isEqualTo(3);

        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(snap);
        assertThat(restored.digest()).isEqualTo(origin.digest());
        assertThat(restored.shadow().adminEntry("rn").refValue()).isEmpty();
        assertThat(restored.shadow().adminEntry("rn").refInitial()).containsExactly(9);
        assertThat(restored.shadow().adminEntry("rnull").refValue()).isNull();
        assertThat(restored.shadow().adminEntry("rbig").refValue()).containsExactly(big);

        // 重启后命中槽重发：载荷应答原样回放，不双推进。
        ApplyResult replay = ApplyResult.parseFrom(restored.applyEntry(
                RaftEntrySamples.atomicRefSample(71, 999, "rn",
                        io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                        new byte[] {5}, null, null, 0, 2, 3_000, 6).toByteArray()));
        assertThat(replay.getStatus())
                .isEqualTo(io.github.lamspace.openlatch.protocol.raft.ApplyStatus.OK);
        assertThat(replay.getAtomicVersion()).isEqualTo(2);
        assertThat(replay.getAtomicValueBytes().toByteArray()).isEmpty();
        // 恢复后新写照常推进（null 态可被期望 null 的 CAS 建立）。
        ApplyResult casFromNull = ApplyResult.parseFrom(restored.applyEntry(
                RaftEntrySamples.atomicRefSample(71, 998, "rnull",
                        io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_CAS,
                        new byte[] {8}, null, null, 0, 9, 3_100, 7).toByteArray()));
        assertThat(casFromNull.getAtomicApplied()).isTrue();
        // rnull 重启前版本已 1（占槽写 null），恢复后新写推进至 2。
        assertThat(casFromNull.getAtomicVersion()).isEqualTo(2);
    }

    @Test
    void legacySnapshotWithoutReferenceFieldsInstallsAndTailRefWorks() throws Exception {
        // 旧版本（v4/v5）快照零 ref 字段——新码加载不坏（未知字段容忍的反向
        // 面：新字段缺省），且恢复后引用形态写入照常可用。
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(81, 1_000, 1).toByteArray());
        origin.applyEntry(RaftEntrySamples.atomic(81, 101, "at",
                io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_ATOMIC_LONG,
                5, 0, 0, 0, 1, 2_000, 2).toByteArray());
        SnapshotState snap = origin.snapshotState();
        assertThat(snap.getLocksList().get(0).hasAtomicRefValue()).isFalse();

        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(snap);
        assertThat(restored.digest()).isEqualTo(origin.digest());
        ApplyResult ref = ApplyResult.parseFrom(restored.applyEntry(
                RaftEntrySamples.atomicRefSample(81, 102, "r",
                        io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                        new byte[] {1}, null, null, 0, 1, 3_000, 3).toByteArray()));
        assertThat(ref.getStatus())
                .isEqualTo(io.github.lamspace.openlatch.protocol.raft.ApplyStatus.OK);
        assertThat(ref.getAtomicValueBytes().toByteArray()).containsExactly(1);
    }

    @Test
    void payloadBoundedSnapshotSizeRegression() {
        // 载荷快照尺寸治理回归（二档基建守门用例）：N 个恰限引用 key、
        // 每 key 多轮版本推进后，快照字节 ≤ N×(2×4096 + 常数)×安全系数，
        // 且随推进轮次零增长（版本历史不入快照）、安装与追赶回放正常。
        final int keys = 64;
        final int rounds = 8;
        final int maxBytes = 4096;
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(91, 1_000, 1).toByteArray());
        for (int round = 0; round < rounds; round++) {
            for (int k = 0; k < keys; k++) {
                byte[] payload = new byte[maxBytes];
                payload[0] = (byte) k;
                payload[1] = (byte) round;
                origin.applyEntry(RaftEntrySamples.atomicRefSample(91, 1000 + round * keys + k,
                        "rk-" + k, io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                        payload, null, null, 0, round * keys + k + 1,
                        2_000 + round * keys + k, 10 + round * keys + k).toByteArray());
            }
        }
        assertThat(origin.applyFailures()).isZero();
        byte[] snapBytes = origin.snapshotState().toByteArray();
        // 上界：每 key 至初值 0 份 + 当前值 1×4KB + 槽双份 2×4KB（最坏 3 份），
        // 常数 512B 容纳 key/序号/版本/家族字段；再乘 1.5 安全系数。
        long bound = (long) keys * (3L * maxBytes + 512) * 3 / 2;
        assertThat((long) snapBytes.length).isLessThanOrEqualTo(bound);

        // 轮次零增长的对照：单轮终态等价载荷分布下，多轮推进不放大快照
        // （以两轮重建对照——历史版本不入快照）。
        LockStateMachineCore lastRoundOnly = new LockStateMachineCore(new CoreConfig());
        lastRoundOnly.applyEntry(RaftEntrySamples.sessionOpen(91, 1_000, 1).toByteArray());
        for (int k = 0; k < keys; k++) {
            byte[] payload = new byte[maxBytes];
            payload[0] = (byte) k;
            payload[1] = (byte) (rounds - 1);
            lastRoundOnly.applyEntry(RaftEntrySamples.atomicRefSample(91, 2000 + k,
                    "rk-" + k, io.github.lamspace.openlatch.protocol.AtomicOp.ATOMIC_SET,
                    payload, null, null, 0, rounds * keys + k + 1,
                    9_000 + k, 10 + rounds * keys + k).toByteArray());
        }
        byte[] singleBytes = lastRoundOnly.snapshotState().toByteArray();
        // 单轮与八轮的快照仅差版本号尺寸，量级一致（不随轮次累积）。
        assertThat(singleBytes.length).isBetween(
                snapBytes.length / 2, snapBytes.length * 2);

        // 安装与追赶：新副本经快照+尾部日志回放到终态。
        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(origin.snapshotState());
        assertThat(restored.digest()).isEqualTo(origin.digest());
        assertThat(restored.shadow().adminEntry("rk-3").refValue()).hasSize(maxBytes);
    }

    @Test
    void barrierGenerationsLedgerAndPendingActionSurviveSnapshotInstall() throws Exception {
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(61, 1_000, 1).toByteArray());
        origin.applyEntry(RaftEntrySamples.sessionOpen(62, 1_000, 2).toByteArray());
        // 世代 1：两方到场合拢（无动作）。
        origin.applyEntry(RaftEntrySamples.barrierAwait(61, 101, "br", 2, false, 2_000, 3)
                .toByteArray());
        ApplyResult t1 = ApplyResult.parseFrom(origin.applyEntry(
                RaftEntrySamples.barrierAwait(62, 102, "br", 0, false, 3_000, 4).toByteArray()));
        assertThat(t1.getStatus())
                .isEqualTo(io.github.lamspace.openlatch.protocol.raft.ApplyStatus.OK);
        // 世代 2：61 到场、62 作为最后到场者携动作 → 动作待决存续。
        origin.applyEntry(RaftEntrySamples.barrierAwait(61, 201, "br", 0, false, 4_000, 5)
                .toByteArray());
        ApplyResult exec = ApplyResult.parseFrom(origin.applyEntry(
                RaftEntrySamples.barrierAwait(62, 202, "br", 0, true, 5_000, 6).toByteArray()));
        assertThat(exec.getBarrierExecutor()).isTrue();

        SnapshotState snap = origin.snapshotState();
        var br = snap.getLocksList().stream()
                .filter(l -> l.getKey().equals("br")).findFirst().orElseThrow();
        assertThat(br.getBarrierParties()).isEqualTo(2);
        assertThat(br.getBarrierGeneration()).isEqualTo(2);
        assertThat(br.getBarrierCurrentArrivalsCount()).isEqualTo(2);
        assertThat(br.getBarrierActionSession()).isEqualTo(62);
        assertThat(br.getBarrierCompletedGeneration()).isEqualTo(1);
        assertThat(br.getBarrierCompletedResult()).isEqualTo(1); // TRIPPED
        assertThat(br.getBarrierCompletedArrivalsCount()).isEqualTo(2);

        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(snap);
        assertThat(restored.digest()).isEqualTo(origin.digest());
        var view = restored.shadow().adminEntry("br");
        assertThat(view.barrierGeneration()).isEqualTo(2);
        assertThat(view.barrierActionPending()).isTrue();
        assertThat(view.barrierArrived()).isEqualTo(2);

        io.github.lamspace.openlatch.protocol.raft.ApplyStatus ok =
                io.github.lamspace.openlatch.protocol.raft.ApplyStatus.OK;
        io.github.lamspace.openlatch.protocol.raft.ApplyStatus broken =
                io.github.lamspace.openlatch.protocol.raft.ApplyStatus.BARRIER_BROKEN;
        // 重启后旧世代迟到重发：按了结记录幂等了结（TRIPPED，不重复计次）。
        ApplyResult late = ApplyResult.parseFrom(restored.applyEntry(
                RaftEntrySamples.barrierAwait(61, 101, "br", 0, false, 6_000, 7).toByteArray()));
        assertThat(late.getStatus()).isEqualTo(ok);
        assertThat(late.getBarrierGeneration()).isEqualTo(1);
        assertThat(restored.shadow().adminEntry("br").barrierArrived()).isEqualTo(2); // 未双计
        // 执行者回报经恢复存续：合拢生效、世代回卷。
        ApplyResult done = ApplyResult.parseFrom(restored.applyEntry(
                RaftEntrySamples.barrierActionDone(62, "br", 2, 7_000, 8).toByteArray()));
        assertThat(done.getStatus()).isEqualTo(ok);
        assertThat(done.getBarrierSettled()).isTrue();
        assertThat(restored.shadow().adminEntry("br").barrierGeneration()).isEqualTo(3);
        assertThat(restored.shadow().adminEntry("br").barrierActionPending()).isFalse();
        // 新世代续用：两方到场再次合拢（世代号单调不回退）。
        restored.applyEntry(RaftEntrySamples.barrierAwait(61, 301, "br", 0, false, 8_000, 9)
                .toByteArray());
        ApplyResult t3 = ApplyResult.parseFrom(restored.applyEntry(
                RaftEntrySamples.barrierAwait(62, 302, "br", 0, false, 9_000, 10).toByteArray()));
        assertThat(t3.getStatus()).isEqualTo(ok);
        assertThat(t3.getBarrierGeneration()).isEqualTo(3);
        // 破障在带裁决路径同样存续：新世代到场后死亡 → 了结 BROKEN 可判。
        origin.applyEntry(RaftEntrySamples.sessionOpen(63, 1_000, 11).toByteArray());
        origin.applyEntry(RaftEntrySamples.barrierAwait(63, 401, "br", 0, false, 10_000, 12)
                .toByteArray());
        origin.applyEntry(RaftEntrySamples.sessionClose(63, 11_000, 13).toByteArray());
        ApplyResult straggler = ApplyResult.parseFrom(origin.applyEntry(
                RaftEntrySamples.barrierAwait(61, 401, "br", 0, false, 12_000, 14).toByteArray()));
        assertThat(straggler.getStatus()).isNotEqualTo(broken); // 61 非破障世代成员
        // 61/401 属存续世代（3 合拢后的 4）：其到场仍挂起。
        assertThat(straggler.getStatus()).isEqualTo(
                io.github.lamspace.openlatch.protocol.raft.ApplyStatus.QUEUED);
        assertThat(origin.digest()).isNotEqualTo(restored.digest()); // 后续条目已推进 origin
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
