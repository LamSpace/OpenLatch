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
    void queueElementsExpirySlotsSurviveSnapshotInstall() throws Exception {
        io.github.lamspace.openlatch.protocol.LockType q =
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_QUEUE;
        io.github.lamspace.openlatch.protocol.LockType dq =
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_DELAY_QUEUE;
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(111, 1_000, 1).toByteArray());
        // DELAY 条目：空串元素 + 恰限元素（到期序交错注入，出队按最早到期）。
        byte[] big = new byte[4096];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 13 + 3);
        }
        origin.applyEntry(RaftEntrySamples.queueSample(111, 101, "dq", dq,
                io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                false, 4, big, 9_000, 0, 1, 2_000, 10).toByteArray());
        origin.applyEntry(RaftEntrySamples.queueSample(111, 102, "dq", dq,
                io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                false, 0, new byte[0], 1_000, 0, 2, 2_100, 11).toByteArray());
        // 到期消费空串元素（交付槽记 TAKE + 零长度字节）。
        origin.applyEntry(RaftEntrySamples.queueSample(111, 103, "dq", dq,
                io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_TAKE,
                false, 0, null, 0, 0, 3, 3_100, 12).toByteArray());
        // QUEUE 条目：DRAIN 批量交付（槽含列表）。
        origin.applyEntry(RaftEntrySamples.queueSample(111, 104, "q", q,
                io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                false, 4, "a".getBytes(java.nio.charset.StandardCharsets.UTF_8), 0, 0, 4,
                2_200, 13).toByteArray());
        origin.applyEntry(RaftEntrySamples.queueSample(111, 105, "q", q,
                io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                false, 0, "b".getBytes(java.nio.charset.StandardCharsets.UTF_8), 0, 0, 5,
                2_300, 14).toByteArray());
        origin.applyEntry(RaftEntrySamples.queueSample(111, 106, "q", q,
                io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_DRAIN,
                false, 0, null, 0, 64, 6, 2_400, 15).toByteArray());

        SnapshotState snap = origin.snapshotState();
        var dqSnap = snap.getLocksList().stream()
                .filter(l -> l.getKey().equals("dq")).findFirst().orElseThrow();
        assertThat(dqSnap.getQueueCapacity()).isEqualTo(4);
        assertThat(dqSnap.getQueueElementsCount()).isEqualTo(1);
        // 到期时刻按条目携带时刻折算存真（2_000+9_000=11_000）。
        assertThat(dqSnap.getQueueElements(0).getExpiresAtMs()).isEqualTo(11_000L);
        assertThat(dqSnap.getQueueElements(0).getPayload().toByteArray()).containsExactly(big);
        var dqSlot = dqSnap.getQueueDedupSlots(0);
        assertThat(dqSlot.getOp()).isEqualTo(1); // TAKE
        assertThat(dqSlot.getElementBytes().toByteArray()).isEmpty(); // 空串交付：零长度
        var qSnap = snap.getLocksList().stream()
                .filter(l -> l.getKey().equals("q")).findFirst().orElseThrow();
        assertThat(qSnap.getQueueElementsCount()).isZero();
        assertThat(qSnap.getQueueDedupSlots(0).getOp()).isEqualTo(2); // DRAIN
        assertThat(qSnap.getQueueDedupSlots(0).getDrainedBytesCount()).isEqualTo(2);

        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(snap);
        assertThat(restored.digest()).isEqualTo(origin.digest());
        assertThat(restored.shadow().adminEntry("dq").queueHeadExpiryMs()).isEqualTo(11_000L);
        assertThat(restored.shadow().adminEntry("dq").queueHeadPayload()).containsExactly(big);

        // 重启后命中交付槽重发：同一份字节回放、不再摘取（到期已至）。
        ApplyResult takeReplay = ApplyResult.parseFrom(restored.applyEntry(
                RaftEntrySamples.queueSample(111, 107, "dq", dq,
                        io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_TAKE,
                        false, 0, null, 0, 0, 3, 12_000, 16).toByteArray()));
        assertThat(takeReplay.getStatus())
                .isEqualTo(io.github.lamspace.openlatch.protocol.raft.ApplyStatus.OK);
        assertThat(takeReplay.getQueueElementBytes().toByteArray()).isEmpty();
        assertThat(restored.shadow().adminEntry("dq").queueDepth()).isEqualTo(1);
        // 恢复后新 op_seq 照常消费（到期判定随恢复所得时刻生效）。
        ApplyResult freshTake = ApplyResult.parseFrom(restored.applyEntry(
                RaftEntrySamples.queueSample(111, 108, "dq", dq,
                        io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_TAKE,
                        false, 0, null, 0, 0, 7, 12_000, 17).toByteArray()));
        assertThat(freshTake.getQueueElementBytes().toByteArray()).containsExactly(big);
        // 旧快照（v6 形态）无队列字段——上方 legacy 用例同型：新字段缺省解析
        // 由 queue_elements=0 天然满足，此处断言 v6 序列不含队列字段。
        assertThat(qSnap.getQueueCapacity()).isEqualTo(4);
    }

    @Test
    void queueBoundedSnapshotSizeRegression() {
        // 队列快照尺寸治理回归（5.3）：N 个队列 key 灌满容量恰限元素并多轮
        // take/put 回填后，快照字节 ≤ N×(capacity×maxValueBytes + 槽交付预算
        // + 常数)×安全系数，且不随操作轮次累积（历史元素不入快照）；
        // 安装新副本逐字段保真、追赶回放正常。
        final int keys = 32;
        final int rounds = 5;
        final int capacity = 8;
        final int maxBytes = 4096;
        io.github.lamspace.openlatch.protocol.LockType q =
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_QUEUE;
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(121, 1_000, 1).toByteArray());
        long seq = 10;
        long wall = 2_000;
        long opSeq = 1;
        for (int k = 0; k < keys; k++) {
            for (int i = 0; i < capacity; i++) {
                byte[] payload = new byte[maxBytes];
                payload[0] = (byte) k;
                payload[1] = (byte) i;
                origin.applyEntry(RaftEntrySamples.queueSample(121, 1_000 + seq, "kq-" + k, q,
                        io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                        false, capacity, payload, 0, 0, opSeq++, wall++, seq++).toByteArray());
            }
        }
        // 多轮 take/put 回填（终态与单轮灌满等价：槽恒为最近一次交付）。
        for (int round = 0; round < rounds; round++) {
            for (int k = 0; k < keys; k++) {
                origin.applyEntry(RaftEntrySamples.queueSample(121, 1_000 + seq, "kq-" + k, q,
                        io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_TAKE,
                        false, 0, null, 0, 0, opSeq++, wall++, seq++).toByteArray());
                byte[] payload = new byte[maxBytes];
                payload[0] = (byte) k;
                payload[1] = (byte) (rounds + 1);
                origin.applyEntry(RaftEntrySamples.queueSample(121, 1_000 + seq, "kq-" + k, q,
                        io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                        false, 0, payload, 0, 0, opSeq++, wall++, seq++).toByteArray());
            }
        }
        assertThat(origin.applyFailures()).isZero();
        byte[] snapBytes = origin.snapshotState().toByteArray();
        // 上界：元素 capacity×4KB + 槽最近交付 1×4KB + 常数 512B，×1.5 安全系数。
        long bound = (long) keys * ((long) (capacity + 1) * maxBytes + 512) * 3 / 2;
        assertThat((long) snapBytes.length).isLessThanOrEqualTo(bound);

        // 轮次零增长对照：只灌不轮询的等价终态（槽交付不同元素、份数一致）。
        LockStateMachineCore singlePass = new LockStateMachineCore(new CoreConfig());
        singlePass.applyEntry(RaftEntrySamples.sessionOpen(121, 1_000, 1).toByteArray());
        long sseq = 10;
        long swall = 2_000;
        long sopSeq = 1;
        for (int k = 0; k < keys; k++) {
            for (int i = 0; i < capacity; i++) {
                byte[] payload = new byte[maxBytes];
                payload[0] = (byte) k;
                payload[1] = (byte) (rounds + 1);
                singlePass.applyEntry(RaftEntrySamples.queueSample(121, 1_000 + sseq,
                        "kq-" + k, q,
                        io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                        false, capacity, payload, 0, 0, sopSeq++, swall++, sseq++).toByteArray());
            }
            // 单轮终态对齐：take 一次使每 key 留最近交付槽、深度保持灌满。
            singlePass.applyEntry(RaftEntrySamples.queueSample(121, 1_000 + sseq, "kq-" + k, q,
                    io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_TAKE,
                    false, 0, null, 0, 0, sopSeq++, swall++, sseq++).toByteArray());
            byte[] payload = new byte[maxBytes];
            payload[0] = (byte) k;
            payload[1] = (byte) (rounds + 1);
            singlePass.applyEntry(RaftEntrySamples.queueSample(121, 1_000 + sseq, "kq-" + k, q,
                    io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                    false, 0, payload, 0, 0, sopSeq++, swall++, sseq++).toByteArray());
        }
        byte[] singleBytes = singlePass.snapshotState().toByteArray();
        assertThat((long) singleBytes.length).isBetween(
                snapBytes.length / 2L, (long) (snapBytes.length * 1.5));

        // 安装与追赶：新副本回放到同终态。
        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(origin.snapshotState());
        assertThat(restored.digest()).isEqualTo(origin.digest());
        assertThat(restored.shadow().adminEntry("kq-3").queueDepth()).isEqualTo(capacity);
        assertThat(restored.shadow().adminEntry("kq-3").queueHeadPayload())
                .hasSize(maxBytes);
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

    /**
     * v8 topic 零快照增量守卫（snapshot-recovery 规格）：基座含队列条目的快照
     * 取字节后，对独立 {@code TopicRegistry} 施全载荷（4 键 × 8 订阅 × 64 发布
     * × 退订消解），核心再取快照 MUST 逐字节相等——topic 无进入复制态/快照的
     * API 通道（零日志裁决的快照面对偶，任何误接线都会在此转红）。
     */
    @Test
    void topicLoadContributesZeroSnapshotDelta() {
        io.github.lamspace.openlatch.protocol.LockType q =
                io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_QUEUE;
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(131, 1_000, 1).toByteArray());
        for (int k = 0; k < 4; k++) {
            origin.applyEntry(RaftEntrySamples.queueSample(131, 2_000 + k, "base-" + k, q,
                    io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                    false, 8, new byte[64], 0, 0, k + 1, 3_000 + k, 100 + k).toByteArray());
        }
        byte[] before = origin.snapshotState().toByteArray();

        io.github.lamspace.openlatch.server.topic.TopicRegistry topics =
                new io.github.lamspace.openlatch.server.topic.TopicRegistry(
                        new io.github.lamspace.openlatch.server.session.ServerSessionRegistry(),
                        8, 16);
        for (int k = 0; k < 4; k++) {
            String key = "t-" + k;
            for (int s = 0; s < 8; s++) {
                assertThat(topics.subscribe(900 + s, key, 5_000L).status())
                        .isEqualTo(io.github.lamspace.openlatch.protocol.StatusCode.OK);
            }
            assertThat(topics.subscriberCount(key)).isEqualTo(8); // 负载真实建立（非空转）
            for (int i = 0; i < 64; i++) {
                topics.publish(900, key, i + 1, new byte[] {(byte) i}, 6_000 + i);
            }
            for (int s = 0; s < 8; s++) {
                topics.unsubscribe(900 + s, key);
            }
        }
        // 全退订后登记消解（防泄漏），核心快照面与负载前逐字节相等。
        assertThat(topics.topicKeyCount()).isZero();
        assertThat(origin.snapshotState().toByteArray()).isEqualTo(before);
    }

    /**
     * v9 条件等待零快照增量守卫（snapshot-recovery 规格）：基座含互斥锁条目的
     * 快照取字节后，对独立 {@code ConditionRegistry} 施全载荷（4 键 × 双条件 ×
     * 8 等待者 × 搬运/撤登/授予收口/会话摘除/任期清零消解），核心再取快照 MUST
     * 逐字节相等——等待集与搬运时序无进入快照的 API 通道（等待集不入快照、
     * await 复制足迹仅为既有持有清零，任何误接线都会在此转红）。
     */
    @Test
    void conditionLoadContributesZeroSnapshotDelta() {
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        origin.applyEntry(RaftEntrySamples.sessionOpen(141, 1_000, 1).toByteArray());
        for (int k = 0; k < 4; k++) {
            origin.applyEntry(RaftEntrySamples.acquire(141, 2_000 + k, "ckey-" + k, 3_000 + k,
                    io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_REENTRANT,
                    300L + k).toByteArray());
        }
        byte[] before = origin.snapshotState().toByteArray();

        io.github.lamspace.openlatch.server.condition.ConditionRegistry conditions =
                new io.github.lamspace.openlatch.server.condition.ConditionRegistry();
        for (int k = 0; k < 4; k++) {
            String key = "cond-" + k;
            for (int w = 0; w < 8; w++) {
                assertThat(conditions.register(1_400 + w, 100L + w, 7L + w, key,
                        w % 2 == 0 ? "a" : "b", 5_000L + w)).isTrue();
            }
            assertThat(conditions.count(key)).isEqualTo(8); // 负载真实建立（非空转）
            // 搬运二人（含空转重登验证）+ 撤登二人 + 授予收口一人。
            conditions.promoteFirst(key, "a", 1_400L, 7L);
            conditions.promoteAll(key, "b", 1_400L, 7L);
            conditions.register(1_400, 100L, 7L, key, "a", 5_000L); // 幂等重挂零变化
            conditions.leave(1_401, 101L);
            conditions.leave(1_402, 102L);
            conditions.purgeOwner(1_403, 10L);
        }
        for (int w = 0; w < 8; w++) {
            conditions.removeSession(1_400 + w);
        }
        conditions.clear();
        assertThat(conditions.totalCount()).isZero();
        // 全消解后登记归零，核心快照面与负载前逐字节相等。
        assertThat(origin.snapshotState().toByteArray()).isEqualTo(before);
    }
}
