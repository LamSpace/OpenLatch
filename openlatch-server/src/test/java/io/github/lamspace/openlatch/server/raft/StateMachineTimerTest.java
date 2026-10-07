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
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TimerOp;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import io.github.lamspace.openlatch.protocol.raft.SnapshotLock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 延时触发复制边界守卫（v11 常驻）：每变异恰一条 {@code TIMER_OP_ENTRY} 且
 * 跨副本回放逐字节一致（绝对到期时刻应用点折算、不读墙钟外生输入）；等待/
 * 取消/查询零条目；**时钟走过到期点日志恒零新增**（"到期误入日志即红"反向
 * 守卫——与"等待误入日志即红"对偶，构成派生裁决的双向常驻约束）；去重槽
 * 重放不双换代、代次单调不回退；死亡对账簿零扰动跨副本同判（"钟照响"的
 * 复制面证据）；编号证据：timer 家族条目面仅 {@code TIMER_OP_ENTRY} 一种、
 * 快照面（{@code SnapshotLock}）无 marked 字段（到期无驻留位的序列化证据）。
 */
class StateMachineTimerTest {

    /** 以序列化字节应用全序列到一个内核并按序收集回执。 */
    private static List<ApplyResult> replay(List<RaftLogEntry> seq, LockStateMachineCore core) {
        List<ApplyResult> out = new ArrayList<>(seq.size());
        try {
            for (RaftLogEntry e : seq) {
                out.add(ApplyResult.parseFrom(core.applyEntry(e.toByteArray())));
            }
        } catch (com.google.protobuf.InvalidProtocolBufferException ex) {
            throw new AssertionError(ex);
        }
        return out;
    }

    /** 快照摘要（digest 口径：账簿态指纹）。 */
    private static String digestOf(LockStateMachineCore core) {
        return core.digest();
    }

    @Test
    void mutationSequenceReplaysByteIdenticalAcrossReplicas() {
        // 装载→重装载（换代）→撤销→再装载：四变异跨副本回执逐字节等。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(11, 1_000, 1),
                RaftEntrySamples.sessionOpen(12, 1_000, 2),
                RaftEntrySamples.timerSample(11, 101, "t", TimerOp.TIMER_OP_SCHEDULE,
                        5_000L, 0, 1_100, 10),
                RaftEntrySamples.timerSample(12, 102, "t", TimerOp.TIMER_OP_SCHEDULE,
                        1_000L, 0, 1_200, 11),
                // 同 rid 重发（应答丢失形）：命中单槽回放声，不双换代。
                RaftEntrySamples.timerSample(12, 102, "t", TimerOp.TIMER_OP_SCHEDULE,
                        1_000L, 0, 1_300, 12),
                RaftEntrySamples.timerSample(11, 103, "t", TimerOp.TIMER_OP_DISARM,
                        null, 0, 1_400, 13),
                RaftEntrySamples.timerSample(11, 104, "t", TimerOp.TIMER_OP_SCHEDULE,
                        0L, 0, 1_500, 14));
        LockStateMachineCore a = new LockStateMachineCore(new CoreConfig());
        LockStateMachineCore c = new LockStateMachineCore(new CoreConfig());
        List<ApplyResult> ra = replay(seq, a);
        List<ApplyResult> rc = replay(seq, c);
        assertThat(a.applyFailures()).isZero();
        assertThat(c.applyFailures()).isZero();
        for (int i = 0; i < seq.size(); i++) {
            assertThat(ra.get(i).toByteArray())
                    .as("entry %d receipt byte-identical", i)
                    .isEqualTo(rc.get(i).toByteArray());
        }
        // 装载回显：代次 1、绝对到期 = 条目时刻 1_100 + 5_000（应用点折算）。
        assertThat(ra.get(2).getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(ra.get(2).getTimerGeneration()).isEqualTo(1);
        assertThat(ra.get(2).getTimerArmed()).isTrue();
        assertThat(ra.get(2).getTimerFireAtMs()).isEqualTo(6_100L);
        // 第二会话装载：代次 2、清旧钟（换代语义）。
        assertThat(ra.get(3).getTimerGeneration()).isEqualTo(2);
        // 同 rid 重发：回放回声（代次仍 2、不双换代）。
        assertThat(ra.get(4).getTimerGeneration()).isEqualTo(2);
        // DISARM：代次保持 2、armed 翻假、fireAt 保持撤销前值。
        assertThat(ra.get(5).getTimerGeneration()).isEqualTo(2);
        assertThat(ra.get(5).getTimerArmed()).isFalse();
        assertThat(ra.get(5).getTimerFireAtMs()).isEqualTo(2_200L);
        // 再装载：代次 3、delay=0 → fireAt=1_500（即刻共见形——但 marked 不进
        // 条目回执，应用点无"已响"位）。
        assertThat(ra.get(6).getTimerGeneration()).isEqualTo(3);
        assertThat(ra.get(6).getTimerArmed()).isTrue();
        assertThat(ra.get(6).getTimerFireAtMs()).isEqualTo(1_500L);
        // 终态镜像一致。
        assertThat(digestOf(a)).isEqualTo(digestOf(c));
        assertThat(a.shadow().adminEntry("t")).isEqualTo(c.shadow().adminEntry("t"));
        assertThat(a.shadow().adminEntry("t").timerGeneration()).isEqualTo(3);
        assertThat(a.shadow().adminEntry("t").timerArmed()).isTrue();
    }

    @Test
    void clockCrossingFireAtProducesZeroLogAndZeroLedgerChange() {
        // 反向守卫（"到期误入日志即红"）：装载后让"时间流逝"——以更大
        // wallClock 的既有系统条目（NOOP）承载，到期点被跨越；断言账簿三元组
        // 与 digest 对流逝前逐字节不变（无任何 timer 条目/字段变化可寻）。
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        replay(List.of(
                RaftEntrySamples.sessionOpen(11, 1_000, 1),
                RaftEntrySamples.timerSample(11, 101, "t", TimerOp.TIMER_OP_SCHEDULE,
                        1_000L, 0, 1_100, 10)), core);
        String digestBefore = digestOf(core);
        byte[] snapBefore = core.snapshotState().toByteArray();
        assertThat(core.shadow().adminEntry("t").timerFireAtMs()).isEqualTo(2_100L);
        // 时钟推进至 9_000（远超 fireAt）：仅一条 NOOP（当选位点确认形）。
        replay(List.of(RaftLogEntry.newBuilder()
                .setType(RaftEntryType.NOOP).setSeq(11).setWallClockMs(9_000).build()), core);
        assertThat(digestOf(core)).isEqualTo(digestBefore);
        assertThat(core.snapshotState().toByteArray()).isEqualTo(snapBefore);
        // 三元组不变且"已否到期"无任何驻留形态可寻（armed 仍 true、fireAt 原值
        // ——marked 只活在 Leader 应答线，此即派生裁决的条目面常驻断言）。
        assertThat(core.shadow().adminEntry("t").timerArmed()).isTrue();
        assertThat(core.shadow().adminEntry("t").timerFireAtMs()).isEqualTo(2_100L);
    }

    @Test
    void sessionCloseLeavesTimerLedgerUntouchedDeterministically() {
        // 装载者死亡：账簿三元组零扰动（死亡钟照响）、跨副本回执逐字节等——
        // 与 phaser"死亡摘除配额可推进"刻意反向的复制面证据。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(21, 1_000, 1),
                RaftEntrySamples.sessionOpen(22, 1_000, 2),
                RaftEntrySamples.timerSample(21, 101, "t", TimerOp.TIMER_OP_SCHEDULE,
                        5_000L, 0, 1_100, 10),
                RaftEntrySamples.sessionClose(21, 1_200, 11));
        LockStateMachineCore a = new LockStateMachineCore(new CoreConfig());
        LockStateMachineCore c = new LockStateMachineCore(new CoreConfig());
        List<ApplyResult> ra = replay(seq, a);
        List<ApplyResult> rc = replay(seq, c);
        for (int i = 0; i < seq.size(); i++) {
            assertThat(ra.get(i).toByteArray()).isEqualTo(rc.get(i).toByteArray());
        }
        assertThat(a.digest()).isEqualTo(c.digest());
        // 死亡前后三元组逐字段等：代次 1、在装、fireAt 不变（死者装载存续）。
        assertThat(a.shadow().adminEntry("t").timerGeneration()).isEqualTo(1);
        assertThat(a.shadow().adminEntry("t").timerArmed()).isTrue();
        assertThat(a.shadow().adminEntry("t").timerFireAtMs()).isEqualTo(6_100L);
        // 存活会话照常共见：DISARM by s2 生效（对死者装载有处置权——触发
        // 归属 key 不归属会话）。
        List<ApplyResult> after = replay(List.of(
                RaftEntrySamples.timerSample(22, 102, "t", TimerOp.TIMER_OP_DISARM,
                        null, 0, 1_300, 12)), a);
        assertThat(after.get(0).getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(after.get(0).getTimerArmed()).isFalse();
    }

    @Test
    void snapshotInstallReplaysTailWithoutDoubleGeneration() {
        // 快照位点跨越装载/撤销：三元组与去重槽恢复；旧 rid 重发命中恢复槽
        // 不双换代、代次单调不回退。
        LockStateMachineCore seed = new LockStateMachineCore(new CoreConfig());
        replay(List.of(
                RaftEntrySamples.sessionOpen(31, 1_000, 1),
                RaftEntrySamples.timerSample(31, 101, "t", TimerOp.TIMER_OP_SCHEDULE,
                        2_000L, 0, 1_100, 10),
                RaftEntrySamples.timerSample(31, 102, "t", TimerOp.TIMER_OP_DISARM,
                        null, 0, 1_200, 11)), seed);
        io.github.lamspace.openlatch.protocol.raft.SnapshotState snap = seed.snapshotState();
        LockStateMachineCore fresh = new LockStateMachineCore(new CoreConfig());
        fresh.installSnapshot(snap);
        assertThat(fresh.shadow().adminEntry("t").timerGeneration()).isEqualTo(1);
        assertThat(fresh.shadow().adminEntry("t").timerArmed()).isFalse();
        List<ApplyResult> after = replay(List.of(
                // 快照位点后旧 rid 重发（DISARM 回声槽）：零迁移零换代。
                RaftEntrySamples.timerSample(31, 102, "t", TimerOp.TIMER_OP_DISARM,
                        null, 0, 1_300, 12),
                // 新装载：代次 2（恢复不回退）。
                RaftEntrySamples.timerSample(31, 103, "t", TimerOp.TIMER_OP_SCHEDULE,
                        500L, 0, 1_400, 13)), fresh);
        assertThat(after.get(0).getTimerGeneration()).isEqualTo(1);
        assertThat(after.get(1).getTimerGeneration()).isEqualTo(2);
        assertThat(after.get(1).getTimerFireAtMs()).isEqualTo(1_900L);
    }

    @Test
    void timerSurfaceHasNoFireEntryAndNoMarkedField() {
        // 编号证据双条：①条目面 timer 家族唯一形态 TIMER_OP_ENTRY——等待/取消/
        // 查询/到期均无日志形态可寻（"到期 fire 条目"若被引入即红）；
        // ②快照面 SnapshotLock 的 timer 字段链 42–45 且无任何 marked 字段
        //（到期无驻留位的序列化面证据，"已到期"不使两份同终态快照产生字节差）。
        assertThat(RaftEntryType.values())
                .extracting(Enum::name)
                .filteredOn(n -> n.startsWith("TIMER"))
                .containsExactly("TIMER_OP_ENTRY");
        assertThat(SnapshotLock.getDescriptor().findFieldByName("timer_generation")
                .getNumber()).isEqualTo(42);
        assertThat(SnapshotLock.getDescriptor().findFieldByName("timer_dedup_slots")
                .getNumber()).isEqualTo(45);
        assertThat(SnapshotLock.getDescriptor().getFields().stream()
                .noneMatch(f -> f.getName().contains("marked"))).isTrue();
    }
}
