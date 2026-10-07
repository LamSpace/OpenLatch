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
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.PhaserOp;
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
 * 相位器复制边界守卫（v10 常驻）：每变异恰一条 {@code PHASER_OP_ENTRY} 且跨
 * 副本回放逐字节一致（含死亡摘除驱动的推进——账簿迁移纯确定性、不读墙钟
 * 外生输入）；等待/取消/查询零条目；去重槽与换代窗口回放不双计数、相位
 * 单调不回退；镜像 digest 含 phaser 字段跨副本等值。编号证据基线由
 * "RaftEntryType 止于 13"更替为"止于 14 且 14 为 phaser 专用"（v11 再更替为
 * "止于 15、15 为 timer 专用"，断言按当前版本口径表述），并同时钉死
 * topic/condition 两代边界在 v10 上界下依然成立（版本相对口径）；租约到期
 * 影子清扫对 phaser 镜像零触碰——静置 phaser 键随到期条目扫过镜像存续、
 * 幻影键不入到期释放集（影子到期误摘修复的回归钉）。
 */
class StateMachinePhaserTest {

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

    @Test
    void mutationSequenceReplaysByteIdenticalAcrossReplicas() {
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(11, 1_000, 1),
                RaftEntrySamples.sessionOpen(12, 1_000, 2),
                // 两会话注册共 3 方配额（s1 两方、s2 一方）。
                RaftEntrySamples.phaserSample(11, 101, "p", PhaserOp.PHASER_OP_REGISTER,
                        2, null, 0, 1_100, 10),
                RaftEntrySamples.phaserSample(12, 102, "p", PhaserOp.PHASER_OP_REGISTER,
                        1, null, 0, 1_200, 11),
                // 到场两发（不推进）+ 第三发恰合拢（相位 0→1）。
                RaftEntrySamples.phaserSample(11, 103, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_300, 12),
                RaftEntrySamples.phaserSample(12, 104, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_400, 13),
                RaftEntrySamples.phaserSample(11, 105, "p", PhaserOp.PHASER_OP_ARRIVE_AND_AWAIT,
                        0, null, 0, 1_500, 14),
                // 跨推进重发（同 rid 命中换代窗口）：幂等回显不双计。
                RaftEntrySamples.phaserSample(11, 103, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_600, 15),
                // ARRIVE_AND_DEREGISTER：到场 + 离场减员（registered 3→2、
                // arrived=1 < 2 不推进）；补一发到场（arrived 2≥2）合拢 1→2。
                RaftEntrySamples.phaserSample(12, 106, "p", PhaserOp.PHASER_OP_ARRIVE_AND_DEREGISTER,
                        0, null, 0, 1_700, 15),
                RaftEntrySamples.phaserSample(11, 107, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_800, 16));
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
        // 合拢回执：rid=105（idx6）恰触达——OK、tripped 经 advanced_keys
        // 呈现、到场相位 0 回显、推进后 arrived 0。
        assertThat(ra.get(6).getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(ra.get(6).getPhaserPhase()).isZero();
        assertThat(ra.get(6).getPhaserArrived()).isZero();
        assertThat(ra.get(6).getPhaserAdvancedKeysList()).containsExactly("p");
        // 跨推进重发（idx7，rid=103 命中换代窗口）：终态回显 prevPhase=0、
        // 无 advanced_keys、arrived 不受扰动。
        assertThat(ra.get(7).getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(ra.get(7).getPhaserPhase()).isZero();
        assertThat(ra.get(7).getPhaserAdvancedKeysList()).isEmpty();
        assertThat(ra.get(7).getPhaserArrived()).isZero();
        // A_D（idx8）：arrived 0→1、registered 3→2 → 1<2 不推进。
        assertThat(ra.get(8).getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(ra.get(8).getPhaserAdvancedKeysList()).isEmpty();
        assertThat(ra.get(8).getPhaserRegistered()).isEqualTo(2);
        assertThat(ra.get(8).getPhaserArrived()).isEqualTo(1);
        // 补一发（idx9）：arrived 2≥2 合拢 1→2。
        assertThat(ra.get(9).getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(ra.get(9).getPhaserAdvancedKeysList()).containsExactly("p");
        // 终态镜像一致（相位 2、registered 1、在场槽清空）。
        assertThat(a.digest()).isEqualTo(c.digest());
        assertThat(a.shadow().adminEntry("p")).isEqualTo(c.shadow().adminEntry("p"));
        assertThat(a.shadow().adminEntry("p").phaserPhase()).isEqualTo(2);
        assertThat(a.shadow().adminEntry("p").phaserRegistered()).isEqualTo(2);
        assertThat(a.shadow().adminEntry("p").phaserArrived()).isZero();
    }

    @Test
    void sessionCloseQuotaRemovalTripsDeterministicallyAcrossReplicas() {
        // s3 到场后死亡：registered 3→2、arrived 保持 2 → SESSION_CLOSE 应用点
        // 即合拢——该推进是账簿确定性迁移，Follower 同判且回执携带 advanced key
        // （死亡不空转的复制面证据）。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(21, 1_000, 1),
                RaftEntrySamples.sessionOpen(22, 1_000, 2),
                RaftEntrySamples.sessionOpen(23, 1_000, 3),
                RaftEntrySamples.phaserSample(21, 101, "p", PhaserOp.PHASER_OP_REGISTER,
                        1, null, 0, 1_100, 10),
                RaftEntrySamples.phaserSample(22, 102, "p", PhaserOp.PHASER_OP_REGISTER,
                        1, null, 0, 1_200, 11),
                RaftEntrySamples.phaserSample(23, 103, "p", PhaserOp.PHASER_OP_REGISTER,
                        1, null, 0, 1_300, 12),
                RaftEntrySamples.phaserSample(21, 104, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_400, 13),
                RaftEntrySamples.phaserSample(22, 105, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_500, 14),
                RaftEntrySamples.sessionClose(23, 1_600, 15));
        LockStateMachineCore a = new LockStateMachineCore(new CoreConfig());
        LockStateMachineCore c = new LockStateMachineCore(new CoreConfig());
        List<ApplyResult> ra = replay(seq, a);
        List<ApplyResult> rc = replay(seq, c);
        for (int i = 0; i < seq.size(); i++) {
            assertThat(ra.get(i).toByteArray()).isEqualTo(rc.get(i).toByteArray());
        }
        assertThat(ra.get(8).getPhaserAdvancedKeysList()).containsExactly("p");
        assertThat(a.digest()).isEqualTo(c.digest());
        assertThat(a.shadow().adminEntry("p").phaserPhase()).isEqualTo(1);
        assertThat(a.shadow().adminEntry("p").phaserRegistered()).isEqualTo(2);
        // 死者槽行摘除但计数不回退（"已到场事实不撤销"）：arrived 恒 2 触发
        // 推进后清零——终态 arrived 0、在场槽不含死者。
        assertThat(a.shadow().adminEntry("p").phaserArrived()).isZero();
    }

    @Test
    void replayAfterSnapshotInstallKeepsMonotonicAndDedup() {
        // 快照安装点跨越合拢：相位/配额/在场槽/换代窗逐字段恢复；旧 rid 重发
        // 命中恢复的去重结构不双计（回放至快照位点后重放尾部）。
        LockStateMachineCore seed = new LockStateMachineCore(new CoreConfig());
        replay(List.of(
                RaftEntrySamples.sessionOpen(31, 1_000, 1),
                RaftEntrySamples.phaserSample(31, 101, "p", PhaserOp.PHASER_OP_REGISTER,
                        2, null, 0, 1_100, 10),
                RaftEntrySamples.phaserSample(31, 102, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_200, 11)), seed);
        io.github.lamspace.openlatch.protocol.raft.SnapshotState snap = seed.snapshotState();
        LockStateMachineCore fresh = new LockStateMachineCore(new CoreConfig());
        fresh.installSnapshot(snap);
        List<ApplyResult> after = replay(List.of(
                // 快照位点后的重发（rid=102 已计入快照账簿在场槽/换代窗）：
                RaftEntrySamples.phaserSample(31, 102, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_300, 12),
                // 补一发即合拢（2/2）。
                RaftEntrySamples.phaserSample(31, 103, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_400, 13)), fresh);
        // 重发终态：恢复账簿在场槽含 (31,102)——重发幂等回显、不双计。
        assertThat(after.get(0).getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(after.get(0).getPhaserAdvancedKeysList()).isEmpty();
        assertThat(after.get(0).getPhaserArrived()).isEqualTo(1);
        assertThat(after.get(1).getPhaserAdvancedKeysList()).containsExactly("p");
        assertThat(fresh.shadow().adminEntry("p").phaserPhase()).isEqualTo(1);
    }

    @Test
    void expireSweepLeavesPhaserMirrorUntouched() {
        // 回归钉（判例原子家族到期误扫夹具形态）：phaser 建账静置（此后
        // 零后续变异）与短租约有持锁共存，到期条目应用点清扫——镜像 phaser
        // 条目逐字段存续、到期摘除集仅含真实到期锁键（计数面同钉）。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(41, 1_000, 1),
                RaftEntrySamples.sessionOpen(42, 1_000, 2),
                RaftEntrySamples.phaserSample(41, 101, "p", PhaserOp.PHASER_OP_REGISTER,
                        2, null, 0, 1_100, 10),
                RaftEntrySamples.phaserSample(42, 102, "p", PhaserOp.PHASER_OP_ARRIVE,
                        0, null, 0, 1_200, 11),
                // 重入锁短租约（200ms）授予：凭证 1、条目时刻下 1_500 到期。
                RaftEntrySamples.acquireWithWait(41, 103, "mk", 1_300,
                        LockType.LOCK_TYPE_REENTRANT, 12, -1, 200, 9),
                // 租约到期条目（携带时刻远超到期）：应用点整表影子清扫。
                RaftEntrySamples.expire("mk", 1, 10_000, 13));
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        List<ApplyResult> rs = replay(seq, core);
        assertThat(core.applyFailures()).isZero();
        ApplyResult expireResult = rs.get(rs.size() - 1);
        assertThat(expireResult.getStatus()).isEqualTo(ApplyStatus.OK);
        // 清扫摘除集仅含真实到期锁 key——phaser 幻影键不入集（到期释放
        // 计数口径按实际释放钉死）。
        assertThat(expireResult.getFreedKeysList()).containsExactly("mk");
        // 静置后镜像 phaser 条目存续且三计数逐字段保真（非空壳重建）。
        assertThat(core.shadow().isPhaser("p")).isTrue();
        assertThat(core.shadow().adminEntry("p").phaserPhase()).isZero();
        assertThat(core.shadow().adminEntry("p").phaserRegistered()).isEqualTo(2);
        assertThat(core.shadow().adminEntry("p").phaserArrived()).isEqualTo(1);
        // 到期锁照常摘除——本修复不触碰真实到期面。
        assertThat(core.shadow().isHeld("mk")).isFalse();
        // 跨副本确定性恒等：双份独立回放摘要相等。
        LockStateMachineCore other = new LockStateMachineCore(new CoreConfig());
        replay(seq, other);
        assertThat(core.digest()).isEqualTo(other.digest());
    }

    @Test
    void waitQueryCancelNeverProduceEntries() {
        // 复制面零贡献断言：仅系统条目（会话登记）时 phaser 流量不增条目——
        // 以"应用等待/查询词形的变异通道缺席"构造：StateMachine 仅识别
        // PHASER_OP_ENTRY 一种 phaser 条目类型，等待/取消/查询无对应条目类型
        // （编号证据臂钉死），故任何此类请求 MUST NOT 有日志形态可寻。
        // phaser 家族的条目面只有变异一种：任何 PHASER_* 条目名必为
        // PHASER_OP_ENTRY（等待/取消/查询无日志形态可寻）。
        assertThat(RaftEntryType.values())
                .extracting(Enum::name)
                .filteredOn(n -> n.startsWith("PHASER"))
                .containsExactly("PHASER_OP_ENTRY");
    }

    @Test
    void numberingEvidenceBaselineMovesTo15AndPriorBoundariesHold() {
        // 更替的守卫基线（v11 版本相对口径）：RaftEntryType 值域止于 15、
        // 15 为 timer 专用、14 为 phaser 专用（v10 时代上界随本代改相对表述）；
        // topic/condition 两代"零占用"证据按版本相对口径在本上界下成立。
        for (RaftEntryType t : RaftEntryType.values()) {
            if (t == RaftEntryType.UNRECOGNIZED) {
                continue;
            }
            assertThat(t.getNumber()).isLessThanOrEqualTo(15);
            assertThat(t.name()).doesNotContain("TOPIC");
            assertThat(t.name()).doesNotContain("CONDITION");
        }
        assertThat(RaftEntryType.PHASER_OP_ENTRY.getNumber()).isEqualTo(14);
        assertThat(RaftEntryType.TIMER_OP_ENTRY.getNumber()).isEqualTo(15);
        // 1–13 既有编号语义不变（判例条目工厂逐一可用，golden 冻结兜底）。
        assertThat(SnapshotLock.getDescriptor().findFieldByName("phaser_phase").getNumber())
                .isEqualTo(35);
    }
}
