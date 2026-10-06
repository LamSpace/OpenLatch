package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.protocol.QueueOp;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 队列复制路径用例组（v7）：队列写条目应用产元素/列表回执、跨副本回放
 * 逐字节一致（条目与终态镜像）、延时到期时刻于应用点按条目携带时刻折算
 * （跨副本逐毫秒一致）、应用点 {@code DENIED} 回弹条目零迁移（摘要等值）、
 * 每会话去重槽应用层重放不双插不偷吃、读条目重放零迁移、配置漂移不分歧
 * （状态机与条目侧无钳制通道——分歧不可能性的构造性证明）。
 */
class StateMachineQueueTest {

    /** QUEUE 形态简写。 */
    private static final io.github.lamspace.openlatch.protocol.LockType QUEUE =
            io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_QUEUE;
    /** DELAY_QUEUE 形态简写。 */
    private static final io.github.lamspace.openlatch.protocol.LockType DELAY =
            io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_DELAY_QUEUE;

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

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void mixedQueueSequenceReplaysByteIdenticalAcrossReplicas() {
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(11, 1_000, 1),
                RaftEntrySamples.sessionOpen(12, 1_000, 2),
                // PUT 定型创建 + 双写。
                RaftEntrySamples.queueSample(11, 101, "q", QUEUE, QueueOp.QUEUE_OP_PUT,
                        false, 4, b("a"), 0, 0, 1, 2_000, 10),
                RaftEntrySamples.queueSample(11, 102, "q", QUEUE, QueueOp.QUEUE_OP_PUT,
                        false, 4, b("b"), 0, 0, 2, 2_100, 11),
                // TAKE 交付 + 回执字节一致。
                RaftEntrySamples.queueSample(12, 103, "q", QUEUE, QueueOp.QUEUE_OP_TAKE,
                        false, 0, null, 0, 0, 1, 2_200, 12),
                // DRAIN 批量（含空串元素）与 SIZE/PEEK 读。
                RaftEntrySamples.queueSample(11, 104, "q", QUEUE, QueueOp.QUEUE_OP_PUT,
                        false, 0, b(""), 0, 0, 3, 2_300, 13),
                RaftEntrySamples.queueSample(12, 105, "q", QUEUE, QueueOp.QUEUE_OP_DRAIN,
                        false, 0, null, 0, 64, 2, 2_400, 14),
                RaftEntrySamples.queueSample(11, 106, "q", QUEUE, QueueOp.QUEUE_OP_SIZE,
                        false, 0, null, 0, 0, 0, 2_500, 15),
                RaftEntrySamples.queueSample(11, 107, "q", QUEUE, QueueOp.QUEUE_OP_PEEK,
                        false, 0, null, 0, 0, 0, 2_600, 16));
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
        // 交付语义抽查：TAKE 得 "a"，DRAIN 得 ["b", ""]（空串元素以零长度
        // 字节出现于列表），SIZE/PEEK 读数与空态。
        assertThat(ra.get(4).getQueueElementBytes().toByteArray()).isEqualTo(b("a"));
        assertThat(ra.get(6).getQueueDrainedBytesList())
                .extracting(com.google.protobuf.ByteString::toByteArray)
                .containsExactly(b("b"), b(""));
        assertThat(ra.get(7).getQueueSize()).isZero();
        assertThat(ra.get(8).hasQueueElementBytes()).isFalse();
        // 终态镜像逐字段一致（记录相等含队列字段）。
        assertThat(a.digest()).isEqualTo(c.digest());
        assertThat(a.shadow().adminEntry("q")).isEqualTo(c.shadow().adminEntry("q"));
        assertThat(a.shadow().adminEntry("q").queueCapacity()).isEqualTo(4);
        assertThat(a.shadow().adminEntry("q").queueDepth()).isZero();
        assertThat(a.shadow().adminEntry("q").queueTotalPayloadBytes()).isZero();
    }

    @Test
    void delayExpiryFoldedFromEntryClockSameOnEveryReplica() {
        // +5s 延时元素：到期时刻=条目携带时刻(2_000)+5_000=7_000，
        // 回放副本无论何时应用，判定随条目时刻恒等。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(21, 1_000, 1),
                RaftEntrySamples.queueSample(21, 101, "dq", DELAY, QueueOp.QUEUE_OP_PUT,
                        false, 3, b("late"), 5_000, 0, 1, 2_000, 10),
                // t=4s：未到期，PEEK 不可见。
                RaftEntrySamples.queueSample(21, 102, "dq", DELAY, QueueOp.QUEUE_OP_PEEK,
                        false, 0, null, 0, 0, 0, 4_000, 11),
                // t=4s：阻塞位按 apply 立即式 → DENIED（回弹场景的条目形态）。
                RaftEntrySamples.queueSample(21, 103, "dq", DELAY, QueueOp.QUEUE_OP_TAKE,
                        true, 0, null, 0, 0, 1, 4_500, 12),
                // t=7s：到期，TAKE 交付。
                RaftEntrySamples.queueSample(21, 104, "dq", DELAY, QueueOp.QUEUE_OP_TAKE,
                        false, 0, null, 0, 0, 2, 7_000, 13));
        LockStateMachineCore a = new LockStateMachineCore(new CoreConfig());
        LockStateMachineCore c = new LockStateMachineCore(new CoreConfig());
        List<ApplyResult> ra = replay(seq, a);
        List<ApplyResult> rc = replay(seq, c);
        for (int i = 0; i < seq.size(); i++) {
            assertThat(ra.get(i).toByteArray()).isEqualTo(rc.get(i).toByteArray());
        }
        assertThat(ra.get(2).hasQueueElementBytes()).isFalse();          // PEEK null
        assertThat(ra.get(3).getStatus()).isEqualTo(ApplyStatus.DENIED); // 未到期消费被拒
        assertThat(ra.get(4).getQueueElementBytes().toByteArray()).isEqualTo(b("late"));
        // 镜像队首到期时刻按条目折算（7_000）与判定无关地存真。
        assertThat(a.digest()).isEqualTo(c.digest());
    }

    @Test
    void headExpiryMirrorTracksFifoHeadForQueueKind() {
        // QUEUE 形态到期恒 0；DELAY 形态镜像队首到期为最早到期元素。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(31, 1_000, 1),
                RaftEntrySamples.queueSample(31, 101, "dq", DELAY, QueueOp.QUEUE_OP_PUT,
                        false, 5, b("late"), 9_000, 0, 1, 1_000, 10),
                RaftEntrySamples.queueSample(31, 102, "dq", DELAY, QueueOp.QUEUE_OP_PUT,
                        false, 0, b("soon"), 2_000, 0, 2, 1_100, 11));
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        replay(seq, core);
        // 到期序：soon(3_100) 先于 late(10_000)，队首到期读数取最早。
        assertThat(core.shadow().adminEntry("dq").queueDepth()).isEqualTo(2);
        assertThat(core.shadow().adminEntry("dq").queueHeadExpiryMs()).isEqualTo(3_100L);
    }

    @Test
    void deniedEntryAppliesAsZeroMutation() {
        // 空队 take 条目（预检竞态漏网的阻塞位形态）应用点 DENIED——不改元素、
        // 不写去重槽（其后同会话写照常命中/覆盖空槽）。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(41, 1_000, 1),
                RaftEntrySamples.sessionOpen(42, 1_000, 2),
                RaftEntrySamples.sessionOpen(43, 1_000, 3),
                RaftEntrySamples.queueSample(41, 101, "q", QUEUE, QueueOp.QUEUE_OP_PUT,
                        false, 1, b("only"), 0, 0, 1, 2_000, 10));
        List<RaftLogEntry> raced = new ArrayList<>(seq);
        // 双 take 单元素：第一条授予、第二条目应用点已无可消费 → DENIED。
        raced.add(RaftEntrySamples.queueSample(42, 102, "q", QUEUE, QueueOp.QUEUE_OP_TAKE,
                true, 0, null, 0, 0, 1, 2_100, 11));
        raced.add(RaftEntrySamples.queueSample(43, 103, "q", QUEUE, QueueOp.QUEUE_OP_TAKE,
                true, 0, null, 0, 0, 1, 2_200, 12));
        LockStateMachineCore withDenied = new LockStateMachineCore(new CoreConfig());
        List<ApplyResult> receipts = replay(raced, withDenied);
        assertThat(receipts.get(4).getStatus()).isEqualTo(ApplyStatus.OK);
        assertThat(receipts.get(5).getStatus()).isEqualTo(ApplyStatus.DENIED);
        // DENIED 条目重放后终态与"只含授予条目"的等价序列逐字节一致。
        List<RaftLogEntry> equivalent = new ArrayList<>(raced);
        equivalent.remove(5);
        LockStateMachineCore eqCore = new LockStateMachineCore(new CoreConfig());
        replay(equivalent, eqCore);
        assertThat(withDenied.digest()).isEqualTo(eqCore.digest());
        assertThat(withDenied.shadow().adminEntry("q")).isEqualTo(eqCore.shadow().adminEntry("q"));
    }

    @Test
    void applyLevelSlotReplayNeverDoubleInsertsOrSteals() {
        // PUT 应答丢失重发（同 op_seq 异 request）：不双插；TAKE 同序重放：
        // 交付同一份字节、不再摘取（SIZE 回 0 为证）；槽被后续成功写覆盖后
        // 旧 op_seq 重发按新 op 判定（在途写互斥纪律保证此路径仅出现于
        // 客户端违约，行为定义为"新操作"）。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(51, 1_000, 1),
                RaftEntrySamples.queueSample(51, 101, "q", QUEUE, QueueOp.QUEUE_OP_PUT,
                        false, 4, b("x"), 0, 0, 5, 2_000, 10),
                RaftEntrySamples.queueSample(51, 102, "q", QUEUE, QueueOp.QUEUE_OP_PUT,
                        false, 4, b("x"), 0, 0, 5, 2_100, 11),
                RaftEntrySamples.queueSample(51, 103, "q", QUEUE, QueueOp.QUEUE_OP_TAKE,
                        false, 0, null, 0, 0, 6, 2_200, 12),
                RaftEntrySamples.queueSample(51, 104, "q", QUEUE, QueueOp.QUEUE_OP_TAKE,
                        false, 0, null, 0, 0, 6, 2_300, 13),
                RaftEntrySamples.queueSample(51, 105, "q", QUEUE, QueueOp.QUEUE_OP_SIZE,
                        false, 0, null, 0, 0, 0, 2_350, 14),
                RaftEntrySamples.queueSample(51, 106, "q", QUEUE, QueueOp.QUEUE_OP_PUT,
                        false, 0, b("y"), 0, 0, 7, 2_400, 15),
                RaftEntrySamples.queueSample(51, 107, "q", QUEUE, QueueOp.QUEUE_OP_TAKE,
                        false, 0, null, 0, 0, 6, 2_500, 16));
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        List<ApplyResult> r = replay(seq, core);
        assertThat(r.get(2).getStatus()).isEqualTo(ApplyStatus.OK);   // 重放已插入（深度保持 1）
        assertThat(r.get(3).getQueueElementBytes().toByteArray()).isEqualTo(b("x"));
        assertThat(r.get(4).getQueueElementBytes().toByteArray()).isEqualTo(b("x")); // 同份交付
        assertThat(r.get(5).getQueueSize()).isZero();                 // 未偷吃第二元素
        assertThat(r.get(6).getStatus()).isEqualTo(ApplyStatus.OK);   // PUT y
        assertThat(r.get(7).getQueueElementBytes().toByteArray()).isEqualTo(b("y"));
        assertThat(core.shadow().adminEntry("q").queueDepth()).isZero();
    }

    @Test
    void readEntriesAreDigestNeutral() {
        List<RaftLogEntry> withReads = List.of(
                RaftEntrySamples.sessionOpen(61, 1_000, 1),
                RaftEntrySamples.queueSample(61, 101, "q", QUEUE, QueueOp.QUEUE_OP_PEEK,
                        false, 0, null, 0, 0, 0, 2_000, 10),
                RaftEntrySamples.queueSample(61, 102, "q", QUEUE, QueueOp.QUEUE_OP_SIZE,
                        false, 0, null, 0, 0, 0, 2_100, 11));
        List<RaftLogEntry> bare = List.of(
                RaftEntrySamples.sessionOpen(61, 1_000, 1));
        assertThat(replayToCoreDigest(withReads)).isEqualTo(replayToCoreDigest(bare));
    }

    /** 读条目零迁移：空队 PEEK/SIZE 与无条目摘要一致。 */
    private static String replayToCoreDigest(List<RaftLogEntry> seq) {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        replay(seq, core);
        return core.digest();
    }

    @Test
    void oversizedElementFromLogStillAppliesNoDivergence() {
        // 构造性证明：apply 与条目侧不接触 maxValueBytes/max-queue-capacity/
        // max-drain-bytes——两内核（同 CoreConfig，钳制属 ServerConfig 层，
        // 状态机不可见）回放同一条含 64KB 大元素的日志，终态逐字节一致。
        byte[] big = new byte[64 * 1024];
        for (int i = 0; i < big.length; i++) {
            big[i] = (byte) (i * 17 + 5);
        }
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(71, 1_000, 1),
                RaftEntrySamples.queueSample(71, 101, "q", QUEUE, QueueOp.QUEUE_OP_PUT,
                        false, 2, big, 0, 0, 1, 2_000, 10),
                RaftEntrySamples.queueSample(71, 102, "q", QUEUE, QueueOp.QUEUE_OP_TAKE,
                        false, 0, null, 0, 0, 2, 2_100, 11));
        LockStateMachineCore a = new LockStateMachineCore(new CoreConfig());
        LockStateMachineCore c = new LockStateMachineCore(new CoreConfig());
        List<ApplyResult> ra = replay(seq, a);
        List<ApplyResult> rc = replay(seq, c);
        assertThat(ra.get(2).getQueueElementBytes().toByteArray()).containsExactly(big);
        assertThat(rc.get(2).toByteArray()).isEqualTo(ra.get(2).toByteArray());
        assertThat(a.digest()).isEqualTo(c.digest());
    }
}
