package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.AtomicOp;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.AtomicRefOpCommand;
import io.github.lamspace.openlatch.core.result.AtomicRefOpResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link AtomicRefEntry} 操作矩阵用例组：五操作语义（无 ADD）、null 与
 * 空字节串两态、版本戳恰 +1 与 GET 零迁移、CAS/CAS_STAMPED 成败、初值
 * presence 主张、{@code op_seq} 单槽去重（成功与失败均占槽、载荷字节级
 * 回放）。纯条目级测试，不经引擎、无时钟。
 */
class AtomicRefEntryTest {

    /** 测试用键。 */
    private static final String K = "k";

    /** 自增序号源：连续写各占一个 op_seq（避免被单槽去重合法拦截）。 */
    private long nextSeq = 1;

    /** 构造引用写命令（会话 1、请求 1，op_seq 自增分配）。 */
    private AtomicRefOpCommand cmd(AtomicOp op, byte[] operand, byte[] expected,
            long expectedVersion, byte[] initial) {
        return new AtomicRefOpCommand(1, 1, K, op, operand, expected,
                expectedVersion, initial, nextSeq++);
    }

    /** 构造带显式 op_seq 的命令（重放用例）。 */
    private static AtomicRefOpCommand cmdSeq(AtomicOp op, byte[] operand, byte[] expected,
            long expectedVersion, byte[] initial, long opSeq) {
        return new AtomicRefOpCommand(1, 1, K, op, operand, expected,
                expectedVersion, initial, opSeq);
    }

    /** 无主张建条目（初值 null 态）。 */
    private static AtomicRefEntry entry() {
        return new AtomicRefEntry(K, LockType.ATOMIC_REFERENCE, null);
    }

    /** 字节串简写。 */
    private static byte[] b(int... vals) {
        byte[] r = new byte[vals.length];
        for (int i = 0; i < vals.length; i++) {
            r[i] = (byte) vals[i];
        }
        return r;
    }

    @Test
    void constructorRejectsNonReferenceKind() {
        assertThatThrownBy(() -> new AtomicRefEntry(K, LockType.ATOMIC_LONG, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void setAndGetAndSetReturnOldAndBumpVersionOnce() {
        AtomicRefEntry e = entry();
        AtomicRefOpResult r = e.op(cmd(AtomicOp.SET, b(1, 2), null, 0, null));
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r.applied()).isTrue();
        assertThat(r.oldValue()).isNull();
        assertThat(r.value()).containsExactly(1, 2);
        assertThat(r.version()).isEqualTo(1);

        AtomicRefOpResult g = e.op(cmd(AtomicOp.GET_AND_SET, b(3), null, 0, null));
        assertThat(g.oldValue()).containsExactly(1, 2);
        assertThat(g.value()).containsExactly(3);
        assertThat(g.version()).isEqualTo(2);

        AtomicRefOpResult get = e.op(cmdSeq(AtomicOp.GET, null, null, 0, null, 0));
        assertThat(get.applied()).isFalse();
        assertThat(get.value()).containsExactly(3);
        assertThat(get.version()).isEqualTo(2);
    }

    @Test
    void addIsOutOfRangeForReferenceForm() {
        AtomicRefEntry e = entry();
        AtomicRefOpResult r = e.op(cmd(AtomicOp.ADD, b(1), null, 0, null));
        assertThat(r.outcome()).isEqualTo(Outcome.REJECT_ATOMIC_RANGE);
        assertThat(r.applied()).isFalse();
        assertThat(r.value()).isNull();
        assertThat(e.version()).isZero();
    }

    @Test
    void nullAndEmptyDistinctThroughCas() {
        AtomicRefEntry e = entry();
        // 初始 null 态：期望 null 的 CAS 命中，落空字节串。
        assertThat(e.op(cmd(AtomicOp.CAS, new byte[0], null, 0, null)).applied()).isTrue();
        assertThat(e.op(cmdSeq(AtomicOp.GET, null, null, 0, null, 0)).value()).isEmpty();
        // 空串态：期望 null 不再命中；期望空串命中。
        assertThat(e.op(cmd(AtomicOp.CAS, b(9), null, 0, null)).applied()).isFalse();
        assertThat(e.op(cmd(AtomicOp.CAS, b(8), new byte[0], 0, null)).applied()).isTrue();
        assertThat(e.version()).isEqualTo(2);
        // 落 null（清空）亦可表达并可被期望。
        assertThat(e.op(cmd(AtomicOp.SET, null, null, 0, null)).applied()).isTrue();
        AtomicRefOpResult back = e.op(cmd(AtomicOp.CAS, b(7), null, 0, null));
        assertThat(back.applied()).isTrue();
        assertThat(back.oldValue()).isNull();
    }

    @Test
    void casStampedRequiresValueAndVersion() {
        AtomicRefEntry e = entry();
        e.op(cmd(AtomicOp.SET, b(1), null, 0, null));
        // 版本不符：applied=false、零推进。
        AtomicRefOpResult stale = e.op(cmd(AtomicOp.CAS_STAMPED, b(2), b(1), 9, null));
        assertThat(stale.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(stale.applied()).isFalse();
        assertThat(stale.value()).containsExactly(1);
        assertThat(stale.version()).isEqualTo(1);
        // 值与版本皆符：落值 +1。
        assertThat(e.op(cmd(AtomicOp.CAS_STAMPED, b(2), b(1), 1, null)).applied()).isTrue();
        // expectedVersion=0 退化为值 CAS。
        assertThat(e.op(cmd(AtomicOp.CAS_STAMPED, b(3), b(2), 0, null)).applied()).isTrue();
        assertThat(e.version()).isEqualTo(3);
    }

    @Test
    void initialClaimPresenceSemantics() {
        // 无主张条目（initial=null）：非 null 主张冲突、null 主张放行。
        AtomicRefEntry free = entry();
        assertThat(free.op(cmd(AtomicOp.SET, b(1), null, 0, b(5))).outcome())
                .isEqualTo(Outcome.REJECT_ATOMIC_INIT);
        assertThat(free.op(cmd(AtomicOp.SET, b(1), null, 0, null)).applied()).isTrue();
        // 主张空串建条目：空串主张匹配、null 主张（不主张）匹配、其他冲突。
        AtomicRefEntry emptyInit = new AtomicRefEntry(K, LockType.ATOMIC_REFERENCE, new byte[0]);
        assertThat(emptyInit.value()).isEmpty();
        assertThat(emptyInit.op(cmd(AtomicOp.SET, b(2), null, 0, new byte[0])).applied()).isTrue();
        assertThat(emptyInit.op(cmd(AtomicOp.SET, b(3), null, 0, null)).applied()).isTrue();
        assertThat(emptyInit.op(cmd(AtomicOp.SET, b(4), null, 0, b(1))).outcome())
                .isEqualTo(Outcome.REJECT_ATOMIC_INIT);
    }

    @Test
    void dedupSlotReplaysByteIdentical() {
        AtomicRefEntry e = entry();
        byte[] payload = new byte[4096];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        AtomicRefOpResult first = e.op(cmdSeq(AtomicOp.SET, payload, null, 0, null, 7));
        assertThat(first.version()).isEqualTo(1);
        // 同 (session, opSeq) 重发：不同载荷亦重放原应答，不再落值。
        AtomicRefOpResult replay = e.op(cmdSeq(AtomicOp.SET, b(99), null, 0, null, 7));
        assertThat(replay.applied()).isTrue();
        assertThat(replay.oldValue()).isNull();
        assertThat(replay.value()).containsExactly(payload);
        assertThat(replay.version()).isEqualTo(1);
        // 未命中的 CAS 也占槽：同槽重放回 applied=false 原形。
        AtomicRefOpResult miss = e.op(cmdSeq(AtomicOp.CAS, b(1), b(2), 0, null, 8));
        assertThat(miss.applied()).isFalse();
        assertThat(miss.value()).containsExactly(payload);
        AtomicRefOpResult missReplay = e.op(cmdSeq(AtomicOp.CAS, b(3), b(4), 0, null, 8));
        assertThat(missReplay.applied()).isFalse();
        assertThat(missReplay.value()).containsExactly(payload);
        assertThat(missReplay.version()).isEqualTo(1);
        // opSeq=0 的 GET 与跨会话（引擎层守卫）不占槽、不互斥。
        assertThat(e.slotOpSeq()).isEqualTo(8);
    }

    @Test
    void lifecycleContractsMirrorScalarEntry() {
        AtomicRefEntry e = entry();
        assertThat(e.family()).isEqualTo(io.github.lamspace.openlatch.core.KeyFamily.ATOMIC);
        assertThat(e.key()).isEqualTo(K);
        assertThat(e.kind()).isEqualTo(LockType.ATOMIC_REFERENCE);
        assertThat(e.leaseToken()).isZero();
        assertThat(e.leaseExpiresAtMs()).isZero();
        assertThat(e.isEmpty()).isFalse();
        assertThat(e.waiterCount()).isZero();
        assertThat(e.sweepNotifiedHead(0, 0, new java.util.ArrayList<>())).isFalse();
        assertThatThrownBy(() -> e.forceExpire(0, 0, new java.util.ArrayList<>()))
                .isInstanceOf(IllegalStateException.class);
        // 会话关闭零触碰。
        e.op(cmd(AtomicOp.SET, b(1), null, 0, null));
        long v = e.version();
        e.removeSession(9, 0, 0, new java.util.ArrayList<>());
        assertThat(e.value()).containsExactly(1);
        assertThat(e.version()).isEqualTo(v);
    }

    @Test
    void restoredFactoryPreservesAllTenFields() {
        AtomicRefEntry e = AtomicRefEntry.restored(K, b(1), null, 5,
                3, 8, true, b(2), b(3), 4);
        assertThat(e.initial()).containsExactly(1);
        assertThat(e.value()).isNull();
        assertThat(e.version()).isEqualTo(5);
        assertThat(e.slotSession()).isEqualTo(3);
        assertThat(e.slotOpSeq()).isEqualTo(8);
        assertThat(e.slotApplied()).isTrue();
        assertThat(e.slotOldValue()).containsExactly(2);
        assertThat(e.slotValue()).containsExactly(3);
        assertThat(e.slotVersion()).isEqualTo(4);
        // 重建后同槽重放仍命中（恢复不重演操作规则）——须同会话同序号。
        AtomicRefOpResult r = e.op(new AtomicRefOpCommand(3, 1, K, AtomicOp.SET,
                b(9), null, 0, null, 8));
        assertThat(r.oldValue()).containsExactly(2);
        assertThat(r.value()).containsExactly(3);
        assertThat(r.version()).isEqualTo(4);
        assertThat(e.version()).isEqualTo(5);
    }

    @Test
    void snapshotExposesReferenceReadingViaRefFields() {
        // 以非 null 主张建条目（初值 {7}），SET 匹配主张后落 42。
        AtomicRefEntry e = new AtomicRefEntry(K, LockType.ATOMIC_REFERENCE, b(7));
        e.op(cmd(AtomicOp.SET, b(42), null, 0, b(7)));
        var snap = e.snapshot(0);
        assertThat(snap.family()).isEqualTo(io.github.lamspace.openlatch.core.KeyFamily.ATOMIC);
        assertThat(snap.atomicKind()).isEqualTo(LockType.ATOMIC_REFERENCE);
        assertThat(snap.atomicInitial()).isZero();
        assertThat(snap.atomicValue()).isZero();
        assertThat(snap.atomicVersion()).isEqualTo(1);
        assertThat(snap.atomicRefInitial()).containsExactly(7);
        assertThat(snap.atomicRefValue()).containsExactly(42);
    }
}
