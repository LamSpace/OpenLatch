package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.AtomicOp;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.AtomicOpCommand;
import io.github.lamspace.openlatch.core.result.AtomicOpResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AtomicEntry} 操作矩阵用例组：六操作语义、版本戳恰 +1 与 GET 零迁移、
 * 条件加断言、CAS/CAS_STAMPED 成败、溢出 wrap（long/int32）、布尔值域、
 * 初值主张、{@code op_seq} 单槽去重（成功与失败均占槽、跨会话不互斥）。
 * 纯条目级测试，不经引擎、无时钟。
 */
class AtomicEntryTest {

    /** 测试用键。 */
    private static final String K = "k";

    /** 构造指定形态写命令（会话 1、请求 1、不主张初值、op_seq 自给定）。 */
    private static AtomicOpCommand cmd(AtomicOp op, LockType kind, long operand, long expected,
            long expectedVersion, long opSeq) {
        return new AtomicOpCommand(1, 1, K, kind, op,
                operand, expected, expectedVersion, 0, opSeq);
    }

    /** 自增序号源：连续写各占一个 op_seq（避免被单槽去重合法拦截）。 */
    private long nextSeq = 1;

    /** 构造 long 形态写命令（缺省不主张初值，op_seq 自增分配）。 */
    private AtomicOpCommand longCmd(AtomicOp op, long operand) {
        return cmd(op, LockType.ATOMIC_LONG, operand, 0, 0, nextSeq++);
    }

    /** 以非零初值建 LONG 条目。 */
    private static AtomicEntry entry(long initial) {
        return new AtomicEntry(K, LockType.ATOMIC_LONG, initial);
    }

    @Test
    void freshEntryGetReturnsZeroStamped() {
        AtomicEntry e = entry(0);
        AtomicOpResult r = e.op(cmd(AtomicOp.GET, LockType.ATOMIC_LONG, 0, 0, 0, 0));
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r.applied()).isFalse();
        assertThat(r.oldValue()).isZero();
        assertThat(r.value()).isZero();
        assertThat(r.version()).isZero();
    }

    @Test
    void setReturnsOldAndBumpsVersionOnceEachWrite() {
        AtomicEntry e = entry(0);
        AtomicOpResult first = e.op(longCmd(AtomicOp.SET, 5));
        assertThat(first.oldValue()).isZero();
        assertThat(first.value()).isEqualTo(5);
        assertThat(first.version()).isEqualTo(1);
        AtomicOpResult second = e.op(longCmd(AtomicOp.SET, 7));
        assertThat(second.oldValue()).isEqualTo(5);
        assertThat(second.value()).isEqualTo(7);
        assertThat(second.version()).isEqualTo(2);
    }

    @Test
    void getAndSetMatchesSetShape() {
        AtomicEntry e = entry(0);
        e.op(longCmd(AtomicOp.SET, 11));
        AtomicOpResult r = e.op(longCmd(AtomicOp.GET_AND_SET, 13));
        assertThat(r.applied()).isTrue();
        assertThat(r.oldValue()).isEqualTo(11);
        assertThat(r.value()).isEqualTo(13);
        assertThat(r.version()).isEqualTo(2);
    }

    @Test
    void addWrapsOnLongOverflow() {
        AtomicEntry e = entry(0);
        e.op(longCmd(AtomicOp.SET, Long.MAX_VALUE));
        AtomicOpResult r = e.op(longCmd(AtomicOp.ADD, 1));
        assertThat(r.value()).isEqualTo(Long.MIN_VALUE);
        assertThat(r.version()).isEqualTo(2);
    }

    @Test
    void conditionalAddMismatchRejectsWithoutTouch() {
        AtomicEntry e = entry(0);
        e.op(longCmd(AtomicOp.ADD, 4));           // version 1
        AtomicOpCommand bad = cmd(AtomicOp.ADD, LockType.ATOMIC_LONG, 1, 0, 99, 2); // ev=99 断言不符
        AtomicOpResult r = e.op(bad);
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r.applied()).isFalse();
        assertThat(r.value()).isEqualTo(4);
        assertThat(r.version()).isEqualTo(1);
        AtomicOpResult good = e.op(cmd(AtomicOp.ADD, LockType.ATOMIC_LONG, 1, 0, 1, 3)); // ev=1 命中
        assertThat(good.applied()).isTrue();
        assertThat(good.value()).isEqualTo(5);
        assertThat(good.version()).isEqualTo(2);
    }

    @Test
    void casSuccessThenFailureLeavesStateUntouched() {
        AtomicEntry e = entry(0);
        e.op(longCmd(AtomicOp.SET, 5));           // value 5, version 1
        AtomicOpResult win = e.op(cmd(AtomicOp.CAS, LockType.ATOMIC_LONG, 9, 5, 0, 2));
        assertThat(win.applied()).isTrue();
        assertThat(win.oldValue()).isEqualTo(5);
        assertThat(win.value()).isEqualTo(9);
        assertThat(win.version()).isEqualTo(2);
        AtomicOpResult lose = e.op(cmd(AtomicOp.CAS, LockType.ATOMIC_LONG, 7, 5, 0, 3));
        assertThat(lose.applied()).isFalse();
        assertThat(lose.value()).isEqualTo(9);
        assertThat(lose.version()).isEqualTo(2);
    }

    @Test
    void casStampedRequiresBothValueAndVersion() {
        AtomicEntry e = entry(0);
        e.op(longCmd(AtomicOp.SET, 5));           // version 1
        // 值对、版本旧（ABA 场景：值回到 5 但版本已推进）——拒。
        AtomicOpResult stale = e.op(cmd(AtomicOp.CAS_STAMPED, LockType.ATOMIC_LONG, 6, 5, 0, 2)); // ev=0 即不主张
        assertThat(stale.applied()).isTrue();     // ev=0 退化为值 CAS（判例语义）
        assertThat(stale.version()).isEqualTo(2);
        AtomicOpResult mismatch = e.op(cmd(AtomicOp.CAS_STAMPED, LockType.ATOMIC_LONG, 7, 6, 1, 3));
        assertThat(mismatch.applied()).isFalse(); // 版本 1 ≠ 当前 2
        assertThat(mismatch.value()).isEqualTo(6);
        assertThat(mismatch.version()).isEqualTo(2);
        AtomicOpResult hit = e.op(cmd(AtomicOp.CAS_STAMPED, LockType.ATOMIC_LONG, 7, 6, 2, 4));
        assertThat(hit.applied()).isTrue();
        assertThat(hit.value()).isEqualTo(7);
        assertThat(hit.version()).isEqualTo(3);
    }

    @Test
    void integerFormWrapsWithinInt32Domain() {
        AtomicEntry e = new AtomicEntry(K, LockType.ATOMIC_INTEGER, 0);
        e.op(cmd(AtomicOp.SET, LockType.ATOMIC_INTEGER, Integer.MAX_VALUE, 0, 0, 1));
        AtomicOpResult r = e.op(cmd(AtomicOp.ADD, LockType.ATOMIC_INTEGER, 1, 0, 0, 2));
        assertThat(r.value()).isEqualTo(Integer.MIN_VALUE);
        AtomicOpResult masked = e.op(cmd(AtomicOp.SET, LockType.ATOMIC_INTEGER, 1L << 32, 0, 0, 3));
        assertThat(masked.value()).isZero();      // int32 截断
    }

    @Test
    void booleanFormRejectsOutOfRangeWithZeroDisturbance() {
        AtomicEntry e = new AtomicEntry(K, LockType.ATOMIC_BOOLEAN, 0);
        AtomicOpResult bad = e.op(cmd(AtomicOp.SET, LockType.ATOMIC_BOOLEAN, 2, 0, 0, 1));
        assertThat(bad.outcome()).isEqualTo(Outcome.REJECT_ATOMIC_RANGE);
        assertThat(bad.version()).isZero();
        AtomicOpResult badCas = e.op(cmd(AtomicOp.CAS, LockType.ATOMIC_BOOLEAN, 1, 2, 0, 2));
        assertThat(badCas.outcome()).isEqualTo(Outcome.REJECT_ATOMIC_RANGE);
        AtomicOpResult ok = e.op(cmd(AtomicOp.SET, LockType.ATOMIC_BOOLEAN, 1, 0, 0, 3));
        assertThat(ok.applied()).isTrue();
        assertThat(ok.value()).isEqualTo(1);
    }

    @Test
    void initialAssertionMismatchRejectsAndZeroDisturbance() {
        AtomicEntry e = entry(100);
        AtomicOpResult bad = e.op(new AtomicOpCommand(1, 1, K, LockType.ATOMIC_LONG,
                AtomicOp.SET, 5, 0, 0, 200, 1));
        assertThat(bad.outcome()).isEqualTo(Outcome.REJECT_ATOMIC_INIT);
        assertThat(bad.value()).isZero();
        assertThat(e.version()).isZero();
        AtomicOpResult ok = e.op(new AtomicOpCommand(1, 1, K, LockType.ATOMIC_LONG,
                AtomicOp.SET, 5, 0, 0, 100, 2));
        assertThat(ok.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(ok.oldValue()).isEqualTo(100);  // 初值即旧值基准
        assertThat(ok.value()).isEqualTo(5);
        assertThat(ok.version()).isEqualTo(1);
    }

    @Test
    void sameSessionOpSeqReplayDoesNotAdvanceVersion() {
        AtomicEntry e = entry(0);
        AtomicOpCommand add = cmd(AtomicOp.ADD, LockType.ATOMIC_LONG, 1, 0, 0, 7);
        AtomicOpResult first = e.op(add);
        AtomicOpResult replay = e.op(add);
        assertThat(replay.outcome()).isEqualTo(first.outcome());
        assertThat(replay.applied()).isEqualTo(first.applied());
        assertThat(replay.oldValue()).isEqualTo(first.oldValue());
        assertThat(replay.value()).isEqualTo(first.value());
        assertThat(replay.version()).isEqualTo(first.version());
        assertThat(e.value()).isEqualTo(1);
        assertThat(e.version()).isEqualTo(1);
    }

    @Test
    void failedCasAlsoOccupiesDedupSlot() {
        AtomicEntry e = entry(0);
        e.op(longCmd(AtomicOp.SET, 9));           // seq 1 → version 1
        AtomicOpCommand miss = cmd(AtomicOp.CAS, LockType.ATOMIC_LONG, 1, 5, 0, 2); // expected 不符
        AtomicOpResult first = e.op(miss);
        assertThat(first.applied()).isFalse();
        // 失败应答亦占槽：未被他写挤占前重发，重放原"失败"应答、不二次判定。
        AtomicOpResult replay = e.op(miss);
        assertThat(replay.applied()).isFalse();
        assertThat(replay.version()).isEqualTo(first.version());
        // 契约边界（设计声明）：槽被后续写挤占后，旧序号迟到重发不再去重——
        // SDK 同 key 在途写互斥使用户路径不可达，此处钉死裸协议语义。
        e.op(cmd(AtomicOp.SET, LockType.ATOMIC_LONG, 5, 0, 0, 3));
        AtomicOpResult late = e.op(miss);
        assertThat(late.applied()).isTrue();      // 重演判定：此刻值恰为 5，CAS 命中
    }

    @Test
    void slotIsNotSharedAcrossSessions() {
        AtomicEntry e = entry(0);
        AtomicOpCommand a = new AtomicOpCommand(1, 1, K, LockType.ATOMIC_LONG,
                AtomicOp.ADD, 1, 0, 0, 0, 7);
        AtomicOpCommand b = new AtomicOpCommand(2, 1, K, LockType.ATOMIC_LONG,
                AtomicOp.ADD, 1, 0, 0, 0, 7);
        e.op(a);
        AtomicOpResult rb = e.op(b);              // 同 seq 不同会话：照常应用
        assertThat(rb.value()).isEqualTo(2);
        assertThat(rb.version()).isEqualTo(2);
    }

    @Test
    void getNeverTouchesDedupSlot() {
        AtomicEntry e = entry(0);
        e.op(cmd(AtomicOp.ADD, LockType.ATOMIC_LONG, 1, 0, 0, 7));
        e.op(cmd(AtomicOp.GET, LockType.ATOMIC_LONG, 0, 0, 0, 0));
        AtomicOpResult rereplay = e.op(cmd(AtomicOp.ADD, LockType.ATOMIC_LONG, 1, 0, 0, 7));
        assertThat(rereplay.value()).isEqualTo(1); // 槽未被 GET 挤占
        assertThat(rereplay.version()).isEqualTo(1);
    }
}
