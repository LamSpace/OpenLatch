package io.github.lamspace.openlatch.core;

import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.AtomicOpCommand;
import io.github.lamspace.openlatch.core.command.LatchCountDownCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.result.AtomicOpResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ATOMIC 家族引擎用例组：写路径懒建与初值主张、GET 不建条目、跨家族与
 * 跨形态互拒、会话死亡值不变（值不绑定归属）、已关会话拒操作、
 * ACQUIRE 携带原子类型守卫、key 校验复用。手工时钟，无 sleep。
 */
class CoreEngineAtomicTest {

    /** 手工时钟。 */
    private MutableClock clock;
    /** 记录型监听器。 */
    private RecordingListener listener;
    /** 被测引擎。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    /** 构造原子命令（会话 sid、请求 1）。 */
    private AtomicOpCommand at(long sid, String key, LockType kind, AtomicOp op,
            long operand, long expected, long expectedVersion, long initial, long opSeq) {
        return new AtomicOpCommand(sid, 1, key, kind, op, operand, expected,
                expectedVersion, initial, opSeq);
    }

    /** 无主张 long 写命令简构。 */
    private AtomicOpResult set(long sid, String key, long v, long opSeq) {
        return engine.atomicOp(at(sid, key, LockType.ATOMIC_LONG, AtomicOp.SET, v, 0, 0, 0, opSeq));
    }

    @Test
    void writeLazilyCreatesWithInitialBaseline() {
        long a = engine.sessionOpened();
        AtomicOpResult r = engine.atomicOp(at(a, "k", LockType.ATOMIC_LONG,
                AtomicOp.SET, 5, 0, 0, 100, 1));
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r.oldValue()).isEqualTo(100);
        assertThat(r.value()).isEqualTo(5);
        assertThat(r.version()).isEqualTo(1);
    }

    @Test
    void getOnAbsentKeyReturnsZeroAndCreatesNothing() {
        long a = engine.sessionOpened();
        AtomicOpResult r = engine.atomicOp(at(a, "k", LockType.ATOMIC_LONG,
                AtomicOp.GET, 0, 0, 0, 0, 0));
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r.value()).isZero();
        assertThat(r.version()).isZero();
        // 条目未建：随后带初值主张的 SET 正常创建（100 为旧值基准）。
        AtomicOpResult w = engine.atomicOp(at(a, "k", LockType.ATOMIC_LONG,
                AtomicOp.SET, 5, 0, 0, 100, 1));
        assertThat(w.oldValue()).isEqualTo(100);
    }

    @Test
    void crossFamilyAndCrossFormRejectionsLeaveStateUntouched() {
        long a = engine.sessionOpened();
        set(a, "k", 7, 1);
        // 锁家族请求打到原子 key。
        assertThat(engine.acquire(new AcquireCommand(a, 2, "k", LockType.REENTRANT, 9, 30_000, true))
                .outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // 屏障通道打到原子 key。
        assertThat(engine.countDown(new LatchCountDownCommand(a, "k", 1, 0))
                .outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // 原子请求打到锁 key。
        long b = engine.sessionOpened();
        assertThat(engine.acquire(new AcquireCommand(b, 3, "lk", LockType.REENTRANT, 9, 30_000, true))
                .outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.atomicOp(at(b, "lk", LockType.ATOMIC_LONG, AtomicOp.GET, 0, 0, 0, 0, 0))
                .outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // 同家族跨形态互拒：INTEGER 打 LONG key。
        assertThat(engine.atomicOp(at(a, "k", LockType.ATOMIC_INTEGER, AtomicOp.SET, 1, 0, 0, 0, 2))
                .outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // 原值逐项不变。
        assertThat(engine.atomicOp(at(a, "k", LockType.ATOMIC_LONG, AtomicOp.GET, 0, 0, 0, 0, 0))
                .value()).isEqualTo(7);
    }

    @Test
    void sessionDeathLeavesValueUntouched() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        set(a, "k", 42, 1);
        engine.sessionClosed(a);
        AtomicOpResult r = engine.atomicOp(at(b, "k", LockType.ATOMIC_LONG, AtomicOp.GET, 0, 0, 0, 0, 0));
        assertThat(r.value()).isEqualTo(42);
        assertThat(r.version()).isEqualTo(1);
        // 死者会话续写被拒，生者可继续写。
        assertThat(set(a, "k", 99, 2).outcome()).isEqualTo(Outcome.REJECT_SESSION);
        assertThat(set(b, "k", 99, 2).outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void closedSessionRejectedBeforeEntryTouch() {
        long dead = 424242L;
        assertThat(set(dead, "k", 1, 1).outcome()).isEqualTo(Outcome.REJECT_SESSION);
    }

    @Test
    void acquireCarryingAtomicKindRejected() {
        long a = engine.sessionOpened();
        assertThat(engine.acquire(new AcquireCommand(a, 1, "k", LockType.ATOMIC_LONG, 9, 30_000, true))
                .outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
    }

    @Test
    void keyValidationSharedWithLockChannels() {
        long a = engine.sessionOpened();
        assertThat(engine.atomicOp(at(a, "", LockType.ATOMIC_LONG, AtomicOp.GET, 0, 0, 0, 0, 0))
                .outcome()).isEqualTo(Outcome.REJECT_KEY_EMPTY);
        assertThat(engine.atomicOp(at(a, null, LockType.ATOMIC_LONG, AtomicOp.GET, 0, 0, 0, 0, 0))
                .outcome()).isEqualTo(Outcome.REJECT_KEY_EMPTY);
    }

    @Test
    void entrySurvivesReleaseAndExpiryPaths() {
        long a = engine.sessionOpened();
        set(a, "k", 5, 1);
        // 无任何租约持有：到期扫描与释放清扫均不触及原子条目。
        assertThat(engine.expireDue()).isZero();
        assertThat(engine.release(new ReleaseCommand(a, "k", 1, 9, 1)).status()
                .name()).isNotEmpty();
        assertThat(engine.atomicOp(at(a, "k", LockType.ATOMIC_LONG, AtomicOp.GET, 0, 0, 0, 0, 0))
                .value()).isEqualTo(5);
        clock.advance(60_000);
        engine.expireDue();
        assertThat(engine.atomicOp(at(a, "k", LockType.ATOMIC_LONG, AtomicOp.GET, 0, 0, 0, 0, 0))
                .value()).isEqualTo(5);
    }
}
