package io.github.lamspace.openlatch.core;

import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.ConditionOp;
import io.github.lamspace.openlatch.core.command.ConditionOpCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.AwaitReleaseResult;
import io.github.lamspace.openlatch.core.result.ConditionOpResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code CoreEngine} 条件门面用例组（v9）：单机全生命周期闭环（折叠 await 排队
 * →他者获取→SIGNAL 搬运→释放接力通知→清除折叠位的重发授予，重入自 1 级起）、
 * 门面守卫（会话/家族/条件名合法性/无条目三形）、acquire 分派的形状守卫兜底、
 * 重挂幂等、深度护栏、会话关闭三路收口之一、集群应用点 release-only 入口。
 */
class CoreEngineConditionTest {

    /** 可变时钟。 */
    private MutableClock clock;
    /** 记录型监听器（通知出口断言）。 */
    private RecordingListener listener;
    /** 被测引擎。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    /** 普通获取命令。 */
    private static AcquireCommand acq(long sid, long rid, String key, LockType type, long tid) {
        return new AcquireCommand(sid, rid, key, type, tid, 0, true);
    }

    /** 折叠 await 命令（condition 非空）。 */
    private static AcquireCommand await(long sid, long rid, String key, long tid, String cond) {
        return new AcquireCommand(sid, rid, key, LockType.REENTRANT, tid, 0, true, 1, 0, cond);
    }

    /** signal 家族命令。 */
    private static ConditionOpCommand cond(long sid, long tid, String key, String name,
            ConditionOp op, long awaitRid) {
        return new ConditionOpCommand(sid, tid, key, name, op, awaitRid);
    }

    /**
     * 单机全生命周期闭环：await 排队回执 QUEUED 且持有清零；他者获取、signal
     * 搬运（此时无通知——锁忙）；释放经监听器送达队首通知；唤醒重发（已无
     * condition）被授予且重入 1 级。
     */
    @Test
    void fullLoopThroughEngineFacade() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        AcquireResult h = engine.acquire(acq(a, 1, "lk", LockType.REENTRANT, 11));
        assertThat(h.outcome()).isEqualTo(Outcome.GRANTED);

        AcquireResult w = engine.acquire(await(a, 2, "lk", 11, "c"));
        assertThat(w.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(w.queuePosition()).isEqualTo(1);
        assertThat(engine.inspectKey("lk").holders()).isEmpty();
        assertThat(engine.conditionWaiterCount("lk")).isEqualTo(1);
        assertThat(engine.conditionWaiters("lk")).singleElement().satisfies(v -> {
            assertThat(v.sessionId()).isEqualTo(a);
            assertThat(v.requestId()).isEqualTo(2);
            assertThat(v.condition()).isEqualTo("c");
        });

        // 他者入锁（快路径），signal 搬运但锁忙不推
        AcquireResult hb = engine.acquire(acq(b, 10, "lk", LockType.REENTRANT, 21));
        assertThat(hb.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.conditionOp(cond(b, 21, "lk", "c", ConditionOp.SIGNAL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.OK);
        assertThat(listener.events()).isEmpty();

        // 释放接力：队首通知经监听器出口送达原折叠请求 id
        assertThat(engine.release(new ReleaseCommand(b, "lk", hb.leaseToken(), 21)).status())
                .isEqualTo(ReleaseStatus.OK);
        assertThat(listener.events()).singleElement().satisfies(e -> {
            assertThat(e.sessionId()).isEqualTo(a);
            assertThat(e.requestId()).isEqualTo(2);
        });

        // 唤醒重发：清除 condition 的普通获取授予，重入自 1 级起
        AcquireResult wake = engine.acquire(acq(a, 2, "lk", LockType.REENTRANT, 11));
        assertThat(wake.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(engine.inspectKey("lk").holders()).singleElement().satisfies(oh -> {
            assertThat(oh.sessionId()).isEqualTo(a);
            assertThat(oh.count()).isEqualTo(1);
        });
        assertThat(engine.conditionWaiterCount("lk")).isZero();
    }

    /** 门面守卫：会话失效、无条目三形（signal/signalAll→NOT_HELD、leave→OK）。 */
    @Test
    void facadeGuards() {
        long s = engine.sessionOpened();
        assertThat(engine.conditionOp(cond(s, 1, "lk", "c", ConditionOp.SIGNAL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.NOT_HELD);
        assertThat(engine.conditionOp(cond(s, 1, "lk", "c", ConditionOp.SIGNAL_ALL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.NOT_HELD);
        assertThat(engine.conditionOp(cond(s, 1, "lk", "c", ConditionOp.LEAVE, 7)).status())
                .isEqualTo(ConditionOpResult.Status.OK);
        long closed = engine.sessionOpened();
        engine.sessionClosed(closed);
        assertThat(engine.conditionOp(cond(closed, 1, "lk", "c", ConditionOp.SIGNAL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.REJECT_SESSION);
    }

    /** 门面守卫：key 与条件名长度/空形拒绝（core 防御兜底，线路在接入层）。 */
    @Test
    void facadeKeyAndConditionShapeGuards() {
        long s = engine.sessionOpened();
        assertThat(engine.conditionOp(cond(s, 1, "", "c", ConditionOp.SIGNAL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.REJECT_KEY_EMPTY);
        assertThat(engine.conditionOp(cond(s, 1, "k".repeat(600), "c",
                ConditionOp.SIGNAL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.REJECT_KEY_TOO_LONG);
        assertThat(engine.conditionOp(cond(s, 1, "lk", "x".repeat(600),
                ConditionOp.SIGNAL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.REJECT_KEY_TOO_LONG);
        assertThat(engine.conditionOp(new ConditionOpCommand(s, 1, "lk", "",
                ConditionOp.SIGNAL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.REJECT_KEY_EMPTY);
        assertThat(engine.conditionOp(new ConditionOpCommand(s, 1, "lk", null,
                ConditionOp.SIGNAL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.REJECT_KEY_EMPTY);
        byte[] ignored = "x".getBytes(StandardCharsets.UTF_8);
        assertThat(ignored.length).isEqualTo(1); // 编码口径引用（长度按 UTF-8 字节）
    }

    /** 家族不匹配：SEMAPHORE key 上的 signal 家族整体 REJECT_TYPE_MISMATCH。 */
    @Test
    void nonLockFamilyRejected() {
        long s = engine.sessionOpened();
        engine.acquire(new AcquireCommand(s, 1, "sem", LockType.SEMAPHORE, 1, 0, true, 1, 4));
        assertThat(engine.conditionOp(cond(s, 1, "sem", "c", ConditionOp.SIGNAL, 0)).status())
                .isEqualTo(ConditionOpResult.Status.REJECT_TYPE_MISMATCH);
        assertThat(engine.conditionOp(cond(s, 1, "sem", "c", ConditionOp.LEAVE, 9)).status())
                .isEqualTo(ConditionOpResult.Status.REJECT_TYPE_MISMATCH);
        assertThat(engine.conditionWaiterCount("sem")).isZero();
        assertThat(engine.conditionWaiters("sem")).isEmpty();
    }

    /** acquire 分派的形状守卫兜底：READ/WRITE 或非 LOCK 家族携带 condition 拒绝且零建条目。 */
    @Test
    void shapeGuardOnAcquireDispatchCreatesNothing() {
        long s = engine.sessionOpened();
        assertThat(engine.acquire(new AcquireCommand(s, 1, "rw", LockType.READ, 1, 0, true,
                1, 0, "c")).outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.acquire(new AcquireCommand(s, 2, "sp", LockType.SEMAPHORE, 1, 0,
                true, 1, 4, "c")).outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.inspect().keys()).isEmpty();
    }

    /** 重挂幂等（引擎级）：同 (会话,请求) 二次折叠回执同位次、登记恒一条。 */
    @Test
    void engineLevelReregisterIdempotent() {
        long s = engine.sessionOpened();
        long holder = engine.sessionOpened();
        engine.acquire(acq(holder, 1, "lk", LockType.REENTRANT, 5));
        assertThat(engine.acquire(await(s, 9, "lk", 1, "c")).queuePosition()).isEqualTo(1);
        assertThat(engine.acquire(await(s, 9, "lk", 1, "c")).queuePosition()).isEqualTo(1);
        assertThat(engine.conditionWaiterCount("lk")).isEqualTo(1);
    }

    /** 合并深度护栏（引擎级）：队列+集合计达限即 REJECT_QUEUE_FULL。 */
    @Test
    void engineLevelDepthGuardrail() {
        engine = new CoreEngine(new CoreConfig(30_000, 1_000, 3_600_000, 5_000, 512, 2),
                clock, listener);
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();
        engine.acquire(acq(a, 1, "lk", LockType.REENTRANT, 11));       // 持有
        assertThat(engine.acquire(acq(b, 2, "lk", LockType.REENTRANT, 21)).outcome())
                .isEqualTo(Outcome.QUEUED);                            // 队列 1
        assertThat(engine.acquire(await(c, 3, "lk", 31, "c")).outcome())
                .isEqualTo(Outcome.QUEUED);                            // 合计 2
        assertThat(engine.acquire(await(c, 4, "lk", 31, "c")).outcome())
                .isEqualTo(Outcome.REJECT_QUEUE_FULL);
        assertThat(engine.conditionWaiterCount("lk")).isEqualTo(1);
    }

    /** 会话关闭收口：仅摘该会话条件登记；他者持有与登记零触碰。 */
    @Test
    void sessionClosedPurgesOnlyOwnRegistrations() {
        long a = engine.sessionOpened();
        long dead = engine.sessionOpened();
        long ghost = engine.sessionOpened();
        engine.acquire(acq(a, 1, "lk", LockType.REENTRANT, 11));
        engine.acquire(await(dead, 5, "lk", 12, "c"));
        engine.acquire(await(ghost, 6, "lk", 13, "c"));
        engine.sessionClosed(dead);
        assertThat(engine.conditionWaiterCount("lk")).isEqualTo(1);
        assertThat(engine.conditionWaiters("lk")).singleElement()
                .satisfies(v -> assertThat(v.sessionId()).isEqualTo(ghost));
        assertThat(engine.inspectKey("lk").holders()).singleElement()
                .satisfies(h -> assertThat(h.sessionId()).isEqualTo(a));
    }

    /** 集群应用点入口：release-only 半程经门面守卫执行，登记恒零。 */
    @Test
    void awaitFoldReleaseIsReleaseOnly() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.acquire(acq(a, 1, "lk", LockType.REENTRANT, 11));
        AwaitReleaseResult r1 = engine.awaitFoldRelease(await(a, 2, "lk", 11, "c"));
        assertThat(r1.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r1.released()).isTrue();
        AwaitReleaseResult replay = engine.awaitFoldRelease(await(a, 2, "lk", 11, "c"));
        assertThat(replay.released()).isFalse();
        assertThat(engine.conditionWaiterCount("lk")).isZero();
        engine.acquire(acq(b, 9, "lk", LockType.REENTRANT, 21));
        assertThat(engine.awaitFoldRelease(await(a, 3, "lk", 11, "c")).released()).isFalse();
    }
}
