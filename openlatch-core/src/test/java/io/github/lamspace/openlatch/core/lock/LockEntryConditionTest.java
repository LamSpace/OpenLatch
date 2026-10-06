package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.ConditionOp;
import io.github.lamspace.openlatch.core.command.ConditionOpCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.ConditionOpResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code LockEntry} 条件等待集判定矩阵（v9，单机折叠路径）：折叠登记的
 * 全量释放一步清零、非持有 ghost 宽容、重挂幂等、合并深度护栏（队列+集合计）、
 * 队首重发防御分支、SIGNAL 到达序搬运与权限三形、SIGNAL_ALL 排除自身、
 * LEAVE 幂等摘除、授予侧收口、会话摘除"死亡不吞锁"、条目回收判据含集、
 * 集群应用点 release-only 半程方法。通知经收集列表断言，不入推送层。
 */
class LockEntryConditionTest {

    /** 凭证发生器（同引擎语义）。 */
    private final AtomicLong tokens = new AtomicLong(100);
    /** 被测条目。 */
    private LockEntry entry;
    /** 默认限额配置。 */
    private CoreConfig config;
    /** 通知收集列表。 */
    private List<Waiter> notify;

    @BeforeEach
    void setUp() {
        entry = new LockEntry("lk", true);
        config = new CoreConfig();
        notify = new ArrayList<>();
    }

    /** 普通排队获取。 */
    private AcquireResult acquire(long sid, long rid, long tid) {
        return entry.acquire(new AcquireCommand(sid, rid, "lk", LockType.REENTRANT, tid,
                0, true), 1000L, tokens::getAndIncrement, 30_000L, config);
    }

    /** 折叠 await（登记半程+释放半程同关键区）。 */
    private AcquireResult fold(long sid, long rid, long tid, String cond) {
        return entry.awaitFold(new AcquireCommand(sid, rid, "lk", LockType.REENTRANT, tid,
                0, true, 1, 0, cond), 2000L, tokens::getAndIncrement, 30_000L, config, notify);
    }

    /** SIGNAL。 */
    private ConditionOpResult signal(long sid, long tid, String cond) {
        return entry.signal(new ConditionOpCommand(sid, tid, "lk", cond,
                ConditionOp.SIGNAL, 0), 3000L, config, notify);
    }

    /** SIGNAL_ALL。 */
    private ConditionOpResult signalAll(long sid, long tid, String cond) {
        return entry.signalAll(new ConditionOpCommand(sid, tid, "lk", cond,
                ConditionOp.SIGNAL_ALL, 0), 3000L, config, notify);
    }

    /** LEAVE。 */
    private ConditionOpResult leave(long sid, long rid) {
        return entry.leave(new ConditionOpCommand(sid, 0, "lk", "c",
                ConditionOp.LEAVE, rid));
    }

    /** 释放。 */
    private ReleaseResult release(long sid, long tid, long token) {
        return entry.release(new ReleaseCommand(sid, "lk", token, tid),
                4000L, config.headReplyTimeoutMs(), notify);
    }

    /**
     * 持有者折叠 await：重入 3 级一步清零、租约清除、登记入集；
     * 后续获取走快路径即授予（证明持有确已释放）。
     */
    @Test
    void holderFoldZeroesReentrancyAndRegisters() {
        acquire(1, 1, 11);
        acquire(1, 2, 11);
        acquire(1, 3, 11);
        AcquireResult fold = fold(1, 50, 11, "c");
        assertThat(fold.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(fold.queuePosition()).isEqualTo(1);
        assertThat(entry.conditionWaiterCount()).isEqualTo(1);
        assertThat(entry.conditionWaiterViews()).singleElement().satisfies(v -> {
            assertThat(v.condition()).isEqualTo("c");
            assertThat(v.sessionId()).isEqualTo(1);
            assertThat(v.requestId()).isEqualTo(50);
            assertThat(v.threadId()).isEqualTo(11);
            assertThat(v.registeredAtMs()).isEqualTo(2000L);
        });
        assertThat(entry.waiterCount()).isZero();
        // 释放半程生效：原凭证作废、他者快路径可入
        assertThat(release(1, 11, 100L).status()).isEqualTo(ReleaseStatus.NOT_HELD);
        assertThat(acquire(2, 60, 21).outcome()).isEqualTo(Outcome.GRANTED);
    }

    /**
     * 折叠释放半程唤醒既有队首：入队等待者收到队首通知（事件收集列表）。
     */
    @Test
    void foldReleaseHalfNotifiesDequeHead() {
        acquire(1, 1, 11);
        assertThat(acquire(2, 20, 21).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(notify).isEmpty();
        assertThat(fold(1, 30, 11, "c").outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(notify).singleElement().satisfies(w -> {
            assertThat(w.sessionId()).isEqualTo(2);
            assertThat(w.requestId()).isEqualTo(20);
            assertThat(w.notified()).isTrue();
        });
    }

    /** 非持有折叠 = ghost 登记（重挂/误用宽容面）：释放零操作、登记照常。 */
    @Test
    void nonHolderFoldRegistersGhostWithoutTouchingState() {
        assertThat(fold(9, 40, 91, "x").outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(entry.conditionWaiterCount()).isEqualTo(1);
        assertThat(notify).isEmpty();
        // 锁从未被 ghost 触碰：持有者可照常获取释放
        AcquireResult h = acquire(1, 1, 11);
        assertThat(h.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(release(1, 11, h.leaseToken()).status()).isEqualTo(ReleaseStatus.OK);
    }

    /** 同 (会话, 请求) 重复折叠 = 幂等重挂：不双登记、不重复释放。 */
    @Test
    void reregisterIsIdempotent() {
        assertThat(fold(9, 40, 91, "x").queuePosition()).isEqualTo(1);
        assertThat(fold(9, 40, 91, "x").queuePosition()).isEqualTo(1);
        assertThat(fold(9, 40, 91, "x").outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(entry.conditionWaiterViews()).hasSize(1);
    }

    /** 合并深度护栏：等待队列 + 条件集合计达上限即拒，集合零扰动。 */
    @Test
    void mergedDepthGuardrailCountsQueueAndSets() {
        config = new CoreConfig(30_000, 1_000, 3_600_000, 5_000, 512, 2);
        entry = new LockEntry("lk", true);
        notify = new ArrayList<>();
        acquire(1, 1, 11);                          // 持有
        assertThat(acquire(2, 20, 21).outcome()).isEqualTo(Outcome.QUEUED); // 队列 1
        assertThat(fold(3, 30, 31, "c").outcome()).isEqualTo(Outcome.QUEUED); // 集 1，合计 2
        assertThat(fold(4, 40, 41, "c").outcome())
                .isEqualTo(Outcome.REJECT_QUEUE_FULL);
        assertThat(entry.conditionWaiterCount()).isEqualTo(1);
        assertThat(entry.waiterCount()).isEqualTo(1);
    }

    /** SIGNAL 按到达序搬运一人，权限归属为当前写侧持有者。 */
    @Test
    void signalPromotesInArrivalOrder() {
        AcquireResult h = acquire(1, 1, 11);
        fold(2, 20, 21, "c");
        fold(3, 30, 31, "c");
        fold(4, 40, 41, "c");
        assertThat(signal(1, 11, "c").status()).isEqualTo(ConditionOpResult.Status.OK);
        assertThat(entry.waiterCount()).isEqualTo(1);
        assertThat(entry.conditionWaiterViews()).hasSize(2);
        notify.clear();
        // 搬运时点锁必被持（权限论证）：通知不在此刻发出，由释放接力
        assertThat(release(1, 11, h.leaseToken()).fullyReleased()).isTrue();
        assertThat(notify).singleElement().satisfies(w ->
                assertThat(w.sessionId()).isEqualTo(2));
    }

    /** 权限三形合并 NOT_HELD：无条目写侧、归属不符。 */
    @Test
    void signalAuthorityMismatch() {
        assertThat(signal(1, 11, "c").status()).isEqualTo(ConditionOpResult.Status.NOT_HELD);
        acquire(1, 1, 11);
        assertThat(signal(2, 21, "c").status()).isEqualTo(ConditionOpResult.Status.NOT_HELD);
        assertThat(signal(1, 12, "c").status()).isEqualTo(ConditionOpResult.Status.NOT_HELD);
        assertThat(entry.conditionWaiterCount()).isZero();
    }

    /** 空集/无此名 signal 与 signalAll = 无操作 OK（signal 是事件，不追溯）。 */
    @Test
    void emptySetSignalIsNoopOk() {
        acquire(1, 1, 11);
        assertThat(signal(1, 11, "nope").status()).isEqualTo(ConditionOpResult.Status.OK);
        assertThat(signalAll(1, 11, "nope").status()).isEqualTo(ConditionOpResult.Status.OK);
        assertThat(notify).isEmpty();
    }

    /**
     * 跨线程 ghost 搬运（集群预检窗"持有者在集"的单机对偶形态）：ghost 登记
     * (1,11) 与当前持有 (1,12) 并存时，signal 搬运非调用线程的 ghost 项。
     * 同线程自 ghost 在单机不可达——折叠即释放、重新持有即触发授予侧收口清除
     * （见 {@link #grantPurgesStaleRegistrationsOfSameOwner}），防御性跳过分支
     * 由集群登记器用例承载。
     */
    @Test
    void signalPromotesCrossThreadGhost() {
        acquire(1, 1, 11);
        fold(1, 30, 11, "c");                    // 释放+登记 (1,11)
        assertThat(acquire(1, 2, 12).outcome()).isEqualTo(Outcome.GRANTED); // 他线程持有
        assertThat(entry.conditionWaiterCount()).isEqualTo(1);              // 授予侧收口不误伤他线程
        assertThat(signal(1, 12, "c").status()).isEqualTo(ConditionOpResult.Status.OK);
        assertThat(entry.waiterCount()).isEqualTo(1);
        assertThat(entry.conditionWaiterCount()).isZero();
        assertThat(notify).isEmpty();            // 搬运时点锁必被持：通知由释放接力
    }

    /** LEAVE 按 (会话, 请求) 摘除且幂等；错请求 id 不误伤。 */
    @Test
    void leaveRemovesOnlyTargetAndIsIdempotent() {
        fold(2, 20, 21, "a");
        fold(2, 21, 21, "b");
        assertThat(leave(2, 20).status()).isEqualTo(ConditionOpResult.Status.OK);
        assertThat(entry.conditionWaiterViews()).singleElement()
                .satisfies(v -> assertThat(v.condition()).isEqualTo("b"));
        assertThat(leave(2, 20).status()).isEqualTo(ConditionOpResult.Status.OK); // 幂等
        assertThat(leave(2, 99).status()).isEqualTo(ConditionOpResult.Status.OK); // 无匹配
        assertThat(entry.conditionWaiterViews()).hasSize(1);
    }

    /** 授予侧收口：同归属 (会话,线程) 被授予即摘其全部条件集陈旧登记。 */
    @Test
    void grantPurgesStaleRegistrationsOfSameOwner() {
        fold(5, 50, 51, "c");
        assertThat(entry.conditionWaiterCount()).isEqualTo(1);
        AcquireResult g = acquire(5, 60, 51);
        assertThat(g.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(entry.conditionWaiterCount()).isZero();
    }

    /** 唤醒后仍持折叠信封的重发（防御分支）：队首命中即授予并摘登记。 */
    @Test
    void wakeResendWithFoldEnvelopeGrantsAtHead() {
        AcquireResult h = acquire(1, 1, 11);
        fold(6, 66, 61, "c");
        assertThat(signal(1, 11, "c").status()).isEqualTo(ConditionOpResult.Status.OK);
        release(1, 11, h.leaseToken());   // 释放接力通知搬运项（sid6 队首）
        assertThat(notify).singleElement().satisfies(w -> assertThat(w.sessionId()).isEqualTo(6));
        notify.clear();
        AcquireResult resend = fold(6, 66, 61, "c");
        assertThat(resend.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(resend.leaseToken()).isPositive();
        assertThat(entry.conditionWaiterCount()).isZero();
        assertThat(entry.waiterCount()).isZero();
    }

    /** 会话摘除：只清该会话条件登记与队列等待，持有/租约零触碰（死亡不吞锁）。 */
    @Test
    void removeSessionClearsSetsButKeepsLockState() {
        AcquireResult h = acquire(1, 1, 11);
        fold(2, 20, 21, "c");
        fold(2, 21, 22, "d");
        entry.removeSession(2, 5000L, config.headReplyTimeoutMs(), notify);
        assertThat(entry.conditionWaiterCount()).isZero();
        assertThat(entry.conditionWaiterViews()).isEmpty();
        // 持有者不受影响：原凭证仍可释放
        assertThat(release(1, 11, h.leaseToken()).status()).isEqualTo(ReleaseStatus.OK);
    }

    /** 条目回收判据含集：登记中不可回收，撤登后归空。 */
    @Test
    void isEmptyGuardsEntryReclaimWhileSetNonEmpty() {
        acquire(1, 1, 11);
        release(1, 11, 100L);
        assertThat(entry.isEmpty()).isTrue();
        fold(2, 20, 21, "c");
        assertThat(entry.isEmpty()).isFalse();
        leave(2, 20);
        assertThat(entry.isEmpty()).isTrue();
    }

    /** 集群应用点 release-only 半程：持有则一步清零，非持有恒 false。 */
    @Test
    void awaitFoldReleaseRunsOnlyReleaseHalf() {
        acquire(1, 1, 11);
        assertThat(entry.awaitFoldRelease(1, 11, 6000L, config, notify)).isTrue();
        assertThat(entry.awaitFoldRelease(1, 11, 6000L, config, notify)).isFalse();
        AcquireResult next = acquire(2, 20, 21);
        assertThat(next.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(entry.awaitFoldRelease(9, 99, 6000L, config, notify)).isFalse();
        assertThat(entry.awaitFoldRelease(2, 21, 6000L, config, notify)).isTrue();
    }
}
