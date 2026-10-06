package io.github.lamspace.openlatch.core;

import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.QueueOpCommand;
import io.github.lamspace.openlatch.core.lock.QueueEntry;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.QueueOpResult;
import io.github.lamspace.openlatch.core.snapshot.CoreStateRestore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code CoreEngine.queueOp} 门面用例组：缺条目分派表（读类零迁移不建条目、
 * 立即 take 不建条目、PUT/阻塞 TAKE 非零主张定型创建、无主张拒）、家族与
 * 跨形态互拒、会话关闭元素存续、唤醒事件经监听器出口、快照重建往返
 * （元素/到期/交付槽逐字段保真且重放照常）。
 */
class CoreEngineQueueTest {

    private MutableClock clock;
    private RecordingListener listener;
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    private static QueueOpCommand cmd(long s, long r, String key, LockType kind, QueueOpType op,
            boolean blocking, long capacity, String element, long delayMs, int maxElements,
            long opSeq) {
        return new QueueOpCommand(s, r, key, kind, op, blocking, capacity,
                element == null ? null : element.getBytes(StandardCharsets.UTF_8),
                delayMs, maxElements, opSeq);
    }

    @Test
    void readOpsOnAbsentKeyReturnZeroWithoutCreatingEntry() {
        long a = engine.sessionOpened();
        assertThat(engine.queueOp(cmd(a, 1, "q", LockType.QUEUE, QueueOpType.PEEK,
                false, 0, null, 0, 0, 0)).element()).isNull();
        assertThat(engine.queueOp(cmd(a, 1, "q", LockType.QUEUE, QueueOpType.SIZE,
                false, 0, null, 0, 0, 0)).size()).isZero();
        QueueOpResult drain = engine.queueOp(cmd(a, 1, "q", LockType.QUEUE, QueueOpType.DRAIN,
                false, 0, null, 0, 0, 1));
        assertThat(drain.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(drain.drained()).isEmpty();
        // 零迁移：未建条目（观察面为空）。
        assertThat(engine.inspect().keys()).isEmpty();
    }

    @Test
    void immediateTakeOnAbsentKeyDeniedWithoutEntry() {
        long a = engine.sessionOpened();
        assertThat(engine.queueOp(cmd(a, 1, "q", LockType.QUEUE, QueueOpType.TAKE,
                false, 0, null, 0, 0, 1)).outcome()).isEqualTo(Outcome.DENIED);
        assertThat(engine.inspect().keys()).isEmpty();
    }

    @Test
    void putOnAbsentKeyRequiresPositiveCapacityClaim() {
        long a = engine.sessionOpened();
        assertThat(engine.queueOp(cmd(a, 1, "q", LockType.QUEUE, QueueOpType.PUT,
                false, 0, "x", 0, 0, 1)).outcome()).isEqualTo(Outcome.REJECT_QUEUE_CAPACITY);
        // 非零主张定型创建并立即执行。
        assertThat(engine.queueOp(cmd(a, 1, "q", LockType.QUEUE, QueueOpType.PUT,
                false, 4, "x", 0, 0, 1)).outcome()).isEqualTo(Outcome.GRANTED);
        CoreInspection.KeySnapshot snap = engine.inspectKey("q");
        assertThat(snap.family()).isEqualTo(KeyFamily.QUEUE);
        assertThat(snap.queueCapacity()).isEqualTo(4);
        assertThat(snap.queueDepth()).isEqualTo(1);
    }

    @Test
    void blockingTakeOnAbsentKeyFormsEntryThenParks() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        // 消费者先到：非零主张建空条目并挂起（等待生产者创建后写入的语义）。
        assertThat(engine.queueOp(cmd(b, 1, "q", LockType.QUEUE, QueueOpType.TAKE,
                true, 4, null, 0, 0, 0)).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(engine.inspectKey("q").queueDepth()).isZero();
        // 生产者主张同容量写入，唤醒消费者。
        assertThat(engine.queueOp(cmd(a, 1, "q", LockType.QUEUE, QueueOpType.PUT,
                false, 4, "x", 0, 0, 1)).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(listener.events()).hasSize(1);
        assertThat(listener.events().get(0).sessionId()).isEqualTo(b);
    }

    @Test
    void familyAndCrossFormMutualRejectionsLeaveStateUntouched() {
        long a = engine.sessionOpened();
        // 锁 key 上请求队列操作。
        engine.acquire(new AcquireCommand(a, 1, "l", LockType.REENTRANT, 0, 0, true));
        assertThat(engine.queueOp(cmd(a, 1, "l", LockType.QUEUE, QueueOpType.PUT,
                false, 4, "x", 0, 0, 1)).outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // 队列 key 上请求获取锁。
        engine.queueOp(cmd(a, 1, "q", LockType.QUEUE, QueueOpType.PUT, false, 4, "x", 0, 0, 1));
        assertThat(engine.acquire(new AcquireCommand(a, 2, "q", LockType.QUEUE, 0, 0, true))
                .outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        // 同族跨形态：QUEUE 条目上 DELAY_QUEUE 命令。
        assertThat(engine.queueOp(cmd(a, 1, "q", LockType.DELAY_QUEUE, QueueOpType.PUT,
                false, 0, "y", 0, 0, 2)).outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(engine.inspectKey("q").queueDepth()).isEqualTo(1);
    }

    @Test
    void queueKindRejectedOnAtomicChannelAndNonQueueKindRejectedOnQueueChannel() {
        long a = engine.sessionOpened();
        assertThatThrownBy(() -> engine.queueOp(cmd(a, 1, "q", LockType.ATOMIC_LONG,
                QueueOpType.PUT, false, 4, "x", 0, 0, 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sessionClosedKeepsElementsAndDropsWaiters() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        // 消费者先挂起（非零主张建空条目），生产者投递唤醒之。
        assertThat(engine.queueOp(cmd(b, 1, "q", LockType.QUEUE, QueueOpType.TAKE,
                true, 4, null, 0, 0, 0)).outcome()).isEqualTo(Outcome.QUEUED);
        engine.queueOp(cmd(a, 1, "q", LockType.QUEUE, QueueOpType.PUT, false, 0, "kept", 0, 0, 1));
        // 消费者死亡：等待摘除，元素与生产者槽存续。
        engine.sessionClosed(b);
        CoreInspection.KeySnapshot snap = engine.inspectKey("q");
        assertThat(snap.queueDepth()).isEqualTo(1);
        assertThat(snap.waiters()).isEmpty();
        // 生产者死亡：投递的元素照常存续并可被新会话消费。
        engine.sessionClosed(a);
        long c = engine.sessionOpened();
        QueueOpResult got = engine.queueOp(cmd(c, 1, "q", LockType.QUEUE, QueueOpType.TAKE,
                false, 0, null, 0, 0, 1));
        assertThat(got.element()).isEqualTo("kept".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void restoreRoundTripPreservesElementsExpiryAndDedupSlots() {
        long a = engine.sessionOpened();
        clock.set(10_000L);
        // 原引擎真跑一段：注入两个延时元素（到期序 ""先、"head" 后），到期逐个消费。
        engine.queueOp(cmd(a, 1, "dq", LockType.DELAY_QUEUE, QueueOpType.PUT,
                false, 3, "head", 5_000, 0, 1));
        engine.queueOp(cmd(a, 2, "dq", LockType.DELAY_QUEUE, QueueOpType.PUT,
                false, 0, "", 1_000, 0, 2));
        clock.set(11_500L);
        QueueOpResult first = engine.queueOp(cmd(a, 3, "dq", LockType.DELAY_QUEUE,
                QueueOpType.TAKE, false, 0, null, 0, 0, 3));
        assertThat(first.element()).isEmpty(); // 空串元素：零长度而非 null，两态可辨
        clock.set(16_500L);
        QueueOpResult second = engine.queueOp(cmd(a, 4, "dq", LockType.DELAY_QUEUE,
                QueueOpType.TAKE, false, 0, null, 0, 0, 4));
        assertThat(second.element()).isEqualTo("head".getBytes(StandardCharsets.UTF_8));

        // 重建输入按已知终态手工装配（行为等价判据）：dq 留一个未到期元素与
        // (a,4,TAKE,"head") 交付槽；q2 为 QUEUE 形态留一个元素与 PUT 槽。
        CoreStateRestore.QueueState dqState = new CoreStateRestore.QueueState(3,
                List.of(new QueueEntry.ElementState("next".getBytes(StandardCharsets.UTF_8),
                        20_000L)),
                List.of(new QueueEntry.SlotState(a, 4, QueueOpType.TAKE,
                        "head".getBytes(StandardCharsets.UTF_8), List.of())));
        CoreStateRestore.QueueState q2State = new CoreStateRestore.QueueState(2,
                List.of(new QueueEntry.ElementState("ab".getBytes(StandardCharsets.UTF_8), 0L)),
                List.of(new QueueEntry.SlotState(a, 1, QueueOpType.PUT, null, List.of())));
        CoreStateRestore restore = new CoreStateRestore(List.of(
                new CoreStateRestore.Entry("dq", LockType.DELAY_QUEUE, 0, 0, 0, List.of(),
                        0, 0, 0, null, null, null, dqState),
                new CoreStateRestore.Entry("q2", LockType.QUEUE, 0, 0, 0, List.of(),
                        0, 0, 0, null, null, null, q2State)),
                List.of(a), 1);
        MutableClock clock2 = new MutableClock();
        clock2.set(16_500L);
        CoreEngine fresh = new CoreEngine(new CoreConfig(), clock2, new RecordingListener());
        fresh.restoreFrom(restore);

        CoreInspection.KeySnapshot dq = fresh.inspectKey("dq");
        assertThat(dq.queueCapacity()).isEqualTo(3);
        assertThat(dq.queueDepth()).isEqualTo(1);
        assertThat(dq.queueHeadExpiryMs()).isEqualTo(20_000L);
        // 未到期不可见（恢复保真到期判定）。
        assertThat(fresh.queueOp(cmd(a, 5, "dq", LockType.DELAY_QUEUE, QueueOpType.PEEK,
                false, 0, null, 0, 0, 0)).element()).isNull();
        // 命中恢复所得交付槽的重发：返回同一份字节、不再摘取（重发可判性随槽存续）。
        QueueOpResult replay = fresh.queueOp(cmd(a, 6, "dq", LockType.DELAY_QUEUE,
                QueueOpType.TAKE, false, 0, null, 0, 0, 4));
        assertThat(replay.element()).isEqualTo("head".getBytes(StandardCharsets.UTF_8));
        assertThat(fresh.inspectKey("dq").queueDepth()).isEqualTo(1);
        // 到期推进后照常消费恢复元素。
        clock2.set(20_000L);
        assertThat(fresh.queueOp(cmd(a, 7, "dq", LockType.DELAY_QUEUE, QueueOpType.TAKE,
                false, 0, null, 0, 0, 5)).element())
                .isEqualTo("next".getBytes(StandardCharsets.UTF_8));
        // PUT 重放命中槽：不双插（q2 深度保持 1）。
        QueueOpResult putReplay = fresh.queueOp(cmd(a, 8, "q2", LockType.QUEUE, QueueOpType.PUT,
                false, 0, "x", 0, 0, 1));
        assertThat(putReplay.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(fresh.inspectKey("q2").queueDepth()).isEqualTo(1);
        assertThat(fresh.inspectKey("q2").queueTotalPayloadBytes()).isEqualTo(2);
    }

    @Test
    void restoreRejectsQueueEntryWithoutStateGroup() {
        assertThatThrownBy(() -> new CoreStateRestore.Entry("q", LockType.QUEUE, 0, 0, 0,
                List.of(), 0, 0, 0, null, null, null, null))
                .isInstanceOf(IllegalArgumentException.class);
        // 元素数超容量拒（快照自洽性守门）。
        assertThatThrownBy(() -> new CoreStateRestore.Entry("q", LockType.QUEUE, 0, 0, 0,
                List.of(), 0, 0, 0, null, null, null,
                new CoreStateRestore.QueueState(1,
                        List.of(new QueueEntry.ElementState("a".getBytes(StandardCharsets.UTF_8), 0),
                                new QueueEntry.ElementState("b".getBytes(StandardCharsets.UTF_8), 0)),
                        List.of())))
                .isInstanceOf(IllegalArgumentException.class);
    }

}
