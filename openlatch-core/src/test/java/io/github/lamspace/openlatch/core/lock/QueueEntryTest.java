package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.KeyFamily;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.QueueOpType;
import io.github.lamspace.openlatch.core.command.QueueOpCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.QueueOpResult;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link QueueEntry} 判定矩阵用例组：FIFO 与容量双态（挂起/立即）、双轨
 * 唤醒与位次、回弹续挂保位、每会话去重槽（PUT 不双插/TAKE 同份重放/
 * DRAIN 列表重放/跨会话互不遮蔽）、容量与形态断言互拒、DELAY 可见性与
 * 同到期 FIFO、等待深度护栏、会话摘除元素零触碰、观察读数与重建工厂。
 * 纯条目级测试（判定时刻以入参传入，不经引擎、无墙钟）。
 */
class QueueEntryTest {

    /** 测试用键。 */
    private static final String K = "q";

    /** 队首通知响应超时（毫秒），全部用例共用。 */
    private static final long HRT = 5_000L;

    /** 判定基准时刻（毫秒）。 */
    private static final long T0 = 100_000L;

    /** 自增写序号源（连续写各占一槽位，避免被合法去重拦截）。 */
    private long nextSeq = 1;

    /** 构造队列命令（会话/请求显式给，op_seq 自增）。 */
    private QueueOpCommand cmd(long session, long request, LockType kind, QueueOpType op,
            boolean blocking, long capacity, String element, long delayMs, int maxElements) {
        return new QueueOpCommand(session, request, K, kind, op, blocking, capacity,
                element == null ? null : element.getBytes(StandardCharsets.UTF_8),
                delayMs, maxElements, nextSeq++);
    }

    /** 构造显式 op_seq 的命令（重放用例）。 */
    private static QueueOpCommand cmdSeq(long session, long request, LockType kind,
            QueueOpType op, boolean blocking, long capacity, String element, long opSeq) {
        return new QueueOpCommand(session, request, K, kind, op, blocking, capacity,
                element == null ? null : element.getBytes(StandardCharsets.UTF_8),
                0, 0, opSeq);
    }

    /** 队列形态简写命令（put/take/drain 走 QUEUE 形态、容量 4）。 */
    private QueueOpCommand q(QueueOpType op, boolean blocking, String element) {
        return cmd(1, 1, LockType.QUEUE, op, blocking, 0, element, 0, 0);
    }

    /** 建队列条目（容量 4，QUEUE 形态）。 */
    private static QueueEntry queue(long capacity) {
        return QueueEntry.created(K, LockType.QUEUE, capacity);
    }

    /** 执行一次命令（收集通知）。 */
    private static QueueOpResult run(QueueEntry e, QueueOpCommand cmd, long now,
            List<Waiter> notify) {
        return e.op(cmd, now, new CoreConfig(), HRT, notify);
    }

    @Test
    void createdRejectsNonQueueKindAndNonPositiveCapacity() {
        assertThatThrownBy(() -> QueueEntry.created(K, LockType.LATCH, 4))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> QueueEntry.created(K, LockType.QUEUE, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void putTakeFollowArrivalFifo() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        assertThat(run(e, q(QueueOpType.PUT, false, "a"), T0, notify).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(run(e, q(QueueOpType.PUT, false, "b"), T0, notify).outcome())
                .isEqualTo(Outcome.GRANTED);
        assertThat(run(e, cmd(2, 1, LockType.QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0),
                T0, notify).element()).isEqualTo("a".getBytes(StandardCharsets.UTF_8));
        assertThat(run(e, cmd(2, 2, LockType.QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0),
                T0, notify).element()).isEqualTo("b".getBytes(StandardCharsets.UTF_8));
        assertThat(e.depth()).isZero();
    }

    @Test
    void fullQueueBlocksPutAndDeniesOffer() {
        QueueEntry e = queue(2);
        List<Waiter> notify = new ArrayList<>();
        run(e, q(QueueOpType.PUT, false, "a"), T0, notify);
        run(e, q(QueueOpType.PUT, false, "b"), T0, notify);
        // 阻塞式第三个 put 挂起（位次 1），立即式 offer 回 DENIED 零扰动。
        QueueOpCommand third = cmd(3, 10, LockType.QUEUE, QueueOpType.PUT, true, 0, "c", 0, 0);
        QueueOpResult parked = run(e, third, T0, notify);
        assertThat(parked.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(parked.queuePosition()).isEqualTo(1);
        assertThat(run(e, cmd(4, 20, LockType.QUEUE, QueueOpType.PUT, false, 0, "d", 0, 0),
                T0, notify).outcome()).isEqualTo(Outcome.DENIED);
        assertThat(e.depth()).isEqualTo(2);
        // 一次 take 腾容量：仅唤醒 put 轨队首（c），d 早已终结不受影响。
        QueueOpResult took = run(e, cmd(5, 30, LockType.QUEUE, QueueOpType.TAKE, false, 0,
                null, 0, 0), T0, notify);
        assertThat(took.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(notify).hasSize(1);
        assertThat(notify.get(0).sessionId()).isEqualTo(3);
        assertThat(notify.get(0).requestId()).isEqualTo(10);
    }

    @Test
    void parkedPutResendTakesCapacityAndPromotesNext() {
        QueueEntry e = queue(2);
        List<Waiter> notify = new ArrayList<>();
        run(e, q(QueueOpType.PUT, false, "a"), T0, notify);
        run(e, q(QueueOpType.PUT, false, "b"), T0, notify);
        // 两个挂起 put（c 位次 1、d 位次 2），再各腾一个容量。
        QueueOpCommand c = cmd(3, 10, LockType.QUEUE, QueueOpType.PUT, true, 0, "c", 0, 0);
        QueueOpCommand d = cmd(4, 11, LockType.QUEUE, QueueOpType.PUT, true, 0, "d", 0, 0);
        assertThat(run(e, c, T0, notify).queuePosition()).isEqualTo(1);
        assertThat(run(e, d, T0, notify).queuePosition()).isEqualTo(2);
        run(e, cmd(5, 30, LockType.QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0), T0, notify);
        notify.clear();
        // c 的重发（唤醒后抵达）：自我摘除、入队——容量随即再度占满，d 继续等待。
        QueueOpResult cResend = run(e, c, T0, notify);
        assertThat(cResend.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(e.depth()).isEqualTo(2); // a 已取走，剩 b,c
        assertThat(notify).isEmpty(); // 每腾一容量只唤一位（不超发唤醒）
        // 再腾一个容量：d 获唤醒。
        run(e, cmd(6, 31, LockType.QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0), T0 + 1, notify);
        assertThat(notify).hasSize(1);
        assertThat(notify.get(0).sessionId()).isEqualTo(4);
    }

    @Test
    void emptyQueueBlocksTakeAndDeniesPoll() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        assertThat(run(e, cmd(2, 5, LockType.QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0),
                T0, notify).outcome()).isEqualTo(Outcome.DENIED);
        QueueOpCommand t = cmd(2, 6, LockType.QUEUE, QueueOpType.TAKE, true, 0, null, 0, 0);
        assertThat(run(e, t, T0, notify).outcome()).isEqualTo(Outcome.QUEUED);
        // put 落地仅唤 take 轨队首。
        run(e, cmd(1, 7, LockType.QUEUE, QueueOpType.PUT, false, 0, "x", 0, 0), T0, notify);
        assertThat(notify).hasSize(1);
        assertThat(notify.get(0).sessionId()).isEqualTo(2);
        // taker 重发取回 x（同请求自我摘除）。
        QueueOpResult got = run(e, t, T0 + 1, notify);
        assertThat(got.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(got.element()).isEqualTo("x".getBytes(StandardCharsets.UTF_8));
        assertThat(e.depth()).isZero();
    }

    @Test
    void bounceKeepsPositionAndNotifiedMarkCleared() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        QueueOpCommand t = cmd(2, 6, LockType.QUEUE, QueueOpType.TAKE, true, 0, null, 0, 0);
        assertThat(run(e, t, T0, notify).queuePosition()).isEqualTo(1);
        run(e, cmd(1, 7, LockType.QUEUE, QueueOpType.PUT, false, 0, "x", 0, 0), T0, notify);
        // 元素被后来者抢走（竞态窗口）：taker 重发抵达时无可见元素——回弹续挂、位次保持。
        run(e, cmd(9, 8, LockType.QUEUE, QueueOpType.DRAIN, false, 0, null, 0, 0), T0, notify);
        QueueOpResult bounce = run(e, t, T0 + 2, notify);
        assertThat(bounce.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(bounce.queuePosition()).isEqualTo(1);
        // 回弹后仍可被下一次事件唤醒（notified 标记已撤销）。
        notify.clear();
        run(e, cmd(1, 9, LockType.QUEUE, QueueOpType.PUT, false, 0, "y", 0, 0), T0 + 3, notify);
        assertThat(notify).hasSize(1);
        assertThat(notify.get(0).sessionId()).isEqualTo(2);
    }

    @Test
    void blockingNewArrivalDoesNotOvertakeParkedWaiters() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        // take 轨已有等待者，容量恰好空出但队首未被重发：新阻塞 take 不得插队取走。
        run(e, cmd(2, 1, LockType.QUEUE, QueueOpType.TAKE, true, 0, null, 0, 0), T0, notify);
        run(e, cmd(1, 2, LockType.QUEUE, QueueOpType.PUT, false, 0, "a", 0, 0), T0, notify);
        run(e, cmd(1, 3, LockType.QUEUE, QueueOpType.PUT, false, 0, "b", 0, 0), T0, notify);
        QueueOpResult second = run(e, cmd(3, 4, LockType.QUEUE, QueueOpType.TAKE, true, 0,
                null, 0, 0), T0 + 1, notify);
        assertThat(second.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(second.queuePosition()).isEqualTo(2);
    }

    @Test
    void putReplayNeverDoubleInserts() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        long seq = nextSeq++;
        assertThat(run(e, cmdSeq(1, 1, LockType.QUEUE, QueueOpType.PUT, false, 0, "x", seq),
                T0, notify).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(run(e, cmdSeq(1, 1, LockType.QUEUE, QueueOpType.PUT, false, 0, "x", seq),
                T0 + 1, notify).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(e.depth()).isEqualTo(1);
    }

    @Test
    void takeReplayDeliversSameBytesNoSecondPop() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        run(e, q(QueueOpType.PUT, false, "a"), T0, notify);
        run(e, q(QueueOpType.PUT, false, "b"), T0, notify);
        long seq = nextSeq++;
        QueueOpResult first = run(e, cmdSeq(2, 5, LockType.QUEUE, QueueOpType.TAKE, false, 0,
                null, seq), T0, notify);
        assertThat(first.element()).isEqualTo("a".getBytes(StandardCharsets.UTF_8));
        QueueOpResult replay = run(e, cmdSeq(2, 5, LockType.QUEUE, QueueOpType.TAKE, false, 0,
                null, seq), T0 + 1, notify);
        assertThat(replay.element()).isEqualTo("a".getBytes(StandardCharsets.UTF_8));
        assertThat(e.depth()).isEqualTo(1); // b 未被偷吃
    }

    @Test
    void putTrackHeadGateBlocksNonHeadResendAndGrantsInArrivalOrder() {
        // 公平套件同款（队列维度，core 确定性形态）：容量 1 争用下，非队首的
        // 早到重发被队首门挡回（位次不变），队首按挂起到达序逐位兑现——
        // 授予序列 == 挂起序列 x, A, B, C。
        QueueEntry e = queue(1);
        List<Waiter> notify = new ArrayList<>();
        run(e, cmd(1, 1, LockType.QUEUE, QueueOpType.PUT, true, 0, "x", 0, 0), T0, notify);
        assertThat(e.depth()).isEqualTo(1);
        QueueOpCommand a = cmd(11, 20, LockType.QUEUE, QueueOpType.PUT, true, 0, "A", 0, 0);
        QueueOpCommand b = cmd(12, 30, LockType.QUEUE, QueueOpType.PUT, true, 0, "B", 0, 0);
        QueueOpCommand c = cmd(13, 40, LockType.QUEUE, QueueOpType.PUT, true, 0, "C", 0, 0);
        assertThat(run(e, a, T0, notify).queuePosition()).isEqualTo(1);
        assertThat(run(e, b, T0, notify).queuePosition()).isEqualTo(2);
        assertThat(run(e, c, T0, notify).queuePosition()).isEqualTo(3);
        // 队首门：C、B 早到重发在 A 兑现之前被挡回原位。
        assertThat(run(e, c, T0 + 1, notify).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(run(e, c, T0 + 1, notify).queuePosition()).isEqualTo(3);
        assertThat(run(e, b, T0 + 1, notify).queuePosition()).isEqualTo(2);
        // 取走 x → A（队首）重发获批；随后逐位：B、C 依序兑现。
        assertThat(run(e, cmd(9, 9, LockType.QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0),
                T0 + 2, notify).element()).isEqualTo("x".getBytes(StandardCharsets.UTF_8));
        assertThat(run(e, a, T0 + 3, notify).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(run(e, cmd(9, 10, LockType.QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0),
                T0 + 4, notify).element()).isEqualTo("A".getBytes(StandardCharsets.UTF_8));
        assertThat(run(e, b, T0 + 5, notify).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(run(e, c, T0 + 6, notify).outcome()).isNotEqualTo(Outcome.GRANTED); // C 仍非队首
        assertThat(run(e, cmd(9, 11, LockType.QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0),
                T0 + 7, notify).element()).isEqualTo("B".getBytes(StandardCharsets.UTF_8));
        assertThat(run(e, c, T0 + 8, notify).outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(run(e, cmd(9, 12, LockType.QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0),
                T0 + 9, notify).element()).isEqualTo("C".getBytes(StandardCharsets.UTF_8));
        assertThat(e.depth()).isZero();
    }

    @Test
    void drainReplayRedeliversSameBatch() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        run(e, q(QueueOpType.PUT, false, "a"), T0, notify);
        run(e, q(QueueOpType.PUT, false, "b"), T0, notify);
        long seq = nextSeq++;
        QueueOpResult first = run(e, cmdSeq(2, 5, LockType.QUEUE, QueueOpType.DRAIN, false, 0,
                null, seq), T0, notify);
        assertThat(first.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(first.drained()).hasSize(2);
        QueueOpResult replay = run(e, cmdSeq(2, 5, LockType.QUEUE, QueueOpType.DRAIN, false, 0,
                null, seq), T0 + 1, notify);
        assertThat(replay.drained()).hasSize(2);
        assertThat(e.depth()).isZero();
    }

    @Test
    void crossSessionSlotsNeverShadowEachOther() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        long s1 = nextSeq++;
        run(e, cmdSeq(1, 9, LockType.QUEUE, QueueOpType.PUT, false, 0, "p1", s1), T0, notify);
        long s2 = nextSeq++;
        run(e, cmdSeq(2, 8, LockType.QUEUE, QueueOpType.PUT, false, 0, "p2", s2), T0, notify);
        // 会话 1 重发命中自己的槽（会话 2 的写入不遮蔽）。
        QueueOpResult replay = run(e, cmdSeq(1, 9, LockType.QUEUE, QueueOpType.PUT, false, 0,
                "p1", s1), T0 + 1, notify);
        assertThat(replay.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(e.depth()).isEqualTo(2);
    }

    @Test
    void capacityAndFormAssertionsRejectWithoutMutation() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        run(e, q(QueueOpType.PUT, false, "a"), T0, notify);
        // 容量主张不符（8≠4）：拒绝且元素零扰动。
        assertThat(run(e, cmd(1, 1, LockType.QUEUE, QueueOpType.SIZE, false, 8, null, 0, 0),
                T0, notify).outcome()).isEqualTo(Outcome.REJECT_QUEUE_CAPACITY);
        // 跨形态（DELAY 命令打在 QUEUE 条目上）：类型不匹配。
        assertThat(run(e, cmd(1, 1, LockType.DELAY_QUEUE, QueueOpType.PUT, false, 0, "b", 0, 0),
                T0, notify).outcome()).isEqualTo(Outcome.REJECT_TYPE_MISMATCH);
        assertThat(e.depth()).isEqualTo(1);
    }

    @Test
    void delayQueueHidesUnexpiredAndKeepsFifoWithinSameExpiry() {
        QueueEntry e = QueueEntry.created(K, LockType.DELAY_QUEUE, 4);
        List<Waiter> notify = new ArrayList<>();
        run(e, cmd(1, 1, LockType.DELAY_QUEUE, QueueOpType.PUT, false, 0, "late", 5_000, 0),
                T0, notify);
        run(e, cmd(1, 2, LockType.DELAY_QUEUE, QueueOpType.PUT, false, 0, "first", 2_000, 0),
                T0, notify);
        run(e, cmd(1, 3, LockType.DELAY_QUEUE, QueueOpType.PUT, false, 0, "second", 2_000, 0),
                T0, notify);
        // t=+1s：头未到期——PEEK 不可见，SIZE 驻留口径仍计 3，未到期不得被越过。
        QueueOpResult peekEarly = run(e, cmd(2, 4, LockType.DELAY_QUEUE, QueueOpType.PEEK,
                false, 0, null, 0, 0), T0 + 1_000, notify);
        assertThat(peekEarly.element()).isNull();
        assertThat(run(e, cmd(2, 5, LockType.DELAY_QUEUE, QueueOpType.SIZE, false, 0, null, 0, 0),
                T0 + 1_000, notify).size()).isEqualTo(3);
        // t=+2s：两个同到期元素按到达序先出。
        QueueOpResult got1 = run(e, cmd(2, 6, LockType.DELAY_QUEUE, QueueOpType.TAKE, false, 0,
                null, 0, 0), T0 + 2_000, notify);
        assertThat(got1.element()).isEqualTo("first".getBytes(StandardCharsets.UTF_8));
        QueueOpResult got2 = run(e, cmd(2, 7, LockType.DELAY_QUEUE, QueueOpType.TAKE, false, 0,
                null, 0, 0), T0 + 2_000, notify);
        assertThat(got2.element()).isEqualTo("second".getBytes(StandardCharsets.UTF_8));
        // +5s 前 late 仍不可见。
        assertThat(run(e, cmd(2, 8, LockType.DELAY_QUEUE, QueueOpType.TAKE, false, 0, null, 0, 0),
                T0 + 3_000, notify).outcome()).isEqualTo(Outcome.DENIED);
        assertThat(e.headExpiryMs()).isEqualTo(T0 + 5_000);
    }

    @Test
    void drainTakesPartialPrefixAndEmptyIsGranted() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        run(e, q(QueueOpType.PUT, false, "a"), T0, notify);
        run(e, q(QueueOpType.PUT, false, "b"), T0, notify);
        QueueOpResult partial = e.op(cmd(2, 1, LockType.QUEUE, QueueOpType.DRAIN, false, 0,
                null, 0, 1), T0, new CoreConfig(), HRT, notify);
        assertThat(partial.drained()).hasSize(1);
        assertThat(e.depth()).isEqualTo(1);
        run(e, q(QueueOpType.PUT, false, "c"), T0, notify);
        run(e, cmd(3, 2, LockType.QUEUE, QueueOpType.DRAIN, false, 0, null, 0, 0), T0, notify);
        QueueOpResult empty = run(e, cmd(3, 3, LockType.QUEUE, QueueOpType.DRAIN, false, 0,
                null, 0, 0), T0, notify);
        assertThat(empty.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(empty.drained()).isEmpty();
    }

    @Test
    void waitDepthGuardIsSharedAcrossTracks() {
        CoreConfig small = new CoreConfig(CoreConfig.DEFAULT_LEASE_MS, CoreConfig.MIN_LEASE_MS,
                CoreConfig.MAX_LEASE_MS, CoreConfig.HEAD_REPLY_TIMEOUT_MS,
                CoreConfig.MAX_KEY_LENGTH, 2);
        QueueEntry e = queue(1);
        List<Waiter> notify = new ArrayList<>();
        // take 挂起（等待者 1）；put a 恰有容量 → 落地并唤醒该 take（已通知）。
        run(e, cmd(1, 1, LockType.QUEUE, QueueOpType.TAKE, true, 0, null, 0, 0), T0, notify);
        assertThat(run(e, cmd(2, 2, LockType.QUEUE, QueueOpType.PUT, true, 0, "a", 0, 0),
                T0, notify)).isNotNull();
        // put b 挂起（等待者合计 2 = 上限）；put c 触护栏。
        assertThat(e.op(cmd(3, 3, LockType.QUEUE, QueueOpType.PUT, true, 0, "b", 0, 0), T0, small,
                HRT, notify).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(e.op(cmd(4, 4, LockType.QUEUE, QueueOpType.PUT, true, 0, "c", 0, 0), T0, small,
                HRT, notify).outcome()).isEqualTo(Outcome.REJECT_QUEUE_FULL);
        assertThat(e.op(cmd(5, 5, LockType.QUEUE, QueueOpType.TAKE, true, 0, null, 0, 0), T0, small,
                HRT, notify).outcome()).isEqualTo(Outcome.REJECT_QUEUE_FULL);
    }

    @Test
    void sessionRemovalKeepsElementsAndDropsWaitersAndSlots() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        // 消费者先挂起（空队），生产者随后投递并唤醒之。
        assertThat(run(e, cmd(2, 2, LockType.QUEUE, QueueOpType.TAKE, true, 0, null, 0, 0),
                T0, notify).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(e.waiterCount()).isEqualTo(1);
        notify.clear();
        run(e, cmd(1, 1, LockType.QUEUE, QueueOpType.PUT, false, 0, "kept", 0, 0), T0 + 1, notify);
        assertThat(notify).hasSize(1); // put 落地唤 take 队首（非 removeSession 产生）
        // 生产者死亡：元素存续、其 PUT 槽摘除；take 队首已被前次 put 标记"已
        // 通知"，让位收集对其不重复推送（唤醒一次性判例）。
        e.removeSession(1, T0 + 2, HRT, notify);
        assertThat(e.depth()).isEqualTo(1);
        assertThat(e.slotsState()).isEmpty(); // 消费者挂起不占槽——槽随"已应用写"产生
        assertThat(notify).hasSize(1);
        assertThat(e.waiterCount()).isEqualTo(1);
        // 第二位消费者挂起于轨尾；随后**已通知队首死亡**——让位收集立即唤醒后继。
        assertThat(run(e, cmd(4, 4, LockType.QUEUE, QueueOpType.TAKE, true, 0, null, 0, 0),
                T0 + 3, notify).queuePosition()).isEqualTo(2);
        e.removeSession(2, T0 + 4, HRT, notify);
        assertThat(notify).hasSize(2);
        assertThat(notify.get(1).sessionId()).isEqualTo(4); // 死亡让位的续唤
        assertThat(e.waiterCount()).isEqualTo(1);
        assertThat(e.depth()).isEqualTo(1);
        e.removeSession(4, T0 + 5, HRT, notify);
        assertThat(e.waiterCount()).isZero();
        assertThat(e.depth()).isEqualTo(1); // 全部相关会话死亡，元素仍在
    }

    @Test
    void sweepRemovesTimedOutNotifiedHeadAndPromotesNext() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        QueueOpCommand t1 = cmd(2, 1, LockType.QUEUE, QueueOpType.TAKE, true, 0, null, 0, 0);
        QueueOpCommand t2 = cmd(3, 2, LockType.QUEUE, QueueOpType.TAKE, true, 0, null, 0, 0);
        run(e, t1, T0, notify);
        run(e, t2, T0, notify);
        // 空队两等待者；put 落地唤队首 t1（已通知）。
        run(e, cmd(1, 3, LockType.QUEUE, QueueOpType.PUT, false, 0, "x", 0, 0), T0 + 1, notify);
        assertThat(notify).hasSize(1);
        // t1 未兑现重发，超时清扫移除之并推进 t2（此时 x 仍在队、谓词满足）。
        List<Waiter> promoted = new ArrayList<>();
        assertThat(e.sweepNotifiedHead(T0 + 1 + HRT, HRT, promoted)).isTrue();
        assertThat(promoted).hasSize(1);
        assertThat(promoted.get(0).sessionId()).isEqualTo(3);
        assertThat(e.depth()).isEqualTo(1); // x 未被误吞
    }

    @Test
    void forceExpireIsUnreachableForLeaselessFamily() {
        QueueEntry e = queue(4);
        assertThatThrownBy(() -> e.forceExpire(T0, HRT, new ArrayList<>()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void observationReadingsAndRestoredFactory() {
        QueueEntry e = QueueEntry.created(K, LockType.DELAY_QUEUE, 3);
        List<Waiter> notify = new ArrayList<>();
        run(e, cmd(1, 1, LockType.DELAY_QUEUE, QueueOpType.PUT, false, 0, "ab", 1_000, 0),
                T0, notify);
        run(e, cmd(1, 2, LockType.DELAY_QUEUE, QueueOpType.PUT, false, 0, "", 2_000, 0),
                T0, notify);
        long takeSeq = nextSeq++;
        run(e, cmdSeq(1, 3, LockType.DELAY_QUEUE, QueueOpType.TAKE, false, 0, null, takeSeq),
                T0 + 1_500, notify); // 取走 "ab"（交付槽持有 2 字节）
        assertThat(e.kind()).isEqualTo(LockType.DELAY_QUEUE);
        assertThat(e.family()).isEqualTo(KeyFamily.QUEUE);
        assertThat(e.isEmpty()).isFalse();
        assertThat(e.leaseToken()).isZero();
        assertThat(e.capacity()).isEqualTo(3);
        assertThat(e.depth()).isEqualTo(1);
        assertThat(e.headExpiryMs()).isEqualTo(T0 + 2_000);
        assertThat(e.totalPayloadBytes()).isZero(); // 空串元素 0 字节
        assertThat(e.headPayload()).isEmpty();
        assertThat(e.slotsState()).hasSize(1);
        assertThat(e.slotsState().get(0).element()).isEqualTo("ab".getBytes(StandardCharsets.UTF_8));

        // 重建工厂逐字段回灌：元素（载荷/到期）与交付槽在新条目上保真。
        // （记录含 byte[] 成员，逐字段比对数组内容而非记录引用相等。）
        QueueEntry r = QueueEntry.restored(K, LockType.DELAY_QUEUE, 3,
                e.elementsState(), e.slotsState());
        assertThat(r.elementsState()).hasSize(e.elementsState().size());
        for (int i = 0; i < e.elementsState().size(); i++) {
            assertThat(r.elementsState().get(i).payload())
                    .isEqualTo(e.elementsState().get(i).payload());
            assertThat(r.elementsState().get(i).expiresAtMs())
                    .isEqualTo(e.elementsState().get(i).expiresAtMs());
        }
        assertThat(r.slotsState()).hasSize(1);
        assertThat(r.slotsState().get(0).sessionId()).isEqualTo(1);
        assertThat(r.slotsState().get(0).element())
                .isEqualTo(e.slotsState().get(0).element());
        assertThat(r.headExpiryMs()).isEqualTo(e.headExpiryMs());
        QueueOpResult replay = r.op(cmdSeq(1, 3, LockType.DELAY_QUEUE, QueueOpType.TAKE, false,
                0, null, takeSeq), T0 + 3_000, new CoreConfig(), HRT, notify);
        assertThat(replay.element()).isEqualTo("ab".getBytes(StandardCharsets.UTF_8));
        assertThat(r.depth()).isEqualTo(1);
    }

    @Test
    void payloadArraysAreDefensivelyCopiedOnBothSides() {
        QueueEntry e = queue(4);
        List<Waiter> notify = new ArrayList<>();
        byte[] mutable = "x".getBytes(StandardCharsets.UTF_8);
        e.op(new QueueOpCommand(1, 1, K, LockType.QUEUE, QueueOpType.PUT, false, 0,
                mutable, 0, 0, nextSeq++), T0, new CoreConfig(), HRT, notify);
        mutable[0] = 'z'; // 调用侧入参数组改动不影响条目存储（命令构造已复制）
        assertThat(e.headPayload()).isEqualTo("x".getBytes(StandardCharsets.UTF_8));

        long takeSeq = nextSeq;
        QueueOpResult got = e.op(new QueueOpCommand(2, 2, K, LockType.QUEUE, QueueOpType.TAKE,
                false, 0, null, 0, 0, takeSeq), T0, new CoreConfig(), HRT, notify);
        assertThat(got.element()).isEqualTo("x".getBytes(StandardCharsets.UTF_8));
        got.element()[0] = 'w'; // 应答数组改动不回染交付槽
        nextSeq++;
        QueueOpResult replay = e.op(new QueueOpCommand(2, 2, K, LockType.QUEUE, QueueOpType.TAKE,
                false, 0, null, 0, 0, takeSeq), T0, new CoreConfig(), HRT, notify);
        assertThat(replay.element()).isEqualTo("x".getBytes(StandardCharsets.UTF_8));
        assertThat(e.depth()).isZero();
    }
}
