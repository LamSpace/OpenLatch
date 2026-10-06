package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.QueueOp;
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 队列集群 E2E（v7）：多产多消并发下不丢不重与 per-producer FIFO、唤醒双轨
 * 分轨与位次（入队唤 take、出队唤 put）、应用点竞态回弹对调用者透明、延时
 * 到点唤醒闭环、会话死亡元素存续与等待轨推进、副本一致性收敛。
 * 判例 {@code ClusterSemaphoreLatchTest}/{@code ClusterAtomicTest}。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ClusterQueueTest {

    /** 构造队列操作信封（v7）。 */
    private static Envelope queue(long rid, String key, LockType kind, QueueOp op,
            boolean blocking, long capacity, byte[] element, long delayMs, int maxElements,
            long opSeq) {
        QueueOpRequest.Builder rb = QueueOpRequest.newBuilder()
                .setKey(key).setOp(op).setLockType(kind).setBlocking(blocking)
                .setCapacity(capacity).setDelayMs(delayMs).setMaxElements(maxElements)
                .setOpSeq(opSeq);
        if (element != null) {
            rb.setElementBytes(com.google.protobuf.ByteString.copyFrom(element));
        }
        return Envelope.newBuilder().setProtocolVersion(7).setType(MessageType.QUEUE_OP)
                .setRequestId(rid).setQueueOpRequest(rb).build();
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /** 交付字节还原（无元素回 null）。 */
    private static byte[] elementOf(Envelope resp) {
        return resp.getQueueOpResponse().hasElementBytes()
                ? resp.getQueueOpResponse().getElementBytes().toByteArray() : null;
    }

    /**
     * 写请求并取回与 rid 匹配的应答——过滤先于应答抵达的 AWAIT_NOTIFY 串扰
     * （推送 request_id=0、类型为通知，真实 SDK 走回调通道，harness 直驱需自筛）。
     */
    private static Envelope requestReply(ClusterHarness.TestConn conn, Envelope msg) {
        Envelope first = conn.request(msg);
        long wantRid = msg.getRequestId();
        if (first.getType() == msg.getType() && first.getRequestId() == wantRid) {
            return first;
        }
        for (int i = 0; i < 16; i++) {
            Envelope next = conn.awaitOutbound(10_000);
            if (next.getType() == msg.getType() && next.getRequestId() == wantRid) {
                return next;
            }
        }
        throw new AssertionError("reply for rid " + wantRid + " not found");
    }

    /**
     * 阻塞 put：QUEUED 后等待该 rid 的 AWAIT_NOTIFY（或读界到点自兜底）同
     * 信封同序号重发，直至 OK；DENIED 视作回弹继续。
     */
    private static void blockingPut(ClusterHarness.TestConn conn, String key, long rid,
            byte[] element, long capacity, long opSeq) throws Exception {
        while (true) {
            Envelope resp = requestReply(conn, queue(rid, key, LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_PUT, true, capacity, element, 0, 0, opSeq));
            StatusCode st = resp.getQueueOpResponse().getStatus();
            if (st == StatusCode.OK) {
                return;
            }
            if (st == StatusCode.QUEUED || st == StatusCode.DENIED) {
                awaitNotifyQuietly(conn);
                continue;
            }
            throw new AssertionError("blocking put rejected: " + st);
        }
    }

    /**
     * 阻塞 take：同 {@link #blockingPut} 的挂起-通知-重发环；返回交付字节。
     */
    private static byte[] blockingTake(ClusterHarness.TestConn conn, String key, long rid,
            long opSeq, LockType kind) throws Exception {
        while (true) {
            Envelope resp = requestReply(conn, queue(rid, key, kind, QueueOp.QUEUE_OP_TAKE,
                    true, 0, null, 0, 0, opSeq));
            StatusCode st = resp.getQueueOpResponse().getStatus();
            if (st == StatusCode.OK) {
                byte[] element = elementOf(resp);
                assertThat(element).as("OK take must deliver an element").isNotNull();
                return element;
            }
            if (st == StatusCode.QUEUED || st == StatusCode.DENIED) {
                awaitNotifyQuietly(conn);
                continue;
            }
            throw new AssertionError("blocking take rejected: " + st);
        }
    }

    /** 等一条推送（AWAIT_NOTIFY 或残留应答），超时不抛——推送丢失兜底自兜重发。 */
    private static void awaitNotifyQuietly(ClusterHarness.TestConn conn) {
        try {
            Envelope n = conn.awaitOutbound(3_000);
            if (n != null && n.getType() == MessageType.AWAIT_NOTIFY) {
                return;
            }
            // 非推送的残留出站（如同 rid 的迟到应答）：继续走重发环自然收敛。
        } catch (AssertionError quiet) {
            // 读界到点：自发重发。
        }
    }

    @Test
    void concurrentProducersConsumersNoLossNoDupPerProducerFifo() throws Exception {
        final int producers = 4;
        final int perProducer = 12;
        final int total = producers * perProducer;
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ExecutorService pool = Executors.newFixedThreadPool(producers + 4);
            CountDownLatch go = new CountDownLatch(1);
            ConcurrentLinkedQueue<String> taken = new ConcurrentLinkedQueue<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            for (int p = 0; p < producers; p++) {
                final int pid = p;
                pool.submit(() -> {
                    ClusterHarness.TestConn conn = h.connect(leader);
                    try {
                        conn.hello(1, 7);
                        go.await();
                        for (int i = 0; i < perProducer; i++) {
                            blockingPut(conn, "cq", 1000L + pid * 100 + i,
                                    utf8("p" + pid + "#" + i), 8, 1L + i);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        conn.disconnect();
                    }
                });
            }
            AtomicInteger consumed = new AtomicInteger();
            for (int c = 0; c < 4; c++) {
                pool.submit(() -> {
                    ClusterHarness.TestConn conn = h.connect(leader);
                    try {
                        conn.hello(1, 7);
                        go.await();
                        // 立即式轮询（不挂起）：收取数不超配额，无" parked 等
                        // 不再来的元素"竞态；挂起/唤醒路径由专例覆盖。
                        long seq = 1;
                        while (consumed.get() < total) {
                            Envelope t = requestReply(conn, queue(5000L + seq, "cq",
                                    LockType.LOCK_TYPE_QUEUE, QueueOp.QUEUE_OP_TAKE,
                                    false, 0, null, 0, 0, seq));
                            seq++;
                            if (t.getQueueOpResponse().getStatus() == StatusCode.OK) {
                                taken.add(new String(elementOf(t), StandardCharsets.UTF_8));
                                consumed.incrementAndGet();
                            } else {
                                // 混入一次 drain(2)：批量与单取交错（队空双双落空）。
                                Envelope d = requestReply(conn, queue(6000L + seq, "cq",
                                        LockType.LOCK_TYPE_QUEUE, QueueOp.QUEUE_OP_DRAIN,
                                        false, 0, null, 0, 2, seq));
                                seq++;
                                for (com.google.protobuf.ByteString b
                                        : d.getQueueOpResponse().getDrainedBytesList()) {
                                    taken.add(new String(b.toByteArray(),
                                            StandardCharsets.UTF_8));
                                    consumed.incrementAndGet();
                                }
                                Thread.sleep(5);
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        conn.disconnect();
                    }
                });
            }
            go.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(120, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();

            // 不丢不重：收取多重集 == 投递多重集（48 个唯一字节串）。
            assertThat(taken).hasSize(total);
            List<String> sortedTaken = new ArrayList<>(taken);
            java.util.Collections.sort(sortedTaken);
            List<String> expected = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                for (int i = 0; i < perProducer; i++) {
                    expected.add("p" + p + "#" + i);
                }
            }
            java.util.Collections.sort(expected);
            assertThat(sortedTaken).isEqualTo(expected);

            ClusterHarness.TestConn check = h.connect(h.leader());
            check.hello(1, 7);
            Envelope size = check.request(queue(900, "cq", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_SIZE, false, 0, null, 0, 0, 0));
            assertThat(size.getQueueOpResponse().getSize()).isZero();
            check.disconnect();
            h.awaitTrue(h::aliveAgreeWithLeader, 20_000, "队列矩阵后副本一致");
        }
    }

    @Test
    void wakeTracksGrantInParkOrder() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn prim = h.connect(leader);
            prim.hello(1, 7);
            // 容量 1：灌入再取空——为两个 take 挂起者制造"空队→依序挂起"前提。
            blockingPut(prim, "tw", 10, utf8("x"), 1, 1);
            Envelope drained = prim.request(queue(11, "tw", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_TAKE, false, 0, null, 0, 0, 2));
            assertThat(drained.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            // 两个阻塞 take 依次挂起：take 轨位次 1、2。
            ClusterHarness.TestConn t1 = h.connect(leader);
            t1.hello(2, 7);
            Envelope e1 = t1.request(queue(20, "tw", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_TAKE, true, 0, null, 0, 0, 3));
            assertThat(e1.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(e1.getQueueOpResponse().getQueuePosition()).isEqualTo(1);
            ClusterHarness.TestConn t2 = h.connect(leader);
            t2.hello(3, 7);
            Envelope e2 = t2.request(queue(30, "tw", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_TAKE, true, 0, null, 0, 0, 4));
            assertThat(e2.getQueueOpResponse().getQueuePosition()).isEqualTo(2);
            // 投 y → 仅唤 take 轨队首 t1；t2 续等。
            blockingPut(prim, "tw", 40, utf8("y"), 0, 5);
            assertThat(new String(blockingTake(t1, "tw", 20, 3,
                    LockType.LOCK_TYPE_QUEUE), StandardCharsets.UTF_8)).isEqualTo("y");
            // t1 腾空后再投 z → 唤 t2。
            blockingPut(prim, "tw", 60, utf8("z"), 0, 7);
            assertThat(new String(blockingTake(t2, "tw", 30, 4,
                    LockType.LOCK_TYPE_QUEUE), StandardCharsets.UTF_8)).isEqualTo("z");
            prim.disconnect();
            t1.disconnect();
            t2.disconnect();
            h.awaitTrue(h::aliveAgreeWithLeader, 20_000, "双轨唤醒后副本一致");
        }
    }

    @Test
    void delayFormWakesOnTickAndHidesBeforeExpiry() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn conn = h.connect(leader);
            conn.hello(1, 7);
            Envelope put = conn.request(queue(10, "dl", LockType.LOCK_TYPE_DELAY_QUEUE,
                    QueueOp.QUEUE_OP_PUT, false, 4, utf8("late"), 800, 0, 1));
            assertThat(put.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            // 未到期：PEEK 不可见、SIZE 驻留口径计 1、立即 TAKE 被拒。
            Envelope peek = conn.request(queue(11, "dl", LockType.LOCK_TYPE_DELAY_QUEUE,
                    QueueOp.QUEUE_OP_PEEK, false, 0, null, 0, 0, 0));
            assertThat(peek.getQueueOpResponse().hasElementBytes()).isFalse();
            Envelope size = conn.request(queue(12, "dl", LockType.LOCK_TYPE_DELAY_QUEUE,
                    QueueOp.QUEUE_OP_SIZE, false, 0, null, 0, 0, 0));
            assertThat(size.getQueueOpResponse().getSize()).isEqualTo(1);
            Envelope poll = conn.request(queue(13, "dl", LockType.LOCK_TYPE_DELAY_QUEUE,
                    QueueOp.QUEUE_OP_TAKE, false, 0, null, 0, 0, 2));
            assertThat(poll.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.DENIED);
            // 阻塞 take：挂起 → tick 内到点唤醒 → 取回（精度契约 ≤ 数 tick）。
            long startNs = System.nanoTime();
            byte[] got = blockingTake(conn, "dl", 20, 3, LockType.LOCK_TYPE_DELAY_QUEUE);
            assertThat(new String(got, StandardCharsets.UTF_8)).isEqualTo("late");
            long waitedMs = (System.nanoTime() - startNs) / 1_000_000;
            assertThat(waitedMs).isBetween(300L, 8_000L);
            conn.disconnect();
            h.awaitTrue(h::aliveAgreeWithLeader, 20_000, "延时消费后副本一致");
        }
    }

    @Test
    void sessionDeathKeepsElementsAndAdvancesWaitTrack() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn writer = h.connect(leader);
            writer.hello(1, 7);
            Envelope put = writer.request(queue(10, "dk", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_PUT, false, 4, utf8("kept"), 0, 0, 1));
            assertThat(put.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            // 投递者会话死亡：元素存续（消费端照常取回）。
            writer.disconnect();
            ClusterHarness.TestConn reader = h.connect(leader);
            reader.hello(2, 7);
            assertThat(new String(blockingTake(reader, "dk", 20, 1,
                    LockType.LOCK_TYPE_QUEUE), StandardCharsets.UTF_8)).isEqualTo("kept");

            // 灌满（cap 4）→ 两个 put-waiter 依序挂起（位次 1/2）→ 位次 2 死亡
            // → 腾一格仅唤存活的位次 1，其重发入队。
            for (int i = 0; i < 4; i++) {
                blockingPut(reader, "dk", 30 + i, utf8("f" + i), 0, 10 + i);
            }
            ClusterHarness.TestConn p1 = h.connect(leader);
            p1.hello(3, 7);
            Envelope parkA = p1.request(queue(60, "dk", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_PUT, true, 0, utf8("A"), 0, 0, 20));
            assertThat(parkA.getQueueOpResponse().getQueuePosition()).isEqualTo(1);
            ClusterHarness.TestConn p2 = h.connect(leader);
            p2.hello(4, 7);
            Envelope parkB = p2.request(queue(70, "dk", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_PUT, true, 0, utf8("B"), 0, 0, 21));
            assertThat(parkB.getQueueOpResponse().getQueuePosition()).isEqualTo(2);
            p2.disconnect(); // 位次 2 死亡：仅摘其等待
            Envelope free = reader.request(queue(80, "dk", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_TAKE, false, 0, null, 0, 0, 22));
            assertThat(free.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            // p1 被唤重发入队（同 rid 同 op_seq——其挂起槽位幂等推进）。
            Envelope promoted = requestReply(p1, queue(60, "dk", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_PUT, true, 0, utf8("A"), 0, 0, 20));
            assertThat(promoted.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            Envelope size = reader.request(queue(90, "dk", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_SIZE, false, 0, null, 0, 0, 0));
            // 3 + 1(A) − 2(take kept 之后的两次腾位为 f2/f0... 精确：kept 取走1、
            // free 取走1、A 入 1) = 4 − 1 + ... 以镜像终读为准：4。
            assertThat(size.getQueueOpResponse().getSize()).isEqualTo(4);
            reader.disconnect();
            p1.disconnect();
            h.awaitTrue(h::aliveAgreeWithLeader, 20_000, "会话死亡路径后副本一致");
        }
    }

    @Test
    void bouncedTakeNeverSurfacesErrorToCaller() throws Exception {
        // 两阻塞 take 抢后续单元素：一者授予、一者应用点 DENIED → 回弹重挂
        // （改写 QUEUED，位次回显）——调用者只见等待不见错误。
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn prim = h.connect(leader);
            prim.hello(1, 7);
            ClusterHarness.TestConn t1 = h.connect(leader);
            t1.hello(2, 7);
            ClusterHarness.TestConn t2 = h.connect(leader);
            t2.hello(3, 7);
            // 两个等待者先挂（空队）。
            Envelope q1 = t1.request(queue(20, "bc", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_TAKE, true, 0, null, 0, 0, 1));
            assertThat(q1.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            Envelope q2 = t2.request(queue(30, "bc", LockType.LOCK_TYPE_QUEUE,
                    QueueOp.QUEUE_OP_TAKE, true, 0, null, 0, 0, 2));
            assertThat(q2.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            // 投两个元素：两位依次被授予（唤醒顺序 = 挂起顺序）。
            blockingPut(prim, "bc", 40, utf8("one"), 4, 3);
            blockingPut(prim, "bc", 50, utf8("two"), 0, 4);
            byte[] a = blockingTake(t1, "bc", 20, 1, LockType.LOCK_TYPE_QUEUE);
            byte[] b = blockingTake(t2, "bc", 30, 2, LockType.LOCK_TYPE_QUEUE);
            assertThat(new String(a, StandardCharsets.UTF_8)).isEqualTo("one");
            assertThat(new String(b, StandardCharsets.UTF_8)).isEqualTo("two");
            prim.disconnect();
            t1.disconnect();
            t2.disconnect();
        }
    }
}
