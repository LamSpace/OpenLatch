package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.server.OpenLatchServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 原子变量端到端（单机服务端）：六操作与 stamped 形态线路往返、初值主张
 * 冲突、并发 CAS 矩阵（Σ成功 == Δ值、版本 == 写次数）、accumulateAndGet
 * 双端并发求和、布尔首到获胜、整型 wrap、会话死亡值存续（不绑定归属）。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClientAtomicIT {

    /** 被测服务器。 */
    private OpenLatchServer server;
    /** 客户端 A。 */
    private OpenLatchClient clientA;
    /** 客户端 B。 */
    private OpenLatchClient clientB;

    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        clientA = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        clientB = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        clientA.connectAsync().get(5, TimeUnit.SECONDS);
        clientB.connectAsync().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        if (clientA != null) {
            clientA.shutdown();
        }
        clientB.shutdown();
        server.stop();
    }

    @Test
    void sixOpsAndStampedRoundTrip() throws Exception {
        OAtomicLong a = clientA.newAtomicLong("rt");
        assertThat(a.get()).isZero();          // 未建条目读数 0
        assertThat(a.getVersion()).isZero();   // 且 GET 不建条目（版本恒 0）
        a.set(5);
        assertThat(a.get()).isEqualTo(5);
        assertThat(a.incrementAndGet()).isEqualTo(6);
        assertThat(a.addAndGet(-2)).isEqualTo(4);
        assertThat(a.getAndSet(9)).isEqualTo(4);
        assertThat(a.compareAndSet(9, 10)).isTrue();
        assertThat(a.compareAndSet(9, 11)).isFalse();
        OAtomicLong.Stamped s = a.getStamped();
        assertThat(s.value()).isEqualTo(10);
        assertThat(s.version()).isEqualTo(5);  // set+inc+add+gas+casHit+casMiss? misses 不推：5 次落值
        // CAS_STAMPED：版本不符被拒（ABA 防御），相符落值。
        assertThat(a.compareAndSetStamped(10, 3, 99)).isFalse();
        assertThat(a.compareAndSetStamped(10, s.version(), 12)).isTrue();
        assertThat(a.get()).isEqualTo(12);
    }

    @Test
    void initialClaimConflictsSurfaceAtFirstOp() throws Exception {
        OAtomicLong creator = clientA.newAtomicLong("ic", 100);
        assertThat(creator.incrementAndGet()).isEqualTo(101); // 首建以 100 起步
        OAtomicLong conflicting = clientB.newAtomicLong("ic", 200);
        assertThatThrownBy(conflicting::get)
                .isInstanceOf(OpenLatchException.class);       // 断言冲突：INVALID_REQUEST
        // 相符主张不受影响。
        OAtomicLong agreeing = clientB.newAtomicLong("ic", 100);
        assertThat(agreeing.get()).isEqualTo(101);
    }

    @Test
    void concurrentCasMatrixConservesDeltaAndVersion() throws Exception {
        OAtomicLong a = clientA.newAtomicLong("cas-matrix");
        OAtomicLong b = clientB.newAtomicLong("cas-matrix");
        a.set(0);
        int threads = 6;
        int perThread = 40;
        ExecutorService pool = Executors.newFixedThreadPool(threads * 2);
        CountDownLatch ready = new CountDownLatch(threads * 2);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger();
        for (int t = 0; t < threads * 2; t++) {
            OAtomicLong handle = t % 2 == 0 ? a : b;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    for (int i = 0; i < perThread; i++) {
                        while (true) {
                            OAtomicLong.Stamped cur = handle.getStamped();
                            if (handle.compareAndSetStamped(cur.value(), cur.version(),
                                    cur.value() + 1)) {
                                successes.incrementAndGet();
                                break;
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        ready.await(10, TimeUnit.SECONDS);
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(90, TimeUnit.SECONDS)).isTrue();
        int total = threads * 2 * perThread;
        assertThat(successes.get()).isEqualTo(total);
        // 线性化守恒：终值 == 成功数（无丢失更新），版本 == 成功数（无间隙推进）。
        assertThat(a.get()).isEqualTo(total);
        assertThat(a.getVersion()).isEqualTo(total + 1); // set(0) 亦是一次落值
    }

    @Test
    void accumulateAndGetCrossClientConvergence() throws Exception {
        OAtomicLong a = clientA.newAtomicLong("acc");
        OAtomicLong b = clientB.newAtomicLong("acc");
        a.set(0);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch done = new CountDownLatch(2);
        pool.submit(() -> { try { for (int i = 0; i < 50; i++) {
            a.accumulateAndGet(2, Long::sum);
        } } catch (InterruptedException e) { Thread.currentThread().interrupt(); } finally { done.countDown(); } });
        pool.submit(() -> { try { for (int i = 0; i < 50; i++) {
            b.accumulateAndGet(3, Long::sum);
        } } catch (InterruptedException e) { Thread.currentThread().interrupt(); } finally { done.countDown(); } });
        assertThat(done.await(90, TimeUnit.SECONDS)).isTrue();
        pool.shutdown();
        assertThat(a.get()).isEqualTo(50 * 2 + 50 * 3);
    }

    @Test
    void booleanFirstWriterWinsAndVersionCountsFlips() throws Exception {
        OAtomicBoolean flagA = clientA.newAtomicBoolean("flag");
        OAtomicBoolean flagB = clientB.newAtomicBoolean("flag");
        assertThat(flagB.get()).isFalse();
        boolean wonA = flagA.compareAndSet(false, true);
        boolean wonB = flagB.compareAndSet(false, true);
        assertThat(wonA ^ wonB).isTrue();
        OAtomicBoolean.Stamped s = flagA.getStamped();
        assertThat(s.value()).isTrue();
        assertThat(s.version()).isEqualTo(1);
        // ABA-free：翻回 false 再翻 true，stamped 判定可区分历史。
        flagA.set(false);
        flagA.set(true);
        assertThat(flagA.compareAndSetStamped(true, s.version(), false)).isFalse();
        assertThat(flagA.getVersion()).isEqualTo(3);
    }

    @Test
    void integerFormWrapsAtInt32Boundary() throws Exception {
        OAtomicInteger counter = clientA.newAtomicInteger("int-wrap");
        counter.set(Integer.MAX_VALUE);
        assertThat(counter.incrementAndGet()).isEqualTo(Integer.MIN_VALUE);
        assertThat(counter.addAndGet(-1)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void sessionDeathLeavesValueUntouched() throws Exception {
        OAtomicLong a = clientA.newAtomicLong("death");
        a.addAndGet(42);
        clientA.shutdown(); // 会话随关停终结（服务端 SESSION_CLOSE）
        clientA = null;
        OAtomicLong b = clientB.newAtomicLong("death");
        assertThat(b.get()).isEqualTo(42);           // 值不随创建者死亡回滚
        assertThat(b.getVersion()).isEqualTo(1);
        assertThat(b.incrementAndGet()).isEqualTo(43); // 生者照常续写
    }

    // ===================== v6 有值引用 =====================

    @Test
    void referencePresenceFormsRoundTripAndDistinctness() throws Exception {
        OAtomicReference r = clientA.newAtomicReference("ref-rt");
        assertThat(r.get()).isNull();                 // 缺 key 读数 null 且不建条目
        assertThat(r.getVersion()).isZero();
        r.set(new byte[] {1, 2, 3});
        assertThat(r.get()).containsExactly(1, 2, 3);
        r.setString("héllo");                         // String 形态按 UTF-8 折叠
        assertThat(r.getAsString()).isEqualTo("héllo");
        assertThat(r.get()).isEqualTo("héllo".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // null 与空字节串两态可区分。
        r.set((byte[]) null);
        assertThat(r.get()).isNull();
        assertThat(r.compareAndSetString("x", "y")).isFalse();  // 当前为 null 态
        assertThat(r.compareAndSet((byte[]) null, new byte[0])).isTrue();
        assertThat(r.get()).isEmpty();
        assertThat(r.compareAndSet((byte[]) null, new byte[] {9})).isFalse(); // 空串 ≠ null
        assertThat(r.getAndSetAsString("done")).isEqualTo("");
        assertThat(r.getAsString()).isEqualTo("done");
        // stamped 落 null 收尾。
        OAtomicReference.Stamped s = r.getStamped();
        assertThat(r.compareAndSetStampedString("done", s.version(), null)).isTrue();
        assertThat(r.get()).isNull();
        assertThat(r.getVersion()).isEqualTo(s.version() + 1);
    }

    @Test
    void referenceOverLimitRejectedWithZeroEffectAndNoRetry() throws Exception {
        OAtomicReference r = clientA.newAtomicReference("ref-big");
        r.set(new byte[] {1});
        long v0 = r.getVersion();
        // 5KB 超默认钳制（4096）：显式拒绝、条目零扰动、无重发不截断。
        assertThatThrownBy(() -> r.set(new byte[5_000]))
                .isInstanceOf(OpenLatchException.class);
        assertThat(r.get()).containsExactly(1);
        assertThat(r.getVersion()).isEqualTo(v0);
    }

    @Test
    void referenceInitialClaimConflictOnForeignBaseline() throws Exception {
        OAtomicReference a = clientA.newAtomicReference("ref-claim",
                "init".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        // 首建旧值基准=主张值，落新值正常。
        assertThat(a.getAndSetAsString("first")).isEqualTo("init");
        // 异主张句柄对既有条目：每次操作初值断言冲突，显式失败。
        OAtomicReference b = clientB.newAtomicReference("ref-claim",
                "other".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThatThrownBy(b::get).isInstanceOf(OpenLatchException.class);
        // 无主张句柄照常。
        OAtomicReference c = clientB.newAtomicReference("ref-claim");
        assertThat(c.getAsString()).isEqualTo("first");
    }

    @Test
    void referenceConcurrentStampedCasConvergesWithoutGaps() throws Exception {
        OAtomicReference a = clientA.newAtomicReference("ref-matrix");
        OAtomicReference b = clientB.newAtomicReference("ref-matrix");
        final int perClient = 60;
        AtomicInteger wins = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(2);
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            Runnable racer0 = () -> raceStampedCas(a, perClient, "c0-", wins, start, done);
            Runnable racer1 = () -> raceStampedCas(b, perClient, "c1-", wins, start, done);
            pool.submit(racer0);
            pool.submit(racer1);
            start.countDown();
            assertThat(done.await(60, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        }
        long finalVersion = a.getVersion();
        // Σ成功 CAS == Δversion（版本稠密推进、无间隙由服务端恰 +1 承诺）。
        assertThat(finalVersion).isEqualTo(wins.get());
        // 终值属候选集（双端前缀可辨）。
        String terminal = a.getAsString();
        assertThat(terminal).matches("c[01]-\\d{1,2}");
    }

    /** 单端 stamped CAS 竞速：读 (value,version) → 期望双符才落，成功计数。 */
    private static void raceStampedCas(OAtomicReference r, int rounds, String prefix,
            AtomicInteger wins, CountDownLatch start, CountDownLatch done) {
        try {
            start.await();
            for (int i = 0; i < rounds; i++) {
                try {
                    OAtomicReference.Stamped cur = r.getStamped();
                    if (r.compareAndSetStampedString(cur.valueAsString(), cur.version(),
                            prefix + i)) {
                        wins.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            done.countDown();
        }
    }

    @Test
    void referenceSessionDeathLeavesPayloadUntouched() throws Exception {
        OAtomicReference a = clientA.newAtomicReference("ref-death");
        a.set("persist".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        clientA.shutdown();
        clientA = null;
        OAtomicReference b = clientB.newAtomicReference("ref-death");
        assertThat(b.getAsString()).isEqualTo("persist"); // 载荷不随创建者死亡回滚
        assertThat(b.getVersion()).isEqualTo(1);
    }
}
