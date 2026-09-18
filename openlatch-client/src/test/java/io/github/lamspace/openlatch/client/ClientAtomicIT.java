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
}
