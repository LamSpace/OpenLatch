/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.core;

import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** §10.1 读写、FIFO 公平、队首响应超时用例组。 */
class CoreEngineReadWriteFairnessTest {

    private MutableClock clock;
    private RecordingListener listener;
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    private AcquireCommand acquire(long s, long r, String key, LockType type, long tid) {
        return new AcquireCommand(s, r, key, type, tid, 30_000, true);
    }

    @Test
    void multipleReadersConcurrentThenWriterQueues() {
        long r1 = engine.sessionOpened();
        long r2 = engine.sessionOpened();
        long w = engine.sessionOpened();

        assertThat(engine.acquire(acquire(r1, 1, "k", LockType.READ, 1)).outcome()).isEqualTo(Outcome.GRANTED);
        // 无写者且队列空 → 读者可并发加入。
        assertThat(engine.acquire(acquire(r2, 2, "k", LockType.READ, 2)).outcome()).isEqualTo(Outcome.GRANTED);

        // 读者持有时写者排队。
        AcquireResult qw = engine.acquire(acquire(w, 3, "k", LockType.WRITE, 3));
        assertThat(qw.outcome()).isEqualTo(Outcome.QUEUED);
    }

    @Test
    void writerHoldsReaderQueues() {
        long w = engine.sessionOpened();
        long r = engine.sessionOpened();
        engine.acquire(acquire(w, 1, "k", LockType.WRITE, 1));

        AcquireResult qr = engine.acquire(acquire(r, 2, "k", LockType.READ, 2));
        assertThat(qr.outcome()).isEqualTo(Outcome.QUEUED);
    }

    @Test
    void strictFifoReaderArrivesBeforeWriterBlocksWriter() {
        long r = engine.sessionOpened();
        long w = engine.sessionOpened();
        long r2 = engine.sessionOpened();

        AcquireResult gr = engine.acquire(acquire(r, 1, "k", LockType.READ, 1));
        assertThat(gr.outcome()).isEqualTo(Outcome.GRANTED);

        AcquireResult qw = engine.acquire(acquire(w, 2, "k", LockType.WRITE, 2));
        assertThat(qw.outcome()).isEqualTo(Outcome.QUEUED);

        // 严格 FIFO：队列非空（即便队首是写者）→ 新读者也排队，不得越过写者。
        AcquireResult qr2 = engine.acquire(acquire(r2, 3, "k", LockType.READ, 3));
        assertThat(qr2.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(qr2.queuePosition()).isEqualTo(2);

        // 读者 r 释放 → 通知队首写者 w。
        engine.release(new ReleaseCommand(r, "k", gr.leaseToken(), 1));
        assertThat(listener.last().sessionId()).isEqualTo(w);
        assertThat(engine.acquire(acquire(w, 2, "k", LockType.WRITE, 2)).outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void grantOrderEqualsEnqueueOrder() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1));
        engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2)); // QUEUED
        engine.acquire(acquire(c, 3, "k", LockType.REENTRANT, 3)); // QUEUED

        engine.release(new ReleaseCommand(a, "k", ga.leaseToken(), 1));
        // 只通知队首 b。
        assertThat(listener.count()).isEqualTo(1);
        assertThat(listener.last().requestId()).isEqualTo(2);

        // b 重发授予。
        AcquireResult gb = engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2));
        assertThat(gb.outcome()).isEqualTo(Outcome.GRANTED);
        // b 释放 → 通知 c。
        engine.release(new ReleaseCommand(b, "k", gb.leaseToken(), 2));
        assertThat(listener.last().requestId()).isEqualTo(3);
        assertThat(engine.acquire(acquire(c, 3, "k", LockType.REENTRANT, 3)).outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void newArrivalCannotOvertakeDuringNotifyWindow() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1));
        engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2)); // QUEUED

        engine.release(new ReleaseCommand(a, "k", ga.leaseToken(), 1));
        // 此时锁无持有者但队列非空（b 处于已通知待重发窗口）。
        assertThat(listener.count()).isEqualTo(1);

        // 新到者 c 必须排队尾，不得越过已通知的 b（规则 3）。
        AcquireResult qc = engine.acquire(acquire(c, 3, "k", LockType.REENTRANT, 3));
        assertThat(qc.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(qc.queuePosition()).isEqualTo(2);
    }

    @Test
    void headReplyTimeoutRemovesAbandonedHeadAndAdvancesQueue() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1));
        engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2)); // QUEUED
        engine.acquire(acquire(c, 3, "k", LockType.REENTRANT, 3)); // QUEUED

        engine.release(new ReleaseCommand(a, "k", ga.leaseToken(), 1));
        assertThat(listener.last().requestId()).isEqualTo(2); // 通知 b

        // b 一直不重发（模拟放弃），越过响应时限。
        clock.advance(CoreConfig.HEAD_REPLY_TIMEOUT_MS + 1);

        int removed = engine.sweepNotifiedHeads();
        assertThat(removed).isEqualTo(1);
        // 新队首 c 被补通知。
        assertThat(listener.last().requestId()).isEqualTo(3);

        assertThat(engine.acquire(acquire(c, 3, "k", LockType.REENTRANT, 3)).outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void releaseFullyReleasedFlagForPartialReader() {
        long r1 = engine.sessionOpened();
        long r2 = engine.sessionOpened();

        AcquireResult g1 = engine.acquire(acquire(r1, 1, "k", LockType.READ, 1));
        AcquireResult g2 = engine.acquire(acquire(r2, 2, "k", LockType.READ, 2));
        assertThat(g1.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(g2.outcome()).isEqualTo(Outcome.GRANTED);

        // 两个读者共享一个凭证。
        assertThat(g1.leaseToken()).isEqualTo(g2.leaseToken());

        ReleaseResult rel1 = engine.release(new ReleaseCommand(r1, "k", g1.leaseToken(), 1));
        assertThat(rel1.status()).isEqualTo(ReleaseStatus.OK);
        assertThat(rel1.fullyReleased()).isFalse(); // r2 仍持有

        ReleaseResult rel2 = engine.release(new ReleaseCommand(r2, "k", g2.leaseToken(), 2));
        assertThat(rel2.fullyReleased()).isTrue();
    }

    /** WRITE 类型两写者互斥（§10.1 读写组补强）：后到写者排队，立即式则 DENIED。 */
    @Test
    void twoWritersMutuallyExclude() {
        long w1 = engine.sessionOpened();
        long w2 = engine.sessionOpened();

        AcquireResult g = engine.acquire(acquire(w1, 1, "k", LockType.WRITE, 1));
        assertThat(g.outcome()).isEqualTo(Outcome.GRANTED);

        AcquireResult q = engine.acquire(acquire(w2, 2, "k", LockType.WRITE, 2));
        assertThat(q.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(q.queuePosition()).isEqualTo(1);

        AcquireResult denied = engine.acquire(
                new AcquireCommand(w2, 3, "k", LockType.WRITE, 3, 30_000, false));
        assertThat(denied.outcome()).isEqualTo(Outcome.DENIED);
    }

    /** 读侧重入按请求租约值整段刷新，与写侧口径一致（design D2）。 */
    @Test
    void readReentrantRefreshesLeaseWithRequestedValue() {
        long a = engine.sessionOpened();
        AcquireResult g1 = engine.acquire(acquire(a, 1, "k", LockType.READ, 1));
        assertThat(g1.grantedLeaseMs()).isEqualTo(30_000);

        clock.advance(10_000);
        AcquireResult g2 = engine.acquire(
                new AcquireCommand(a, 2, "k", LockType.READ, 1, 10_000, true));
        assertThat(g2.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(g2.leaseToken()).isEqualTo(g1.leaseToken());
        assertThat(g2.grantedLeaseMs()).isEqualTo(10_000);

        clock.advance(10_000);
        assertThat(engine.expireDue()).isEqualTo(1); // t=20s 到期：刷新生效
    }

    /** 新读者加入既有读者群：共享租约按请求值刷新，最后加入者决定全体到期（design D2 推论）。 */
    @Test
    void readerJoinRefreshesSharedLeaseWithRequestedValue() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.acquire(acquire(a, 1, "k", LockType.READ, 1)); // 30s

        clock.advance(10_000);
        AcquireResult join = engine.acquire(
                new AcquireCommand(b, 2, "k", LockType.READ, 2, 10_000, true));
        assertThat(join.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(join.grantedLeaseMs()).isEqualTo(10_000);

        clock.advance(10_000);
        // a 的读者随共享租约在 t=20s 一并到期（请求值口径对加入路径同样生效）。
        assertThat(engine.expireDue()).isEqualTo(1);
        assertThat(engine.release(new ReleaseCommand(a, "k", join.leaseToken(), 1)).status())
                .isEqualTo(ReleaseStatus.NOT_HELD);
    }
}
