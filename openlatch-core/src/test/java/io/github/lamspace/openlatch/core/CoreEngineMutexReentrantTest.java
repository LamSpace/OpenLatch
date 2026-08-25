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

/** §10.1 互斥、重入用例组。 */
class CoreEngineMutexReentrantTest {

    private MutableClock clock;
    private RecordingListener listener;
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    private AcquireCommand acquire(long s, long r, String key, LockType type, long tid, boolean queue) {
        return new AcquireCommand(s, r, key, type, tid, 30_000, queue);
    }

    @Test
    void uncontendedAcquireGrants() {
        long a = engine.sessionOpened();
        AcquireResult r = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, true));
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r.leaseToken()).isNotZero();
        assertThat(r.grantedLeaseMs()).isEqualTo(30_000);
    }

    @Test
    void contendedAcquireQueuesThenGrantedAfterRelease() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();

        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, true));
        assertThat(ga.outcome()).isEqualTo(Outcome.GRANTED);

        AcquireResult qb = engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2, true));
        assertThat(qb.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(qb.queuePosition()).isEqualTo(1);
        assertThat(listener.count()).isZero();

        ReleaseResult rel = engine.release(new ReleaseCommand(a, "k", ga.leaseToken(), 1));
        assertThat(rel.status()).isEqualTo(ReleaseStatus.OK);
        assertThat(rel.fullyReleased()).isTrue();
        // 释放后仅通知队首 B。
        assertThat(listener.count()).isEqualTo(1);
        assertThat(listener.last().sessionId()).isEqualTo(b);
        assertThat(listener.last().requestId()).isEqualTo(2);

        // B 以原 requestId 重发 → 队首命中授予。
        AcquireResult rb = engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2, true));
        assertThat(rb.outcome()).isEqualTo(Outcome.GRANTED);
    }

    @Test
    void reentrantIncrementsCountAndReleaseByCount() {
        long a = engine.sessionOpened();
        AcquireResult g1 = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, true));
        assertThat(g1.outcome()).isEqualTo(Outcome.GRANTED);
        long token = g1.leaseToken();

        // 同归属再入（不同 requestId）。
        AcquireResult g2 = engine.acquire(acquire(a, 2, "k", LockType.REENTRANT, 1, true));
        assertThat(g2.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(g2.leaseToken()).isEqualTo(token);

        // 释放一次：计数归 1，未完全释放。
        ReleaseResult r1 = engine.release(new ReleaseCommand(a, "k", token, 1));
        assertThat(r1.status()).isEqualTo(ReleaseStatus.OK);
        assertThat(r1.fullyReleased()).isFalse();

        // 再释放一次：完全释放。
        ReleaseResult r2 = engine.release(new ReleaseCommand(a, "k", token, 1));
        assertThat(r2.status()).isEqualTo(ReleaseStatus.OK);
        assertThat(r2.fullyReleased()).isTrue();
    }

    @Test
    void crossSessionSameThreadIdIsNotReentrant() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 5, true));

        AcquireResult q = engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 5, true));
        assertThat(q.outcome()).isEqualTo(Outcome.QUEUED);
    }

    @Test
    void simpleLockSelfAcquireQueues() {
        long a = engine.sessionOpened();
        AcquireResult g = engine.acquire(acquire(a, 1, "k", LockType.SIMPLE, 1, true));
        assertThat(g.outcome()).isEqualTo(Outcome.GRANTED);

        // 不可重入：同归属再次获取排队等待自己，直至租约到期兜底。
        AcquireResult q = engine.acquire(acquire(a, 2, "k", LockType.SIMPLE, 1, true));
        assertThat(q.outcome()).isEqualTo(Outcome.QUEUED);
    }

    @Test
    void immediateTryLockDeniedWhenHeld() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, true));

        AcquireResult denied = engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2, false));
        assertThat(denied.outcome()).isEqualTo(Outcome.DENIED);
        assertThat(listener.count()).isZero(); // 立即式失败不入队、不通知
    }

    @Test
    void immediateTryLockGrantsWhenFree() {
        long a = engine.sessionOpened();
        AcquireResult r = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, false));
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
    }
}
