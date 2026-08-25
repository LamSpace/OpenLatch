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

/** §10.1 会话、幂等、限额用例组。 */
class CoreEngineSessionIdempotencyLimitTest {

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
    void unknownSessionRejected() {
        AcquireResult r = engine.acquire(acquire(999_999L, 1, "k", LockType.REENTRANT, 1));
        assertThat(r.outcome()).isEqualTo(Outcome.REJECT_SESSION);
    }

    @Test
    void keyEmptyAndTooLongRejected() {
        long a = engine.sessionOpened();
        assertThat(engine.acquire(acquire(a, 1, "", LockType.REENTRANT, 1)).outcome())
                .isEqualTo(Outcome.REJECT_KEY_EMPTY);

        String longKey = "x".repeat(CoreConfig.MAX_KEY_LENGTH + 1);
        assertThat(engine.acquire(acquire(a, 2, longKey, LockType.REENTRANT, 1)).outcome())
                .isEqualTo(Outcome.REJECT_KEY_TOO_LONG);
    }

    @Test
    void queueDepthLimitRejected() {
        CoreConfig cfg = new CoreConfig(30_000, 1_000, 3_600_000, 5_000, 512, 2);
        CoreEngine limited = new CoreEngine(cfg, clock, new RecordingListener());
        long holder = limited.sessionOpened();
        limited.acquire(acquire(holder, 1, "k", LockType.REENTRANT, 1));

        long q1 = limited.sessionOpened();
        long q2 = limited.sessionOpened();
        long q3 = limited.sessionOpened();
        assertThat(limited.acquire(acquire(q1, 2, "k", LockType.REENTRANT, 1)).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(limited.acquire(acquire(q2, 3, "k", LockType.REENTRANT, 2)).outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(limited.acquire(acquire(q3, 4, "k", LockType.REENTRANT, 3)).outcome())
                .isEqualTo(Outcome.REJECT_QUEUE_FULL);
    }

    @Test
    void duplicateAcquireDoesNotEnqueueTwice() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1));

        engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2)); // QUEUED
        // 同 (session, requestId) 重复 → 返回位次，不二次入队。
        AcquireResult dup = engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2));
        assertThat(dup.outcome()).isEqualTo(Outcome.QUEUED);
        assertThat(dup.queuePosition()).isEqualTo(1);

        // 队列仍只有 1 个等待者（a 释放后仅通知 b）。
        ReleaseResult rel = engine.release(new ReleaseCommand(a, "k", ga.leaseToken(), 1));
        assertThat(rel.fullyReleased()).isTrue();
        assertThat(listener.count()).isEqualTo(1);
    }

    @Test
    void sessionClosedReleasesHoldsAndRemovesWaiters() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        long c = engine.sessionOpened();

        // a 持有 k1（写）、k2（读），并在 k3 上排队（k3 由 b 持有）。
        AcquireResult gk1 = engine.acquire(acquire(a, 1, "k1", LockType.REENTRANT, 1));
        AcquireResult gk2 = engine.acquire(acquire(a, 2, "k2", LockType.READ, 1));
        engine.acquire(acquire(b, 3, "k3", LockType.REENTRANT, 2));
        engine.acquire(acquire(a, 4, "k3", LockType.REENTRANT, 1)); // a 在 k3 排队

        // c 在 k1 排队。
        engine.acquire(acquire(c, 5, "k1", LockType.REENTRANT, 3));

        assertThat(gk1.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(gk2.outcome()).isEqualTo(Outcome.GRANTED);

        listener.clear();
        engine.sessionClosed(a);

        // k1：a 释放 → 通知 c。
        assertThat(listener.events()).anyMatch(e -> e.key().equals("k1") && e.sessionId() == c);

        // k1 被 c 重发授予。
        assertThat(engine.acquire(acquire(c, 5, "k1", LockType.REENTRANT, 3)).outcome()).isEqualTo(Outcome.GRANTED);

        // a 关闭后其请求被拒。
        assertThat(engine.acquire(acquire(a, 9, "k9", LockType.REENTRANT, 1)).outcome())
                .isEqualTo(Outcome.REJECT_SESSION);
    }

    @Test
    void sessionClosedIdempotent() {
        long a = engine.sessionOpened();
        engine.sessionClosed(a);
        engine.sessionClosed(a); // 第二次无副作用
    }
}
