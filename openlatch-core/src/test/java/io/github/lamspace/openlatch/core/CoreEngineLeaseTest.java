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
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** §10.1 租约用例组（手工时钟，无 sleep）。 */
class CoreEngineLeaseTest {

    /** 手工时钟：用例以相对推进驱动租约到期，无 sleep。 */
    private MutableClock clock;
    /** 记录型监听器：捕获队首通知事件供断言。 */
    private RecordingListener listener;
    /** 被测引擎，每用例以默认 {@link CoreConfig} 重建。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    /**
     * AcquireCommand 工厂：隐藏固定前提 {@code queueIfBusy=true}（排队式请求）。
     * 租约由调用方传入——多数用例传 30_000，钳制用例传 0/上限外/下限外边界值。
     */
    private AcquireCommand acquire(long s, long r, String key, LockType type, long tid, long leaseMs) {
        return new AcquireCommand(s, r, key, type, tid, leaseMs, true);
    }

    @Test
    void leaseExpiresForcesReleaseAndNotifiesHead() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();

        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, 30_000));
        assertThat(ga.grantedLeaseMs()).isEqualTo(30_000);
        engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2, 30_000)); // QUEUED

        clock.advance(30_001);
        int released = engine.expireDue();
        assertThat(released).isEqualTo(1);

        // 到期强制释放 → 通知队首 b。
        assertThat(listener.count()).isEqualTo(1);
        assertThat(listener.last().sessionId()).isEqualTo(b);

        // b 重发授予。
        assertThat(engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2, 30_000)).outcome())
                .isEqualTo(Outcome.GRANTED);
    }

    @Test
    void renewExtendsLease() {
        long a = engine.sessionOpened();
        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, 30_000));

        clock.advance(10_000);
        RenewResult rr = engine.renew(new RenewCommand(a, "k", ga.leaseToken(), 30_000));
        assertThat(rr.status()).isEqualTo(ReleaseStatus.OK);

        // 原到期点（+30s）已过，但续租后仍持有。
        clock.advance(20_000);
        assertThat(engine.expireDue()).isZero();

        // 再过 10s 到达续租后的到期点（+40s）。
        clock.advance(10_000);
        assertThat(engine.expireDue()).isEqualTo(1);
    }

    @Test
    void expiredTokenCannotReleaseOrRenew() {
        long a = engine.sessionOpened();
        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, 30_000));
        long token = ga.leaseToken();

        clock.advance(30_001);
        assertThat(engine.expireDue()).isEqualTo(1);

        // 过期后旧凭证释放/续租均被拒（锁已被强制释放，条目已移除）。
        ReleaseResult rel = engine.release(new ReleaseCommand(a, "k", token, 1));
        assertThat(rel.status()).isEqualTo(ReleaseStatus.NOT_HELD);
        RenewResult rn = engine.renew(new RenewCommand(a, "k", token, 30_000));
        assertThat(rn.status()).isEqualTo(ReleaseStatus.NOT_HELD);
    }

    @Test
    void wrongTokenCannotReleaseOthersLock() {
        long a = engine.sessionOpened();
        long b = engine.sessionOpened();
        AcquireResult ga = engine.acquire(acquire(a, 1, "k", LockType.REENTRANT, 1, 30_000));

        // b 用错误凭证尝试释放 a 的锁 → INVALID_TOKEN，锁不受影响。
        ReleaseResult rel = engine.release(new ReleaseCommand(b, "k", ga.leaseToken() + 999, 2));
        assertThat(rel.status()).isEqualTo(ReleaseStatus.INVALID_TOKEN);

        // a 仍持有。
        AcquireResult q = engine.acquire(acquire(b, 2, "k", LockType.REENTRANT, 2, 30_000));
        assertThat(q.outcome()).isEqualTo(Outcome.QUEUED);
    }

    @Test
    void leaseMsClampedToBounds() {
        long a = engine.sessionOpened();
        // 请求 0 → 默认 30s。
        AcquireResult d = engine.acquire(acquire(a, 1, "k1", LockType.REENTRANT, 1, 0));
        assertThat(d.grantedLeaseMs()).isEqualTo(30_000);

        // 请求超上限 → 钳制到 1h。
        AcquireResult big = engine.acquire(acquire(a, 2, "k2", LockType.REENTRANT, 1, 9_999_999));
        assertThat(big.grantedLeaseMs()).isEqualTo(3_600_000);

        // 请求低于下限 → 钳制到 1s。
        AcquireResult small = engine.acquire(acquire(a, 3, "k3", LockType.REENTRANT, 1, 1));
        assertThat(small.grantedLeaseMs()).isEqualTo(1_000);
    }
}
