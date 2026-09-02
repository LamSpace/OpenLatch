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
import io.github.lamspace.openlatch.core.snapshot.CoreStateRestore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * §7.1 快照重建入口用例组（core-lock-engine spec"快照状态重建入口"）：
 * 手工时钟驱动，验证 {@code restoreFrom} 注入状态后的操作正确性、发号跳界、
 * 到期堆回填与零状态守卫。
 */
class CoreEngineRestoreTest {

    /** 手工时钟：继承到期时刻以 base±偏移构造，无 sleep。 */
    private MutableClock clock;
    /** 记录型监听器：捕获队首通知事件供断言。 */
    private RecordingListener listener;
    /** 被测引擎，每用例以默认 {@link CoreConfig} 重建（restoreFrom 要求全新引擎）。 */
    private CoreEngine engine;

    @BeforeEach
    void setUp() {
        clock = new MutableClock();
        listener = new RecordingListener();
        engine = new CoreEngine(new CoreConfig(), clock, listener);
    }

    /** 引擎侧内部会话 id 分配（模拟集群路径 logical→internal 映射的产物）。 */
    private long session() {
        return engine.sessionOpened();
    }

    /**
     * 重建输入条目工厂：以当前时刻 + 指定偏移构造到期时刻。
     */
    private CoreStateRestore.Entry entry(String key, LockType type, long token, long leaseMs,
            long expiresInMs, CoreStateRestore.Holder... holders) {
        return new CoreStateRestore.Entry(key, type, token, leaseMs,
                clock.nowMs() + expiresInMs, List.of(holders));
    }

    @Test
    void inheritedWriteHoldReleasesLayerByLayerAndRejectsForeignToken() {
        long holder = session();
        long other = session();
        // 重入 2 层的写持有 + 一个只登记无持有的会话。
        engine.restoreFrom(new CoreStateRestore(
                List.of(entry("k1", LockType.REENTRANT, 7, 30_000, 10_000,
                        new CoreStateRestore.Holder(holder, 11, 2))),
                List.of(holder, other), 8));

        ReleaseResult first = engine.release(new ReleaseCommand(holder, "k1", 7, 11));
        assertThat(first.status()).isEqualTo(ReleaseStatus.OK);
        assertThat(first.fullyReleased()).isFalse();

        ReleaseResult wrongToken = engine.release(new ReleaseCommand(holder, "k1", 6, 11));
        assertThat(wrongToken.status()).isEqualTo(ReleaseStatus.INVALID_TOKEN);

        ReleaseResult foreignSession = engine.release(new ReleaseCommand(other, "k1", 7, 11));
        assertThat(foreignSession.status()).isEqualTo(ReleaseStatus.NOT_HELD);

        ReleaseResult last = engine.release(new ReleaseCommand(holder, "k1", 7, 11));
        assertThat(last.status()).isEqualTo(ReleaseStatus.OK);
        assertThat(last.fullyReleased()).isTrue();
    }

    @Test
    void inheritedReadHoldsReleasePerHolderCounted() {
        long r1 = session();
        long r2 = session();
        engine.restoreFrom(new CoreStateRestore(
                List.of(entry("rk", LockType.READ, 5, 30_000, 10_000,
                        new CoreStateRestore.Holder(r1, 21, 1),
                        new CoreStateRestore.Holder(r2, 22, 2))),
                List.of(r1, r2), 6));

        // r1 释放：r2 仍持有（计数 2 → 1 → 0 前锁不空）。
        ReleaseResult r1Release = engine.release(new ReleaseCommand(r1, "rk", 5, 21));
        assertThat(r1Release.status()).isEqualTo(ReleaseStatus.OK);
        assertThat(r1Release.fullyReleased()).isFalse();
        ReleaseResult r2First = engine.release(new ReleaseCommand(r2, "rk", 5, 22));
        assertThat(r2First.fullyReleased()).isFalse();
        assertThat(engine.release(new ReleaseCommand(r2, "rk", 5, 22)).fullyReleased()).isTrue();
    }

    @Test
    void sessionClosedReleasesAllInheritedHolds() {
        long s = session();
        long t = session();
        engine.restoreFrom(new CoreStateRestore(
                List.of(entry("a", LockType.WRITE, 9, 30_000, 10_000,
                                new CoreStateRestore.Holder(s, 31, 1)),
                        entry("b", LockType.READ, 10, 30_000, 10_000,
                                new CoreStateRestore.Holder(s, 32, 3),
                                new CoreStateRestore.Holder(t, 33, 1))),
                List.of(s, t), 11));

        engine.sessionClosed(s);

        // s 的持有全部消失；b 仍有 t 的 1 层（租约存续，凭证不变）。
        assertThat(engine.release(new ReleaseCommand(s, "a", 9, 31)).status())
                .isEqualTo(ReleaseStatus.REJECT_SESSION);
        ReleaseResult tRelease = engine.release(new ReleaseCommand(t, "b", 10, 33));
        assertThat(tRelease.status()).isEqualTo(ReleaseStatus.OK);
        assertThat(tRelease.fullyReleased()).isTrue();
    }

    @Test
    void renewOnInheritedLeaseRefreshesExpiry() {
        long s = session();
        engine.restoreFrom(new CoreStateRestore(
                List.of(entry("k", LockType.REENTRANT, 42, 30_000, 5_000,
                        new CoreStateRestore.Holder(s, 41, 1))),
                List.of(s), 43));

        RenewResult ok = engine.renew(new RenewCommand(s, "k", 42, 20_000));
        assertThat(ok.status()).isEqualTo(ReleaseStatus.OK);
        assertThat(ok.newExpiresAtMs()).isEqualTo(clock.nowMs() + 20_000);

        // 刷新后旧到期时刻的堆记录经陈旧校验跳过：未到 20s 不释放。
        clock.advance(6_000);
        assertThat(engine.expireDue()).isZero();
        clock.advance(15_000);
        assertThat(engine.expireDue()).isEqualTo(1);
    }

    @Test
    void expireDueSweepsInheritedLeaseWithoutNativeGrant() {
        long s = session();
        engine.restoreFrom(new CoreStateRestore(
                List.of(entry("k", LockType.REENTRANT, 3, 30_000, 1_000,
                        new CoreStateRestore.Holder(s, 7, 1))),
                List.of(s), 4));

        clock.advance(1_000);
        assertThat(engine.expireDue()).isEqualTo(1);

        // 到期后同 key 可全新授予，凭证大于继承凭证 3。
        AcquireResult r = engine.acquire(new AcquireCommand(s, 1, "k",
                LockType.REENTRANT, 7, 30_000, true));
        assertThat(r.outcome()).isEqualTo(Outcome.GRANTED);
        assertThat(r.leaseToken()).isGreaterThan(3);
    }

    @Test
    void grantedTokensNeverCollideWithRestoredMax() {
        long s = session();
        engine.restoreFrom(new CoreStateRestore(
                List.of(entry("k", LockType.REENTRANT, 1_000_000, 30_000, 10_000,
                        new CoreStateRestore.Holder(s, 1, 1))),
                List.of(s), 1_000_001));

        long firstToken = engine.acquire(new AcquireCommand(s, 1, "n1",
                LockType.REENTRANT, 1, 30_000, true)).leaseToken();
        long secondToken = engine.acquire(new AcquireCommand(s, 2, "n2",
                LockType.REENTRANT, 1, 30_000, true)).leaseToken();
        assertThat(firstToken).isEqualTo(1_000_001);
        assertThat(secondToken).isGreaterThan(firstToken);
    }

    @Test
    void restoreFromRequiresFreshUnusedEngineAndSingleApplication() {
        long s = session();
        CoreStateRestore restore = new CoreStateRestore(
                List.of(entry("k", LockType.REENTRANT, 5, 30_000, 10_000,
                        new CoreStateRestore.Holder(s, 1, 1))),
                List.of(s), 6);

        // 已消耗发号器的引擎上重建被机械拒绝。
        engine.acquire(new AcquireCommand(s, 1, "dirty", LockType.REENTRANT,
                1, 30_000, false));
        assertThatThrownBy(() -> engine.restoreFrom(restore))
                .isInstanceOf(IllegalStateException.class);

        // 全新引擎上一次成功、第二次拒绝。
        CoreEngine fresh = new CoreEngine(new CoreConfig(), clock, new RecordingListener());
        long sid = fresh.sessionOpened();
        fresh.restoreFrom(new CoreStateRestore(
                List.of(new CoreStateRestore.Entry("k", LockType.REENTRANT, 5, 30_000,
                        clock.nowMs() + 10_000, List.of(new CoreStateRestore.Holder(sid, 1, 1)))),
                List.of(sid), 6));
        assertThatThrownBy(() -> fresh.restoreFrom(CoreStateRestore.empty()))
                .isInstanceOf(IllegalStateException.class);
    }
}
