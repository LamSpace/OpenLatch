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

package io.github.lamspace.openlatch.client.internal;

import io.github.lamspace.openlatch.client.LockType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 本地持锁簿记单测（task 5.1）：只记归属不记计数（design.md D4）。
 */
class HeldLockRegistryTest {

    /** 登记与按键查询。 */
    @Test
    void registerAndGet() {
        HeldLockRegistry registry = new HeldLockRegistry();
        HeldLockRegistry.HeldEntry entry = registry.register("k", 1L, LockType.REENTRANT, 55L, 30_000, 9L, 1000L);

        assertThat(registry.get("k", 1L)).isSameAs(entry);
        assertThat(entry.leaseToken()).isEqualTo(55L);
        assertThat(entry.grantedLeaseMs()).isEqualTo(30_000);
        assertThat(entry.sessionId()).isEqualTo(9L);
        assertThat(entry.lastRenewAtMs()).isEqualTo(1000L);
    }

    /** 同 (key, threadId) 重复登记（重入）返回既有条目且不覆盖。 */
    @Test
    void reentrantRegisterKeepsExisting() {
        HeldLockRegistry registry = new HeldLockRegistry();
        HeldLockRegistry.HeldEntry first = registry.register("k", 1L, LockType.REENTRANT, 55L, 30_000, 9L, 1000L);
        HeldLockRegistry.HeldEntry second = registry.register("k", 1L, LockType.REENTRANT, 55L, 30_000, 9L, 2000L);

        assertThat(second).isSameAs(first);
        assertThat(first.lastRenewAtMs()).isEqualTo(1000L);
        assertThat(registry.entries()).hasSize(1);
    }

    /** 不同 threadId 各自独立（读锁多读者场景）。 */
    @Test
    void distinctThreadsHaveDistinctEntries() {
        HeldLockRegistry registry = new HeldLockRegistry();
        registry.register("k", 1L, LockType.READ, 55L, 30_000, 9L, 1000L);
        registry.register("k", 2L, LockType.READ, 55L, 30_000, 9L, 1000L);

        assertThat(registry.entries()).hasSize(2);
    }

    /** 移除后不可查。 */
    @Test
    void removeEntry() {
        HeldLockRegistry registry = new HeldLockRegistry();
        registry.register("k", 1L, LockType.REENTRANT, 55L, 30_000, 9L, 1000L);

        assertThat(registry.remove("k", 1L)).isNotNull();
        assertThat(registry.get("k", 1L)).isNull();
    }

    /** 续租成功后刷新本地时间戳。 */
    @Test
    void markRenewedUpdatesTimestamp() {
        HeldLockRegistry registry = new HeldLockRegistry();
        HeldLockRegistry.HeldEntry entry = registry.register("k", 1L, LockType.REENTRANT, 55L, 30_000, 9L, 1000L);

        entry.markRenewed(2000L);
        assertThat(entry.lastRenewAtMs()).isEqualTo(2000L);
    }
}
