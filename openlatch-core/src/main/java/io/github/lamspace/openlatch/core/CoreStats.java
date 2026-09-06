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

/**
 * 引擎统计观察面的一次性快照（Phase 3 T2，spec"只读统计观察面"）。
 *
 * <p>由 {@code CoreEngine.stats()} 弱一致遍历产出——各字段读自条目锁内、
 * 但彼此间不构成同一瞬间的原子视图；消费者（server 层 gauge 绑定）按
 * 采样语义使用。LATCH 家族无持有者概念，不出现在 held 计数中，其 awaiter
 * 计入 {@code totalWaiters}。
 *
 * @param heldLocks      锁家族中当前持有中（租约非零）的条目数
 * @param heldSemaphores Semaphore 家族中持有许可（租约非零）的条目数
 * @param totalWaiters   全部等待队列条目总数（含 latch awaiter）
 * @param maxQueueDepth  单 key 等待队列深度的最大值（采样时刻）
 * @param sessionCount   登记在册的会话数
 */
public record CoreStats(
        int heldLocks,
        int heldSemaphores,
        int totalWaiters,
        int maxQueueDepth,
        int sessionCount) {
}
