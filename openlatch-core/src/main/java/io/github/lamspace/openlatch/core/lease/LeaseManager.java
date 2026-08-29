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

package io.github.lamspace.openlatch.core.lease;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 到期最小堆。只入不删：释放/续租不删堆记录，靠到期时的陈旧校验跳过（设计说明书 §4.6）。
 * 内部自同步；{@code expireDue} 先取出到期项，再逐条取条目锁。
 *
 * <p><b>锁序</b>：{@link #offer} 会被调用方在持有条目锁时调用（授予与续租
 * 路径），故锁序恒为「条目锁 → 堆锁」单向嵌套；堆锁内绝不反向取条目锁，
 * 两向不成环（设计说明书 §4.9.3）。
 */
public final class LeaseManager {

    /**
     * 堆记录：按到期时刻升序，到期时刻相同时以 key、token 稳定排序。
     *
     * @param expiresAtMs 租约到期时刻（毫秒）
     * @param key         锁键
     * @param leaseToken  登记时的租约凭证
     */
    public record HeapEntry(long expiresAtMs, String key, long leaseToken)
            implements Comparable<HeapEntry> {
        @Override
        public int compareTo(HeapEntry o) {
            int c = Long.compare(expiresAtMs, o.expiresAtMs);
            if (c != 0) {
                return c;
            }
            c = key.compareTo(o.key);
            if (c != 0) {
                return c;
            }
            return Long.compare(leaseToken, o.leaseToken);
        }
    }

    /** 构造空的到期堆。 */
    public LeaseManager() {
    }

    /** 到期最小堆，按到期时刻升序。 */
    private final PriorityQueue<HeapEntry> heap = new PriorityQueue<>();

    /**
     * 登记一条租约记录。
     *
     * @param key         锁键
     * @param leaseToken  租约凭证
     * @param expiresAtMs 到期时刻（毫秒）
     */
    public void offer(String key, long leaseToken, long expiresAtMs) {
        synchronized (this) {
            heap.offer(new HeapEntry(expiresAtMs, key, leaseToken));
        }
    }

    /**
     * 取出所有 {@code expiresAtMs <= nowMs} 的记录（按到期升序）。
     *
     * @param nowMs 当前时刻（毫秒）
     * @return 已到期的堆记录列表
     */
    public List<HeapEntry> drainExpired(long nowMs) {
        List<HeapEntry> out = new ArrayList<>();
        synchronized (this) {
            while (!heap.isEmpty() && heap.peek().expiresAtMs() <= nowMs) {
                out.add(heap.poll());
            }
        }
        return out;
    }
}
