package io.github.lamspace.openlatch.core.lease;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 到期最小堆。只入不删：释放/续租不删堆记录，靠到期时的陈旧校验跳过（设计说明书 §4.6）。
 * 内部自同步，独立于条目锁；{@code expireDue} 先取出到期项，再逐条取条目锁，无交叉持锁。
 */
public final class LeaseManager {

    /** 堆记录：按到期时刻升序，到期时刻相同时以 key、token 稳定排序。 */
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

    private final PriorityQueue<HeapEntry> heap = new PriorityQueue<>();

    /** 登记一条租约记录。 */
    public void offer(String key, long leaseToken, long expiresAtMs) {
        synchronized (this) {
            heap.offer(new HeapEntry(expiresAtMs, key, leaseToken));
        }
    }

    /** 取出所有 {@code expiresAtMs <= nowMs} 的记录（按到期升序）。 */
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
