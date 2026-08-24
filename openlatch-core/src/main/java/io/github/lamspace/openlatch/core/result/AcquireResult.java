package io.github.lamspace.openlatch.core.result;

/**
 * 获取锁结果。
 *
 * @param leaseToken     {@link Outcome#GRANTED} 时有效：解锁与续租凭证
 * @param grantedLeaseMs {@link Outcome#GRANTED} 时实际生效租约时长
 * @param queuePosition  {@link Outcome#QUEUED} 时有效：队列位次（1 起）
 */
public record AcquireResult(
        Outcome outcome,
        long leaseToken,
        long grantedLeaseMs,
        int queuePosition) {
}
