package io.github.lamspace.openlatch.core.command;

import io.github.lamspace.openlatch.core.LockType;

/**
 * 获取锁命令。归属由 {@code (sessionId, threadId)} 唯一确定。
 *
 * @param queueIfBusy 是否在锁被占时排队。{@code false} 对应协议 {@code wait_ms == 0}
 *                    的立即式获取（忙则拒绝）；core 不感知等待时限（见设计说明书 D3）。
 */
public record AcquireCommand(
        long sessionId,
        long requestId,
        String key,
        LockType lockType,
        long threadId,
        long requestedLeaseMs,
        boolean queueIfBusy) {
}
