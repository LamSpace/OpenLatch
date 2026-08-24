package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.LockType;

/**
 * 等待者。{@code notifyDeadlineMs > 0} 表示"已通知、待重发"状态（设计说明书 §4.5）。
 * 通知队首时以 {@link #withDeadline(long)} 生成新实例替换队首，保持不可变。
 */
public record Waiter(
        long sessionId,
        long requestId,
        LockType lockType,
        long threadId,
        long enqueuedAtMs,
        long notifyDeadlineMs) {

    public Owner owner() {
        return new Owner(sessionId, threadId);
    }

    public boolean notified() {
        return notifyDeadlineMs > 0;
    }

    public Waiter withDeadline(long deadlineMs) {
        return new Waiter(sessionId, requestId, lockType, threadId, enqueuedAtMs, deadlineMs);
    }
}
