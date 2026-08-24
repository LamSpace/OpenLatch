package io.github.lamspace.openlatch.core.lock;

/**
 * 锁归属，由 {@code (sessionId, threadId)} 唯一确定（概要设计 §6.4）。
 */
public record Owner(long sessionId, long threadId) {
}
