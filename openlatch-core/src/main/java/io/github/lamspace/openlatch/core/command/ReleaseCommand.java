package io.github.lamspace.openlatch.core.command;

/**
 * 释放锁命令。
 */
public record ReleaseCommand(
        long sessionId,
        String key,
        long leaseToken,
        long threadId) {
}
