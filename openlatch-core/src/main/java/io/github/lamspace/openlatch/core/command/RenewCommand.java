package io.github.lamspace.openlatch.core.command;

/**
 * 续租命令。
 */
public record RenewCommand(
        long sessionId,
        String key,
        long leaseToken,
        long requestedLeaseMs) {
}
