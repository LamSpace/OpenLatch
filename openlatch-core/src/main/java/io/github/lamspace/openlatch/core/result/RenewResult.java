package io.github.lamspace.openlatch.core.result;

/**
 * 续租结果。
 *
 * @param newExpiresAtMs {@link ReleaseStatus#OK} 时有效：新的租约到期时刻
 */
public record RenewResult(ReleaseStatus status, long newExpiresAtMs) {
}
