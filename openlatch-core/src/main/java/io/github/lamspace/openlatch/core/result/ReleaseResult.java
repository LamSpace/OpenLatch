package io.github.lamspace.openlatch.core.result;

/**
 * 释放锁结果。
 *
 * @param fullyReleased 持有计数归零（锁完全释放）时为 true
 */
public record ReleaseResult(ReleaseStatus status, boolean fullyReleased) {
}
