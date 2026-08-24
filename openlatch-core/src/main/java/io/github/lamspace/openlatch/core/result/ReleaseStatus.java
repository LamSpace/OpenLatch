package io.github.lamspace.openlatch.core.result;

/**
 * 释放/续租的状态。{@code OK} 表示操作成功（释放成功或续租成功）。
 */
public enum ReleaseStatus {
    OK,
    INVALID_TOKEN,
    NOT_HELD,
    REJECT_SESSION
}
