package io.github.lamspace.openlatch.core;

/**
 * 核心引擎限额与租约配置。各默认值与设计说明书 §5.7 对齐。
 */
public record CoreConfig(
        long defaultLeaseMs,
        long minLeaseMs,
        long maxLeaseMs,
        long headReplyTimeoutMs,
        int maxKeyLength,
        int maxQueueDepthPerKey) {

    public static final long DEFAULT_LEASE_MS = 30_000L;
    public static final long MIN_LEASE_MS = 1_000L;
    public static final long MAX_LEASE_MS = 3_600_000L;
    public static final long HEAD_REPLY_TIMEOUT_MS = 5_000L;
    public static final int MAX_KEY_LENGTH = 512;
    public static final int MAX_QUEUE_DEPTH_PER_KEY = 4096;

    /** 全默认配置。 */
    public CoreConfig() {
        this(DEFAULT_LEASE_MS, MIN_LEASE_MS, MAX_LEASE_MS,
                HEAD_REPLY_TIMEOUT_MS, MAX_KEY_LENGTH, MAX_QUEUE_DEPTH_PER_KEY);
    }
}
