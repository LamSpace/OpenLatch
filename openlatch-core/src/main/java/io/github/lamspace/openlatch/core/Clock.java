package io.github.lamspace.openlatch.core;

/**
 * 时间源接口。生产用 {@link SystemClock}；测试用手工时钟推进租约，无需 sleep。
 */
public interface Clock {
    long nowMs();
}
