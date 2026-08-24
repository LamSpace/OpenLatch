package io.github.lamspace.openlatch.core;

/**
 * 生产环境时间源，基于 {@link System#currentTimeMillis()}。
 */
public final class SystemClock implements Clock {
    @Override
    public long nowMs() {
        return System.currentTimeMillis();
    }
}
