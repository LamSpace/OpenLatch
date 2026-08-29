package io.github.lamspace.openlatch.poc.harness;

import io.github.lamspace.openlatch.core.Clock;

/**
 * 条目时刻时间源（design D3）：状态机 apply 线程内返回"条目携带时刻"，
 * apply 线程之外回落系统时钟。利用两库 applier 的单线程语义实现
 * {@code CoreEngine} 零改动的回放确定化（详设 §4.3）。
 */
public final class EntryClock implements Clock {

    private static final ThreadLocal<Long> APPLY_NOW = new ThreadLocal<>();

    /** 在 apply 线程内标记当前条目的携带时刻。 */
    public static void setApplyNow(long ms) {
        APPLY_NOW.set(ms);
    }

    /** 条目应用结束后清除标记。 */
    public static void clearApplyNow() {
        APPLY_NOW.remove();
    }

    @Override
    public long nowMs() {
        Long v = APPLY_NOW.get();
        return v != null ? v : System.currentTimeMillis();
    }
}
