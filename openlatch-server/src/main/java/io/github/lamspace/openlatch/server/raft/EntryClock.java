/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.core.Clock;

/**
 * 条目时刻时间源（详设 §4.3.4，design D2）：在状态机应用线程内返回
 * "条目携带时刻"，线程之外回落系统时钟，使 {@link io.github.lamspace.openlatch.core.CoreEngine}
 * 在零改动的情况下满足 Raft 回放确定性——同一日志序列在任何副本、任何
 * 物理时刻重放，租约到期/续租结果完全一致。
 *
 * <p><b>成立前提（契约边界）</b>：
 * <ol>
 *   <li>状态机应用为单线程串行（Ratis {@code StateMachineUpdater} 线程模型，
 *       见 design D10——仅应用已提交条目且逐条串行）；</li>
 *   <li>标记的 set/clear 与全部引擎调用发生在同一线程，apply 路径 MUST NOT
 *       向其它线程逃逸执行引擎调用（否则该调用静默读到系统时钟，回放结果
 *       依赖物理时间，确定性被破坏且无任何报错）。</li>
 * </ol>
 * 前提 (1) 由 {@link LockStateMachine} 的应用入口统一维护标记，业务代码
 * MUST NOT 在引擎调用外围自行读写。
 *
 * <p><b>线程模型</b>：可安全共享于任意线程；每个线程的标记相互独立
 * （thread-local），读侧（{@link #nowMs()}）与写侧（{@link #setApplyNow(long)}）
 * 同线程时生效，异线程互不可见。
 */
public final class EntryClock implements Clock {

    /** 当前线程的条目时刻标记；{@code null} 表示不在应用条目上下文中。 */
    private static final ThreadLocal<Long> APPLY_NOW = new ThreadLocal<>();

    /**
     * 构造时间源实例（无状态：所有可变部分均为线程标记）。
     */
    public EntryClock() {
    }

    /**
     * 标记当前线程正在应用的条目时刻。
     *
     * @param ms 条目携带的 leader 发起时刻（毫秒时间戳）
     */
    public static void setApplyNow(long ms) {
        APPLY_NOW.set(ms);
    }

    /**
     * 清除当前线程的条目时刻标记，恢复系统时钟语义。
     * 必须与 {@link #setApplyNow(long)} 配对（try/finally），否则线程后续
     * 的时钟读取将停留在陈旧条目时刻。
     */
    public static void clearApplyNow() {
        APPLY_NOW.remove();
    }

    /**
     * 当前时刻：应用上下文中返回条目携带时刻，否则返回系统时钟。
     *
     * @return 毫秒时间戳
     */
    @Override
    public long nowMs() {
        Long v = APPLY_NOW.get();
        return v != null ? v : System.currentTimeMillis();
    }
}
