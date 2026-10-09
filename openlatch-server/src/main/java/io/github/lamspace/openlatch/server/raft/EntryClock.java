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

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.lamspace.openlatch.core.Clock;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;

/**
 * 条目时刻时间源：在状态机应用作用域内返回"条目携带时刻"，作用域之外回落系统时钟，
 * 使 {@link io.github.lamspace.openlatch.core.CoreEngine} 在零改动的情况下满足 Raft
 * 回放确定性——同一日志序列在任何副本、任何物理时刻重放，租约到期/续租结果完全一致。
 *
 * <p><b>注入机制（有界作用域）</b>：条目时刻经 {@link #withApplyNow(long, ApplyOp)}
 * 绑定到应用调用路径的有界作用域，作用域退出即自动还原——MUST NOT 依赖"设值 +
 * try/finally 清值"的显式清理配对（漏清即把陈旧条目时刻静默泄漏给该线程后续读取）。
 *
 * <p><b>传播边界（契约）</b>：
 * <ul>
 *   <li>作用域内同线程、以及作用域内<b>新建的</b>子线程：读到该条目携带时刻
 *       （有界作用域对派生线程的继承语义）；</li>
 *   <li>作用域之外的既有线程（网络线程、定时器线程等）与作用域退出后的同线程：
 *       回落系统时钟（{@link System#currentTimeMillis()}）。</li>
 * </ul>
 *
 * <p><b>成立前提（契约边界）</b>：
 * <ol>
 *   <li>状态机应用为单线程串行（Ratis {@code StateMachineUpdater} 线程模型——仅应用
 *       已提交条目且逐条串行）；</li>
 *   <li>{@link #withApplyNow(long, ApplyOp)} 的绑定与全部引擎调用发生在同一作用域内，
 *       apply 路径 MUST NOT 向作用域外线程逃逸执行引擎调用（否则该调用静默读到系统
 *       时钟，回放结果依赖物理时间，确定性被破坏且无任何报错）；作用域内新建子线程
 *       继承条目时刻，故逃逸检测口径以作用域继承语义为准——继承即视为仍在应用上下文内。</li>
 * </ol>
 * 前提 (1)(2) 由 {@link LockStateMachine} 的应用入口统一维护，业务代码 MUST NOT 在
 * 引擎调用外围自行读写。
 *
 * <p><b>线程模型</b>：实例可安全共享于任意线程；绑定为有界作用域，作用域外的读取相互
 * 独立、互不可见。
 */
public final class EntryClock implements Clock {

    /** 当前应用作用域的条目时刻绑定；未绑定表示不在应用上下文内，读取回落系统时钟。 */
    private static final ScopedValue<Long> APPLY_NOW = ScopedValue.newInstance();

    /**
     * 构造时间源实例（无状态：绑定存于有界作用域，实例本身无可变字段）。
     */
    public EntryClock() {
    }

    /**
     * 当前时刻：应用作用域内返回条目携带时刻，作用域之外返回系统时钟。
     *
     * @return 毫秒时间戳
     */
    @Override
    public long nowMs() {
        return APPLY_NOW.isBound() ? APPLY_NOW.get() : System.currentTimeMillis();
    }

    /**
     * 在"条目时刻"有界作用域内执行应用操作并返回其结果：作用域内（含域内新建的子线程）
     * 读取 {@link #nowMs()} 得到 {@code ms}，作用域退出即还原。
     *
     * <p><b>异常语义</b>：操作抛出的 {@link InvalidProtocolBufferException} 或
     * {@link RuntimeException} 原样传播（调用方按既有
     * {@code catch (InvalidProtocolBufferException | RuntimeException)} 面收口）；
     * 其余受检异常在 {@link ApplyOp} 契约下不可达，若发生则包为
     * {@link IllegalStateException}。
     *
     * @param ms 条目携带的 leader 发起时刻（毫秒时间戳）
     * @param op 应用操作；MUST NOT 向作用域外线程逃逸执行引擎调用
     * @return {@code op} 的应用结果
     * @throws InvalidProtocolBufferException {@code op} 载荷不可解析（由调用方转 INTERNAL_ERROR）
     */
    static ApplyResult withApplyNow(long ms, ApplyOp op) throws InvalidProtocolBufferException {
        try {
            return ScopedValue.where(APPLY_NOW, ms).call(op::run);
        } catch (InvalidProtocolBufferException | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("apply op threw unexpected checked exception", e);
        }
    }

    /**
     * 条目时刻作用域内执行的应用操作。
     *
     * <p>形参即应用条目的语义：返回应用回执。仅声明
     * {@link InvalidProtocolBufferException}（载荷不可解析），与
     * {@link LockStateMachineCore} 各 apply 分支的异常面一致。
     */
    @FunctionalInterface
    interface ApplyOp {

        /**
         * 在条目时刻作用域内执行应用操作。
         *
         * @return 应用回执
         * @throws InvalidProtocolBufferException 载荷不可解析
         */
        ApplyResult run() throws InvalidProtocolBufferException;
    }
}
