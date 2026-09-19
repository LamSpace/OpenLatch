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

package io.github.lamspace.openlatch.core.result;

/**
 * 循环屏障到场结果。
 *
 * <p>判别位为 {@code outcome}：{@link Outcome#GRANTED}=所属世代已合拢
 * 了结（含当回合拢的无动作直答与旧世代重发命中了结记录）；
 * {@link Outcome#QUEUED}=挂起等待放行通知，或执行者形态（当回合拢且
 * 携带动作，{@code executor=true}，本等待以动作了结回报终结）；
 * {@link Outcome#BARRIER_BROKEN}=所属世代已破障；其余为拒绝类结果
 * （零扰动）。
 *
 * @param outcome      判定结果
 * @param queuePosition 队列位次（{@code QUEUED} 时 1 起；其余为 0）
 * @param generation   本等待项加入（或所属）的世代号；拒绝类结果为 0
 * @param executor     执行者标记：{@code QUEUED} 为真表示本方被指定执行
 *                     barrierAction，动作完成须以动作了结命令回报；
 *                     {@code GRANTED} 为真仅出现于动作世代尚未了结的形态
 *                     之外——无动作合拢直答与了结重发的该位恒假
 * @param parties      条目定型许可数回显（拒绝类结果可为 0）
 * @param settled      本次调用是否使一个世代到达终态（合拢）——true 时
 *                     Leader 需对该 key 的在队等待者广播放行；动作挂起
 *                     形态与旧世代重发了结均为 false
 */
public record BarrierAwaitResult(Outcome outcome, int queuePosition, long generation,
        boolean executor, long parties, boolean settled) {

    /**
     * 便捷工厂：挂起等待（普通到场者）。
     *
     * @param position   队列位次（1 起）
     * @param generation 所属世代号
     * @param parties    定型许可数
     * @return QUEUED 结果
     */
    public static BarrierAwaitResult queued(int position, long generation, long parties) {
        return new BarrierAwaitResult(Outcome.QUEUED, position, generation, false, parties, false);
    }

    /**
     * 便捷工厂：挂起等待且被指定为动作执行者。
     *
     * @param generation 所属世代号
     * @param parties    定型许可数
     * @return QUEUED + 执行者标记结果
     */
    public static BarrierAwaitResult queuedExecutor(long generation, long parties) {
        return new BarrierAwaitResult(Outcome.QUEUED, 0, generation, true, parties, false);
    }

    /**
     * 便捷工厂：世代合拢了结（放行）。
     *
     * @param generation 了结的世代号
     * @param parties    定型许可数
     * @param settled    本次调用是否为世代终态的促成者（true 仅当回合拢；
     *                   旧世代重发命中了结记录为 false）
     * @return GRANTED 结果
     */
    public static BarrierAwaitResult tripped(long generation, long parties, boolean settled) {
        return new BarrierAwaitResult(Outcome.GRANTED, 0, generation, false, parties, settled);
    }

    /**
     * 便捷工厂：世代破障了结。
     *
     * @param generation 了结的世代号
     * @param parties    定型许可数
     * @return BARRIER_BROKEN 结果
     */
    public static BarrierAwaitResult broken(long generation, long parties) {
        return new BarrierAwaitResult(Outcome.BARRIER_BROKEN, 0, generation, false, parties, false);
    }

    /**
     * 便捷工厂：拒绝类结果（条目状态零扰动）。
     *
     * @param outcome 拒绝原因
     * @param parties 定型许可数（可得时回显）
     * @return 拒绝结果
     */
    public static BarrierAwaitResult rejected(Outcome outcome, long parties) {
        return new BarrierAwaitResult(outcome, 0, 0, false, parties, false);
    }
}
