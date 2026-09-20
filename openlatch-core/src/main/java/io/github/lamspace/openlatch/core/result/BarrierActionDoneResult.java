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
 * 循环屏障动作了结结果。
 *
 * <p>判别位为 {@code outcome}：{@link Outcome#GRANTED}=该世代以动作回报
 * 定型合拢（或已合拢世代的幂等重复回报）；{@link Outcome#BARRIER_BROKEN}
 * =执行者动作期间世代已破（回报幂等收场，本方 await 以破障终结）；
 * {@link Outcome#REJECT_BARRIER_ACTION}=非指定执行者、世代号未知或该世代
 * 未处于动作待决（条目状态零扰动）。
 *
 * @param outcome    判定结果
 * @param generation 本次了结（或幂等命中的）世代号；拒绝类结果为 0
 * @param settled    本次回报是否实际使世代合拢生效（幂等重复回报为
 *                   false）——true 时 Leader 需对该 key 广播放行
 */
public record BarrierActionDoneResult(Outcome outcome, long generation, boolean settled) {

    /**
     * 便捷工厂：合拢生效或幂等重复。
     *
     * @param generation 世代号
     * @return GRANTED 结果
     */
    public static BarrierActionDoneResult ok(long generation) {
        return new BarrierActionDoneResult(Outcome.GRANTED, generation, true);
    }

    /**
     * 便捷工厂：已合拢世代的幂等重复回报（不触发广播）。
     *
     * @param generation 世代号
     * @return GRANTED 结果（settled=false）
     */
    public static BarrierActionDoneResult replayed(long generation) {
        return new BarrierActionDoneResult(Outcome.GRANTED, generation, false);
    }

    /**
     * 便捷工厂：世代已破。
     *
     * @param generation 世代号
     * @return BARRIER_BROKEN 结果
     */
    public static BarrierActionDoneResult broken(long generation) {
        return new BarrierActionDoneResult(Outcome.BARRIER_BROKEN, generation, false);
    }

    /**
     * 便捷工厂：回报不被受理。
     *
     * @return REJECT_BARRIER_ACTION 结果
     */
    public static BarrierActionDoneResult rejected() {
        return new BarrierActionDoneResult(Outcome.REJECT_BARRIER_ACTION, 0, false);
    }
}
