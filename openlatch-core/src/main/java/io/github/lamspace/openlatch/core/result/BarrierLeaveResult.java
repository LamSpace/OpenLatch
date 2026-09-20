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
 * 循环屏障离场结果。
 *
 * <p>离场即破障生效与否不改变本结果形态：成功离场、对已终结世代的
 * 幂等无操作与纯破障主张均回 {@link Outcome#GRANTED}（server 层映射
 * {@code OK}），破障事实经该世代其余等待者的重发了结可见；会话/家族/
 * key 类拒绝回对应 {@code REJECT_*}。
 *
 * @param outcome 判定结果
 * @param generationBroke   本次离场是否实际打破了当前世代——true 时 Leader 需对
 *                该 key 的在队等待者广播破障放行；幂等无操作为 false
 */
public record BarrierLeaveResult(Outcome outcome, boolean generationBroke) {

    /**
     * 便捷工厂：离场（含幂等无操作）。
     *
     * @return GRANTED 结果（未破任何世代）
     */
    public static BarrierLeaveResult ok() {
        return new BarrierLeaveResult(Outcome.GRANTED, false);
    }

    /**
     * 便捷工厂：离场并实际破障当前世代。
     *
     * @return GRANTED + broke 结果
     */
    public static BarrierLeaveResult broke() {
        return new BarrierLeaveResult(Outcome.GRANTED, true);
    }

    /**
     * 便捷工厂：拒绝类结果。
     *
     * @param outcome 拒绝原因
     * @return 拒绝结果
     */
    public static BarrierLeaveResult rejected(Outcome outcome) {
        return new BarrierLeaveResult(outcome, false);
    }
}
