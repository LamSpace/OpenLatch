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
 * 延时触发操作结果（TIMER 家族命令通道的统一应答形态）。
 *
 * <p><b>择用规则</b>：{@code outcome} 为终局判别——{@link Outcome#GRANTED}
 * 表完成（装载/撤销/读数/到期即刻了结），{@link Outcome#QUEUED} 表等待登记
 * 已生效（唤醒经通知-重发闭环另行送达），{@link Outcome#DENIED} 表当代已
 * 撤销装载、等待不可再满足（既有码值经 timer 首次回到可达面——"本次等待
 * 不可能成功"与队列"元素不可满足"同构语义）；其余为拒绝。三元组
 * （{@code generation}/{@code armed}/{@code fireAtMs}）恒为裁决时刻的账簿
 * 快照（观察值，供应答回显与 QUERY 读数；MUST NOT 参与后续判定）。
 *
 * <p><b>marked 折算语义</b>：{@code marked} 是裁决时刻的派生读数
 * （{@code armed ∧ now ≥ fireAtMs}）——只在应答面承载"已共见"终态判定，
 * MUST NOT 回写账簿（无"已响"驻留位，到期零条目裁决的结果侧）。拒绝态
 * 与 QUEUED 登记回执的 {@code marked} 恒 false（QUEUED 时的三元组为受理
 * 时刻观察值）。
 *
 * <p><b>代次回显语义</b>：SCHEDULE 回显新代次（换代清钟后）与折算的绝对
 * 到期时刻；DISARM 回显当代代次与 {@code armed=false}（{@code fireAtMs}
 * 保持撤销前值——历史观察值不重写）；AWAIT 终态与 QUERY 回显当值三元组。
 *
 * @param outcome   裁决结果判别
 * @param generation 装载代次回显（拒绝态为 0）
 * @param armed     裁决时刻装载态（拒绝态为 false）
 * @param fireAtMs  绝对到期时刻回显（拒绝态为 0；DISARM 后保持历史值）
 * @param marked    到期共见读数（{@code armed ∧ 判定时刻 ≥ fireAtMs}）
 * @param disarmed  本 DISARM 条目是否发生了"在装 → 代终结"迁移（仅 GRANTED
 *                  可为真——集群应用点据此排空 Leader 簿记唤醒全体在等旁观者；
 *                  幂等回声形态为 false，单机形态无需消费）
 */
public record TimerOpResult(Outcome outcome, long generation, boolean armed,
        long fireAtMs, boolean marked, boolean disarmed) {

    /**
     * 完成裁决（GRANTED）携三元组与到期共见读数。
     *
     * @param generation 代次回显
     * @param armed      装载态读数
     * @param fireAtMs   绝对到期时刻读数
     * @param marked     到期共见读数
     * @return GRANTED 结果
     */
    public static TimerOpResult granted(long generation, boolean armed, long fireAtMs,
            boolean marked) {
        return new TimerOpResult(Outcome.GRANTED, generation, armed, fireAtMs, marked, false);
    }

    /**
     * 完成裁决（GRANTED）携代终结标记（DISARM 恰使在装转代终结的形态）。
     *
     * @param generation 代次回显
     * @param fireAtMs   保持的绝对到期时刻
     * @param disarmed   本次操作是否发生"在装 → 代终结"迁移
     * @return GRANTED 结果
     */
    public static TimerOpResult disarmed(long generation, long fireAtMs, boolean disarmed) {
        return new TimerOpResult(Outcome.GRANTED, generation, false, fireAtMs, false, disarmed);
    }

    /**
     * 等待登记完成（QUEUED）携受理时刻三元组观察值。
     *
     * @param generation 受理时刻代次
     * @param fireAtMs   受理时刻绝对到期时刻
     * @return QUEUED 结果
     */
    public static TimerOpResult queued(long generation, long fireAtMs) {
        return new TimerOpResult(Outcome.QUEUED, generation, true, fireAtMs, false, false);
    }

    /**
     * 撤销终态了结（DENIED）：当代已 DISARMED、等待不可再满足（携当代
     * 三元组读数，{@code marked} 恒 false）。
     *
     * @param generation 当代代次
     * @param fireAtMs   保持的绝对到期时刻
     * @return DENIED 结果
     */
    public static TimerOpResult denied(long generation, long fireAtMs) {
        return new TimerOpResult(Outcome.DENIED, generation, false, fireAtMs, false, false);
    }

    /**
     * 拒绝裁决（零扰动），三元组为 0。
     *
     * @param outcome 拒绝判别（非 GRANTED/QUEUED/DENIED）
     * @return 拒绝结果
     */
    public static TimerOpResult rejected(Outcome outcome) {
        return new TimerOpResult(outcome, 0, false, 0, false, false);
    }
}
