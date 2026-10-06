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
 * 相位器操作结果（PHASER 家族命令通道的统一应答形态）。
 *
 * <p><b>择用规则</b>：{@code outcome} 为终局判别——{@link Outcome#GRANTED}
 * 表完成（到场/注册/离场/撤销/读数/即刻了结），{@link Outcome#QUEUED} 表
 * 等待登记已生效（唤醒经通知-重发闭环另行送达）；其余为拒绝。三计数
 * （{@code phase}/{@code registered}/{@code arrived}）恒为裁决时刻的账簿
 * 快照（观察值，供应答回显与 QUERY 读数；MUST NOT 参与后续判定）。
 *
 * <p><b>相位回显语义</b>：到场族的 {@code phase} 为到场相位号（该操作
 * 发生时条目所处相位，恰触发合拢的回显仍为到场时相位——推进经后续
 * 查询或唤醒可见，对齐 JDK {@code arrive()} 返回值语义）；等待族的
 * {@code phase} 在 {@link Outcome#QUEUED} 时为受理时刻当前相位、了结
 * 重发命中时为新当前相位（JDK {@code awaitAdvance} 返回对偶）。
 *
 * @param outcome   裁决结果判别
 * @param phase     相位回显（拒绝态为 0）
 * @param registered 回执时刻注册总数快照（拒绝态为 0）
 * @param arrived    回执时刻当前相位到场数快照（拒绝态为 0）
 * @param tripped    本操作是否触发了相位推进（仅 GRANTED 可为真——集群
 *                   应用点据此在回执携带 {@code phaser_advanced_keys}，
 *                   驱动 Leader 侧等待簿记的唤醒广播；单机形态无需消费）
 */
public record PhaserOpResult(Outcome outcome, long phase, int registered, int arrived,
        boolean tripped) {

    /**
     * 完成裁决（GRANTED）携三计数回显。
     *
     * @param phase      相位回显
     * @param registered 注册总数快照
     * @param arrived    到场数快照
     * @return GRANTED 结果
     */
    public static PhaserOpResult granted(long phase, int registered, int arrived) {
        return new PhaserOpResult(Outcome.GRANTED, phase, registered, arrived, false);
    }

    /**
     * 完成裁决（GRANTED）携推进标记（到场恰触发合拢的形态）。
     *
     * @param phase      相位回显（到场时相位）
     * @param registered 注册总数快照
     * @param arrived    到场数快照（推进后为 0）
     * @param tripped    本次操作是否推进了相位
     * @return GRANTED 结果
     */
    public static PhaserOpResult granted(long phase, int registered, int arrived,
            boolean tripped) {
        return new PhaserOpResult(Outcome.GRANTED, phase, registered, arrived, tripped);
    }

    /**
     * 等待登记完成（QUEUED）携三计数回显。
     *
     * @param phase      受理时刻当前相位
     * @param registered 注册总数快照
     * @param arrived    到场数快照
     * @return QUEUED 结果
     */
    public static PhaserOpResult queued(long phase, int registered, int arrived) {
        return new PhaserOpResult(Outcome.QUEUED, phase, registered, arrived, false);
    }

    /**
     * 拒绝裁决（零扰动），三计数为 0。
     *
     * @param outcome 拒绝判别（非 GRANTED/QUEUED）
     * @return 拒绝结果
     */
    public static PhaserOpResult rejected(Outcome outcome) {
        return new PhaserOpResult(outcome, 0, 0, 0, false);
    }
}
