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
 * signal 家族操作结果（v9）。恒即时回执——无挂起/立即拒绝形态
 * （等待挂起属 await 折叠侧、归 {@link AcquireResult} 词表）。
 *
 * <p><b>判定顺序</b>（{@code CoreEngine.conditionOp} 门面，首个不满足者即
 * 返回值）：会话存在（{@link Status#REJECT_SESSION}）→ key 与条件名合法
 * （{@link Status#REJECT_KEY_EMPTY}/{@link Status#REJECT_KEY_TOO_LONG}）→
 * 条目存在且为 LOCK 家族（{@link Status#REJECT_TYPE_MISMATCH}，LEAVE 例外
 * ——无条目即无登记，幂等走 {@link Status#OK}）→ op 分派内部判定
 * （SIGNAL/SIGNAL_ALL 权限归属不匹配 {@link Status#NOT_HELD}）。
 *
 * <p><b>语义注记</b>：{@link Status#OK} 对 SIGNAL/SIGNAL_ALL 仅表"搬运已
 * 生效或空集无操作"，MUST NOT 被表述为"等待者已唤醒或已获授予"——被搬运者
 * 的了结经等待-通知-重发闭环另行完成（"signal 是事件"契约）。
 * {@link Status#NOT_HELD} 沿用既有语义（与 {@link ReleaseStatus#NOT_HELD}
 * 同词）：无条目、无写侧持有、归属不匹配三形合并——JDK
 * {@code IllegalMonitorStateException} 的线路对偶。server 层按判例映射协议
 * 状态码（{@code NOT_HELD}/{@code SESSION_EXPIRED}/{@code KEY_*}/
 * {@code INVALID_REQUEST}/{@code OK}）。
 *
 * @param status 结果状态
 * @param op     回显的操作词（指标与管理归线）
 */
public record ConditionOpResult(Status status, ConditionOpEcho op) {

    /**
     * 成功/无操作便捷构造。
     *
     * @param op 回显操作词
     * @return OK 结果
     */
    public static ConditionOpResult ok(ConditionOpEcho op) {
        return new ConditionOpResult(Status.OK, op);
    }

    /**
     * 拒绝结果便捷构造。
     *
     * @param status 拒绝状态
     * @param op     回显操作词
     * @return 拒绝结果
     */
    public static ConditionOpResult rejected(Status status, ConditionOpEcho op) {
        return new ConditionOpResult(status, op);
    }

    /**
     * signal 家族裁决状态词表。
     */
    public enum Status {
        /** 搬运生效或幂等无操作（LEAVE/空集/无条目）。 */
        OK,
        /** 权限归属不匹配：无条目、无写侧持有或 (会话,线程) 非当前持有。 */
        NOT_HELD,
        /** 会话不存在或已关闭，先于一切条目检查。 */
        REJECT_SESSION,
        /** key 为空串。 */
        REJECT_KEY_EMPTY,
        /** key 或条件名超长（与 key 长度纪律同上限）。 */
        REJECT_KEY_TOO_LONG,
        /** key 条目存在但非 LOCK 家族（条件骑互斥锁形态）。 */
        REJECT_TYPE_MISMATCH
    }

    /**
     * 操作词回显承载（core 词表 {@code ConditionOp} 到结果的映射由调用点
     * 完成，协议词表桥接在 server 层）。
     */
    public enum ConditionOpEcho {
        /** 唤醒一人。 */
        SIGNAL,
        /** 唤醒全员。 */
        SIGNAL_ALL,
        /** 撤登。 */
        LEAVE
    }
}
