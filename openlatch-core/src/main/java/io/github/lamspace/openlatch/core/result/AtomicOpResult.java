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
 * 原子变量操作结果（不可变值对象）。
 *
 * <p><b>应答四元组</b> {@code (applied, oldValue, value, version)} 恒随
 * {@link #outcome} 携带：{@code GRANTED} 时四元组为有效读数；拒绝态
 * （{@link Outcome#REJECT_ATOMIC_INIT} / {@link Outcome#REJECT_ATOMIC_RANGE}
 * 及会话/key 类）四元组为零值形。CAS 家族"未落值"不是拒绝——
 * {@code GRANTED + applied=false} 表达，区分于断言/形状类拒绝。
 *
 * @param outcome 结果状态（见类注释判别口径）
 * @param applied 写操作是否落值（CAS 家族成败判别；GET 与非落值拒绝恒 {@code false}）
 * @param oldValue 操作前值（GET 与未落值时为当前值）
 * @param value    操作后（或当前）值
 * @param version  操作后版本戳（写成功恰 +1；GET 与未落值不推）
 */
public record AtomicOpResult(Outcome outcome, boolean applied, long oldValue,
                             long value, long version) {

    /**
     * 构造拒绝结果（四元组零值形）。
     *
     * @param outcome 拒绝态
     * @return 拒绝结果
     */
    public static AtomicOpResult rejected(Outcome outcome) {
        return new AtomicOpResult(outcome, false, 0, 0, 0);
    }
}
