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
 * 有值引用原子操作结果（不可变值对象）。
 *
 * <p><b>应答四元组</b> {@code (applied, oldValue, value, version)} 恒随
 * {@link #outcome} 携带：{@code GRANTED} 时四元组为有效读数（载荷为
 * 字节数组，{@code null} 即 null 态、零长度即空字节串——两态可区分）；
 * 拒绝态（{@link Outcome#REJECT_ATOMIC_INIT} /
 * {@link Outcome#REJECT_ATOMIC_RANGE} 及会话/key/家族类）四元组为零值形
 * （{@code applied=false}、两载荷 {@code null}、{@code version=0}）。
 * CAS 家族"未落值"不是拒绝——{@code GRANTED + applied=false} 表达。
 *
 * <p><b>数组共享纪律</b>：{@code oldValue}/{@code value} 引用条目存储的
 * 载荷数组（条目侧只读、迁移以引用替换），消费方 MUST NOT 修改；
 * 需要可变副本自行克隆。判据比较请用
 * {@link java.util.Arrays#equals}。
 *
 * @param outcome 结果状态（判别口径同标量形态 {@link AtomicOpResult}）
 * @param applied 写操作是否落值（CAS 家族成败判别；GET 与非落值拒绝恒 {@code false}）
 * @param oldValue 操作前载荷（GET 与未落值时为当前载荷；{@code null}=null 态）
 * @param value    操作后（或当前）载荷（{@code null}=null 态）
 * @param version  操作后版本戳（写成功恰 +1；GET 与未落值不推）
 */
public record AtomicRefOpResult(Outcome outcome, boolean applied, byte[] oldValue,
                                byte[] value, long version) {

    /**
     * 构造拒绝结果（四元组零值形）。
     *
     * @param outcome 拒绝态
     * @return 拒绝结果
     */
    public static AtomicRefOpResult rejected(Outcome outcome) {
        return new AtomicRefOpResult(outcome, false, null, null, 0);
    }
}
