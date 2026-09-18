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

package io.github.lamspace.openlatch.core;

/**
 * 原子变量操作类型（ATOMIC 家族命令通道的操作判别）。
 *
 * <p>与协议 {@code openlatch.AtomicOp} 逐项对应，server 层负责数值映射；
 * core 不感知线路形态。全部操作即时完成——无挂起、无排队、无租约，
 * 结果线性化于条目锁的临界区。
 */
public enum AtomicOp {
    /** 读 {@code (value, version)}：零迁移，不建条目、不推版本戳。 */
    GET,
    /** 落操作数为新值，返回旧值。 */
    SET,
    /** 同 {@link #SET}（应答同形的语义别名，对应 JDK {@code getAndSet}）。 */
    GET_AND_SET,
    /** 值加增量（按形态值域 wrap）；{@code expectedVersion > 0} 时为条件加。 */
    ADD,
    /** 值 CAS：期望值相符才落新值（ABA 风险由契约声明）。 */
    CAS,
    /** 值 + 版本双 CAS（ABA-free）：期望版本为 0 时退化为值 CAS。 */
    CAS_STAMPED
}
