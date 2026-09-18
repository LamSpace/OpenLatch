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

package io.github.lamspace.openlatch.core.command;

import io.github.lamspace.openlatch.core.AtomicOp;
import io.github.lamspace.openlatch.core.LockType;

/**
 * 原子变量操作命令（不可变值对象）。
 *
 * <p>各字段的消费随 {@link #op} 而异：{@link AtomicOp#GET} 仅消费
 * {@link #sessionId()} / {@link #key()} / {@link #initialValue()}（后者在
 * 条目存在时作断言）；{@link AtomicOp#SET} 与 {@link AtomicOp#GET_AND_SET}
 * 消费 {@link #operand()}；{@link AtomicOp#ADD} 消费 {@link #operand()}
 * 与 {@link #expectedVersion()}（{@code > 0} 为断言）；
 * {@link AtomicOp#CAS} 消费 {@link #expected()} 与 {@link #operand()}；
 * {@link AtomicOp#CAS_STAMPED} 额外消费 {@link #expectedVersion()}
 * （0 为不主张，退化为值 CAS）。
 *
 * @param sessionId        发起会话（存在性校验，MUST NOT 登记入触及集）
 * @param requestId        连接内请求 id（仅诊断与回执关联，不参与判定）
 * @param key              原子变量键
 * @param kind             形态判别（{@link LockType#ATOMIC_LONG} /
 *                         {@link LockType#ATOMIC_INTEGER} /
 *                         {@link LockType#ATOMIC_BOOLEAN}）
 * @param op               操作类型
 * @param operand          操作数（新值 / 增量 / CAS 更新值）
 * @param expected         CAS / CAS_STAMPED 期望值
 * @param expectedVersion  版本断言（ADD 条件加与 CAS_STAMPED 消费；0 为不主张）
 * @param initialValue     建条目初值主张（判例：非零主张、0 不主张）
 * @param opSeq            会话内单调写序号（去重槽位键；0 表示不参与去重，
 *                         GET 恒 0，写路径正常取值 {@code >= 1}）
 */
public record AtomicOpCommand(long sessionId, long requestId, String key, LockType kind,
                              AtomicOp op, long operand, long expected, long expectedVersion,
                              long initialValue, long opSeq) {
}
