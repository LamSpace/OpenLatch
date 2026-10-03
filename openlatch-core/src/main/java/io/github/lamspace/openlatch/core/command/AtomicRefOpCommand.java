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

/**
 * 有值引用原子操作命令（不可变值对象）。
 *
 * <p>与标量形态的 {@link AtomicOpCommand} 平行：操作集为
 * {@link AtomicOp#GET} / {@link AtomicOp#SET} / {@link AtomicOp#GET_AND_SET} /
 * {@link AtomicOp#CAS} / {@link AtomicOp#CAS_STAMPED}，{@link AtomicOp#ADD}
 * 对本形态为值域外操作（条目以 {@code REJECT_ATOMIC_RANGE} 拒绝）。
 * 载荷字段以 {@code null} 引用表达协议层的"缺省"形态，与零长度数组
 * （空字节串）可区分——三态语义：
 * <ul>
 *   <li>{@code operand}：SET/GET_AND_SET/CAS/CAS_STAMPED 的落值；
 *       {@code null} 即"落 null"（清空），GET 不消费；</li>
 *   <li>{@code expected}：CAS/CAS_STAMPED 的期望值；{@code null} 即
 *       "期望 null"（建立空态判定），与零长度期望互斥；</li>
 *   <li>{@code initial}：建条目初值主张；{@code null} 为不主张（判例标量
 *       形态的 0 不主张），非 null（含零长度）即主张——条目不存在以此值
 *       创建，存在则与定型初值字节比对。</li>
 * </ul>
 *
 * <p><b>数组所有权</b>：命令一经构造，其载荷数组归条目侧接管——构造方
 * MUST NOT 在传入后修改这些数组（条目存储引用不做复制，值迁移以引用
 * 替换表达）；接收方对存储数组只读。字节级比较以
 * {@link java.util.Arrays#equals} 判定。
 *
 * @param sessionId       发起会话（存在性校验，MUST NOT 登记入触及集）
 * @param requestId       连接内请求 id（仅诊断与回执关联，不参与判定）
 * @param key             原子变量键
 * @param op              操作类型
 * @param operand         落值载荷（{@code null}=落 null 态）
 * @param expected        CAS/CAS_STAMPED 期望载荷（{@code null}=期望 null）
 * @param expectedVersion 版本断言（CAS_STAMPED 消费；0 为不主张）
 * @param initial         初值主张载荷（{@code null} 为不主张）
 * @param opSeq           会话内单调写序号（去重槽位键；0 不参与去重，GET 恒 0）
 */
public record AtomicRefOpCommand(long sessionId, long requestId, String key, AtomicOp op,
                                 byte[] operand, byte[] expected, long expectedVersion,
                                 byte[] initial, long opSeq) {
}
