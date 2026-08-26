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
 * 获取结果。相较设计说明书 §4.2 将 {@code REJECT_KEY} 细分为空/超长两值，
 * 使 server 层无需重校验即可映射到协议 {@code KEY_EMPTY} / {@code KEY_TOO_LONG}。
 *
 * <p><b>返回步骤与优先级</b>：会话校验（{@link #REJECT_SESSION}）→
 * key 校验（{@link #REJECT_KEY_EMPTY} / {@link #REJECT_KEY_TOO_LONG}）→
 * 条目内规则（重入/快路径/队首重发 → {@link #GRANTED}，立即式被占 →
 * {@link #DENIED}，幂等去重或入队 → {@link #QUEUED}，队列满 →
 * {@link #REJECT_QUEUE_FULL}）。会话校验在预检与条目锁内各执行一次，
 * 两个检查点均返回 {@link #REJECT_SESSION}。
 */
public enum Outcome {
    /** 授予：携带租约凭证与实际租约。重入/快路径/队首重发命中均返回此值。 */
    GRANTED,
    /** 排队：携带 1 起的队列位次，等待队首通知后重发；重复请求去重时也返回当前位次。 */
    QUEUED,
    /** 拒绝：锁被占用且请求为立即式（不排队）。仅当请求未排队时可能返回。 */
    DENIED,
    /** 拒绝：锁键为 {@code null} 或空，先于长度与条目内规则校验。 */
    REJECT_KEY_EMPTY,
    /** 拒绝：锁键的 UTF-8 字节长度超过上限，先于条目内规则校验。 */
    REJECT_KEY_TOO_LONG,
    /** 拒绝：该 key 等待队列已满，先于入队动作检查。 */
    REJECT_QUEUE_FULL,
    /** 拒绝：会话不存在或已关闭；预检与条目锁内权威校验均可返回。 */
    REJECT_SESSION
}
