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

/**
 * 条件变量 signal 家族命令（v9）。身份与形状约束由 server 接入层先行
 * 裁决（判定唯一在接入层，判例 QUEUE/LATCH 家族的接入层形状裁决纪律），
 * 本记录承载裁决后的合法形态：
 * SIGNAL/SIGNAL_ALL 消费 {@code threadId}（权限归属）与 {@code condition}
 * （寻址），{@code awaitRequestId} 恒 0；LEAVE 消费 {@code condition}、
 * {@code awaitRequestId}（指向原折叠 ACQUIRE 的请求 id），{@code threadId}
 * 不参与判定。
 *
 * @param sessionId        发起请求的会话
 * @param threadId         权限归属线程（SIGNAL/SIGNAL_ALL 的持有检查输入）
 * @param key              锁键
 * @param condition        条件名（命名寻址，非空且 ≤maxKeyLength 已在入口裁决）
 * @param op               操作词
 * @param awaitRequestId   LEAVE 时指向原携带 condition 的 ACQUIRE 的 request_id；
 *                         其余恒 0
 */
public record ConditionOpCommand(
        long sessionId,
        long threadId,
        String key,
        String condition,
        ConditionOp op,
        long awaitRequestId) {
}
