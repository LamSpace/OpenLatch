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

import io.github.lamspace.openlatch.core.LockType;

/**
 * 获取锁命令。归属由 {@code (sessionId, threadId)} 唯一确定。
 *
 * @param sessionId        发起请求的会话
 * @param requestId        请求 id，同一 (会话, 请求) 幂等去重
 * @param key              锁键
 * @param lockType         请求的锁类型
 * @param threadId         发起请求的客户端线程标识
 * @param requestedLeaseMs 期望租约时长（毫秒），{@code 0} 表示使用默认租约
 * @param queueIfBusy      无快路径（锁被占用，或虽无持有者但等待队列非空——队首
 *                         已通知、待重发窗口，规则 3 禁止越过在队者）时是否排队。
 *                         {@code false} 对应协议 {@code wait_ms == 0} 的立即式获取，
 *                         无快路径即返回 {@code DENIED}；core 不感知等待时限
 *                         （等待模式折算见详设 §3.2.2）。
 */
public record AcquireCommand(
        long sessionId,
        long requestId,
        String key,
        LockType lockType,
        long threadId,
        long requestedLeaseMs,
        boolean queueIfBusy) {
}
