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

package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.LockType;

/**
 * 等待者。{@code notifyDeadlineMs > 0} 表示"已通知、待重发"状态（设计说明书 §4.5）。
 * 通知队首时以 {@link #withDeadline(long)} 生成新实例替换队首，保持不可变。
 *
 * @param sessionId        等待者所属会话
 * @param requestId        获取请求的请求 id
 * @param lockType         请求的锁类型
 * @param threadId         发起请求的客户端线程标识
 * @param enqueuedAtMs     入队时刻（毫秒）
 * @param notifyDeadlineMs 通知响应截止时刻（毫秒），{@code 0} 表示尚未通知
 */
public record Waiter(
        long sessionId,
        long requestId,
        LockType lockType,
        long threadId,
        long enqueuedAtMs,
        long notifyDeadlineMs) {

    /**
     * 归属。
     *
     * @return 由会话与线程构成的锁归属
     */
    public Owner owner() {
        return new Owner(sessionId, threadId);
    }

    /**
     * 是否已被通知。
     *
     * @return 已通知（待重发）返回 true
     */
    public boolean notified() {
        return notifyDeadlineMs > 0;
    }

    /**
     * 生成替换通知截止时刻的新实例，其余字段不变。
     *
     * @param deadlineMs 新的通知响应截止时刻（毫秒）
     * @return 新实例
     */
    public Waiter withDeadline(long deadlineMs) {
        return new Waiter(sessionId, requestId, lockType, threadId, enqueuedAtMs, deadlineMs);
    }
}
