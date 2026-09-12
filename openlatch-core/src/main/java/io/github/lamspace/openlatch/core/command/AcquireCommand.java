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
 *                         无快路径即返回 {@code DENIED}；core 不感知等待时限。
 * @param permits          请求许可数（仅 SEMAPHORE 有效，{@code >= 1}；锁家族
 *                         请求不参与判定，缺省 1）
 * @param permitsTotal     Semaphore 许可总量断言：建条目时必填 {@code > 0}
 *                         （缺失回 {@code REJECT_SEMAPHORE_TOTAL}）；既有条目上
 *                         非零值须与定型值一致、{@code 0} 为不主张（纯加入）；
 *                         锁家族请求 MUST 为 0（server 层合法性预检）
 */
public record AcquireCommand(
        long sessionId,
        long requestId,
        String key,
        LockType lockType,
        long threadId,
        long requestedLeaseMs,
        boolean queueIfBusy,
        int permits,
        int permitsTotal) {

    /**
     * 锁家族便捷构造：许可参数取缺省
     * （{@code permits = 1}、{@code permitsTotal = 0}），语义与锁请求一致。
     *
     * @param sessionId        发起请求的会话
     * @param requestId        请求 id
     * @param key              锁键
     * @param lockType         锁类型
     * @param threadId         客户端线程标识
     * @param requestedLeaseMs 期望租约时长（毫秒）
     * @param queueIfBusy      无快路径时是否排队
     */
    public AcquireCommand(long sessionId, long requestId, String key, LockType lockType,
            long threadId, long requestedLeaseMs, boolean queueIfBusy) {
        this(sessionId, requestId, key, lockType, threadId, requestedLeaseMs, queueIfBusy, 1, 0);
    }
}
